package resolve

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"coordos/design-institute/qualification"
)

type Action string

type VerifyInput struct {
	ExecutorRef string
	ProjectRef  string
	SPURef      string
	Action      Action
	ValidOn     time.Time
	TenantID    int
}

type VerifyResult struct {
	Pass              bool                     `json:"pass"`
	Summary           string                   `json:"summary"`
	RequiredQualTypes []qualification.QualType `json:"required_qual_types,omitempty"`
	MissingQualTypes  []qualification.QualType `json:"missing_qual_types,omitempty"`
	Reasons           []string                 `json:"reasons,omitempty"`
}

type ResolveInput struct {
	Tenant         string
	TenantID       int
	ProjectRef     string
	SPURef         string
	Role           string
	Action         Action
	HeadOfficeOnly bool
	ValidOn        time.Time
	Limit          int
	NeedQualTypes  []qualification.QualType
}

type Candidate struct {
	ExecutorRef      string                   `json:"executor_ref"`
	Name             string                   `json:"name"`
	MatchedQualTypes []qualification.QualType `json:"matched_qual_types,omitempty"`
	ActiveProjects   int                      `json:"active_projects"`
	ProjectLimit     int                      `json:"project_limit"`
	Available        bool                     `json:"available"`
	CapabilityLevel  float64                  `json:"capability_level"`
	SPUPassRate      float64                  `json:"spu_pass_rate"`
	CredentialCount  int                      `json:"credential_count"`
	Score            float64                  `json:"score"`
}

type RejectedCandidate struct {
	ExecutorRef      string                   `json:"executor_ref"`
	Name             string                   `json:"name,omitempty"`
	Reason           string                   `json:"reason"`
	MissingQualTypes []qualification.QualType `json:"missing_qual_types,omitempty"`
}

type OccupiedState struct {
	ExecutorRef    string            `json:"executor_ref"`
	ActiveProjects int               `json:"active_projects"`
	ProjectLimit   int               `json:"project_limit"`
	Available      bool              `json:"available"`
	Projects       []OccupiedProject `json:"projects"`
}

type OccupiedProject struct {
	ProjectRef  string    `json:"project_ref"`
	ProjectName string    `json:"project_name"`
	Role        string    `json:"role"`
	Since       time.Time `json:"since"`
}

type AssignQualificationInput struct {
	QualificationID int64
	ExecutorRef     string
	ProjectRef      string
}

type QualificationAssignment struct {
	ID              int64                  `json:"id"`
	QualificationID int64                  `json:"qualification_id"`
	ExecutorRef     string                 `json:"executor_ref"`
	ProjectRef      string                 `json:"project_ref"`
	Status          string                 `json:"status"`
	TenantID        int                    `json:"tenant_id"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
	ReleasedAt      *time.Time             `json:"released_at,omitempty"`
	QualType        qualification.QualType `json:"qual_type,omitempty"`
	CertNo          string                 `json:"cert_no,omitempty"`
}

type executorStatsLite struct {
	PassRate        float64
	CapabilityLevel float64
}

func (s *Service) Verify(ctx context.Context, in VerifyInput) (*VerifyResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("resolve service db is not configured")
	}
	if strings.TrimSpace(in.ExecutorRef) == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	validOn := in.ValidOn
	if validOn.IsZero() {
		validOn = time.Now()
	}

	required, headOfficeOnly := deriveResolveRequirements(in.SPURef, in.Action, nil)
	qualMap, _, _, err := s.loadQualificationsMap(ctx, s.pickTenant(in.TenantID), validOn)
	if err != nil {
		return nil, err
	}

	res := &VerifyResult{
		Pass:              true,
		RequiredQualTypes: required,
		MissingQualTypes:  make([]qualification.QualType, 0),
		Reasons:           make([]string, 0),
	}

	if headOfficeOnly && !s.isHeadOfficeExecutor(in.ExecutorRef) {
		res.Pass = false
		res.Reasons = append(res.Reasons, "executor is not under head office namespace")
	}

	has := qualMap[in.ExecutorRef]
	for _, need := range required {
		if _, ok := has[need]; !ok {
			res.Pass = false
			res.MissingQualTypes = append(res.MissingQualTypes, need)
		}
	}

	if res.Pass {
		res.Summary = "verified"
	} else {
		if len(res.MissingQualTypes) > 0 {
			res.Reasons = append(res.Reasons, "missing required qualifications")
		}
		res.Summary = "verification failed"
	}
	return res, nil
}

func (s *Service) ResolveWithDetails(ctx context.Context, in ResolveInput) ([]*Candidate, []*RejectedCandidate, error) {
	if s.db == nil {
		return nil, nil, fmt.Errorf("resolve service db is not configured")
	}
	tenantID := s.pickTenant(in.TenantID)
	validOn := in.ValidOn
	if validOn.IsZero() {
		validOn = time.Now()
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 20
	}

	required, derivedHeadOfficeOnly := deriveResolveRequirements(in.SPURef, in.Action, in.NeedQualTypes)
	headOfficeOnly := in.HeadOfficeOnly || derivedHeadOfficeOnly
	qualMap, certCount, projectLimitMap, err := s.loadQualificationsMap(ctx, tenantID, validOn)
	if err != nil {
		return nil, nil, err
	}
	activeProjectsMap, err := s.loadActiveProjectsMap(ctx, tenantID)
	if err != nil {
		return nil, nil, err
	}
	statsMap, _ := s.loadExecutorStatsMap(ctx, tenantID)

	executors, err := s.loadExecutors(ctx, tenantID)
	if err != nil {
		return nil, nil, err
	}

	candidates := make([]*Candidate, 0, len(executors))
	rejected := make([]*RejectedCandidate, 0)

	for ref, name := range executors {
		if headOfficeOnly && !s.isHeadOfficeExecutor(ref) {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef: ref,
				Name:        name,
				Reason:      "head_office_only",
			})
			continue
		}

		has := qualMap[ref]
		missing := make([]qualification.QualType, 0)
		matched := make([]qualification.QualType, 0)
		for _, need := range required {
			if _, ok := has[need]; ok {
				matched = append(matched, need)
				continue
			}
			missing = append(missing, need)
		}
		if len(missing) > 0 {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef:      ref,
				Name:             name,
				Reason:           "missing_qualification",
				MissingQualTypes: missing,
			})
			continue
		}

		active := activeProjectsMap[ref]
		projectLimit := projectLimitMap[ref]
		if projectLimit <= 0 {
			projectLimit = 3
		}
		if active+1 > projectLimit {
			rejected = append(rejected, &RejectedCandidate{
				ExecutorRef: ref,
				Name:        name,
				Reason:      "capacity_full",
			})
			continue
		}

		stats := statsMap[ref]
		remainingRatio := clamp01(float64(projectLimit-active) / float64(projectLimit))
		capabilityNorm := clamp01(stats.CapabilityLevel / 5.0)
		passRate := clamp01(stats.PassRate)
		credScore := clamp01(float64(certCount[ref]) / 3.0)

		score := remainingRatio*0.4 + capabilityNorm*0.3 + passRate*0.2 + credScore*0.1
		candidates = append(candidates, &Candidate{
			ExecutorRef:      ref,
			Name:             name,
			MatchedQualTypes: matched,
			ActiveProjects:   active,
			ProjectLimit:     projectLimit,
			Available:        true,
			CapabilityLevel:  stats.CapabilityLevel,
			SPUPassRate:      passRate,
			CredentialCount:  certCount[ref],
			Score:            score,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			if candidates[i].CapabilityLevel == candidates[j].CapabilityLevel {
				return candidates[i].SPUPassRate > candidates[j].SPUPassRate
			}
			return candidates[i].CapabilityLevel > candidates[j].CapabilityLevel
		}
		return candidates[i].Score > candidates[j].Score
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}

	return candidates, rejected, nil
}

func (s *Service) Occupied(ctx context.Context, executorRef string) (*OccupiedState, error) {
	if s.db == nil {
		return nil, fmt.Errorf("resolve service db is not configured")
	}
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}

	activeProjects, err := s.listExecutorActiveProjects(ctx, s.tenantID, executorRef)
	if err != nil {
		return nil, err
	}
	limit, err := s.getExecutorProjectLimit(ctx, s.tenantID, executorRef, time.Now())
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 3
	}

	return &OccupiedState{
		ExecutorRef:    executorRef,
		ActiveProjects: len(activeProjects),
		ProjectLimit:   limit,
		Available:      len(activeProjects) < limit,
		Projects:       activeProjects,
	}, nil
}

func (s *Service) BindQualification(ctx context.Context, in AssignQualificationInput) (*QualificationAssignment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("resolve service db is not configured")
	}
	if in.QualificationID <= 0 {
		return nil, fmt.Errorf("qualification_id is required")
	}
	in.ProjectRef = strings.TrimSpace(in.ProjectRef)
	if in.ProjectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}

	tenantID := s.tenantID
	var qualExecutorRef sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT executor_ref
		FROM qualifications
		WHERE id=$1 AND tenant_id=$2 AND deleted=FALSE
		LIMIT 1
	`, in.QualificationID, tenantID).Scan(&qualExecutorRef)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("qualification not found")
		}
		return nil, err
	}
	if strings.TrimSpace(in.ExecutorRef) == "" {
		in.ExecutorRef = strings.TrimSpace(qualExecutorRef.String)
	}
	if in.ExecutorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	if qualExecutorRef.Valid && strings.TrimSpace(qualExecutorRef.String) != "" && strings.TrimSpace(qualExecutorRef.String) != in.ExecutorRef {
		return nil, fmt.Errorf("qualification executor_ref mismatch")
	}

	var existingID int64
	err = s.db.QueryRowContext(ctx, `
		SELECT id
		FROM qualification_assignments
		WHERE qualification_id=$1 AND status='ACTIVE'
		LIMIT 1
	`, in.QualificationID).Scan(&existingID)
	if err == nil && existingID > 0 {
		return nil, fmt.Errorf("qualification is already assigned")
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	out := &QualificationAssignment{}
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO qualification_assignments(
			qualification_id, executor_ref, project_ref, status, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,'ACTIVE',$4,NOW(),NOW())
		RETURNING id, qualification_id, executor_ref, project_ref, status, tenant_id, created_at, updated_at
	`, in.QualificationID, in.ExecutorRef, in.ProjectRef, tenantID).Scan(
		&out.ID, &out.QualificationID, &out.ExecutorRef, &out.ProjectRef, &out.Status, &out.TenantID, &out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Service) ListQualificationAssignmentsByProject(ctx context.Context, projectRef string) ([]*QualificationAssignment, error) {
	if s.db == nil {
		return nil, fmt.Errorf("resolve service db is not configured")
	}
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return nil, fmt.Errorf("project_ref is required")
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			qa.id, qa.qualification_id, qa.executor_ref, qa.project_ref, qa.status, qa.tenant_id,
			qa.created_at, qa.updated_at, qa.released_at,
			COALESCE(q.qual_type,''), COALESCE(q.cert_no,'')
		FROM qualification_assignments qa
		LEFT JOIN qualifications q ON q.id = qa.qualification_id
		WHERE qa.tenant_id=$1 AND qa.project_ref=$2
		ORDER BY qa.created_at DESC
	`, s.tenantID, projectRef)
	if err != nil {
		if isMissingTableErr(err, "qualification_assignments") {
			return make([]*QualificationAssignment, 0), nil
		}
		return nil, err
	}
	defer rows.Close()

	out := make([]*QualificationAssignment, 0)
	for rows.Next() {
		item := &QualificationAssignment{}
		var releasedAt sql.NullTime
		var qualTypeRaw, certNo string
		if err := rows.Scan(
			&item.ID, &item.QualificationID, &item.ExecutorRef, &item.ProjectRef, &item.Status, &item.TenantID,
			&item.CreatedAt, &item.UpdatedAt, &releasedAt, &qualTypeRaw, &certNo,
		); err != nil {
			return nil, err
		}
		if releasedAt.Valid {
			t := releasedAt.Time
			item.ReleasedAt = &t
		}
		item.QualType = qualification.QualType(strings.TrimSpace(qualTypeRaw))
		item.CertNo = strings.TrimSpace(certNo)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Service) pickTenant(tenantID int) int {
	if tenantID > 0 {
		return tenantID
	}
	if s.tenantID > 0 {
		return s.tenantID
	}
	return 10000
}

func (s *Service) isHeadOfficeExecutor(executorRef string) bool {
	if strings.TrimSpace(s.headOfficeRefBase) == "" {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(executorRef), strings.TrimSpace(s.headOfficeRefBase))
}

func deriveResolveRequirements(spuRef string, action Action, explicit []qualification.QualType) ([]qualification.QualType, bool) {
	needs := make([]qualification.QualType, 0, len(explicit)+2)
	seen := map[qualification.QualType]struct{}{}
	addNeed := func(q qualification.QualType) {
		if q == "" {
			return
		}
		if _, ok := seen[q]; ok {
			return
		}
		seen[q] = struct{}{}
		needs = append(needs, q)
	}
	for _, q := range explicit {
		addNeed(q)
	}

	headOfficeOnly := false
	spu := strings.ToLower(strings.TrimSpace(spuRef))
	act := strings.ToUpper(strings.TrimSpace(string(action)))

	if strings.Contains(spu, "review") || strings.Contains(spu, "审图") || act == "ISSUE_REVIEW_CERT" {
		addNeed(qualification.QualRegStructure)
		headOfficeOnly = true
	}
	if strings.Contains(spu, "pile_foundation") ||
		strings.Contains(spu, "pier_rebar") ||
		strings.Contains(spu, "superstructure") {
		addNeed(qualification.QualRegStructure)
	}

	return needs, headOfficeOnly
}

func (s *Service) loadExecutors(ctx context.Context, tenantID int) (map[string]string, error) {
	out := map[string]string{}
	rows, err := s.db.QueryContext(ctx, `
		SELECT executor_ref, COALESCE(name,'')
		FROM employees
		WHERE tenant_id=$1
		  AND executor_ref IS NOT NULL
		  AND executor_ref <> ''
		  AND (end_date IS NULL OR end_date > NOW())
	`, tenantID)
	if err != nil {
		rows, err = s.db.QueryContext(ctx, `
			SELECT executor_ref, COALESCE(name,'')
			FROM employees
			WHERE tenant_id=$1
			  AND executor_ref IS NOT NULL
			  AND executor_ref <> ''
		`, tenantID)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	for rows.Next() {
		var ref, name string
		if err := rows.Scan(&ref, &name); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		out[ref] = strings.TrimSpace(name)
	}
	return out, rows.Err()
}

func (s *Service) loadQualificationsMap(ctx context.Context, tenantID int, validOn time.Time) (map[string]map[qualification.QualType]struct{}, map[string]int, map[string]int, error) {
	qualMap := map[string]map[qualification.QualType]struct{}{}
	certCount := map[string]int{}
	projectLimitMap := map[string]int{}

	rows, err := s.db.QueryContext(ctx, `
		SELECT executor_ref, qual_type, COALESCE(max_concurrent_projects, 0)
		FROM qualifications
		WHERE tenant_id=$1
		  AND deleted=FALSE
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND executor_ref IS NOT NULL
		  AND executor_ref <> ''
		  AND (valid_until IS NULL OR valid_until >= $2)
	`, tenantID, validOn)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var ref, qualTypeRaw string
		var maxConcurrent int
		if err := rows.Scan(&ref, &qualTypeRaw, &maxConcurrent); err != nil {
			return nil, nil, nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		if _, ok := qualMap[ref]; !ok {
			qualMap[ref] = map[qualification.QualType]struct{}{}
		}
		qt := qualification.QualType(strings.TrimSpace(qualTypeRaw))
		if qt != "" {
			if _, exists := qualMap[ref][qt]; !exists {
				qualMap[ref][qt] = struct{}{}
				certCount[ref]++
			}
		}
		if maxConcurrent > projectLimitMap[ref] {
			projectLimitMap[ref] = maxConcurrent
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	return qualMap, certCount, projectLimitMap, nil
}

func (s *Service) loadActiveProjectsMap(ctx context.Context, tenantID int) (map[string]int, error) {
	out := map[string]int{}
	rows, err := s.db.QueryContext(ctx, `
		SELECT executor_ref, COUNT(DISTINCT project_ref)
		FROM qualification_assignments
		WHERE tenant_id=$1 AND status='ACTIVE'
		GROUP BY executor_ref
	`, tenantID)
	if err != nil {
		if !isMissingTableErr(err, "qualification_assignments") {
			return nil, err
		}
		rows, err = s.db.QueryContext(ctx, `
			SELECT executor_ref, COUNT(DISTINCT ref)
			FROM project_nodes
			WHERE tenant_id=$1
			  AND executor_ref IS NOT NULL
			  AND executor_ref <> ''
			  AND status IN ('INITIATED','CONTRACTED','IN_PROGRESS')
			GROUP BY executor_ref
		`, tenantID)
		if err != nil {
			return out, nil
		}
	}
	defer rows.Close()

	for rows.Next() {
		var ref string
		var count int
		if err := rows.Scan(&ref, &count); err != nil {
			return nil, err
		}
		out[strings.TrimSpace(ref)] = count
	}
	return out, rows.Err()
}

func (s *Service) loadExecutorStatsMap(ctx context.Context, tenantID int) (map[string]executorStatsLite, error) {
	out := map[string]executorStatsLite{}
	rows, err := s.db.QueryContext(ctx, `
		SELECT executor_ref, COALESCE(spu_pass_rate,0), COALESCE(capability_level_num,0), COALESCE(capability_level,'')
		FROM executor_stats
		WHERE tenant_id=$1
	`, tenantID)
	if err != nil {
		if isMissingTableErr(err, "executor_stats") {
			return out, nil
		}
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var ref, capText string
		var passRateRaw, capNum float64
		if err := rows.Scan(&ref, &passRateRaw, &capNum, &capText); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref == "" {
			continue
		}
		passRate := passRateRaw
		if passRate > 1 {
			passRate = passRate / 100.0
		}
		capLevel := capNum
		if capLevel <= 0 {
			capLevel = parseCapabilityLevelText(capText)
		}
		out[ref] = executorStatsLite{
			PassRate:        clamp01(passRate),
			CapabilityLevel: math.Max(0, capLevel),
		}
	}
	return out, rows.Err()
}

func (s *Service) listExecutorActiveProjects(ctx context.Context, tenantID int, executorRef string) ([]OccupiedProject, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT qa.project_ref, COALESCE(pn.name,''), qa.created_at
		FROM qualification_assignments qa
		LEFT JOIN project_nodes pn
		       ON pn.tenant_id = qa.tenant_id
		      AND pn.ref = qa.project_ref
		WHERE qa.tenant_id=$1
		  AND qa.executor_ref=$2
		  AND qa.status='ACTIVE'
		ORDER BY qa.created_at DESC
	`, tenantID, executorRef)
	if err != nil {
		if !isMissingTableErr(err, "qualification_assignments") {
			return nil, err
		}
		rows, err = s.db.QueryContext(ctx, `
			SELECT ref, COALESCE(name,''), created_at
			FROM project_nodes
			WHERE tenant_id=$1
			  AND executor_ref=$2
			  AND status IN ('INITIATED','CONTRACTED','IN_PROGRESS')
			ORDER BY created_at DESC
		`, tenantID, executorRef)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	out := make([]OccupiedProject, 0)
	for rows.Next() {
		var p OccupiedProject
		p.Role = "EXECUTOR"
		if err := rows.Scan(&p.ProjectRef, &p.ProjectName, &p.Since); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Service) getExecutorProjectLimit(ctx context.Context, tenantID int, executorRef string, validOn time.Time) (int, error) {
	var limit sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT MAX(COALESCE(max_concurrent_projects,0))
		FROM qualifications
		WHERE tenant_id=$1
		  AND executor_ref=$2
		  AND deleted=FALSE
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND (valid_until IS NULL OR valid_until >= $3)
	`, tenantID, executorRef, validOn).Scan(&limit)
	if err != nil {
		return 0, err
	}
	if !limit.Valid || limit.Int64 <= 0 {
		return 3, nil
	}
	return int(limit.Int64), nil
}

func parseCapabilityLevelText(raw string) float64 {
	s := strings.ToUpper(strings.TrimSpace(raw))
	if s == "" {
		return 0
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return n
	}
	switch s {
	case "CHIEF_ENGINEER":
		return 5
	case "SENIOR_ENGINEER":
		return 4
	case "REGISTERED_ENGINEER":
		return 3
	case "LEAD_ENGINEER":
		return 2
	case "ASSISTANT", "RISK":
		return 1
	default:
		return 0
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func isMissingTableErr(err error, table string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") && strings.Contains(msg, strings.ToLower(table))
}
