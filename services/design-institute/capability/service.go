package capability

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

type Service struct {
	db       *sql.DB
	tenantID int
}

func NewService(db *sql.DB, tenantID int) *Service {
	return &Service{db: db, tenantID: tenantID}
}

type ViolationRecord struct {
	ID            int64     `json:"id"`
	ExecutorRef   string    `json:"executor_ref"`
	ViolationType string    `json:"violation_type"`
	Severity      string    `json:"severity"`
	ProjectRef    string    `json:"project_ref,omitempty"`
	UTXORef       string    `json:"utxo_ref,omitempty"`
	Description   string    `json:"description,omitempty"`
	Penalty       float64   `json:"penalty"`
	RecordedAt    time.Time `json:"recorded_at"`
	TenantID      int       `json:"tenant_id"`
}

type RecordViolationInput struct {
	ExecutorRef   string   `json:"executor_ref"`
	ViolationType string   `json:"violation_type"`
	Severity      string   `json:"severity"`
	ProjectRef    string   `json:"project_ref"`
	UTXORef       string   `json:"utxo_ref"`
	Description   string   `json:"description"`
	Penalty       *float64 `json:"penalty,omitempty"`
}

type ExecutorStats struct {
	ExecutorRef     string    `json:"executor_ref"`
	SPUPassRate     float64   `json:"spu_pass_rate"`
	TotalUTXOs      int       `json:"total_utxos"`
	ViolationCount  int       `json:"violation_count"`
	CapabilityLevel float64   `json:"capability_level"`
	SpecialtySPUs   []string  `json:"specialty_spus"`
	LastComputedAt  time.Time `json:"last_computed_at"`
}

type ComputeStatsResult struct {
	TotalExecutors int `json:"total_executors"`
	Updated        int `json:"updated"`
	Failed         int `json:"failed"`
}

type OrgCapacity struct {
	NamespaceRef           string         `json:"namespace_ref"`
	Deep                   bool           `json:"deep"`
	ExecutorCount          int            `json:"executor_count"`
	AverageCapability      float64        `json:"average_capability"`
	CapacityUtilization    float64        `json:"capacity_utilization"`
	SkillCoverage          []string       `json:"skill_coverage"`
	BottleneckSPUs         []string       `json:"bottleneck_spus"`
	AtRiskCerts            int            `json:"at_risk_certs"`
	CapabilityDistribution map[string]int `json:"capability_distribution"`
	Children               []*OrgCapacity `json:"children,omitempty"`
}

type executorScoreSignal struct {
	passRate      float64
	totalUTXOs    int
	totalProjects int
	violationCnt  int
	penaltySum    float64
	lastViolation *time.Time
	qualTypes     []string
	specialtySPUs []string
}

func (s *Service) RecordViolation(ctx context.Context, in RecordViolationInput) (*ViolationRecord, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	executorRef := strings.TrimSpace(in.ExecutorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	violationType := strings.TrimSpace(in.ViolationType)
	if violationType == "" {
		violationType = "RULE_BREACH"
	}
	severityRaw := strings.ToUpper(strings.TrimSpace(in.Severity))
	if severityRaw == "" {
		severityRaw = "MAJOR"
	}
	severityDB, penalty := severityPenalty(severityRaw)
	if in.Penalty != nil {
		penalty = *in.Penalty
	}
	recordedAt := time.Now().UTC()

	item := &ViolationRecord{
		ExecutorRef:   executorRef,
		ViolationType: violationType,
		Severity:      severityRaw,
		ProjectRef:    strings.TrimSpace(in.ProjectRef),
		UTXORef:       strings.TrimSpace(in.UTXORef),
		Description:   strings.TrimSpace(in.Description),
		Penalty:       penalty,
		RecordedAt:    recordedAt,
		TenantID:      s.tenantID,
	}

	if err := s.db.QueryRowContext(ctx, `
		INSERT INTO violation_records (
			executor_ref,
			violation_type,
			severity,
			project_ref,
			utxo_ref,
			description,
			penalty,
			recorded_at,
			rule_code,
			message,
			occurred_at,
			tenant_id,
			created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
		RETURNING id
	`,
		item.ExecutorRef,
		item.ViolationType,
		severityDB,
		item.ProjectRef,
		item.UTXORef,
		item.Description,
		item.Penalty,
		item.RecordedAt,
		item.ViolationType,
		item.Description,
		item.RecordedAt,
		item.TenantID,
	).Scan(&item.ID); err != nil {
		return nil, err
	}

	// Preserve caller-facing severity vocabulary while persisting normalized values.
	item.Severity = severityRaw

	go func(ref string) {
		bg, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = s.ComputeStatsForExecutor(bg, ref)
	}(item.ExecutorRef)

	return item, nil
}

func (s *Service) ListViolationsByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*ViolationRecord, int, error) {
	if s.db == nil {
		return nil, 0, fmt.Errorf("database is not configured")
	}
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, 0, fmt.Errorf("executor_ref is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM violation_records
		WHERE tenant_id=$1 AND executor_ref=$2
	`, s.tenantID, executorRef).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id,
		       executor_ref,
		       COALESCE(violation_type, rule_code, ''),
		       severity,
		       COALESCE(project_ref, ''),
		       COALESCE(utxo_ref, ''),
		       COALESCE(description, message, ''),
		       COALESCE(penalty, 0),
		       COALESCE(recorded_at, occurred_at, created_at),
		       tenant_id
		FROM violation_records
		WHERE tenant_id=$1 AND executor_ref=$2
		ORDER BY COALESCE(recorded_at, occurred_at, created_at) DESC, id DESC
		LIMIT $3 OFFSET $4
	`, s.tenantID, executorRef, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]*ViolationRecord, 0, limit)
	for rows.Next() {
		item := &ViolationRecord{}
		if err := rows.Scan(
			&item.ID,
			&item.ExecutorRef,
			&item.ViolationType,
			&item.Severity,
			&item.ProjectRef,
			&item.UTXORef,
			&item.Description,
			&item.Penalty,
			&item.RecordedAt,
			&item.TenantID,
		); err != nil {
			return nil, 0, err
		}
		item.Severity = normalizeSeverity(item.Severity)
		items = append(items, item)
	}
	return items, total, rows.Err()
}

func (s *Service) GetStats(ctx context.Context, executorRef string) (*ExecutorStats, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}

	item, err := s.readStats(ctx, executorRef)
	if err == sql.ErrNoRows || item == nil {
		return s.ComputeStatsForExecutor(ctx, executorRef)
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) ComputeStats(ctx context.Context) (*ComputeStatsResult, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT executor_ref
		FROM (
			SELECT executor_ref FROM achievement_utxos WHERE tenant_id=$1 AND executor_ref <> ''
			UNION
			SELECT executor_ref FROM qualifications WHERE tenant_id=$1 AND deleted=FALSE AND executor_ref <> ''
			UNION
			SELECT executor_ref FROM violation_records WHERE tenant_id=$1 AND executor_ref <> ''
			UNION
			SELECT executor_ref FROM employees WHERE tenant_id=$1 AND deleted=FALSE AND executor_ref <> ''
		) t
	`, s.tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	refs := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &ComputeStatsResult{TotalExecutors: len(refs)}
	for _, ref := range refs {
		if _, err := s.ComputeStatsForExecutor(ctx, ref); err != nil {
			result.Failed++
			continue
		}
		result.Updated++
	}
	return result, nil
}

func (s *Service) ComputeStatsForExecutor(ctx context.Context, executorRef string) (*ExecutorStats, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}

	signal, err := s.collectSignals(ctx, executorRef)
	if err != nil {
		return nil, err
	}

	baseLevel := baseCapabilityByQualifications(signal.qualTypes)
	capabilityLevel := computeCapabilityLevel(baseLevel, signal.passRate, signal.totalUTXOs, signal.violationCnt, signal.penaltySum)
	now := time.Now().UTC()

	score := int(math.Round(capabilityLevel*20 + signal.passRate*40 - math.Abs(signal.penaltySum)*10))
	if score < 0 {
		score = 0
	}
	if score > 200 {
		score = 200
	}

	var id int64
	if err := s.db.QueryRowContext(ctx, `
		INSERT INTO executor_stats (
			executor_ref,
			spu_pass_rate,
			total_utxos,
			violation_count,
			capability_level_num,
			specialty_spus,
			last_computed_at,
			total_projects,
			total_violations,
			last_violation_at,
			score,
			capability_level,
			tenant_id,
			updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (tenant_id, executor_ref)
		DO UPDATE SET
			spu_pass_rate=EXCLUDED.spu_pass_rate,
			total_utxos=EXCLUDED.total_utxos,
			violation_count=EXCLUDED.violation_count,
			capability_level_num=EXCLUDED.capability_level_num,
			specialty_spus=EXCLUDED.specialty_spus,
			last_computed_at=EXCLUDED.last_computed_at,
			total_projects=EXCLUDED.total_projects,
			total_violations=EXCLUDED.total_violations,
			last_violation_at=EXCLUDED.last_violation_at,
			score=EXCLUDED.score,
			capability_level=EXCLUDED.capability_level,
			updated_at=EXCLUDED.updated_at
		RETURNING id
	`,
		executorRef,
		signal.passRate,
		signal.totalUTXOs,
		signal.violationCnt,
		capabilityLevel,
		pq.Array(signal.specialtySPUs),
		now,
		signal.totalProjects,
		signal.violationCnt,
		signal.lastViolation,
		score,
		capabilityGrade(capabilityLevel),
		s.tenantID,
		now,
	).Scan(&id); err != nil {
		return nil, err
	}

	return &ExecutorStats{
		ExecutorRef:     executorRef,
		SPUPassRate:     round2(signal.passRate),
		TotalUTXOs:      signal.totalUTXOs,
		ViolationCount:  signal.violationCnt,
		CapabilityLevel: round2(capabilityLevel),
		SpecialtySPUs:   append([]string(nil), signal.specialtySPUs...),
		LastComputedAt:  now,
	}, nil
}

func (s *Service) GetOrgCapacity(ctx context.Context, namespaceRef string, deep bool) (*OrgCapacity, error) {
	if s.db == nil {
		return nil, fmt.Errorf("database is not configured")
	}
	namespaceRef = strings.TrimSpace(namespaceRef)
	if namespaceRef == "" {
		return nil, fmt.Errorf("namespace ref is required")
	}
	visited := map[string]struct{}{}
	return s.buildOrgCapacity(ctx, namespaceRef, deep, visited)
}

func (s *Service) buildOrgCapacity(ctx context.Context, namespaceRef string, deep bool, visited map[string]struct{}) (*OrgCapacity, error) {
	if _, ok := visited[namespaceRef]; ok {
		return &OrgCapacity{NamespaceRef: namespaceRef, Deep: deep}, nil
	}
	visited[namespaceRef] = struct{}{}

	scopeRefs := []string{namespaceRef}
	if deep {
		refs, err := s.listDescendantNamespaces(ctx, namespaceRef)
		if err != nil {
			return nil, err
		}
		if len(refs) > 0 {
			scopeRefs = refs
		}
	}

	executorRefs, err := s.listExecutorsByNamespaces(ctx, scopeRefs)
	if err != nil {
		return nil, err
	}

	out := &OrgCapacity{
		NamespaceRef:           namespaceRef,
		Deep:                   deep,
		ExecutorCount:          len(executorRefs),
		SkillCoverage:          []string{},
		BottleneckSPUs:         []string{},
		CapabilityDistribution: map[string]int{},
		Children:               []*OrgCapacity{},
	}

	if len(executorRefs) == 0 {
		if !deep {
			out.Children = nil
		}
		return out, nil
	}

	avgCap, dist, err := s.capabilitySnapshot(ctx, executorRefs)
	if err != nil {
		return nil, err
	}
	out.AverageCapability = round2(avgCap)
	out.CapabilityDistribution = dist

	totalCap, occupied, err := s.capacityLoad(ctx, executorRefs)
	if err != nil {
		return nil, err
	}
	if totalCap > 0 {
		out.CapacityUtilization = round4(float64(occupied) / float64(totalCap))
	}

	skillCoverage, err := s.skillCoverage(ctx, executorRefs)
	if err != nil {
		return nil, err
	}
	out.SkillCoverage = skillCoverage

	bottlenecks, err := s.bottleneckSPUs(ctx, executorRefs)
	if err != nil {
		return nil, err
	}
	out.BottleneckSPUs = bottlenecks

	atRisk, err := s.atRiskCerts(ctx, executorRefs)
	if err != nil {
		return nil, err
	}
	out.AtRiskCerts = atRisk

	if deep {
		children, err := s.listDirectChildren(ctx, namespaceRef)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			item, err := s.buildOrgCapacity(ctx, child, true, visited)
			if err != nil {
				return nil, err
			}
			out.Children = append(out.Children, item)
		}
	}
	if len(out.Children) == 0 {
		out.Children = nil
	}
	return out, nil
}

func (s *Service) readStats(ctx context.Context, executorRef string) (*ExecutorStats, error) {
	item := &ExecutorStats{}
	var specialty pq.StringArray
	var passRate sql.NullFloat64
	var levelNum sql.NullFloat64
	var lastComputed sql.NullTime
	err := s.db.QueryRowContext(ctx, `
		SELECT executor_ref,
		       COALESCE(spu_pass_rate, 0),
		       COALESCE(total_utxos, 0),
		       COALESCE(violation_count, COALESCE(total_violations, 0)),
		       COALESCE(capability_level_num, 0),
		       COALESCE(specialty_spus, '{}'::text[]),
		       last_computed_at
		FROM executor_stats
		WHERE tenant_id=$1 AND executor_ref=$2
		LIMIT 1
	`, s.tenantID, executorRef).Scan(
		&item.ExecutorRef,
		&passRate,
		&item.TotalUTXOs,
		&item.ViolationCount,
		&levelNum,
		&specialty,
		&lastComputed,
	)
	if err != nil {
		return nil, err
	}
	if passRate.Valid {
		item.SPUPassRate = round2(passRate.Float64)
	}
	if levelNum.Valid {
		item.CapabilityLevel = round2(levelNum.Float64)
	}
	item.SpecialtySPUs = []string(specialty)
	if lastComputed.Valid {
		item.LastComputedAt = lastComputed.Time
	}
	return item, nil
}

func (s *Service) collectSignals(ctx context.Context, executorRef string) (*executorScoreSignal, error) {
	now := time.Now().UTC()
	yearAgo := now.AddDate(-1, 0, 0)

	sig := &executorScoreSignal{}
	var yearTotal int
	var yearSettled int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FILTER (WHERE ingested_at >= $3),
		       COUNT(*) FILTER (WHERE ingested_at >= $3 AND status='SETTLED'),
		       COUNT(*),
		       COUNT(DISTINCT project_ref)
		FROM achievement_utxos
		WHERE tenant_id=$1 AND executor_ref=$2
	`, s.tenantID, executorRef, yearAgo).Scan(&yearTotal, &yearSettled, &sig.totalUTXOs, &sig.totalProjects); err != nil {
		return nil, err
	}
	if yearTotal > 0 {
		sig.passRate = float64(yearSettled) / float64(yearTotal)
	}

	var lastViolation sql.NullTime
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(COALESCE(penalty, 0)), 0),
		       MAX(COALESCE(recorded_at, occurred_at, created_at))
		FROM violation_records
		WHERE tenant_id=$1 AND executor_ref=$2
	`, s.tenantID, executorRef).Scan(&sig.violationCnt, &sig.penaltySum, &lastViolation); err != nil {
		return nil, err
	}
	if lastViolation.Valid {
		t := lastViolation.Time
		sig.lastViolation = &t
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT qual_type
		FROM qualifications
		WHERE tenant_id=$1
		  AND deleted=FALSE
		  AND executor_ref=$2
		  AND status IN ('VALID','EXPIRE_SOON','APPLYING')
	`, s.tenantID, executorRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	qualTypes := make([]string, 0)
	for rows.Next() {
		var qt string
		if err := rows.Scan(&qt); err != nil {
			return nil, err
		}
		qt = strings.TrimSpace(qt)
		if qt != "" {
			qualTypes = append(qualTypes, qt)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sig.qualTypes = qualTypes

	spuRows, err := s.db.QueryContext(ctx, `
		SELECT spu_ref
		FROM achievement_utxos
		WHERE tenant_id=$1
		  AND executor_ref=$2
		  AND status='SETTLED'
		GROUP BY spu_ref
		ORDER BY COUNT(*) DESC, spu_ref ASC
		LIMIT 6
	`, s.tenantID, executorRef)
	if err != nil {
		return nil, err
	}
	defer spuRows.Close()
	specialties := make([]string, 0)
	for spuRows.Next() {
		var spu string
		if err := spuRows.Scan(&spu); err != nil {
			return nil, err
		}
		spu = strings.TrimSpace(spu)
		if spu != "" {
			specialties = append(specialties, spu)
		}
	}
	if err := spuRows.Err(); err != nil {
		return nil, err
	}
	sig.specialtySPUs = specialties
	return sig, nil
}

func (s *Service) listDescendantNamespaces(ctx context.Context, rootRef string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE tree AS (
			SELECT ref
			FROM namespaces
			WHERE tenant_id=$1 AND ref=$2
			UNION ALL
			SELECT n.ref
			FROM namespaces n
			JOIN tree t ON n.parent_ref=t.ref
			WHERE n.tenant_id=$1
		)
		SELECT DISTINCT ref FROM tree
	`, s.tenantID, rootRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return []string{rootRef}, nil
	}
	sort.Strings(refs)
	return refs, nil
}

func (s *Service) listDirectChildren(ctx context.Context, parentRef string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ref
		FROM namespaces
		WHERE tenant_id=$1 AND parent_ref=$2
		ORDER BY ref
	`, s.tenantID, parentRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	return refs, rows.Err()
}

func (s *Service) listExecutorsByNamespaces(ctx context.Context, refs []string) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT executor_ref
		FROM employees
		WHERE tenant_id=$1
		  AND deleted=FALSE
		  AND company_ref = ANY($2)
		  AND executor_ref <> ''
	`, s.tenantID, pq.Array(refs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, err
		}
		ref = strings.TrimSpace(ref)
		if ref != "" {
			out = append(out, ref)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func (s *Service) capabilitySnapshot(ctx context.Context, executorRefs []string) (float64, map[string]int, error) {
	dist := map[string]int{
		"level_ge_4": 0,
		"level_ge_3": 0,
		"level_ge_2": 0,
		"level_lt_2": 0,
	}
	if len(executorRefs) == 0 {
		return 0, dist, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(capability_level_num,
			CASE capability_level
				WHEN 'EXCELLENT' THEN 4.2
				WHEN 'STABLE' THEN 3.2
				WHEN 'WATCH' THEN 2.3
				WHEN 'RISK' THEN 1.2
				ELSE 0
			END)
		FROM executor_stats
		WHERE tenant_id=$1 AND executor_ref = ANY($2)
	`, s.tenantID, pq.Array(executorRefs))
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	count := 0
	sum := 0.0
	for rows.Next() {
		var level float64
		if err := rows.Scan(&level); err != nil {
			return 0, nil, err
		}
		count++
		sum += level
		switch {
		case level >= 4:
			dist["level_ge_4"]++
		case level >= 3:
			dist["level_ge_3"]++
		case level >= 2:
			dist["level_ge_2"]++
		default:
			dist["level_lt_2"]++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	if count == 0 {
		return 0, dist, nil
	}
	return sum / float64(count), dist, nil
}

func (s *Service) capacityLoad(ctx context.Context, executorRefs []string) (int64, int64, error) {
	if len(executorRefs) == 0 {
		return 0, 0, nil
	}
	var totalCapacity int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(limit_per_executor), 0)
		FROM (
			SELECT executor_ref,
			       GREATEST(MAX(COALESCE(max_concurrent_projects, 5)), 1) AS limit_per_executor
			FROM qualifications
			WHERE tenant_id=$1
			  AND deleted=FALSE
			  AND status IN ('VALID','EXPIRE_SOON')
			  AND executor_ref = ANY($2)
			GROUP BY executor_ref
		) t
	`, s.tenantID, pq.Array(executorRefs)).Scan(&totalCapacity); err != nil {
		return 0, 0, err
	}

	var occupied int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT ref)
		FROM project_nodes
		WHERE tenant_id=$1
		  AND status IN ('INITIATED','CONTRACTED','IN_PROGRESS')
		  AND executor_ref = ANY($2)
	`, s.tenantID, pq.Array(executorRefs)).Scan(&occupied); err != nil {
		return 0, 0, err
	}
	return totalCapacity, occupied, nil
}

func (s *Service) skillCoverage(ctx context.Context, executorRefs []string) ([]string, error) {
	if len(executorRefs) == 0 {
		return []string{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT spu_ref
		FROM achievement_utxos
		WHERE tenant_id=$1
		  AND status='SETTLED'
		  AND executor_ref = ANY($2)
		ORDER BY spu_ref
	`, s.tenantID, pq.Array(executorRefs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var spu string
		if err := rows.Scan(&spu); err != nil {
			return nil, err
		}
		spu = strings.TrimSpace(spu)
		if spu != "" {
			out = append(out, spu)
		}
	}
	return out, rows.Err()
}

func (s *Service) bottleneckSPUs(ctx context.Context, executorRefs []string) ([]string, error) {
	if len(executorRefs) == 0 {
		return []string{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT spu_ref
		FROM (
			SELECT spu_ref,
			       COUNT(DISTINCT executor_ref) AS exec_count,
			       COUNT(*) AS total_count
			FROM achievement_utxos
			WHERE tenant_id=$1
			  AND status='SETTLED'
			  AND executor_ref = ANY($2)
			GROUP BY spu_ref
		) t
		ORDER BY exec_count ASC, total_count DESC, spu_ref ASC
		LIMIT 5
	`, s.tenantID, pq.Array(executorRefs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, 5)
	for rows.Next() {
		var spu string
		if err := rows.Scan(&spu); err != nil {
			return nil, err
		}
		out = append(out, spu)
	}
	return out, rows.Err()
}

func (s *Service) atRiskCerts(ctx context.Context, executorRefs []string) (int, error) {
	if len(executorRefs) == 0 {
		return 0, nil
	}
	var count int
	now := time.Now().UTC()
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM qualifications
		WHERE tenant_id=$1
		  AND deleted=FALSE
		  AND status IN ('VALID','EXPIRE_SOON')
		  AND executor_ref = ANY($2)
		  AND valid_until IS NOT NULL
		  AND valid_until >= $3
		  AND valid_until <= $4
	`, s.tenantID, pq.Array(executorRefs), now, now.AddDate(0, 0, 90)).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func severityPenalty(raw string) (string, float64) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	switch s {
	case "MINOR", "LOW":
		return "LOW", -0.2
	case "MAJOR", "HIGH":
		return "HIGH", -1.0
	case "CRITICAL":
		return "CRITICAL", -2.0
	case "MEDIUM":
		return "MEDIUM", -0.6
	default:
		return "HIGH", -1.0
	}
}

func normalizeSeverity(raw string) string {
	s := strings.ToUpper(strings.TrimSpace(raw))
	switch s {
	case "LOW", "MINOR":
		return "MINOR"
	case "HIGH", "MAJOR":
		return "MAJOR"
	case "CRITICAL":
		return "CRITICAL"
	case "MEDIUM":
		return "MAJOR"
	default:
		return s
	}
}

func baseCapabilityByQualifications(qualTypes []string) float64 {
	if len(qualTypes) == 0 {
		return 2.0
	}
	base := 2.0
	for _, qt := range qualTypes {
		q := strings.ToUpper(strings.TrimSpace(qt))
		switch {
		case q == "CHIEF_ENGINEER":
			if base < 5.0 {
				base = 5.0
			}
		case q == "SENIOR_ENGINEER":
			if base < 4.0 {
				base = 4.0
			}
		case q == "ENGINEER" || strings.HasPrefix(q, "REG_"):
			if base < 3.0 {
				base = 3.0
			}
		case strings.HasPrefix(q, "ROLE_"):
			if base < 2.0 {
				base = 2.0
			}
		}
	}
	return base
}

func computeCapabilityLevel(baseLevel, passRate float64, totalUTXOs, violationCount int, penaltySum float64) float64 {
	level := baseLevel
	if totalUTXOs >= 20 && passRate >= 0.95 && violationCount == 0 {
		level += 0.5
	}
	if totalUTXOs >= 50 {
		level += 0.2
	}
	level += (passRate - 0.8) * 0.8
	level += penaltySum
	if level < 0 {
		level = 0
	}
	if level > 5 {
		level = 5
	}
	return level
}

func capabilityGrade(level float64) string {
	switch {
	case level >= 4.5:
		return "CHIEF_ENGINEER"
	case level >= 4:
		return "SENIOR_ENGINEER"
	case level >= 3:
		return "REGISTERED_ENGINEER"
	case level >= 2:
		return "LEAD_ENGINEER"
	default:
		return "ASSISTANT"
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
