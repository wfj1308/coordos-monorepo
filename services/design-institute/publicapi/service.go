package publicapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type CapabilitySummary struct {
	TenantID    int              `json:"tenant_id"`
	Counters    map[string]int64 `json:"counters"`
	GeneratedAt time.Time        `json:"generated_at"`
}

type Product struct {
	Seq                    int      `json:"seq"`
	SPURef                 string   `json:"spu_ref"`
	Name                   string   `json:"name"`
	Kind                   string   `json:"kind"`
	Stage                  string   `json:"stage"`
	BlockingForNext        bool     `json:"blocking_for_next"`
	RequiredQualifications []string `json:"required_qualifications,omitempty"`
}

type PublicAchievement struct {
	ID            int64      `json:"id"`
	UTXORef       string     `json:"utxo_ref"`
	SPURef        string     `json:"spu_ref"`
	ProjectRef    string     `json:"project_ref"`
	ExecutorRef   string     `json:"executor_ref"`
	ExperienceRef *string    `json:"experience_ref,omitempty"`
	ProofHash     string     `json:"proof_hash"`
	Status        string     `json:"status"`
	Source        string     `json:"source"`
	IngestedAt    time.Time  `json:"ingested_at"`
	SettledAt     *time.Time `json:"settled_at,omitempty"`
}

type AchievementFilter struct {
	ProjectRef string
	SPURef     string
	Limit      int
	Offset     int
}

type PartnerQualificationItem struct {
	QualType    string   `json:"qual_type"`
	Label       string   `json:"label"`
	CertNo      string   `json:"cert_no"`
	Specialty   string   `json:"specialty,omitempty"`
	Scope       string   `json:"scope,omitempty"`
	VerifyURL   string   `json:"verify_url,omitempty"`
	IssuedBy    string   `json:"issued_by,omitempty"`
	Level       string   `json:"level,omitempty"`
	SourceURL   string   `json:"source_url,omitempty"`
	UpdatedAt   string   `json:"updated_at,omitempty"`
	GenesisRef  string   `json:"genesis_ref,omitempty"`
	CreditCode  string   `json:"credit_code,omitempty"`
	ValidUntil  string   `json:"valid_until,omitempty"`
	RuleBinding []string `json:"rule_binding,omitempty"`
}

type PartnerCapabilityLayer struct {
	SPUTypes                []string         `json:"spu_types"`
	SPUTypeCount            int              `json:"spu_type_count"`
	ExecutableExecutorCount int              `json:"executable_executor_count"`
	RegisteredEngineerCount int              `json:"registered_engineer_count"`
	QualificationTypeCounts map[string]int64 `json:"qualification_type_counts"`
	AverageCapabilityLevel  string           `json:"average_capability_level"`
	ExecutionsLast1Y        int64            `json:"executions_last_1y"`
}

type PartnerAchievementSummary struct {
	ProjectRef       string     `json:"project_ref"`
	SettledUTXOCount int64      `json:"settled_utxo_count"`
	LatestSettledAt  *time.Time `json:"latest_settled_at,omitempty"`
	ProofHashes      []string   `json:"proof_hashes"`
}

type PartnerSpecialtyCapacity struct {
	Specialty          string `json:"specialty"`
	QualifiedExecutors int64  `json:"qualified_executors"`
	CapacityLimit      int64  `json:"capacity_limit"`
	OccupiedEstimate   int64  `json:"occupied_estimate"`
	RemainingCapacity  int64  `json:"remaining_capacity"`
}

type PartnerProfile struct {
	TenantID       int       `json:"tenant_id"`
	TenantRef      string    `json:"tenant_ref"`
	TargetAudience string    `json:"target_audience"`
	LaunchEntries  []string  `json:"launch_entries"`
	GeneratedAt    time.Time `json:"generated_at"`

	QualificationLayer struct {
		Items []PartnerQualificationItem `json:"items"`
		Count int                        `json:"count"`
	} `json:"qualification_layer"`

	CapabilityLayer PartnerCapabilityLayer `json:"capability_layer"`

	AchievementLayer struct {
		Years int                         `json:"years"`
		Items []PartnerAchievementSummary `json:"items"`
		Count int                         `json:"count"`
	} `json:"achievement_layer"`

	CapacityLayer struct {
		InHandProjectCount int64                      `json:"in_hand_project_count"`
		TotalCapacityLimit int64                      `json:"total_capacity_limit"`
		RemainingCapacity  int64                      `json:"remaining_capacity"`
		BySpecialty        []PartnerSpecialtyCapacity `json:"by_specialty"`
	} `json:"capacity_layer"`
}

type partnerProfileData struct {
	Qualifications      []PartnerQualificationItem
	ExecutorRanks       map[string]int
	RegisteredEngineers map[string]struct{}
	QualTypeCounts      map[string]int64
	ExecutionsLast1Y    int64
	Achievements        []PartnerAchievementSummary
	InHandProjects      int64
	SpecialtyCaps       []PartnerSpecialtyCapacity
	TotalCapacity       int64
}

type Store interface {
	CapabilityCounters(ctx context.Context, tenantID int) (map[string]int64, error)
	ListAchievements(ctx context.Context, tenantID int, f AchievementFilter) ([]*PublicAchievement, int, error)
	BuildPartnerProfileData(ctx context.Context, tenantID int, namespaceRef string, execSince time.Time, settledSince time.Time, settledLimit int) (*partnerProfileData, error)
}

type Service struct {
	store       Store
	tenantID    int
	catalogPath string
}

func NewService(store Store, tenantID int, catalogPath string) *Service {
	catalogPath = strings.TrimSpace(catalogPath)
	if catalogPath == "" {
		catalogPath = "specs/spu/bridge/catalog.v1.json"
	}
	return &Service{
		store:       store,
		tenantID:    tenantID,
		catalogPath: catalogPath,
	}
}

func (s *Service) Capabilities(ctx context.Context) (*CapabilitySummary, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	counters, err := s.store.CapabilityCounters(ctx, s.tenantID)
	if err != nil {
		return nil, err
	}
	return &CapabilitySummary{
		TenantID:    s.tenantID,
		Counters:    counters,
		GeneratedAt: time.Now().UTC(),
	}, nil
}

func (s *Service) Products() ([]*Product, error) {
	type catalogChainItem struct {
		Seq                    int      `json:"seq"`
		SPURef                 string   `json:"spu_ref"`
		Name                   string   `json:"name"`
		Kind                   string   `json:"kind"`
		Stage                  string   `json:"stage"`
		BlockingForNext        bool     `json:"blocking_for_next"`
		RequiredQualifications []string `json:"required_qualifications"`
	}
	type catalogFile struct {
		ClosureChain []catalogChainItem `json:"closure_chain"`
	}

	raw, resolvedPath, err := readCatalogFile(s.catalogPath)
	if err != nil {
		return nil, fmt.Errorf("read spu catalog failed: %w", err)
	}
	var cat catalogFile
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, fmt.Errorf("parse spu catalog failed at %s: %w", resolvedPath, err)
	}

	out := make([]*Product, 0, len(cat.ClosureChain))
	for _, item := range cat.ClosureChain {
		out = append(out, &Product{
			Seq:                    item.Seq,
			SPURef:                 strings.TrimSpace(item.SPURef),
			Name:                   strings.TrimSpace(item.Name),
			Kind:                   strings.TrimSpace(item.Kind),
			Stage:                  strings.TrimSpace(item.Stage),
			BlockingForNext:        item.BlockingForNext,
			RequiredQualifications: item.RequiredQualifications,
		})
	}
	return out, nil
}

func readCatalogFile(path string) ([]byte, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "specs/spu/bridge/catalog.v1.json"
	}
	candidates := buildCatalogCandidates(path)
	var lastErr error
	for _, candidate := range candidates {
		raw, err := os.ReadFile(candidate)
		if err == nil {
			return raw, candidate, nil
		}
		lastErr = err
	}
	return nil, "", fmt.Errorf("%w (candidates=%v)", lastErr, candidates)
}

func buildCatalogCandidates(path string) []string {
	clean := filepath.Clean(path)
	candidates := []string{clean}
	if filepath.IsAbs(clean) {
		return candidates
	}
	// Running from services/design-institute: ../../specs/...
	candidates = append(candidates, filepath.Clean(filepath.Join("..", "..", clean)))
	// Running from repository root with explicit services path.
	candidates = append(candidates, filepath.Clean(filepath.Join("services", "design-institute", clean)))
	return uniqueStrings(candidates)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func (s *Service) Achievements(ctx context.Context, f AchievementFilter) ([]*PublicAchievement, int, error) {
	if s.store == nil {
		return nil, 0, fmt.Errorf("store is nil")
	}
	if f.Limit <= 0 {
		f.Limit = 20
	}
	if f.Limit > 200 {
		f.Limit = 200
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.store.ListAchievements(ctx, s.tenantID, f)
}

func (s *Service) PartnerProfileForCooperation(ctx context.Context, namespace string) (*PartnerProfile, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store is nil")
	}
	nsRef, nsCode := normalizeNamespaceRef(namespace)
	now := time.Now().UTC()
	products, err := s.Products()
	if err != nil {
		return nil, err
	}
	data, err := s.store.BuildPartnerProfileData(
		ctx,
		s.tenantID,
		nsRef,
		now.AddDate(-1, 0, 0),
		now.AddDate(-3, 0, 0),
		12,
	)
	if err != nil {
		return nil, err
	}

	spuRefs := make([]string, 0, len(products))
	for _, p := range products {
		if p == nil {
			continue
		}
		ref := strings.TrimSpace(p.SPURef)
		if ref == "" {
			continue
		}
		if nsRef != "" && strings.HasPrefix(ref, "v://") {
			rest := strings.TrimPrefix(ref, "v://")
			if idx := strings.Index(rest, "/"); idx > 0 {
				ref = "v://" + nsCode + "/" + rest[idx+1:]
			}
		}
		spuRefs = append(spuRefs, ref)
	}

	profile := &PartnerProfile{
		TenantID:       s.tenantID,
		TenantRef:      nsRef,
		TargetAudience: "COOPERATIVE_DESIGN_INSTITUTES",
		LaunchEntries:  []string{"CAPABILITY_DECLARATION", "PRODUCT_ADDRESSING"},
		GeneratedAt:    now,
	}
	profile.QualificationLayer.Items = data.Qualifications
	profile.QualificationLayer.Count = len(data.Qualifications)
	profile.CapabilityLayer = PartnerCapabilityLayer{
		SPUTypes:                spuRefs,
		SPUTypeCount:            len(spuRefs),
		ExecutableExecutorCount: len(data.ExecutorRanks),
		RegisteredEngineerCount: len(data.RegisteredEngineers),
		QualificationTypeCounts: data.QualTypeCounts,
		AverageCapabilityLevel:  averageCapabilityLevel(data.ExecutorRanks),
		ExecutionsLast1Y:        data.ExecutionsLast1Y,
	}
	profile.AchievementLayer.Years = 3
	profile.AchievementLayer.Items = data.Achievements
	profile.AchievementLayer.Count = len(data.Achievements)
	profile.CapacityLayer.InHandProjectCount = data.InHandProjects
	profile.CapacityLayer.TotalCapacityLimit = data.TotalCapacity
	remaining := data.TotalCapacity - data.InHandProjects
	if remaining < 0 {
		remaining = 0
	}
	profile.CapacityLayer.RemainingCapacity = remaining
	profile.CapacityLayer.BySpecialty = data.SpecialtyCaps
	return profile, nil
}

type PGStore struct {
	db *sql.DB
}

func NewPGStore(db *sql.DB) Store {
	return &PGStore{db: db}
}

func (s *PGStore) CapabilityCounters(ctx context.Context, tenantID int) (map[string]int64, error) {
	var activeProjects int64
	var activePersonQualifications int64
	var activeCompanyQualifications int64
	var activeRights int64
	var totalAchievements int64
	var settledAchievements int64

	err := s.db.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM project_nodes WHERE tenant_id=$1 AND status <> 'ARCHIVED'),
			(SELECT COUNT(*) FROM qualifications WHERE tenant_id=$1 AND deleted=FALSE AND status='VALID' AND holder_type='PERSON'),
			(SELECT COUNT(*) FROM qualifications WHERE tenant_id=$1 AND deleted=FALSE AND status='VALID' AND holder_type='COMPANY'),
			(SELECT COUNT(*) FROM rights WHERE tenant_id=$1 AND status='ACTIVE'),
			(SELECT COUNT(*) FROM achievement_utxos WHERE tenant_id=$1),
			(SELECT COUNT(*) FROM achievement_utxos WHERE tenant_id=$1 AND status='SETTLED')
	`, tenantID).Scan(
		&activeProjects,
		&activePersonQualifications,
		&activeCompanyQualifications,
		&activeRights,
		&totalAchievements,
		&settledAchievements,
	)
	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"active_projects":               activeProjects,
		"active_person_qualifications":  activePersonQualifications,
		"active_company_qualifications": activeCompanyQualifications,
		"active_rights":                 activeRights,
		"total_achievements":            totalAchievements,
		"settled_achievements":          settledAchievements,
	}, nil
}

func (s *PGStore) ListAchievements(ctx context.Context, tenantID int, f AchievementFilter) ([]*PublicAchievement, int, error) {
	where := []string{"tenant_id=$1"}
	args := []any{tenantID}
	argPos := 2

	if v := strings.TrimSpace(f.ProjectRef); v != "" {
		where = append(where, fmt.Sprintf("project_ref=$%d", argPos))
		args = append(args, v)
		argPos++
	}
	if v := strings.TrimSpace(f.SPURef); v != "" {
		where = append(where, fmt.Sprintf("spu_ref=$%d", argPos))
		args = append(args, v)
		argPos++
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	countSQL := "SELECT COUNT(*) FROM achievement_utxos WHERE " + whereSQL
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listSQL := fmt.Sprintf(`
		SELECT id, utxo_ref, spu_ref, project_ref, executor_ref, experience_ref,
		       proof_hash, status, source, ingested_at, settled_at
		FROM achievement_utxos
		WHERE %s
		ORDER BY ingested_at DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, argPos, argPos+1)
	args = append(args, f.Limit, f.Offset)

	rows, err := s.db.QueryContext(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := make([]*PublicAchievement, 0)
	for rows.Next() {
		item := &PublicAchievement{}
		if err := rows.Scan(
			&item.ID,
			&item.UTXORef,
			&item.SPURef,
			&item.ProjectRef,
			&item.ExecutorRef,
			&item.ExperienceRef,
			&item.ProofHash,
			&item.Status,
			&item.Source,
			&item.IngestedAt,
			&item.SettledAt,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (s *PGStore) BuildPartnerProfileData(
	ctx context.Context,
	tenantID int,
	namespaceRef string,
	execSince time.Time,
	settledSince time.Time,
	settledLimit int,
) (*partnerProfileData, error) {
	if settledLimit <= 0 {
		settledLimit = 12
	}
	out := &partnerProfileData{
		Qualifications:      make([]PartnerQualificationItem, 0),
		ExecutorRanks:       map[string]int{},
		RegisteredEngineers: map[string]struct{}{},
		QualTypeCounts:      map[string]int64{},
		Achievements:        make([]PartnerAchievementSummary, 0),
		SpecialtyCaps:       make([]PartnerSpecialtyCapacity, 0),
	}

	genesisItems, err := s.listCompanyQualFromGenesis(ctx, tenantID, namespaceRef)
	if err != nil {
		return nil, err
	}
	if len(genesisItems) > 0 {
		out.Qualifications = append(out.Qualifications, genesisItems...)
	} else {
		qualRows, err := s.db.QueryContext(ctx, `
			SELECT
				COALESCE(qual_type, ''),
				COALESCE(cert_no, ''),
				COALESCE(specialty, ''),
				COALESCE(scope, ''),
				COALESCE(attachment_url, ''),
				COALESCE(issued_by, ''),
				COALESCE(level, ''),
				valid_until,
				updated_at
			FROM qualifications
			WHERE tenant_id=$1 AND deleted=FALSE AND status='VALID' AND holder_type='COMPANY'
			ORDER BY updated_at DESC
		`, tenantID)
		if err != nil {
			return nil, err
		}
		defer qualRows.Close()
		for qualRows.Next() {
			var qualType, certNo, specialty, scope, attachmentURL, issuedBy, level string
			var validUntil sql.NullTime
			var updatedAt time.Time
			if err := qualRows.Scan(
				&qualType,
				&certNo,
				&specialty,
				&scope,
				&attachmentURL,
				&issuedBy,
				&level,
				&validUntil,
				&updatedAt,
			); err != nil {
				return nil, err
			}
			item := PartnerQualificationItem{
				QualType:  strings.TrimSpace(qualType),
				Label:     companyQualificationLabel(strings.TrimSpace(qualType)),
				CertNo:    strings.TrimSpace(certNo),
				Specialty: strings.TrimSpace(specialty),
				Scope:     strings.TrimSpace(scope),
				IssuedBy:  strings.TrimSpace(issuedBy),
				Level:     strings.TrimSpace(level),
				SourceURL: strings.TrimSpace(attachmentURL),
				UpdatedAt: updatedAt.UTC().Format(time.RFC3339),
			}
			item = normalizePartnerQualificationItem(item)
			if validUntil.Valid {
				item.ValidUntil = validUntil.Time.UTC().Format("2006-01-02")
			}
			item.VerifyURL = buildMOHURDVerifyURL(item.CertNo)
			out.Qualifications = append(out.Qualifications, item)
		}
		if err := qualRows.Err(); err != nil {
			return nil, err
		}
	}

	personRows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(executor_ref, ''), COALESCE(qual_type, ''), COALESCE(specialty, ''), COALESCE(max_concurrent_projects, 0)
		FROM qualifications
		WHERE tenant_id=$1 AND deleted=FALSE AND status='VALID' AND holder_type='PERSON' AND executor_ref <> ''
		  AND ($2 = '' OR executor_ref LIKE $3)
	`, tenantID, namespaceRef, namespaceLikePattern(namespaceRef, "executor"))
	if err != nil {
		return nil, err
	}
	defer personRows.Close()

	type execAgg struct {
		qualTypes     map[string]struct{}
		specialty     string
		maxConcurrent int
	}
	execMap := map[string]*execAgg{}
	for personRows.Next() {
		var executorRef, qualType, specialty string
		var maxConcurrent int
		if err := personRows.Scan(&executorRef, &qualType, &specialty, &maxConcurrent); err != nil {
			return nil, err
		}
		executorRef = strings.TrimSpace(executorRef)
		if executorRef == "" {
			continue
		}
		item := execMap[executorRef]
		if item == nil {
			item = &execAgg{
				qualTypes: map[string]struct{}{},
			}
			execMap[executorRef] = item
		}
		qt := strings.TrimSpace(qualType)
		if qt != "" {
			item.qualTypes[qt] = struct{}{}
			out.QualTypeCounts[qt]++
			if isEngineerQualType(qt) {
				out.RegisteredEngineers[executorRef] = struct{}{}
			}
		}
		normalizedSpecialty := normalizeExecutorSpecialty(strings.TrimSpace(specialty), qt)
		if shouldAdoptSpecialty(item.specialty, normalizedSpecialty) {
			item.specialty = normalizedSpecialty
		}
		if maxConcurrent > item.maxConcurrent {
			item.maxConcurrent = maxConcurrent
		}
	}
	if err := personRows.Err(); err != nil {
		return nil, err
	}

	specialtyCaps := map[string]int64{}
	for executorRef, agg := range execMap {
		rank := capabilityRankFromQualTypes(agg.qualTypes)
		out.ExecutorRanks[executorRef] = rank
		limit := int64(executorProjectLimit(rank, agg.qualTypes, agg.maxConcurrent))
		out.TotalCapacity += limit
		specialty := normalizeSpecialtyDisplay(agg.specialty)
		specialtyCaps[specialty] += limit
	}

	var inHand int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM project_nodes
		WHERE tenant_id=$1 AND status NOT IN ('ARCHIVED','SETTLED')
		  AND ($2 = '' OR ref LIKE $3)
	`, tenantID, namespaceRef, namespaceLikePattern(namespaceRef, "project")).Scan(&inHand); err != nil {
		return nil, err
	}
	out.InHandProjects = inHand

	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM achievement_utxos
		WHERE tenant_id=$1 AND ingested_at >= $2
		  AND ($3 = '' OR project_ref LIKE $4 OR spu_ref LIKE $5 OR utxo_ref LIKE $6)
	`, tenantID, execSince, namespaceRef,
		namespaceLikePattern(namespaceRef, "project"),
		namespaceLikePattern(namespaceRef, "spu"),
		namespaceLikePattern(namespaceRef, "utxo"),
	).Scan(&out.ExecutionsLast1Y); err != nil {
		return nil, err
	}

	projectRows, err := s.db.QueryContext(ctx, `
		SELECT project_ref, COUNT(*) AS settled_count, MAX(settled_at) AS latest_settled_at
		FROM achievement_utxos
		WHERE tenant_id=$1 AND status='SETTLED' AND settled_at IS NOT NULL AND settled_at >= $2
		  AND ($3 = '' OR project_ref LIKE $4 OR spu_ref LIKE $5 OR utxo_ref LIKE $6)
		GROUP BY project_ref
		ORDER BY settled_count DESC, latest_settled_at DESC
		LIMIT $7
	`, tenantID, settledSince, namespaceRef,
		namespaceLikePattern(namespaceRef, "project"),
		namespaceLikePattern(namespaceRef, "spu"),
		namespaceLikePattern(namespaceRef, "utxo"),
		settledLimit)
	if err != nil {
		return nil, err
	}
	defer projectRows.Close()
	for projectRows.Next() {
		var projectRef string
		var settledCount int64
		var latestSettledAt sql.NullTime
		if err := projectRows.Scan(&projectRef, &settledCount, &latestSettledAt); err != nil {
			return nil, err
		}
		proofRows, err := s.db.QueryContext(ctx, `
			SELECT proof_hash
			FROM achievement_utxos
			WHERE tenant_id=$1 AND status='SETTLED' AND project_ref=$2
			  AND ($3 = '' OR project_ref LIKE $4 OR spu_ref LIKE $5 OR utxo_ref LIKE $6)
			ORDER BY settled_at DESC NULLS LAST, ingested_at DESC
			LIMIT 3
		`, tenantID, projectRef, namespaceRef,
			namespaceLikePattern(namespaceRef, "project"),
			namespaceLikePattern(namespaceRef, "spu"),
			namespaceLikePattern(namespaceRef, "utxo"))
		if err != nil {
			return nil, err
		}
		proofs := make([]string, 0, 3)
		for proofRows.Next() {
			var proofHash string
			if err := proofRows.Scan(&proofHash); err != nil {
				_ = proofRows.Close()
				return nil, err
			}
			proofHash = strings.TrimSpace(proofHash)
			if proofHash != "" {
				proofs = append(proofs, proofHash)
			}
		}
		if err := proofRows.Err(); err != nil {
			_ = proofRows.Close()
			return nil, err
		}
		_ = proofRows.Close()

		item := PartnerAchievementSummary{
			ProjectRef:       strings.TrimSpace(projectRef),
			SettledUTXOCount: settledCount,
			ProofHashes:      proofs,
		}
		if latestSettledAt.Valid {
			tm := latestSettledAt.Time.UTC()
			item.LatestSettledAt = &tm
		}
		out.Achievements = append(out.Achievements, item)
	}
	if err := projectRows.Err(); err != nil {
		return nil, err
	}

	if out.TotalCapacity <= 0 {
		out.TotalCapacity = inHand
	}
	specialtyNames := make([]string, 0, len(specialtyCaps))
	for specialty := range specialtyCaps {
		specialtyNames = append(specialtyNames, specialty)
	}
	sort.Strings(specialtyNames)
	for _, specialty := range specialtyNames {
		limit := specialtyCaps[specialty]
		var occupied int64
		if out.TotalCapacity > 0 {
			occupied = int64(math.Round(float64(inHand) * float64(limit) / float64(out.TotalCapacity)))
		}
		if occupied > limit {
			occupied = limit
		}
		remaining := limit - occupied
		if remaining < 0 {
			remaining = 0
		}
		out.SpecialtyCaps = append(out.SpecialtyCaps, PartnerSpecialtyCapacity{
			Specialty:          specialty,
			QualifiedExecutors: maxInt64(1, limit/5),
			CapacityLimit:      limit,
			OccupiedEstimate:   occupied,
			RemainingCapacity:  remaining,
		})
	}
	return out, nil
}

func companyQualificationLabel(qualType string) string {
	switch qualType {
	case "QUAL_HIGHWAY_INDUSTRY_A":
		return "公路行业（公路、特大桥梁、特长隧道、交通工程）专业甲级"
	case "QUAL_MUNICIPAL_INDUSTRY_A":
		return "市政行业（排水工程、城镇燃气工程、道路工程、桥梁工程）专业甲级"
	case "QUAL_ARCH_COMPREHENSIVE_A":
		return "建筑行业（建筑工程）甲级"
	case "QUAL_LANDSCAPE_SPECIAL_A":
		return "风景园林工程设计专项甲级"
	case "QUAL_WATER_INDUSTRY_B":
		return "水利行业乙级"
	case "COMPREHENSIVE_A":
		return "综合甲级资质"
	case "SPECIAL_A":
		return "专项甲级资质"
	case "INDUSTRY_A":
		return "行业甲级资质"
	default:
		return qualType
	}
}
func (s *PGStore) listCompanyQualFromGenesis(ctx context.Context, tenantID int, namespaceRef string) ([]PartnerQualificationItem, error) {
	pattern := "%/genesis/qual/%"
	if namespaceRef != "" {
		pattern = strings.TrimSpace(namespaceRef) + "/genesis/qual/%"
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			ref,
			resource_type,
			name,
			COALESCE(constraints->>'cert_no', ''),
			COALESCE(constraints->>'scope', ''),
			COALESCE(constraints->>'verifiable_url', ''),
			COALESCE(constraints->>'issued_by', ''),
			COALESCE(constraints->>'grade', ''),
			COALESCE(constraints->>'credit_code', ''),
			COALESCE(constraints->>'valid_until', ''),
			COALESCE(constraints->>'rule_binding', ''),
			created_at
		FROM genesis_utxos
		WHERE tenant_id=$1 AND status='ACTIVE' AND resource_type LIKE 'QUAL_%' AND ref LIKE $2
		ORDER BY ref
	`, tenantID, pattern)
	if err != nil {
		if isMissingTableError(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]PartnerQualificationItem, 0)
	for rows.Next() {
		var (
			ref            string
			resourceType   string
			label          string
			certNo         string
			scopeRaw       string
			verifyURL      string
			issuedBy       string
			grade          string
			creditCode     string
			validUntil     string
			ruleBindingRaw string
			createdAt      time.Time
		)
		if err := rows.Scan(
			&ref,
			&resourceType,
			&label,
			&certNo,
			&scopeRaw,
			&verifyURL,
			&issuedBy,
			&grade,
			&creditCode,
			&validUntil,
			&ruleBindingRaw,
			&createdAt,
		); err != nil {
			return nil, err
		}

		item := PartnerQualificationItem{
			QualType:    strings.TrimSpace(resourceType),
			Label:       strings.TrimSpace(label),
			CertNo:      strings.TrimSpace(certNo),
			Scope:       normalizeScope(scopeRaw),
			VerifyURL:   strings.TrimSpace(verifyURL),
			IssuedBy:    strings.TrimSpace(issuedBy),
			Level:       normalizeGrade(grade),
			UpdatedAt:   createdAt.UTC().Format(time.RFC3339),
			GenesisRef:  strings.TrimSpace(ref),
			CreditCode:  strings.TrimSpace(creditCode),
			ValidUntil:  strings.TrimSpace(validUntil),
			RuleBinding: parseJSONArrayText(ruleBindingRaw),
		}
		item = normalizePartnerQualificationItem(item)
		if item.Label == "" {
			item.Label = item.QualType
		}
		if item.VerifyURL == "" {
			item.VerifyURL = buildMOHURDVerifyURL(item.CertNo)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sortPartnerQualifications(items)
	return items, nil
}

type qualificationDisplayDefaults struct {
	Label    string
	Scope    []string
	IssuedBy string
}

var zhongbeiQualificationDisplayByGenesisRef = map[string]qualificationDisplayDefaults{
	"v://zhongbei/genesis/qual/highway_a": {
		Label:    "公路行业（公路、特大桥梁、特长隧道、交通工程）专业甲级",
		Scope:    []string{"公路", "特大桥梁", "特长隧道", "交通工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://cn.zhongbei/genesis/qual/highway_a": {
		Label:    "公路行业（公路、特大桥梁、特长隧道、交通工程）专业甲级",
		Scope:    []string{"公路", "特大桥梁", "特长隧道", "交通工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://zhongbei/genesis/qual/municipal_a": {
		Label:    "市政行业（排水工程、城镇燃气工程、道路工程、桥梁工程）专业甲级",
		Scope:    []string{"排水工程", "城镇燃气工程", "道路工程", "桥梁工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://cn.zhongbei/genesis/qual/municipal_a": {
		Label:    "市政行业（排水工程、城镇燃气工程、道路工程、桥梁工程）专业甲级",
		Scope:    []string{"排水工程", "城镇燃气工程", "道路工程", "桥梁工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://zhongbei/genesis/qual/arch_a": {
		Label:    "建筑行业（建筑工程）甲级",
		Scope:    []string{"建筑工程", "建筑装饰工程", "建筑幕墙工程", "轻型钢结构工程", "建筑智能化系统", "照明工程", "消防设施工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://cn.zhongbei/genesis/qual/arch_a": {
		Label:    "建筑行业（建筑工程）甲级",
		Scope:    []string{"建筑工程", "建筑装饰工程", "建筑幕墙工程", "轻型钢结构工程", "建筑智能化系统", "照明工程", "消防设施工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://zhongbei/genesis/qual/landscape_a": {
		Label:    "风景园林工程设计专项甲级",
		Scope:    []string{"风景园林工程设计"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://cn.zhongbei/genesis/qual/landscape_a": {
		Label:    "风景园林工程设计专项甲级",
		Scope:    []string{"风景园林工程设计"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://zhongbei/genesis/qual/water_b": {
		Label:    "水利行业乙级",
		Scope:    []string{"水利工程"},
		IssuedBy: "住房和城乡建设部",
	},
	"v://cn.zhongbei/genesis/qual/water_b": {
		Label:    "水利行业乙级",
		Scope:    []string{"水利工程"},
		IssuedBy: "住房和城乡建设部",
	},
}

func normalizePartnerQualificationItem(item PartnerQualificationItem) PartnerQualificationItem {
	ref := strings.TrimSpace(item.GenesisRef)
	if defaults, ok := zhongbeiQualificationDisplayByGenesisRef[ref]; ok {
		if defaults.Label != "" {
			item.Label = defaults.Label
		}
		if len(defaults.Scope) > 0 {
			item.Scope = strings.Join(defaults.Scope, ", ")
		}
		if defaults.IssuedBy != "" {
			item.IssuedBy = defaults.IssuedBy
		}
	}

	item.Label = strings.TrimSpace(item.Label)
	item.Scope = strings.TrimSpace(item.Scope)
	item.IssuedBy = strings.TrimSpace(item.IssuedBy)
	return item
}

func normalizeExecutorSpecialty(raw, qualType string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || isUnknownSpecialty(raw) {
		return defaultSpecialtyForQualType(qualType)
	}
	if decoded, ok := tryDecodeUTF8Mojibake(raw); ok {
		raw = decoded
	} else if looksLikeMojibake(raw) {
		return defaultSpecialtyForQualType(qualType)
	}
	raw = normalizeSpecialtyAlias(raw)
	if raw == "" || isUnknownSpecialty(raw) {
		return defaultSpecialtyForQualType(qualType)
	}
	return raw
}

func shouldAdoptSpecialty(current, candidate string) bool {
	current = strings.TrimSpace(current)
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	if current == "" || isUnknownSpecialty(current) || looksLikeMojibake(current) {
		return true
	}
	return false
}

func isUnknownSpecialty(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "", "?", "??", "-", "--", "N/A", "NA", "NULL", "UNSPECIFIED":
		return true
	default:
		return false
	}
}

func looksLikeMojibake(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	// 包含 Unicode 替换字符（U+FFFD）通常表示上游出现了编码损坏。
	if strings.ContainsRune(value, '\uFFFD') {
		return true
	}
	if containsHanRune(value) {
		return false
	}

	latin1ish := 0
	for _, r := range value {
		if r >= 0x00C0 && r <= 0x00FF {
			latin1ish++
		}
	}
	return latin1ish > 0
}

func tryDecodeUTF8Mojibake(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}

	raw := make([]byte, 0, len(value))
	for _, r := range value {
		if r > 0xFF {
			return "", false
		}
		raw = append(raw, byte(r))
	}
	if !utf8.Valid(raw) {
		return "", false
	}

	decoded := strings.TrimSpace(string(raw))
	if decoded == "" || decoded == value {
		return "", false
	}
	if !containsHanRune(decoded) {
		return "", false
	}
	return decoded, true
}

func containsHanRune(value string) bool {
	for _, r := range value {
		if unicode.Is(unicode.Han, r) {
			return true
		}
	}
	return false
}

func defaultSpecialtyForQualType(qualType string) string {
	switch strings.ToUpper(strings.TrimSpace(qualType)) {
	case "REG_STRUCTURE", "REG_STRUCTURE_2":
		return "结构工程"
	case "REG_ARCH":
		return "建筑设计"
	case "REG_CIVIL_GEOTEC":
		return "岩土工程"
	case "REG_CIVIL_WATER":
		return "水利水电"
	case "REG_COST":
		return "土建"
	case "REG_ELECTRIC_POWER":
		return "供配电"
	case "REG_ELECTRIC_TRANS":
		return "发输变电"
	case "REG_MEP_POWER":
		return "动力"
	case "REG_MEP_WATER":
		return "给水排水"
	case "REG_MEP_HVAC":
		return "暖通空调"
	default:
		return "综合"
	}
}

func normalizeSpecialtyDisplay(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || isUnknownSpecialty(value) {
		return "综合"
	}
	if decoded, ok := tryDecodeUTF8Mojibake(value); ok {
		value = decoded
	}
	value = normalizeSpecialtyAlias(value)
	if value == "" || isUnknownSpecialty(value) || looksLikeMojibake(value) {
		return "综合"
	}
	return value
}

func normalizeSpecialtyAlias(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	switch strings.ToUpper(value) {
	case "BRIDGE":
		return "桥梁"
	case "STRUCTURE", "STRUCTURAL":
		return "结构工程"
	case "ARCHITECTURE", "ARCH":
		return "建筑设计"
	case "GEOTECH", "GEOTECHNICAL":
		return "岩土工程"
	case "WATER", "WATER ENGINEERING":
		return "水利水电"
	case "MEP WATER", "WATER SUPPLY", "WATER SUPPLY AND DRAINAGE":
		return "给水排水"
	case "MEP HVAC", "HVAC":
		return "暖通空调"
	case "MEP POWER", "POWER":
		return "动力"
	case "ELECTRIC POWER", "POWER DISTRIBUTION":
		return "供配电"
	case "ELECTRIC TRANS", "TRANSMISSION":
		return "发输变电"
	case "COST", "CIVIL COST":
		return "土建"
	default:
		return value
	}
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), `relation "genesis_utxos" does not exist`)
}

func normalizeScope(raw string) string {
	parts := parseJSONArrayText(raw)
	if len(parts) > 0 {
		return strings.Join(parts, ", ")
	}
	return strings.TrimSpace(raw)
}

func parseJSONArrayText(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err == nil {
		filtered := make([]string, 0, len(out))
		for _, item := range out {
			item = strings.TrimSpace(item)
			if item != "" {
				filtered = append(filtered, item)
			}
		}
		return filtered
	}
	var one string
	if err := json.Unmarshal([]byte(raw), &one); err == nil {
		one = strings.TrimSpace(one)
		if one == "" {
			return nil
		}
		return []string{one}
	}
	return nil
}

func normalizeGrade(raw string) string {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "A":
		return "A"
	case "B":
		return "B"
	case "C":
		return "C"
	default:
		return strings.TrimSpace(raw)
	}
}

func sortPartnerQualifications(items []PartnerQualificationItem) {
	order := map[string]int{
		"v://zhongbei/genesis/qual/highway_a":      0,
		"v://cn.zhongbei/genesis/qual/highway_a":   0,
		"v://zhongbei/genesis/qual/municipal_a":    1,
		"v://cn.zhongbei/genesis/qual/municipal_a": 1,
		"v://zhongbei/genesis/qual/arch_a":         2,
		"v://cn.zhongbei/genesis/qual/arch_a":      2,
		"v://zhongbei/genesis/qual/landscape_a":    3,
		"v://cn.zhongbei/genesis/qual/landscape_a": 3,
		"v://zhongbei/genesis/qual/water_b":        4,
		"v://cn.zhongbei/genesis/qual/water_b":     4,
	}
	sort.SliceStable(items, func(i, j int) bool {
		li, iok := order[strings.TrimSpace(items[i].GenesisRef)]
		lj, jok := order[strings.TrimSpace(items[j].GenesisRef)]
		switch {
		case iok && jok:
			return li < lj
		case iok:
			return true
		case jok:
			return false
		default:
			if items[i].Label == items[j].Label {
				return items[i].QualType < items[j].QualType
			}
			return items[i].Label < items[j].Label
		}
	})
}

func buildMOHURDVerifyURL(certNo string) string {
	certNo = strings.TrimSpace(certNo)
	if certNo == "" {
		return ""
	}
	return "https://jzsc.mohurd.gov.cn/data/company?q=" + url.QueryEscape(certNo)
}

func capabilityRankFromQualTypes(qualTypes map[string]struct{}) int {
	rank := 0
	for qualType := range qualTypes {
		switch qualType {
		case "REG_STRUCTURE":
			if rank < 5 {
				rank = 5
			}
		case "REG_STRUCTURE_2":
			if rank < 4 {
				rank = 4
			}
		case "REG_ARCH", "REG_CIVIL", "REG_ELECTRIC", "REG_MECH", "REG_CIVIL_GEOTEC", "REG_CIVIL_WATER", "REG_ELECTRIC_POWER", "REG_ELECTRIC_TRANS", "REG_MEP_POWER", "REG_MEP_WATER", "REG_MEP_HVAC":
			if rank < 4 {
				rank = 4
			}
		case "REG_COST":
			if rank < 3 {
				rank = 3
			}
		case "SENIOR_ENGINEER":
			if rank < 3 {
				rank = 3
			}
		case "ENGINEER":
			if rank < 2 {
				rank = 2
			}
		default:
			if rank < 1 {
				rank = 1
			}
		}
	}
	return rank
}

func capabilityLevelByRank(rank int) string {
	switch {
	case rank >= 6:
		return "PLATFORM_ENGINE"
	case rank >= 5:
		return "REGISTERED_STRUCTURAL_ENGINEER"
	case rank >= 4:
		return "REGISTERED_ENGINEER"
	case rank >= 3:
		return "SENIOR_ENGINEER"
	case rank >= 2:
		return "ENGINEER"
	case rank >= 1:
		return "ASSISTANT_ENGINEER"
	default:
		return "NONE"
	}
}

func averageCapabilityLevel(executorRanks map[string]int) string {
	if len(executorRanks) == 0 {
		return "NONE"
	}
	sum := 0
	for _, rank := range executorRanks {
		sum += rank
	}
	avg := int(math.Round(float64(sum) / float64(len(executorRanks))))
	return capabilityLevelByRank(avg)
}

func executorProjectLimit(rank int, qualTypes map[string]struct{}, configuredLimit int) int {
	if configuredLimit > 0 {
		return configuredLimit
	}
	if _, ok := qualTypes["SENIOR_ENGINEER"]; ok {
		return 8
	}
	if rank >= 4 {
		return 6
	}
	return 5
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func normalizeNamespaceRef(raw string) (string, string) {
	ns := strings.TrimSpace(raw)
	ns = strings.TrimPrefix(ns, "v://")
	if idx := strings.Index(ns, "/"); idx >= 0 {
		ns = ns[:idx]
	}
	ns = strings.TrimSpace(ns)
	if ns == "" {
		return "", ""
	}
	return "v://" + ns, ns
}

func namespaceLikePattern(namespaceRef, kind string) string {
	namespaceRef = strings.TrimSpace(namespaceRef)
	if namespaceRef == "" {
		return "%"
	}
	if strings.TrimSpace(kind) == "" {
		return namespaceRef + "/%"
	}
	return namespaceRef + "/" + strings.TrimSpace(kind) + "/%"
}

func isEngineerQualType(qualType string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(qualType)), "REG_")
}

func deriveTenantRef(spuRefs []string, tenantID int) string {
	for _, ref := range spuRefs {
		ref = strings.TrimSpace(ref)
		if !strings.HasPrefix(ref, "v://") {
			continue
		}
		rest := strings.TrimPrefix(ref, "v://")
		idx := strings.Index(rest, "/")
		if idx <= 0 {
			continue
		}
		tenant := strings.TrimSpace(rest[:idx])
		if tenant == "" {
			continue
		}
		return "v://" + tenant
	}
	return fmt.Sprintf("v://%d", tenantID)
}
