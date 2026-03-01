package publicapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
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

type Store interface {
	CapabilityCounters(ctx context.Context, tenantID int) (map[string]int64, error)
	ListAchievements(ctx context.Context, tenantID int, f AchievementFilter) ([]*PublicAchievement, int, error)
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

	raw, err := os.ReadFile(s.catalogPath)
	if err != nil {
		return nil, fmt.Errorf("read spu catalog failed: %w", err)
	}
	var cat catalogFile
	if err := json.Unmarshal(raw, &cat); err != nil {
		return nil, fmt.Errorf("parse spu catalog failed: %w", err)
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
