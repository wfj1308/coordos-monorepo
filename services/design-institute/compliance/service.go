package compliance

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ViolationRecord struct {
	ID          int64     `json:"id"`
	ExecutorRef string    `json:"executor_ref"`
	ProjectRef  string    `json:"project_ref"`
	RuleCode    string    `json:"rule_code"`
	Severity    string    `json:"severity"`
	Message     string    `json:"message"`
	OccurredAt  time.Time `json:"occurred_at"`
	TenantID    int       `json:"tenant_id"`
	CreatedAt   time.Time `json:"created_at"`
}

type ExecutorStats struct {
	ID              int64      `json:"id"`
	ExecutorRef     string     `json:"executor_ref"`
	TotalProjects   int        `json:"total_projects"`
	TotalUTXOs      int        `json:"total_utxos"`
	TotalViolations int        `json:"total_violations"`
	LastViolationAt *time.Time `json:"last_violation_at,omitempty"`
	Score           int        `json:"score"`
	CapabilityLevel string     `json:"capability_level"`
	TenantID        int        `json:"tenant_id"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

type ViolationFilter struct {
	ExecutorRef *string
	ProjectRef  *string
	RuleCode    *string
	Severity    *string
	Limit       int
	Offset      int
}

type CreateViolationInput struct {
	ExecutorRef string `json:"executor_ref"`
	ProjectRef  string `json:"project_ref"`
	RuleCode    string `json:"rule_code"`
	Severity    string `json:"severity"`
	Message     string `json:"message"`
	OccurredAt  string `json:"occurred_at"`
}

type Store interface {
	CreateViolation(ctx context.Context, item *ViolationRecord) error
	ListViolations(ctx context.Context, tenantID int, f ViolationFilter) ([]*ViolationRecord, int, error)
	GetExecutorStats(ctx context.Context, tenantID int, executorRef string) (*ExecutorStats, error)
	UpsertExecutorStats(ctx context.Context, s *ExecutorStats) error
	AggregateExecutor(ctx context.Context, tenantID int, executorRef string) (totalProjects int, totalUTXOs int, totalViolations int, lastViolationAt *time.Time, err error)
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) ReportViolation(ctx context.Context, in CreateViolationInput) (*ViolationRecord, error) {
	executorRef := strings.TrimSpace(in.ExecutorRef)
	projectRef := strings.TrimSpace(in.ProjectRef)
	ruleCode := strings.TrimSpace(in.RuleCode)
	severity := strings.ToUpper(strings.TrimSpace(in.Severity))
	if executorRef == "" || projectRef == "" || ruleCode == "" {
		return nil, fmt.Errorf("executor_ref, project_ref and rule_code are required")
	}
	if severity == "" {
		severity = "MEDIUM"
	}
	switch severity {
	case "LOW", "MEDIUM", "HIGH", "CRITICAL":
	default:
		return nil, fmt.Errorf("invalid severity")
	}

	occurredAt := time.Now().UTC()
	if strings.TrimSpace(in.OccurredAt) != "" {
		t, err := parseTime(in.OccurredAt)
		if err != nil {
			return nil, fmt.Errorf("invalid occurred_at")
		}
		occurredAt = t
	}

	item := &ViolationRecord{
		ExecutorRef: executorRef,
		ProjectRef:  projectRef,
		RuleCode:    ruleCode,
		Severity:    severity,
		Message:     strings.TrimSpace(in.Message),
		OccurredAt:  occurredAt,
		TenantID:    s.tenantID,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.store.CreateViolation(ctx, item); err != nil {
		return nil, err
	}
	if _, err := s.RefreshExecutorStats(ctx, executorRef); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) ListViolations(ctx context.Context, f ViolationFilter) ([]*ViolationRecord, int, error) {
	if f.Limit <= 0 {
		f.Limit = 20
	}
	if f.Offset < 0 {
		f.Offset = 0
	}
	return s.store.ListViolations(ctx, s.tenantID, f)
}

func (s *Service) GetExecutorStats(ctx context.Context, executorRef string) (*ExecutorStats, error) {
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	stats, err := s.store.GetExecutorStats(ctx, s.tenantID, executorRef)
	if err != nil {
		return nil, err
	}
	if stats != nil {
		return stats, nil
	}
	return s.RefreshExecutorStats(ctx, executorRef)
}

func (s *Service) RefreshExecutorStats(ctx context.Context, executorRef string) (*ExecutorStats, error) {
	executorRef = strings.TrimSpace(executorRef)
	if executorRef == "" {
		return nil, fmt.Errorf("executor_ref is required")
	}
	totalProjects, totalUTXOs, totalViolations, lastViolationAt, err := s.store.AggregateExecutor(ctx, s.tenantID, executorRef)
	if err != nil {
		return nil, err
	}

	score := 100 + minInt(totalUTXOs/10, 20) - totalViolations*8
	if score < 0 {
		score = 0
	}
	level := "RISK"
	switch {
	case score >= 120:
		level = "EXCELLENT"
	case score >= 90:
		level = "STABLE"
	case score >= 70:
		level = "WATCH"
	}

	now := time.Now().UTC()
	stats := &ExecutorStats{
		ExecutorRef:     executorRef,
		TotalProjects:   totalProjects,
		TotalUTXOs:      totalUTXOs,
		TotalViolations: totalViolations,
		LastViolationAt: lastViolationAt,
		Score:           score,
		CapabilityLevel: level,
		TenantID:        s.tenantID,
		UpdatedAt:       now,
	}
	if err := s.store.UpsertExecutorStats(ctx, stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func parseTime(raw string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02", raw)
}

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) CreateViolation(ctx context.Context, item *ViolationRecord) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO violation_records (
			executor_ref, project_ref, rule_code, severity, message,
			occurred_at, tenant_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id
	`,
		item.ExecutorRef,
		item.ProjectRef,
		item.RuleCode,
		item.Severity,
		item.Message,
		item.OccurredAt,
		item.TenantID,
		item.CreatedAt,
	).Scan(&item.ID)
}

func (s *PGStore) ListViolations(ctx context.Context, tenantID int, f ViolationFilter) ([]*ViolationRecord, int, error) {
	where := []string{"tenant_id=$1"}
	args := []any{tenantID}
	pos := 2
	if f.ExecutorRef != nil && strings.TrimSpace(*f.ExecutorRef) != "" {
		where = append(where, fmt.Sprintf("executor_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ExecutorRef))
		pos++
	}
	if f.ProjectRef != nil && strings.TrimSpace(*f.ProjectRef) != "" {
		where = append(where, fmt.Sprintf("project_ref=$%d", pos))
		args = append(args, strings.TrimSpace(*f.ProjectRef))
		pos++
	}
	if f.RuleCode != nil && strings.TrimSpace(*f.RuleCode) != "" {
		where = append(where, fmt.Sprintf("rule_code=$%d", pos))
		args = append(args, strings.TrimSpace(*f.RuleCode))
		pos++
	}
	if f.Severity != nil && strings.TrimSpace(*f.Severity) != "" {
		where = append(where, fmt.Sprintf("severity=$%d", pos))
		args = append(args, strings.ToUpper(strings.TrimSpace(*f.Severity)))
		pos++
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM violation_records WHERE "+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listSQL := fmt.Sprintf(`
		SELECT id, executor_ref, project_ref, rule_code, severity, message, occurred_at, tenant_id, created_at
		FROM violation_records
		WHERE %s
		ORDER BY occurred_at DESC, id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, pos, pos+1)
	args = append(args, f.Limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := make([]*ViolationRecord, 0)
	for rows.Next() {
		item := &ViolationRecord{}
		if err := rows.Scan(
			&item.ID,
			&item.ExecutorRef,
			&item.ProjectRef,
			&item.RuleCode,
			&item.Severity,
			&item.Message,
			&item.OccurredAt,
			&item.TenantID,
			&item.CreatedAt,
		); err != nil {
			return nil, 0, err
		}
		out = append(out, item)
	}
	return out, total, rows.Err()
}

func (s *PGStore) GetExecutorStats(ctx context.Context, tenantID int, executorRef string) (*ExecutorStats, error) {
	item := &ExecutorStats{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, executor_ref, total_projects, total_utxos, total_violations,
		       last_violation_at, score, capability_level, tenant_id, updated_at
		FROM executor_stats
		WHERE tenant_id=$1 AND executor_ref=$2
		LIMIT 1
	`, tenantID, executorRef).Scan(
		&item.ID,
		&item.ExecutorRef,
		&item.TotalProjects,
		&item.TotalUTXOs,
		&item.TotalViolations,
		&item.LastViolationAt,
		&item.Score,
		&item.CapabilityLevel,
		&item.TenantID,
		&item.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *PGStore) UpsertExecutorStats(ctx context.Context, item *ExecutorStats) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO executor_stats (
			executor_ref, total_projects, total_utxos, total_violations,
			last_violation_at, score, capability_level, tenant_id, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (tenant_id, executor_ref)
		DO UPDATE SET
			total_projects=EXCLUDED.total_projects,
			total_utxos=EXCLUDED.total_utxos,
			total_violations=EXCLUDED.total_violations,
			last_violation_at=EXCLUDED.last_violation_at,
			score=EXCLUDED.score,
			capability_level=EXCLUDED.capability_level,
			updated_at=EXCLUDED.updated_at
		RETURNING id
	`,
		item.ExecutorRef,
		item.TotalProjects,
		item.TotalUTXOs,
		item.TotalViolations,
		item.LastViolationAt,
		item.Score,
		item.CapabilityLevel,
		item.TenantID,
		item.UpdatedAt,
	).Scan(&item.ID)
}

func (s *PGStore) AggregateExecutor(ctx context.Context, tenantID int, executorRef string) (int, int, int, *time.Time, error) {
	var totalProjects int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
			SELECT DISTINCT ref
			FROM project_nodes
			WHERE tenant_id=$1 AND executor_ref=$2
		) t
	`, tenantID, executorRef).Scan(&totalProjects); err != nil {
		return 0, 0, 0, nil, err
	}

	var totalUTXOs int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM achievement_utxos
		WHERE tenant_id=$1 AND executor_ref=$2
	`, tenantID, executorRef).Scan(&totalUTXOs); err != nil {
		return 0, 0, 0, nil, err
	}

	var totalViolations int
	var lastViolation sql.NullTime
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(occurred_at)
		FROM violation_records
		WHERE tenant_id=$1 AND executor_ref=$2
	`, tenantID, executorRef).Scan(&totalViolations, &lastViolation); err != nil {
		return 0, 0, 0, nil, err
	}
	var last *time.Time
	if lastViolation.Valid {
		t := lastViolation.Time
		last = &t
	}
	return totalProjects, totalUTXOs, totalViolations, last, nil
}
