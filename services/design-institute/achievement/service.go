package achievement

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type AchievementUTXO struct {
	ID          int64
	UTXORef     string
	SPURef      string
	ProjectRef  string
	ExecutorRef string
	GenesisRef  *string
	ContractID  *int64
	Payload     json.RawMessage
	ProofHash   string
	Status      string // PENDING/SETTLED/DISPUTED/LEGACY
	Source      string // SPU_INGEST/LEGACY_IMPORT/MANUAL
	TenantID    int
	IngestedAt  time.Time
	SettledAt   *time.Time
}

type Store interface {
	Create(ctx context.Context, a *AchievementUTXO) (int64, error)
	Get(ctx context.Context, id int64) (*AchievementUTXO, error)
	GetByUTXORef(ctx context.Context, utxoRef string) (*AchievementUTXO, error)
	SetContract(ctx context.Context, id, contractID int64) error
	SetSettled(ctx context.Context, id int64, settledAt time.Time) error
	List(ctx context.Context, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByExecutorWithTenants(ctx context.Context, executorRef string, tenantIDs []int, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByProject(ctx context.Context, projectRef string) ([]*AchievementUTXO, error)
	ListByContract(ctx context.Context, contractID int64) ([]*AchievementUTXO, error)
	CountByExecutorAndPeriod(ctx context.Context, executorRef string, from, to time.Time) (int, float64, error)
}

type Service struct {
	store    Store
	tenantID int
	// rule002Checker is optional and injected by composition root.
	// When present, manual review-certificate UTXO creation enforces RULE-002.
	rule002Checker Rule002Checker
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

// Rule002Checker validates whether an executor can issue review certificates.
type Rule002Checker interface {
	CheckValidForRule002(ctx context.Context, executorRef string) (bool, error)
}

// SetRule002Checker injects RULE-002 validator from qualification service.
func (s *Service) SetRule002Checker(checker Rule002Checker) {
	s.rule002Checker = checker
}

func (s *Service) Get(ctx context.Context, id int64) (*AchievementUTXO, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error) {
	return s.store.ListByExecutor(ctx, executorRef, limit, offset)
}

func (s *Service) List(ctx context.Context, limit, offset int) ([]*AchievementUTXO, int, error) {
	return s.store.List(ctx, limit, offset)
}

func (s *Service) ListByExecutorAcrossTenants(
	ctx context.Context,
	executorRef string,
	tenantIDs []int,
	limit, offset int,
) ([]*AchievementUTXO, int, error) {
	if len(tenantIDs) == 0 {
		return s.store.ListByExecutor(ctx, executorRef, limit, offset)
	}
	return s.store.ListByExecutorWithTenants(ctx, executorRef, tenantIDs, limit, offset)
}

func (s *Service) ListByProject(ctx context.Context, projectRef string) ([]*AchievementUTXO, error) {
	return s.store.ListByProject(ctx, projectRef)
}

func (s *Service) ListByContract(ctx context.Context, contractID int64) ([]*AchievementUTXO, error) {
	return s.store.ListByContract(ctx, contractID)
}

// PGStore
type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, a *AchievementUTXO) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO achievement_utxos (
			utxo_ref, spu_ref, project_ref, executor_ref,
			genesis_ref, contract_id, payload, proof_hash,
			status, source, tenant_id, ingested_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id`,
		a.UTXORef, a.SPURef, a.ProjectRef, a.ExecutorRef,
		a.GenesisRef, a.ContractID, a.Payload, a.ProofHash,
		a.Status, a.Source, a.TenantID, a.IngestedAt,
	).Scan(&id)
	return id, err
}

func (s *PGStore) Get(ctx context.Context, id int64) (*AchievementUTXO, error) {
	return s.scan(s.db.QueryRowContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE id=$1`, id))
}

func (s *PGStore) GetByUTXORef(ctx context.Context, utxoRef string) (*AchievementUTXO, error) {
	return s.scan(s.db.QueryRowContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE utxo_ref=$1`, utxoRef))
}

func (s *PGStore) SetContract(ctx context.Context, id, contractID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE achievement_utxos SET contract_id=$1 WHERE id=$2`, contractID, id)
	return err
}

func (s *PGStore) SetSettled(ctx context.Context, id int64, settledAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE achievement_utxos SET status='SETTLED', settled_at=$1 WHERE id=$2`, settledAt, id)
	return err
}

func (s *PGStore) List(ctx context.Context, limit, offset int) ([]*AchievementUTXO, int, error) {
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM achievement_utxos`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos
		 ORDER BY ingested_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	list, err := s.scanRows(rows)
	return list, total, err
}

func (s *PGStore) ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error) {
	var total int
	s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM achievement_utxos WHERE executor_ref=$1`, executorRef).Scan(&total)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE executor_ref=$1
		 ORDER BY ingested_at DESC LIMIT $2 OFFSET $3`, executorRef, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	list, err := s.scanRows(rows)
	return list, total, err
}

func (s *PGStore) ListByExecutorWithTenants(
	ctx context.Context,
	executorRef string,
	tenantIDs []int,
	limit, offset int,
) ([]*AchievementUTXO, int, error) {
	if len(tenantIDs) == 0 {
		return s.ListByExecutor(ctx, executorRef, limit, offset)
	}

	placeholders := make([]string, 0, len(tenantIDs))
	args := make([]any, 0, len(tenantIDs)+3)
	args = append(args, executorRef)
	for i, tenantID := range tenantIDs {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+2))
		args = append(args, tenantID)
	}
	tenantClause := strings.Join(placeholders, ",")

	var total int
	countSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM achievement_utxos WHERE executor_ref=$1 AND tenant_id IN (%s)`,
		tenantClause,
	)
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	listSQL := fmt.Sprintf(`
		SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		       contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		FROM achievement_utxos
		WHERE executor_ref=$1 AND tenant_id IN (%s)
		ORDER BY ingested_at DESC LIMIT $%d OFFSET $%d`,
		tenantClause, len(args)+1, len(args)+2,
	)
	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx, listSQL, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	list, err := s.scanRows(rows)
	return list, total, err
}

func (s *PGStore) ListByProject(ctx context.Context, projectRef string) ([]*AchievementUTXO, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE project_ref=$1 ORDER BY ingested_at DESC`, projectRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanRows(rows)
}

func (s *PGStore) ListByContract(ctx context.Context, contractID int64) ([]*AchievementUTXO, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE contract_id=$1 ORDER BY ingested_at DESC`, contractID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanRows(rows)
}

func (s *PGStore) CountByExecutorAndPeriod(ctx context.Context, executorRef string, from, to time.Time) (int, float64, error) {
	var count int
	var amount float64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM((payload->>'amount')::float),0)
		FROM achievement_utxos
		WHERE executor_ref=$1 AND status='SETTLED'
		  AND ingested_at>=$2 AND ingested_at<$3`,
		executorRef, from, to,
	).Scan(&count, &amount)
	return count, amount, err
}

func (s *PGStore) scan(row *sql.Row) (*AchievementUTXO, error) {
	a := &AchievementUTXO{}
	err := row.Scan(&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
		&a.GenesisRef, &a.ContractID, &a.Payload, &a.ProofHash,
		&a.Status, &a.Source, &a.TenantID, &a.IngestedAt, &a.SettledAt)
	return a, err
}

func (s *PGStore) scanRows(rows *sql.Rows) ([]*AchievementUTXO, error) {
	var list []*AchievementUTXO
	for rows.Next() {
		a := &AchievementUTXO{}
		if err := rows.Scan(&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
			&a.GenesisRef, &a.ContractID, &a.Payload, &a.ProofHash,
			&a.Status, &a.Source, &a.TenantID, &a.IngestedAt, &a.SettledAt); err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	return list, rows.Err()
}

type CreateManualInput struct {
	SPURef      string          `json:"spu_ref"`
	ProjectRef  string          `json:"project_ref"`
	ExecutorRef string          `json:"executor_ref"`
	ContractID  *int64          `json:"contract_id"`
	Payload     json.RawMessage `json:"payload"`
	Note        string          `json:"note"`
}

func (s *Service) CreateManual(ctx context.Context, in CreateManualInput) (*AchievementUTXO, error) {
	if in.ProjectRef == "" || in.ExecutorRef == "" {
		return nil, fmt.Errorf("project_ref and executor_ref are required")
	}
	if in.SPURef == "" {
		in.SPURef = "v://zhongbei/spu/manual/document@v1"
	}
	if in.Payload == nil {
		in.Payload = json.RawMessage(`{}`)
	}
	// Manual review certificate creation must satisfy RULE-002.
	if s.rule002Checker != nil && strings.Contains(in.SPURef, "review_certificate") {
		ok, err := s.rule002Checker.CheckValidForRule002(ctx, in.ExecutorRef)
		if err != nil {
			return nil, fmt.Errorf("rule002 validation failed: %w", err)
		}
		if !ok {
			return nil, fmt.Errorf("RULE-002: executor %s has no valid review qualification", in.ExecutorRef)
		}
	}

	a := &AchievementUTXO{
		UTXORef:     fmt.Sprintf("v://zhongbei/utxo/manual/%d", time.Now().UnixNano()),
		SPURef:      in.SPURef,
		ProjectRef:  in.ProjectRef,
		ExecutorRef: in.ExecutorRef,
		ContractID:  in.ContractID,
		Payload:     in.Payload,
		ProofHash:   "",
		Status:      "PENDING",
		Source:      "MANUAL",
		TenantID:    s.tenantID,
		IngestedAt:  time.Now(),
	}

	id, err := s.store.Create(ctx, a)
	if err != nil {
		return nil, fmt.Errorf("create manual achievement failed: %w", err)
	}
	a.ID = id
	return a, nil
}
