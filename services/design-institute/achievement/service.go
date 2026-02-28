package achievement

import (
	"context"
	"database/sql"
	"encoding/json"
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
	ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByProject(ctx context.Context, projectRef string) ([]*AchievementUTXO, error)
	ListByContract(ctx context.Context, contractID int64) ([]*AchievementUTXO, error)
	CountByExecutorAndPeriod(ctx context.Context, executorRef string, from, to time.Time) (int, float64, error)
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Get(ctx context.Context, id int64) (*AchievementUTXO, error) {
	return s.store.Get(ctx, id)
}

func (s *Service) ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error) {
	return s.store.ListByExecutor(ctx, executorRef, limit, offset)
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
