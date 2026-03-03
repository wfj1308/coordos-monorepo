package resolve

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// ResolvedAchievement 匹配 resolved_achievements 视图的列
type ResolvedAchievement struct {
	ID            int64
	Ref           string
	UTXORef       string
	NamespaceRef  string
	ProjectRef    string
	ExecutorRef   string
	Payload       json.RawMessage
	ProofHash     string
	Status        string
	Source        string
	TenantID      int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	AnchorChain   sql.NullString
	AnchorTxHash  sql.NullString
	AnchorBlock   sql.NullInt64
	AnchoredAt    sql.NullTime
	AnchorStatus  sql.NullString
}

// ResolvedGenesisUTXO 匹配 resolved_genesis_utxos 视图的列
type ResolvedGenesisUTXO struct {
	ID             int64
	Ref            string
	ResourceType   string
	Name           string
	TotalAmount    int64
	AvailableAmount int64
	Unit           string
	ConstraintJSON json.RawMessage
	ProofHash      string
	Status         string
	TenantID       int
	CreatedAt      time.Time
	AnchorChain    sql.NullString
	AnchorTxHash   sql.NullString
	AnchorBlock    sql.NullInt64
	AnchoredAt     sql.NullTime
	AnchorStatus   sql.NullString
}

// ResolvedExecutor defines the data for a resolved executor, including their qualifications.
type ResolvedExecutor struct {
	Ref            string                  `json:"ref"`
	Name           string                  `json:"name"`
	CompanyRef     string                  `json:"company_ref"`
	Qualifications []ResolvedQualification `json:"qualifications"`
}

// ResolvedQualification is a simplified struct for an executor's qualification.
type ResolvedQualification struct {
	QualType   string    `json:"qual_type"`
	CertNo     string    `json:"cert_no"`
	Specialty  string    `json:"specialty"`
	ValidUntil time.Time `json:"valid_until"`
}

// Repository 处理解析服务的数据库操作
type Repository struct {
	db *sql.DB
}

// NewRepository 创建一个新的 Repository 实例
func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// FindAchievementByRef 通过 v:// ref 查询 resolved_achievements 视图
func (r *Repository) FindAchievementByRef(ctx context.Context, ref string) (*ResolvedAchievement, error) {
	query := `
		SELECT 
			id, ref, utxo_ref, namespace_ref, project_ref, executor_ref, payload,
			proof_hash, status, source, tenant_id, created_at, updated_at,
			anchor_chain, anchor_tx_hash, anchor_block, anchored_at, anchor_status
		FROM resolved_achievements
		WHERE ref = $1
		LIMIT 1;
	`
	row := r.db.QueryRowContext(ctx, query, ref)
	
	var item ResolvedAchievement
	err := row.Scan(
		&item.ID, &item.Ref, &item.UTXORef, &item.NamespaceRef, &item.ProjectRef, &item.ExecutorRef, &item.Payload,
		&item.ProofHash, &item.Status, &item.Source, &item.TenantID, &item.CreatedAt, &item.UpdatedAt,
		&item.AnchorChain, &item.AnchorTxHash, &item.AnchorBlock, &item.AnchoredAt, &item.AnchorStatus,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// FindExecutorByRef queries for an executor and their valid qualifications by their v:// ref.
func (r *Repository) FindExecutorByRef(ctx context.Context, ref string) (*ResolvedExecutor, error) {
	// First, get the basic employee info.
	empQuery := `SELECT name, company_ref FROM employees WHERE executor_ref = $1 LIMIT 1;`
	var executor ResolvedExecutor
	executor.Ref = ref
	if err := r.db.QueryRowContext(ctx, empQuery, ref).Scan(&executor.Name, &executor.CompanyRef); err != nil {
		return nil, err // Returns sql.ErrNoRows if not found
	}

	// Second, get all valid qualifications for that executor.
	qualQuery := `
		SELECT qual_type, cert_no, specialty, valid_until
		FROM qualifications
		WHERE executor_ref = $1 AND status = 'VALID' AND deleted = FALSE
		ORDER BY qual_type;
	`
	rows, err := r.db.QueryContext(ctx, qualQuery, ref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var quals []ResolvedQualification
	for rows.Next() {
		var q ResolvedQualification
		if err := rows.Scan(&q.QualType, &q.CertNo, &q.Specialty, &q.ValidUntil); err != nil {
			return nil, err
		}
		quals = append(quals, q)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	executor.Qualifications = quals
	return &executor, nil
}

// FindGenesisByRef 通过 v:// ref 查询 resolved_genesis_utxos 视图
func (r *Repository) FindGenesisByRef(ctx context.Context, ref string) (*ResolvedGenesisUTXO, error) {
	query := `
		SELECT
			id, ref, resource_type, name, total_amount, available_amount, unit,
			constraint_json, proof_hash, status, tenant_id, created_at,
			anchor_chain, anchor_tx_hash, anchor_block, anchored_at, anchor_status
		FROM resolved_genesis_utxos
		WHERE ref = $1
		LIMIT 1;
	`
	row := r.db.QueryRowContext(ctx, query, ref)

	var item ResolvedGenesisUTXO
	err := row.Scan(
		&item.ID, &item.Ref, &item.ResourceType, &item.Name, &item.TotalAmount, &item.AvailableAmount, &item.Unit,
		&item.ConstraintJSON, &item.ProofHash, &item.Status, &item.TenantID, &item.CreatedAt,
		&item.AnchorChain, &item.AnchorTxHash, &item.AnchorBlock, &item.AnchoredAt, &item.AnchorStatus,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}