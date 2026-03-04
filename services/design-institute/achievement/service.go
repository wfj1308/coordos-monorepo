package achievement

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	Payload     *json.RawMessage
	ProofHash   string
	Status      string
	Source      string
	TenantID    int
	IngestedAt  time.Time
	SettledAt   *time.Time
}

type Store interface {
	Create(ctx context.Context, a *AchievementUTXO) (int64, error)
	Get(ctx context.Context, id int64) (*AchievementUTXO, error)
	GetByUTXORef(ctx context.Context, utxoRef string) (*AchievementUTXO, error)
	GetByProofHash(ctx context.Context, proofHash string) (*AchievementUTXO, error)
	SetContract(ctx context.Context, id, contractID int64) error
	SetSettled(ctx context.Context, id int64, settledAt time.Time) error
	SettleByProject(ctx context.Context, projectRef string, settledAt time.Time) (int64, error)
	List(ctx context.Context, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByExecutor(ctx context.Context, executorRef string, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByExecutorWithTenants(ctx context.Context, executorRef string, tenantIDs []int, limit, offset int) ([]*AchievementUTXO, int, error)
	ListByProject(ctx context.Context, projectRef string) ([]*AchievementUTXO, error)
	ListByContract(ctx context.Context, contractID int64) ([]*AchievementUTXO, error)
	CountByExecutorAndPeriod(ctx context.Context, executorRef string, from, to time.Time) (int, float64, error)
	BindCredential(ctx context.Context, achievementID, credentialID int64, projectRef, executorRef string) error
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

func (s *Service) AutoSettleProject(ctx context.Context, projectRef string) (int64, error) {
	projectRef = strings.TrimSpace(projectRef)
	if projectRef == "" {
		return 0, fmt.Errorf("project_ref is required")
	}
	return s.store.SettleByProject(ctx, projectRef, time.Now().UTC())
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

func (s *PGStore) GetByProofHash(ctx context.Context, proofHash string) (*AchievementUTXO, error) {
	return s.scan(s.db.QueryRowContext(ctx,
		`SELECT id,utxo_ref,spu_ref,project_ref,executor_ref,genesis_ref,
		        contract_id,payload,proof_hash,status,source,tenant_id,ingested_at,settled_at
		 FROM achievement_utxos WHERE proof_hash=$1`, proofHash))
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

func (s *PGStore) SettleByProject(ctx context.Context, projectRef string, settledAt time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE achievement_utxos
		SET status='SETTLED', settled_at=$1
		WHERE project_ref=$2 AND status<>'SETTLED'
	`, settledAt, projectRef)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
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

func (s *PGStore) BindCredential(ctx context.Context, achievementID, credentialID int64, projectRef, executorRef string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO resource_bindings (
			resource_ref, resource_type, project_ref, executor_ref,
			achievement_utxo_id, credential_id, status, tenant_id, bound_at
		) VALUES ($1,$2,$3,$4,$5,$6,'ACTIVE',
			(SELECT tenant_id FROM achievement_utxos WHERE id=$5 LIMIT 1),
			NOW())
		ON CONFLICT DO NOTHING`,
		fmt.Sprintf("achievement:%d:credential:%d", achievementID, credentialID),
		"CREDENTIAL_BINDING",
		projectRef,
		executorRef,
		achievementID,
		credentialID,
	)
	return err
}

func (s *PGStore) scan(row *sql.Row) (*AchievementUTXO, error) {
	a := &AchievementUTXO{}
	var payload []byte
	err := row.Scan(&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
		&a.GenesisRef, &a.ContractID, &payload, &a.ProofHash,
		&a.Status, &a.Source, &a.TenantID, &a.IngestedAt, &a.SettledAt)
	if err != nil {
		return nil, err
	}
	if len(payload) > 0 {
		a.Payload = (*json.RawMessage)(&payload)
	}
	return a, nil
}

func (s *PGStore) scanRows(rows *sql.Rows) ([]*AchievementUTXO, error) {
	var list []*AchievementUTXO
	for rows.Next() {
		a := &AchievementUTXO{}
		var payload []byte
		if err := rows.Scan(&a.ID, &a.UTXORef, &a.SPURef, &a.ProjectRef, &a.ExecutorRef,
			&a.GenesisRef, &a.ContractID, &payload, &a.ProofHash,
			&a.Status, &a.Source, &a.TenantID, &a.IngestedAt, &a.SettledAt); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			a.Payload = (*json.RawMessage)(&payload)
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

func (s *Service) GetByProofHash(ctx context.Context, proofHash string) (*AchievementUTXO, error) {
	proofHash = strings.TrimSpace(proofHash)
	if proofHash == "" {
		return nil, sql.ErrNoRows
	}
	item, err := s.store.GetByProofHash(ctx, proofHash)
	if err == nil {
		return item, nil
	}
	if err != sql.ErrNoRows {
		return nil, err
	}
	if strings.HasPrefix(strings.ToLower(proofHash), "sha256:") {
		return s.store.GetByProofHash(ctx, strings.TrimPrefix(proofHash, "sha256:"))
	}
	return s.store.GetByProofHash(ctx, "sha256:"+proofHash)
}

func (s *Service) ValidateAchievement(ctx context.Context, utxo *AchievementUTXO, projectNode map[string]any) error {
	if utxo == nil {
		return fmt.Errorf("achievement utxo is nil")
	}
	if projectNode == nil {
		return fmt.Errorf("project node not found")
	}
	if utxo.ProjectRef == "" {
		return fmt.Errorf("achievement has no project_ref")
	}
	nodeRef, ok := projectNode["ref"].(string)
	if !ok || nodeRef == "" {
		return fmt.Errorf("project node has no ref")
	}
	if utxo.ProofHash == "" {
		return fmt.Errorf("achievement has no proof_hash")
	}
	var payload json.RawMessage
	if utxo.Payload != nil {
		payload = *utxo.Payload
	} else {
		payload = json.RawMessage("{}")
	}
	expected := ComputeProofHash(utxo.SPURef, utxo.ProjectRef, utxo.ExecutorRef, payload)
	if !hashEquals(utxo.ProofHash, expected) {
		return fmt.Errorf("proof_hash mismatch: expected %s (or sha256:%s), got %s", expected, expected, utxo.ProofHash)
	}
	return nil
}

func ComputeProofHash(spuRef, projectRef, executorRef string, payload json.RawMessage) string {
	payload = canonicalizeJSON(payload)
	h := sha256.New()
	h.Write([]byte(spuRef))
	h.Write([]byte(projectRef))
	h.Write([]byte(executorRef))
	h.Write(payload)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func ComputeProofHashWithNamespace(spuRef, projectRef, executorRef, namespace string, payload json.RawMessage) string {
	payload = canonicalizeJSON(payload)
	h := sha256.New()
	h.Write([]byte(spuRef))
	h.Write([]byte(projectRef))
	h.Write([]byte(executorRef))
	h.Write([]byte(namespace))
	h.Write(payload)
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
}

func (s *Service) CreateManual(ctx context.Context, in CreateManualInput) (*AchievementUTXO, error) {
	if in.ProjectRef == "" || in.ExecutorRef == "" {
		return nil, fmt.Errorf("project_ref and executor_ref are required")
	}
	if in.SPURef == "" {
		in.SPURef = "v://cn.zhongbei/spu/manual/document@v1"
	}
	if in.Payload == nil {
		in.Payload = json.RawMessage(`{}`)
	}
	in.Payload = canonicalizeJSON(in.Payload)
	namespace := extractNamespaceFromRef(in.ProjectRef)
	if namespace == "" {
		namespace = extractNamespaceFromRef(in.ExecutorRef)
	}
	if namespace == "" {
		namespace = "cn.zhongbei"
	}
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
		UTXORef:     fmt.Sprintf("v://%s/utxo/manual/%d", namespace, time.Now().UnixNano()),
		SPURef:      in.SPURef,
		ProjectRef:  in.ProjectRef,
		ExecutorRef: in.ExecutorRef,
		ContractID:  in.ContractID,
		Payload:     &in.Payload,
		ProofHash:   ComputeProofHashWithNamespace(in.SPURef, in.ProjectRef, in.ExecutorRef, namespace, in.Payload),
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

// BindCredential 绑定资质到业绩（用证留痕）
func (s *Service) BindCredential(ctx context.Context, achievementID, credentialID int64, projectRef, executorRef string) error {
	return s.store.BindCredential(ctx, achievementID, credentialID, projectRef, executorRef)
}

func extractNamespaceFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "v://") {
		return ""
	}
	without := strings.TrimPrefix(ref, "v://")
	if without == "" {
		return ""
	}
	idx := strings.IndexByte(without, '/')
	if idx <= 0 {
		return normalizeNamespaceSegment(strings.ToLower(strings.TrimSpace(without)))
	}
	return normalizeNamespaceSegment(strings.ToLower(strings.TrimSpace(without[:idx])))
}

func normalizeNamespaceSegment(segment string) string {
	switch strings.TrimSpace(strings.ToLower(segment)) {
	case "", "10000", "zhongbei":
		return "cn.zhongbei"
	default:
		return strings.TrimSpace(strings.ToLower(segment))
	}
}

func normalizeHash(raw string) string {
	v := strings.ToLower(strings.TrimSpace(raw))
	v = strings.TrimPrefix(v, "sha256:")
	return v
}

func hashEquals(actual string, candidates ...string) bool {
	got := normalizeHash(actual)
	if got == "" {
		return false
	}
	for _, c := range candidates {
		if got == normalizeHash(c) && normalizeHash(c) != "" {
			return true
		}
	}
	return false
}

func canonicalizeJSON(raw json.RawMessage) json.RawMessage {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return json.RawMessage(`{}`)
	}
	var v any
	if err := json.Unmarshal(trimmed, &v); err != nil {
		return json.RawMessage(trimmed)
	}
	normalized, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(trimmed)
	}
	return json.RawMessage(normalized)
}
