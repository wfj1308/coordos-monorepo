package projectcore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

var pgSeq atomic.Uint64

// PGStore owns a PostgreSQL connection and exposes typed store adapters.
type PGStore struct {
	db *sql.DB
}

func OpenPG(dsn string) (*PGStore, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("empty postgres dsn")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	return NewPGStore(db)
}

func NewPGStore(db *sql.DB) (*PGStore, error) {
	if db == nil {
		return nil, errors.New("nil db")
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	s := &PGStore{db: db}
	if err := s.initSchema(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *PGStore) Close() error   { return s.db.Close() }
func (s *PGStore) SQLDB() *sql.DB { return s.db }

func (s *PGStore) ProjectTree() ProjectTreeStore { return &pgProjectTreeStore{db: s.db} }
func (s *PGStore) Genesis() GenesisUTXOStore     { return &pgGenesisStore{db: s.db} }
func (s *PGStore) GenesisFull() GenesisUTXOFullStore {
	return &pgGenesisStore{db: s.db}
}
func (s *PGStore) Contracts() ContractStore { return &pgContractStore{db: s.db} }
func (s *PGStore) Audit() AuditStore        { return &pgAuditStore{db: s.db} }

// UpsertContract is an operational helper for loading/updating contract records.
func (s *PGStore) UpsertContract(c *Contract) error {
	return (&pgContractStore{db: s.db}).UpsertContract(c)
}

func (s *PGStore) initSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS coordos_project_nodes (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			parent_ref TEXT NULL,
			status TEXT NOT NULL,
			owner_ref TEXT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			payload JSONB NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_coordos_project_parent ON coordos_project_nodes (tenant_id, parent_ref)`,
		`CREATE TABLE IF NOT EXISTS coordos_genesis_utxos (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			payload JSONB NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_coordos_genesis_project ON coordos_genesis_utxos (tenant_id, project_ref)`,
		`CREATE TABLE IF NOT EXISTS coordos_contracts (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			status TEXT NOT NULL,
			amount_with_tax BIGINT NOT NULL DEFAULT 0,
			paid_amount BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			payload JSONB NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_coordos_contract_project ON coordos_contracts (tenant_id, project_ref)`,
		`CREATE TABLE IF NOT EXISTS coordos_audit_events (
			event_id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			project_ref TEXT NULL,
			actor_ref TEXT NULL,
			verb TEXT NOT NULL,
			payload JSONB NOT NULL,
			proof_hash TEXT NULL,
			timestamp TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_coordos_audit_tenant_project ON coordos_audit_events (tenant_id, project_ref, timestamp DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("init project-core pg schema failed: %w", err)
		}
	}
	return nil
}

type pgProjectTreeStore struct{ db *sql.DB }

func (s *pgProjectTreeStore) GetNode(ref VRef) (*ProjectNode, error) {
	tenantID, err := tenantFromRef(ref)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = s.db.QueryRow(
		`SELECT payload FROM coordos_project_nodes WHERE tenant_id = $1 AND ref = $2`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("project node not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var node ProjectNode
	if err := json.Unmarshal(payload, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

func (s *pgProjectTreeStore) GetChildren(ref VRef) ([]*ProjectNode, error) {
	tenantID, err := tenantFromRef(ref)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(
		`SELECT payload
		 FROM coordos_project_nodes
		 WHERE tenant_id = $1 AND parent_ref = $2
		 ORDER BY created_at ASC`,
		tenantID, string(ref),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*ProjectNode, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var node ProjectNode
		if err := json.Unmarshal(payload, &node); err != nil {
			return nil, err
		}
		out = append(out, &node)
	}
	return out, rows.Err()
}

func (s *pgProjectTreeStore) GetAncestors(ref VRef) ([]*ProjectNode, error) {
	node, err := s.GetNode(ref)
	if err != nil {
		return nil, err
	}
	ancestors := make([]*ProjectNode, 0)
	for node.ParentRef != "" {
		parent, err := s.GetNode(node.ParentRef)
		if err != nil {
			return nil, err
		}
		ancestors = append(ancestors, parent)
		node = parent
	}
	return ancestors, nil
}

func (s *pgProjectTreeStore) CreateNode(node *ProjectNode) error {
	if node == nil {
		return errors.New("nil project node")
	}
	tenantID, err := chooseTenant(node.TenantID, node.Ref)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	node.UpdatedAt = now
	node.TenantID = tenantID
	payload, err := json.Marshal(node)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`INSERT INTO coordos_project_nodes
		 (tenant_id, ref, parent_ref, status, owner_ref, created_at, updated_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (tenant_id, ref) DO UPDATE SET
		   parent_ref = EXCLUDED.parent_ref,
		   status = EXCLUDED.status,
		   owner_ref = EXCLUDED.owner_ref,
		   updated_at = EXCLUDED.updated_at,
		   payload = EXCLUDED.payload`,
		tenantID, string(node.Ref), nullVRef(node.ParentRef), string(node.Status), nullVRef(node.OwnerRef),
		node.CreatedAt, node.UpdatedAt, payload,
	)
	return err
}

func (s *pgProjectTreeStore) UpdateStatus(ref VRef, status LifecycleStatus) error {
	node, err := s.GetNode(ref)
	if err != nil {
		return err
	}
	node.Status = status
	node.UpdatedAt = time.Now().UTC()
	return s.CreateNode(node)
}

func (s *pgProjectTreeStore) ValidateChildConstraint(child *ProjectNode) error {
	if child == nil || child.ParentRef == "" {
		return nil
	}
	parent, err := s.GetNode(child.ParentRef)
	if err != nil {
		return err
	}
	if child.Depth != 0 && child.Depth != parent.Depth+1 {
		return fmt.Errorf("invalid child depth")
	}
	if parent.Constraint.Energy.CapitalReserve > 0 &&
		child.Constraint.Energy.CapitalReserve > parent.Constraint.Energy.CapitalReserve {
		return fmt.Errorf("child capital reserve exceeds parent")
	}
	return nil
}

type pgGenesisStore struct{ db *sql.DB }

func (s *pgGenesisStore) Get(ref VRef) (*GenesisUTXO, error) {
	full, err := s.GetFull(ref)
	if err != nil {
		return nil, err
	}
	return fullToThinGenesis(full), nil
}

func (s *pgGenesisStore) Create(utxo *GenesisUTXO) error {
	if utxo == nil {
		return errors.New("nil genesis")
	}
	return s.CreateFull(thinToFullGenesis(utxo))
}

func (s *pgGenesisStore) ConsumeQuota(ref VRef, amount int64) (*GenesisUTXO, error) {
	if amount <= 0 {
		return nil, errors.New("amount must be positive")
	}
	tenantID, err := tenantFromRef(ref)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var payload []byte
	err = tx.QueryRowContext(
		ctx,
		`SELECT payload FROM coordos_genesis_utxos WHERE tenant_id = $1 AND ref = $2 FOR UPDATE`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("genesis not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}

	var g GenesisUTXOFull
	if err := json.Unmarshal(payload, &g); err != nil {
		return nil, err
	}
	if g.Status != GenesisActive {
		return nil, fmt.Errorf("genesis status is %s", g.Status)
	}
	if g.RemainingQuota() < amount {
		return nil, fmt.Errorf("quota not enough: remaining=%d requested=%d", g.RemainingQuota(), amount)
	}
	g.ConsumedQuota += amount
	if g.RemainingQuota() <= 0 {
		g.Status = GenesisExhausted
	}
	g.ProofHash = g.ComputeProofHash()
	gPayload, err := json.Marshal(g)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE coordos_genesis_utxos SET status = $1, updated_at = $2, payload = $3 WHERE tenant_id = $4 AND ref = $5`,
		string(g.Status), time.Now().UTC(), gPayload, tenantID, string(ref),
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return fullToThinGenesis(&g), nil
}

func (s *pgGenesisStore) GetRemainingQuota(ref VRef) (int64, error) {
	g, err := s.GetFull(ref)
	if err != nil {
		return 0, err
	}
	return g.RemainingQuota(), nil
}

func (s *pgGenesisStore) GetFull(ref VRef) (*GenesisUTXOFull, error) {
	tenantID, err := tenantFromRef(ref)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = s.db.QueryRow(
		`SELECT payload FROM coordos_genesis_utxos WHERE tenant_id = $1 AND ref = $2`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("genesis not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var g GenesisUTXOFull
	if err := json.Unmarshal(payload, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func (s *pgGenesisStore) CreateFull(utxo *GenesisUTXOFull) error {
	if utxo == nil {
		return errors.New("nil genesis full")
	}
	tenantID, err := chooseTenant(utxo.TenantID, utxo.Ref)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if utxo.CreatedAt.IsZero() {
		utxo.CreatedAt = now
	}
	if utxo.Status == "" {
		utxo.Status = GenesisActive
	}
	if utxo.ProofHash == "" {
		utxo.ProofHash = utxo.ComputeProofHash()
	}
	utxo.TenantID = tenantID
	payload, err := json.Marshal(utxo)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(
		`INSERT INTO coordos_genesis_utxos
		 (tenant_id, ref, project_ref, status, created_at, updated_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (tenant_id, ref) DO UPDATE SET
		   project_ref = EXCLUDED.project_ref,
		   status = EXCLUDED.status,
		   updated_at = EXCLUDED.updated_at,
		   payload = EXCLUDED.payload`,
		tenantID, string(utxo.Ref), string(utxo.ProjectRef), string(utxo.Status),
		utxo.CreatedAt, now, payload,
	)
	return err
}

func (s *pgGenesisStore) UpdateFull(utxo *GenesisUTXOFull) error {
	if utxo == nil {
		return errors.New("nil genesis full")
	}
	utxo.ProofHash = utxo.ComputeProofHash()
	return s.CreateFull(utxo)
}

type pgContractStore struct{ db *sql.DB }

func (s *pgContractStore) Get(contractRef VRef) (*Contract, error) {
	tenantID, err := tenantFromRef(contractRef)
	if err != nil {
		return nil, err
	}
	var payload []byte
	err = s.db.QueryRow(
		`SELECT payload FROM coordos_contracts WHERE tenant_id = $1 AND ref = $2`,
		tenantID, string(contractRef),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("contract not found: %s", contractRef)
	}
	if err != nil {
		return nil, err
	}
	var c Contract
	if err := json.Unmarshal(payload, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *pgContractStore) GetByProject(projectRef VRef) ([]*Contract, error) {
	tenantID, err := tenantFromRef(projectRef)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(
		`SELECT payload
		 FROM coordos_contracts
		 WHERE tenant_id = $1 AND project_ref = $2
		 ORDER BY created_at DESC`,
		tenantID, string(projectRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*Contract, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var c Contract
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, err
		}
		out = append(out, &c)
	}
	return out, rows.Err()
}

func (s *pgContractStore) GetRemainingAmount(ref VRef) (int64, error) {
	tenantID, err := tenantFromRef(ref)
	if err != nil {
		return 0, err
	}
	var amount, paid int64
	err = s.db.QueryRow(
		`SELECT amount_with_tax, paid_amount FROM coordos_contracts WHERE tenant_id = $1 AND ref = $2`,
		tenantID, string(ref),
	).Scan(&amount, &paid)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("contract not found: %s", ref)
	}
	if err != nil {
		return 0, err
	}
	if amount-paid < 0 {
		return 0, nil
	}
	return amount - paid, nil
}

func (s *pgContractStore) ValidatePayment(contractRef VRef, amount int64) error {
	if amount <= 0 {
		return errors.New("payment amount must be positive")
	}
	remaining, err := s.GetRemainingAmount(contractRef)
	if err != nil {
		return err
	}
	if amount > remaining {
		return fmt.Errorf("amount exceeds remaining contract balance")
	}
	return nil
}

func (s *pgContractStore) UpsertContract(c *Contract) error {
	if c == nil {
		return errors.New("nil contract")
	}
	tenantID, err := chooseTenant(c.TenantID, c.Ref)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now
	}
	c.TenantID = tenantID
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO coordos_contracts
		 (tenant_id, ref, project_ref, status, amount_with_tax, paid_amount, created_at, updated_at, payload)
		 VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8)
		 ON CONFLICT (tenant_id, ref) DO UPDATE SET
		   project_ref = EXCLUDED.project_ref,
		   status = EXCLUDED.status,
		   amount_with_tax = EXCLUDED.amount_with_tax,
		   updated_at = EXCLUDED.updated_at,
		   payload = EXCLUDED.payload`,
		tenantID, string(c.Ref), string(c.ProjectRef), c.Status, c.AmountWithTax, c.CreatedAt, now, payload,
	)
	return err
}

type pgAuditStore struct{ db *sql.DB }

func (s *pgAuditStore) RecordEvent(evt ProjectEvent, tenantID string) (string, error) {
	tid, err := chooseTenantWithFallback(tenantID, evt.TenantID, evt.ProjectRef)
	if err != nil {
		return "", err
	}
	if evt.EventID == "" {
		evt.EventID = nextID("audit")
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	payload := evt.Payload
	if payload == nil {
		payload = map[string]interface{}{}
	}
	jb, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	_, err = s.db.Exec(
		`INSERT INTO coordos_audit_events
		 (event_id, tenant_id, project_ref, actor_ref, verb, payload, proof_hash, timestamp)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (event_id) DO UPDATE SET
		   tenant_id = EXCLUDED.tenant_id,
		   project_ref = EXCLUDED.project_ref,
		   actor_ref = EXCLUDED.actor_ref,
		   verb = EXCLUDED.verb,
		   payload = EXCLUDED.payload,
		   proof_hash = EXCLUDED.proof_hash,
		   timestamp = EXCLUDED.timestamp`,
		evt.EventID, tid, nullVRef(evt.ProjectRef), nullVRef(evt.ActorRef), string(evt.Verb), jb, nullString(evt.Signature), evt.Timestamp,
	)
	if err != nil {
		return "", err
	}
	return evt.EventID, nil
}

func (s *pgAuditStore) RecordViolation(rule string, evt ProjectEvent, detail string) (string, error) {
	if evt.Payload == nil {
		evt.Payload = map[string]interface{}{}
	}
	evt.Payload["rule"] = rule
	evt.Payload["detail"] = detail
	evt.Verb = ProjectVerb("VIOLATION")
	if evt.EventID == "" {
		evt.EventID = nextID("violation")
	}
	return s.RecordEvent(evt, evt.TenantID)
}

func chooseTenant(explicit string, ref VRef) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return strings.TrimSpace(explicit), nil
	}
	return tenantFromRef(ref)
}

func chooseTenantWithFallback(primary, secondary string, ref VRef) (string, error) {
	if strings.TrimSpace(primary) != "" {
		return strings.TrimSpace(primary), nil
	}
	if strings.TrimSpace(secondary) != "" {
		return strings.TrimSpace(secondary), nil
	}
	return tenantFromRef(ref)
}

func tenantFromRef(ref VRef) (string, error) {
	s := strings.TrimSpace(string(ref))
	if s == "" {
		return "", errors.New("empty ref")
	}
	if !strings.HasPrefix(s, "v://") {
		return "", fmt.Errorf("invalid ref format: %s", ref)
	}
	rest := strings.TrimPrefix(s, "v://")
	idx := strings.IndexByte(rest, '/')
	if idx <= 0 {
		return "", fmt.Errorf("invalid ref format: %s", ref)
	}
	tenant := strings.TrimSpace(rest[:idx])
	if tenant == "" {
		return "", fmt.Errorf("invalid ref format: %s", ref)
	}
	return tenant, nil
}

func thinToFullGenesis(g *GenesisUTXO) *GenesisUTXOFull {
	status := GenesisUTXOStatus(g.Status)
	if status == "" {
		status = GenesisActive
	}
	return &GenesisUTXOFull{
		Ref:             g.Ref,
		ProjectRef:      g.ProjectRef,
		ParentRef:       g.ParentRef,
		TenantID:        g.TenantID,
		TotalQuota:      g.TotalQuota,
		ConsumedQuota:   g.ConsumedQuota,
		AllocatedQuota:  0,
		FrozenQuota:     0,
		UnitPrice:       g.UnitPrice,
		PriceTolerance:  g.PriceTolerance,
		QualityStandard: g.QualityStandard,
		PaymentNodes:    g.PaymentNodes,
		Status:          status,
		CreatedAt:       g.CreatedAt,
		ProofHash:       g.ProofHash,
	}
}

func fullToThinGenesis(g *GenesisUTXOFull) *GenesisUTXO {
	if g == nil {
		return nil
	}
	return &GenesisUTXO{
		Ref:             g.Ref,
		ProjectRef:      g.ProjectRef,
		ParentRef:       g.ParentRef,
		TenantID:        g.TenantID,
		TotalQuota:      g.TotalQuota,
		ConsumedQuota:   g.ConsumedQuota,
		UnitPrice:       g.UnitPrice,
		PriceTolerance:  g.PriceTolerance,
		QualityStandard: g.QualityStandard,
		PaymentNodes:    g.PaymentNodes,
		Status:          string(g.Status),
		CreatedAt:       g.CreatedAt,
		ProofHash:       g.ProofHash,
	}
}

func nullVRef(v VRef) any {
	if v == "" {
		return nil
	}
	return string(v)
}

func nullString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), pgSeq.Add(1))
}
