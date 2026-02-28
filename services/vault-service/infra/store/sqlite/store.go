package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"

	_ "modernc.org/sqlite"
)

// DB owns one sqlite connection pool and exposes typed store implementations.
type DB struct {
	db *sql.DB
}

var seq atomic.Uint64

func Open(path string) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		path = "file:coordos_vault.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	return NewWithDB(db)
}

func NewWithDB(db *sql.DB) (*DB, error) {
	if db == nil {
		return nil, errors.New("nil db")
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	d := &DB{db: db}
	if err := d.initSchema(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DB) Close() error   { return d.db.Close() }
func (d *DB) SQLDB() *sql.DB { return d.db }

func (d *DB) ProjectTree() store.ProjectTreeStore { return &projectTreeStore{db: d.db} }
func (d *DB) Genesis() store.GenesisStore         { return &genesisStore{db: d.db} }
func (d *DB) Contracts() store.ContractStore      { return &contractStore{db: d.db} }
func (d *DB) Parcels() store.ParcelStore          { return &parcelStore{db: d.db} }
func (d *DB) UTXOs() store.UTXOStore              { return &utxoStore{db: d.db} }
func (d *DB) Settlements() store.SettlementStore  { return &settlementStore{db: d.db} }
func (d *DB) Wallets() store.WalletStore          { return &walletStore{db: d.db} }
func (d *DB) Audit() store.AuditStore             { return &auditStore{db: d.db} }

func (d *DB) initSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS vault_project_nodes (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			parent_ref TEXT,
			status TEXT,
			owner_ref TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_project_parent ON vault_project_nodes(tenant_id, parent_ref)`,
		`CREATE TABLE IF NOT EXISTS vault_genesis (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_vault_genesis_project ON vault_genesis(tenant_id, project_ref)`,
		`CREATE TABLE IF NOT EXISTS vault_contracts (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			branch_ref TEXT,
			status TEXT,
			kind TEXT,
			amount_with_tax INTEGER NOT NULL DEFAULT 0,
			paid_amount INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE TABLE IF NOT EXISTS vault_parcels (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			contract_ref TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE TABLE IF NOT EXISTS vault_utxos (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			parcel_ref TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE TABLE IF NOT EXISTS vault_settlements (
			tenant_id TEXT NOT NULL,
			ref TEXT NOT NULL,
			project_ref TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, ref)
		)`,
		`CREATE TABLE IF NOT EXISTS vault_wallets (
			tenant_id TEXT NOT NULL,
			owner_ref TEXT NOT NULL,
			balance INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant_id, owner_ref)
		)`,
		`CREATE TABLE IF NOT EXISTS vault_wallet_ledger (
			id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			owner_ref TEXT NOT NULL,
			amount INTEGER NOT NULL,
			note TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS vault_audit_events (
			event_id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			project_ref TEXT,
			actor_ref TEXT,
			verb TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			proof_hash TEXT,
			payload TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := d.db.Exec(stmt); err != nil {
			return fmt.Errorf("init sqlite schema: %w", err)
		}
	}
	return nil
}

func toJSON(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseTS(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, v)
}

func ts(t time.Time) string {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC().Format(time.RFC3339Nano)
}

type projectTreeStore struct{ db *sql.DB }

func (s *projectTreeStore) CreateNode(tenantID string, node *pc.ProjectNode) error {
	if node == nil {
		return errors.New("nil project node")
	}
	now := time.Now().UTC()
	if node.TenantID == "" {
		node.TenantID = tenantID
	}
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	if node.UpdatedAt.IsZero() {
		node.UpdatedAt = now
	}
	payload, err := toJSON(node)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_project_nodes (tenant_id, ref, parent_ref, status, owner_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		tenantID, string(node.Ref), nullRef(node.ParentRef), string(node.Status), nullRef(node.OwnerRef),
		ts(node.CreatedAt), ts(node.UpdatedAt), payload,
	)
	return err
}

func (s *projectTreeStore) GetNode(tenantID string, ref pc.VRef) (*pc.ProjectNode, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_project_nodes WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("project node not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var node pc.ProjectNode
	if err := json.Unmarshal([]byte(payload), &node); err != nil {
		return nil, err
	}
	return &node, nil
}

func (s *projectTreeStore) GetChildren(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_project_nodes
		 WHERE tenant_id = ? AND parent_ref = ?
		 ORDER BY created_at, ref`,
		tenantID, string(ref),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*pc.ProjectNode, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var node pc.ProjectNode
		if err := json.Unmarshal([]byte(payload), &node); err != nil {
			return nil, err
		}
		out = append(out, &node)
	}
	return out, rows.Err()
}

func (s *projectTreeStore) GetAncestors(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error) {
	node, err := s.GetNode(tenantID, ref)
	if err != nil {
		return nil, err
	}
	ancestors := make([]*pc.ProjectNode, 0)
	for node.ParentRef != "" {
		parent, err := s.GetNode(tenantID, node.ParentRef)
		if err != nil {
			return nil, err
		}
		ancestors = append(ancestors, parent)
		node = parent
	}
	return ancestors, nil
}

func (s *projectTreeStore) UpdateStatus(tenantID string, ref pc.VRef, status pc.LifecycleStatus) error {
	node, err := s.GetNode(tenantID, ref)
	if err != nil {
		return err
	}
	node.Status = status
	node.UpdatedAt = time.Now().UTC()
	return s.UpdateNode(tenantID, node)
}

func (s *projectTreeStore) UpdateNode(tenantID string, node *pc.ProjectNode) error {
	if node == nil {
		return errors.New("nil project node")
	}
	if node.TenantID == "" {
		node.TenantID = tenantID
	}
	if node.CreatedAt.IsZero() {
		node.CreatedAt = time.Now().UTC()
	}
	node.UpdatedAt = time.Now().UTC()
	payload, err := toJSON(node)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_project_nodes (tenant_id, ref, parent_ref, status, owner_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   parent_ref = excluded.parent_ref,
		   status = excluded.status,
		   owner_ref = excluded.owner_ref,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(node.Ref), nullRef(node.ParentRef), string(node.Status), nullRef(node.OwnerRef),
		ts(node.CreatedAt), ts(node.UpdatedAt), payload,
	)
	return err
}

func (s *projectTreeStore) ValidateChildConstraint(tenantID string, child *pc.ProjectNode) error {
	if child == nil || child.ParentRef == "" {
		return nil
	}
	parent, err := s.GetNode(tenantID, child.ParentRef)
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

func (s *projectTreeStore) ListByTenant(tenantID string, filter store.ProjectFilter) ([]*pc.ProjectNode, int64, error) {
	where := []string{"tenant_id = ?"}
	args := []any{tenantID}
	if filter.Status != nil {
		where = append(where, "status = ?")
		args = append(args, string(*filter.Status))
	}
	if filter.OwnerRef != nil {
		where = append(where, "owner_ref = ?")
		args = append(args, string(*filter.OwnerRef))
	}
	cond := strings.Join(where, " AND ")

	var total int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM vault_project_nodes WHERE `+cond, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	rows, err := s.db.Query(
		`SELECT payload FROM vault_project_nodes WHERE `+cond+` ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		args...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]*pc.ProjectNode, 0, limit)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, 0, err
		}
		var node pc.ProjectNode
		if err := json.Unmarshal([]byte(payload), &node); err != nil {
			return nil, 0, err
		}
		items = append(items, &node)
	}
	return items, total, rows.Err()
}

type genesisStore struct{ db *sql.DB }

func (s *genesisStore) CreateFull(tenantID string, g *pc.GenesisUTXOFull) error {
	if g == nil {
		return errors.New("nil genesis")
	}
	now := time.Now().UTC()
	if g.TenantID == "" {
		g.TenantID = tenantID
	}
	if g.CreatedAt.IsZero() {
		g.CreatedAt = now
	}
	if g.Status == "" {
		g.Status = pc.GenesisActive
	}
	if g.ProofHash == "" {
		g.ProofHash = g.ComputeProofHash()
	}
	payload, err := toJSON(g)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_genesis (tenant_id, ref, project_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   project_ref = excluded.project_ref,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(g.Ref), string(g.ProjectRef), ts(g.CreatedAt), ts(now), payload,
	)
	return err
}

func (s *genesisStore) GetFull(tenantID string, ref pc.VRef) (*pc.GenesisUTXOFull, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_genesis WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("genesis not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var g pc.GenesisUTXOFull
	if err := json.Unmarshal([]byte(payload), &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func (s *genesisStore) UpdateFull(tenantID string, g *pc.GenesisUTXOFull) error {
	if g == nil {
		return errors.New("nil genesis")
	}
	g.ProofHash = g.ComputeProofHash()
	payload, err := toJSON(g)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`UPDATE vault_genesis SET project_ref = ?, updated_at = ?, payload = ?
		 WHERE tenant_id = ? AND ref = ?`,
		string(g.ProjectRef), ts(time.Now().UTC()), payload, tenantID, string(g.Ref),
	)
	return err
}

func (s *genesisStore) GetRemainingQuota(tenantID string, ref pc.VRef) (int64, error) {
	g, err := s.GetFull(tenantID, ref)
	if err != nil {
		return 0, err
	}
	return g.RemainingQuota(), nil
}

func (s *genesisStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*pc.GenesisUTXOFull, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_genesis WHERE tenant_id = ? AND project_ref = ? ORDER BY created_at DESC`,
		tenantID, string(projectRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*pc.GenesisUTXOFull, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var g pc.GenesisUTXOFull
		if err := json.Unmarshal([]byte(payload), &g); err != nil {
			return nil, err
		}
		items = append(items, &g)
	}
	return items, rows.Err()
}

func nullRef(v pc.VRef) any {
	if v == "" {
		return nil
	}
	return string(v)
}

type contractStore struct{ db *sql.DB }

func (s *contractStore) Create(tenantID string, c *store.Contract) error {
	if c == nil {
		return errors.New("nil contract")
	}
	now := time.Now().UTC()
	if c.TenantID == "" {
		c.TenantID = tenantID
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now
	}
	payload, err := toJSON(c)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_contracts (
		   tenant_id, ref, project_ref, branch_ref, status, kind, amount_with_tax, paid_amount, created_at, updated_at, payload
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   project_ref = excluded.project_ref,
		   branch_ref = excluded.branch_ref,
		   status = excluded.status,
		   kind = excluded.kind,
		   amount_with_tax = excluded.amount_with_tax,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(c.Ref), string(c.ProjectRef), nullRef(c.BranchRef), c.Status, c.ContractKind, c.AmountWithTax,
		ts(c.CreatedAt), ts(now), payload,
	)
	return err
}

func (s *contractStore) Get(tenantID string, ref pc.VRef) (*store.Contract, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_contracts WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("contract not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var c store.Contract
	if err := json.Unmarshal([]byte(payload), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *contractStore) Update(tenantID string, c *store.Contract) error {
	return s.Create(tenantID, c)
}

func (s *contractStore) List(tenantID string, f store.ContractFilter) ([]*store.Contract, int64, error) {
	where := []string{"tenant_id = ?"}
	args := []any{tenantID}
	if f.ProjectRef != nil {
		where = append(where, "project_ref = ?")
		args = append(args, string(*f.ProjectRef))
	}
	if f.BranchRef != nil {
		where = append(where, "branch_ref = ?")
		args = append(args, string(*f.BranchRef))
	}
	if f.Status != nil {
		where = append(where, "status = ?")
		args = append(args, *f.Status)
	}
	if f.Kind != nil {
		where = append(where, "kind = ?")
		args = append(args, *f.Kind)
	}
	cond := strings.Join(where, " AND ")
	var total int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM vault_contracts WHERE `+cond, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	rows, err := s.db.Query(
		`SELECT payload FROM vault_contracts WHERE `+cond+` ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		args...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]*store.Contract, 0, limit)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, 0, err
		}
		var c store.Contract
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			return nil, 0, err
		}
		items = append(items, &c)
	}
	return items, total, rows.Err()
}

func (s *contractStore) GetRemainingAmount(tenantID string, ref pc.VRef) (int64, error) {
	var amount, paid int64
	err := s.db.QueryRow(
		`SELECT amount_with_tax, paid_amount FROM vault_contracts WHERE tenant_id = ? AND ref = ?`,
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

func (s *contractStore) ValidatePayment(tenantID string, ref pc.VRef, amount int64) error {
	if amount <= 0 {
		return errors.New("payment amount must be positive")
	}
	remaining, err := s.GetRemainingAmount(tenantID, ref)
	if err != nil {
		return err
	}
	if amount > remaining {
		return fmt.Errorf("amount exceeds remaining contract balance")
	}
	return nil
}

type parcelStore struct{ db *sql.DB }

func (s *parcelStore) Create(tenantID string, p *store.Parcel) error {
	if p == nil {
		return errors.New("nil parcel")
	}
	now := time.Now().UTC()
	if p.TenantID == "" {
		p.TenantID = tenantID
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = now
	}
	payload, err := toJSON(p)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_parcels (tenant_id, ref, project_ref, contract_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   project_ref = excluded.project_ref,
		   contract_ref = excluded.contract_ref,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(p.Ref), string(p.ProjectRef), nullRef(p.ContractRef), ts(p.CreatedAt), ts(now), payload,
	)
	return err
}

func (s *parcelStore) Get(tenantID string, ref pc.VRef) (*store.Parcel, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_parcels WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("parcel not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var p store.Parcel
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *parcelStore) Update(tenantID string, p *store.Parcel) error { return s.Create(tenantID, p) }

func (s *parcelStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.Parcel, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_parcels WHERE tenant_id = ? AND project_ref = ? ORDER BY created_at DESC`,
		tenantID, string(projectRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*store.Parcel, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var p store.Parcel
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		items = append(items, &p)
	}
	return items, rows.Err()
}

func (s *parcelStore) ListByContract(tenantID string, contractRef pc.VRef) ([]*store.Parcel, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_parcels WHERE tenant_id = ? AND contract_ref = ? ORDER BY created_at DESC`,
		tenantID, string(contractRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*store.Parcel, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var p store.Parcel
		if err := json.Unmarshal([]byte(payload), &p); err != nil {
			return nil, err
		}
		items = append(items, &p)
	}
	return items, rows.Err()
}

type utxoStore struct{ db *sql.DB }

func (s *utxoStore) Create(tenantID string, u *store.UTXO) error {
	if u == nil {
		return errors.New("nil utxo")
	}
	now := time.Now().UTC()
	if u.TenantID == "" {
		u.TenantID = tenantID
	}
	if u.CreatedAt.IsZero() {
		u.CreatedAt = now
	}
	payload, err := toJSON(u)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_utxos (tenant_id, ref, project_ref, parcel_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   project_ref = excluded.project_ref,
		   parcel_ref = excluded.parcel_ref,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(u.Ref), string(u.ProjectRef), nullRef(u.ParcelRef), ts(u.CreatedAt), ts(now), payload,
	)
	return err
}

func (s *utxoStore) Get(tenantID string, ref pc.VRef) (*store.UTXO, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_utxos WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("utxo not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var u store.UTXO
	if err := json.Unmarshal([]byte(payload), &u); err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *utxoStore) Update(tenantID string, u *store.UTXO) error { return s.Create(tenantID, u) }

func (s *utxoStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.UTXO, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_utxos WHERE tenant_id = ? AND project_ref = ? ORDER BY created_at DESC`,
		tenantID, string(projectRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*store.UTXO, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var u store.UTXO
		if err := json.Unmarshal([]byte(payload), &u); err != nil {
			return nil, err
		}
		items = append(items, &u)
	}
	return items, rows.Err()
}

func (s *utxoStore) ListByParcel(tenantID string, parcelRef pc.VRef) ([]*store.UTXO, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_utxos WHERE tenant_id = ? AND parcel_ref = ? ORDER BY created_at DESC`,
		tenantID, string(parcelRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*store.UTXO, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var u store.UTXO
		if err := json.Unmarshal([]byte(payload), &u); err != nil {
			return nil, err
		}
		items = append(items, &u)
	}
	return items, rows.Err()
}

type settlementStore struct{ db *sql.DB }

func (s *settlementStore) Create(tenantID string, se *store.Settlement) error {
	if se == nil {
		return errors.New("nil settlement")
	}
	now := time.Now().UTC()
	if se.TenantID == "" {
		se.TenantID = tenantID
	}
	if se.CreatedAt.IsZero() {
		se.CreatedAt = now
	}
	payload, err := toJSON(se)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_settlements (tenant_id, ref, project_ref, created_at, updated_at, payload)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(tenant_id, ref) DO UPDATE SET
		   project_ref = excluded.project_ref,
		   updated_at = excluded.updated_at,
		   payload = excluded.payload`,
		tenantID, string(se.Ref), string(se.ProjectRef), ts(se.CreatedAt), ts(now), payload,
	)
	return err
}

func (s *settlementStore) Get(tenantID string, ref pc.VRef) (*store.Settlement, error) {
	var payload string
	err := s.db.QueryRow(
		`SELECT payload FROM vault_settlements WHERE tenant_id = ? AND ref = ?`,
		tenantID, string(ref),
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("settlement not found: %s", ref)
	}
	if err != nil {
		return nil, err
	}
	var se store.Settlement
	if err := json.Unmarshal([]byte(payload), &se); err != nil {
		return nil, err
	}
	return &se, nil
}

func (s *settlementStore) Update(tenantID string, se *store.Settlement) error {
	return s.Create(tenantID, se)
}

func (s *settlementStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.Settlement, error) {
	rows, err := s.db.Query(
		`SELECT payload FROM vault_settlements WHERE tenant_id = ? AND project_ref = ? ORDER BY created_at DESC`,
		tenantID, string(projectRef),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*store.Settlement, 0)
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var se store.Settlement
		if err := json.Unmarshal([]byte(payload), &se); err != nil {
			return nil, err
		}
		items = append(items, &se)
	}
	return items, rows.Err()
}

type walletStore struct{ db *sql.DB }

func (s *walletStore) GetOrCreate(tenantID string, ownerRef pc.VRef) (*store.Wallet, error) {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	w, err := s.getOrCreateTx(tx, tenantID, ownerRef)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return w, nil
}

func (s *walletStore) Credit(tenantID string, ownerRef pc.VRef, amount int64, note string) error {
	return s.applyDelta(tenantID, ownerRef, amount, note)
}

func (s *walletStore) Debit(tenantID string, ownerRef pc.VRef, amount int64, note string) error {
	return s.applyDelta(tenantID, ownerRef, -amount, note)
}

func (s *walletStore) GetBalance(tenantID string, ownerRef pc.VRef) (int64, error) {
	var balance int64
	err := s.db.QueryRow(
		`SELECT balance FROM vault_wallets WHERE tenant_id = ? AND owner_ref = ?`,
		tenantID, string(ownerRef),
	).Scan(&balance)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return balance, err
}

func (s *walletStore) ListLedger(tenantID string, ownerRef pc.VRef, limit int) ([]*store.LedgerEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT id, amount, note, created_at
		 FROM vault_wallet_ledger
		 WHERE tenant_id = ? AND owner_ref = ?
		 ORDER BY created_at DESC
		 LIMIT ?`,
		tenantID, string(ownerRef), limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*store.LedgerEntry, 0, limit)
	for rows.Next() {
		var id, note, createdAt string
		var amount int64
		if err := rows.Scan(&id, &amount, &note, &createdAt); err != nil {
			return nil, err
		}
		t, err := parseTS(createdAt)
		if err != nil {
			return nil, err
		}
		out = append(out, &store.LedgerEntry{
			ID:        id,
			OwnerRef:  ownerRef,
			Amount:    amount,
			Note:      note,
			CreatedAt: t,
		})
	}
	return out, rows.Err()
}

func (s *walletStore) getOrCreateTx(tx *sql.Tx, tenantID string, ownerRef pc.VRef) (*store.Wallet, error) {
	var payload string
	err := tx.QueryRow(
		`SELECT payload FROM vault_wallets WHERE tenant_id = ? AND owner_ref = ?`,
		tenantID, string(ownerRef),
	).Scan(&payload)
	if err == nil {
		var w store.Wallet
		if err := json.Unmarshal([]byte(payload), &w); err != nil {
			return nil, err
		}
		return &w, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	now := time.Now().UTC()
	w := &store.Wallet{OwnerRef: ownerRef, Balance: 0, TenantID: tenantID, UpdatedAt: now}
	payload, err = toJSON(w)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(
		`INSERT INTO vault_wallets (tenant_id, owner_ref, balance, updated_at, payload) VALUES (?, ?, 0, ?, ?)`,
		tenantID, string(ownerRef), ts(now), payload,
	); err != nil {
		return nil, err
	}
	return w, nil
}

func (s *walletStore) applyDelta(tenantID string, ownerRef pc.VRef, delta int64, note string) error {
	if delta == 0 {
		return errors.New("amount must not be zero")
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	w, err := s.getOrCreateTx(tx, tenantID, ownerRef)
	if err != nil {
		return err
	}
	if delta < 0 && w.Balance < -delta {
		return fmt.Errorf("insufficient balance")
	}
	w.Balance += delta
	w.UpdatedAt = time.Now().UTC()
	payload, err := toJSON(w)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(
		`UPDATE vault_wallets SET balance = ?, updated_at = ?, payload = ? WHERE tenant_id = ? AND owner_ref = ?`,
		w.Balance, ts(w.UpdatedAt), payload, tenantID, string(ownerRef),
	); err != nil {
		return err
	}
	if strings.TrimSpace(note) == "" {
		note = "n/a"
	}
	if _, err := tx.Exec(
		`INSERT INTO vault_wallet_ledger (id, tenant_id, owner_ref, amount, note, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		nextID("ledger"), tenantID, string(ownerRef), delta, note, ts(time.Now().UTC()),
	); err != nil {
		return err
	}
	return tx.Commit()
}

type auditStore struct{ db *sql.DB }

func (s *auditStore) RecordEvent(tenantID string, evt store.AuditEvent) (string, error) {
	if evt.EventID == "" {
		evt.EventID = nextID("evt")
	}
	if evt.TenantID == "" {
		evt.TenantID = tenantID
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	payload, err := toJSON(evt.Payload)
	if err != nil {
		return "", err
	}
	_, err = s.db.Exec(
		`INSERT INTO vault_audit_events (event_id, tenant_id, project_ref, actor_ref, verb, timestamp, proof_hash, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(event_id) DO UPDATE SET
		   tenant_id = excluded.tenant_id,
		   project_ref = excluded.project_ref,
		   actor_ref = excluded.actor_ref,
		   verb = excluded.verb,
		   timestamp = excluded.timestamp,
		   proof_hash = excluded.proof_hash,
		   payload = excluded.payload`,
		evt.EventID, evt.TenantID, nullRef(evt.ProjectRef), nullRef(evt.ActorRef), evt.Verb, ts(evt.Timestamp), evt.ProofHash, payload,
	)
	if err != nil {
		return "", err
	}
	return evt.EventID, nil
}

func (s *auditStore) RecordViolation(tenantID, rule string, evt store.AuditEvent, detail string) (string, error) {
	payload, err := toJSON(map[string]any{
		"rule":   rule,
		"detail": detail,
		"event":  evt,
	})
	if err != nil {
		return "", err
	}
	id := nextID("vio")
	_, err = s.db.Exec(
		`INSERT INTO vault_audit_events (event_id, tenant_id, project_ref, actor_ref, verb, timestamp, proof_hash, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id, tenantID, nullRef(evt.ProjectRef), nullRef(evt.ActorRef), "VIOLATION:"+rule, ts(time.Now().UTC()), evt.ProofHash, payload,
	)
	if err != nil {
		return "", err
	}
	return id, nil
}

func (s *auditStore) QueryEvents(tenantID string, f store.AuditFilter) ([]store.AuditEvent, error) {
	where := []string{"tenant_id = ?"}
	args := []any{tenantID}
	if f.ProjectRef != nil {
		where = append(where, "project_ref = ?")
		args = append(args, string(*f.ProjectRef))
	}
	if f.ActorRef != nil {
		where = append(where, "actor_ref = ?")
		args = append(args, string(*f.ActorRef))
	}
	if f.Verb != nil {
		where = append(where, "verb = ?")
		args = append(args, *f.Verb)
	}
	if f.From != nil {
		where = append(where, "timestamp >= ?")
		args = append(args, ts(*f.From))
	}
	if f.To != nil {
		where = append(where, "timestamp <= ?")
		args = append(args, ts(*f.To))
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit)
	rows, err := s.db.Query(
		`SELECT event_id, tenant_id, project_ref, actor_ref, verb, timestamp, proof_hash, payload
		 FROM vault_audit_events
		 WHERE `+strings.Join(where, " AND ")+`
		 ORDER BY timestamp DESC
		 LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]store.AuditEvent, 0, limit)
	for rows.Next() {
		var evt store.AuditEvent
		var projectRef, actorRef sql.NullString
		var timestamp, payload string
		if err := rows.Scan(&evt.EventID, &evt.TenantID, &projectRef, &actorRef, &evt.Verb, &timestamp, &evt.ProofHash, &payload); err != nil {
			return nil, err
		}
		if projectRef.Valid {
			evt.ProjectRef = pc.VRef(projectRef.String)
		}
		if actorRef.Valid {
			evt.ActorRef = pc.VRef(actorRef.String)
		}
		t, err := parseTS(timestamp)
		if err != nil {
			return nil, err
		}
		evt.Timestamp = t
		if payload != "" && payload != "null" {
			_ = json.Unmarshal([]byte(payload), &evt.Payload)
		}
		out = append(out, evt)
	}
	return out, rows.Err()
}

func nextID(prefix string) string {
	n := seq.Add(1)
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(n, 10)
}
