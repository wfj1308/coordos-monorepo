package rocksdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pc "coordos/project-core"
	"coordos/vault-service/infra/store"
)

const (
	// defaultDBFile is used when backend path is empty or points to a directory.
	defaultDBFile = "coordos_vault_rocks_native.json"
)

// seq provides monotonic IDs for audit and wallet ledger records.
var seq atomic.Uint64

// DB is a native file-backed implementation used by the "rocksdb" backend.
//
// Design notes:
//  1. Native file-backed persistence keeps the backend self-contained.
//  2. State is persisted as a single JSON snapshot for deterministic recovery.
//  3. All write paths are serialized under one mutex and flushed atomically.
//  4. This keeps behavior stable today and leaves a clear migration path to
//     true RocksDB/LSM blocks later without changing upper-layer interfaces.
type DB struct {
	path     string // public path exposed as file:/... for compatibility
	filePath string // local filesystem path used for persistence

	mu    sync.RWMutex
	state *persistedState
}

// persistedState holds all logical buckets required by store interfaces.
type persistedState struct {
	ProjectNodes map[string]json.RawMessage `json:"project_nodes"`
	Genesis      map[string]json.RawMessage `json:"genesis"`
	Contracts    map[string]json.RawMessage `json:"contracts"`
	Parcels      map[string]json.RawMessage `json:"parcels"`
	UTXOs        map[string]json.RawMessage `json:"utxos"`
	Settlements  map[string]json.RawMessage `json:"settlements"`

	Wallets map[string]store.Wallet        `json:"wallets"`
	Ledgers map[string][]store.LedgerEntry `json:"ledgers"`
	Audits  map[string]store.AuditEvent    `json:"audits"`
}

func newState() *persistedState {
	return &persistedState{
		ProjectNodes: make(map[string]json.RawMessage),
		Genesis:      make(map[string]json.RawMessage),
		Contracts:    make(map[string]json.RawMessage),
		Parcels:      make(map[string]json.RawMessage),
		UTXOs:        make(map[string]json.RawMessage),
		Settlements:  make(map[string]json.RawMessage),
		Wallets:      make(map[string]store.Wallet),
		Ledgers:      make(map[string][]store.LedgerEntry),
		Audits:       make(map[string]store.AuditEvent),
	}
}

func (s *persistedState) ensure() {
	if s.ProjectNodes == nil {
		s.ProjectNodes = make(map[string]json.RawMessage)
	}
	if s.Genesis == nil {
		s.Genesis = make(map[string]json.RawMessage)
	}
	if s.Contracts == nil {
		s.Contracts = make(map[string]json.RawMessage)
	}
	if s.Parcels == nil {
		s.Parcels = make(map[string]json.RawMessage)
	}
	if s.UTXOs == nil {
		s.UTXOs = make(map[string]json.RawMessage)
	}
	if s.Settlements == nil {
		s.Settlements = make(map[string]json.RawMessage)
	}
	if s.Wallets == nil {
		s.Wallets = make(map[string]store.Wallet)
	}
	if s.Ledgers == nil {
		s.Ledgers = make(map[string][]store.LedgerEntry)
	}
	if s.Audits == nil {
		s.Audits = make(map[string]store.AuditEvent)
	}
}

// Open opens/creates a native rocksdb-compatible storage file.
func Open(path string) (*DB, error) {
	normalized, err := normalizePath(path)
	if err != nil {
		return nil, err
	}
	filePath := filepath.FromSlash(strings.TrimPrefix(normalized, "file:"))

	st := newState()
	if raw, err := os.ReadFile(filePath); err == nil {
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, st); err != nil {
				return nil, fmt.Errorf("decode rocksdb state: %w", err)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read rocksdb state: %w", err)
	}
	st.ensure()

	db := &DB{
		path:     normalized,
		filePath: filePath,
		state:    st,
	}
	return db, nil
}

// Path returns normalized backend path.
func (d *DB) Path() string { return d.path }

func (d *DB) Close() error { return nil }

func (d *DB) ProjectTree() store.ProjectTreeStore { return &projectTreeStore{db: d} }
func (d *DB) Genesis() store.GenesisStore         { return &genesisStore{db: d} }
func (d *DB) Contracts() store.ContractStore      { return &contractStore{db: d} }
func (d *DB) Parcels() store.ParcelStore          { return &parcelStore{db: d} }
func (d *DB) UTXOs() store.UTXOStore              { return &utxoStore{db: d} }
func (d *DB) Settlements() store.SettlementStore  { return &settlementStore{db: d} }
func (d *DB) Wallets() store.WalletStore          { return &walletStore{db: d} }
func (d *DB) Audit() store.AuditStore             { return &auditStore{db: d} }

func normalizePath(path string) (string, error) {
	p := strings.TrimSpace(path)
	if p == "" {
		p = defaultDBFile
	}

	// Keep compatibility with legacy "file:..." settings by stripping query.
	if strings.HasPrefix(strings.ToLower(p), "file:") {
		p = strings.TrimPrefix(p, "file:")
		if idx := strings.IndexRune(p, '?'); idx >= 0 {
			p = p[:idx]
		}
	}

	clean := filepath.Clean(p)
	if filepath.Ext(clean) == "" {
		if err := os.MkdirAll(clean, 0o755); err != nil {
			return "", fmt.Errorf("prepare rocksdb directory: %w", err)
		}
		clean = filepath.Join(clean, defaultDBFile)
	} else {
		dir := filepath.Dir(clean)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return "", fmt.Errorf("prepare rocksdb parent directory: %w", err)
			}
		}
	}
	return "file:" + filepath.ToSlash(clean), nil
}

// persistLocked flushes in-memory state with atomic replace semantics.
func (d *DB) persistLocked() error {
	d.state.ensure()
	raw, err := json.MarshalIndent(d.state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode rocksdb state: %w", err)
	}

	tmp := d.filePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write rocksdb temp file: %w", err)
	}
	if err := os.Rename(tmp, d.filePath); err != nil {
		return fmt.Errorf("replace rocksdb state: %w", err)
	}
	return nil
}

func refKey(tenantID string, ref pc.VRef) string { return tenantID + "|" + string(ref) }
func ownerKey(tenantID string, owner pc.VRef) string {
	return tenantID + "|" + string(owner)
}

func marshalRaw(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func unmarshalRaw[T any](raw json.RawMessage, v *T) error {
	return json.Unmarshal(raw, v)
}

func nextID(prefix string) string {
	n := seq.Add(1)
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(n, 10)
}

type projectTreeStore struct{ db *DB }

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

	k := refKey(tenantID, node.Ref)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if _, ok := s.db.state.ProjectNodes[k]; ok {
		return fmt.Errorf("project node already exists: %s", node.Ref)
	}
	raw, err := marshalRaw(node)
	if err != nil {
		return err
	}
	s.db.state.ProjectNodes[k] = raw
	return s.db.persistLocked()
}

func (s *projectTreeStore) GetNode(tenantID string, ref pc.VRef) (*pc.ProjectNode, error) {
	k := refKey(tenantID, ref)
	s.db.mu.RLock()
	raw, ok := s.db.state.ProjectNodes[k]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("project node not found: %s", ref)
	}
	var node pc.ProjectNode
	if err := unmarshalRaw(raw, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

func (s *projectTreeStore) GetChildren(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*pc.ProjectNode, 0)
	for k, raw := range s.db.state.ProjectNodes {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var n pc.ProjectNode
		if err := unmarshalRaw(raw, &n); err != nil {
			return nil, err
		}
		if n.ParentRef == ref {
			out = append(out, &n)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Ref < out[j].Ref
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *projectTreeStore) GetAncestors(tenantID string, ref pc.VRef) ([]*pc.ProjectNode, error) {
	node, err := s.GetNode(tenantID, ref)
	if err != nil {
		return nil, err
	}
	out := make([]*pc.ProjectNode, 0)
	for node.ParentRef != "" {
		parent, err := s.GetNode(tenantID, node.ParentRef)
		if err != nil {
			return nil, err
		}
		out = append(out, parent)
		node = parent
	}
	return out, nil
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
	now := time.Now().UTC()
	if node.TenantID == "" {
		node.TenantID = tenantID
	}
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	node.UpdatedAt = now

	raw, err := marshalRaw(node)
	if err != nil {
		return err
	}
	k := refKey(tenantID, node.Ref)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.ProjectNodes[k] = raw
	return s.db.persistLocked()
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
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	items := make([]*pc.ProjectNode, 0)
	for k, raw := range s.db.state.ProjectNodes {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var n pc.ProjectNode
		if err := unmarshalRaw(raw, &n); err != nil {
			return nil, 0, err
		}
		if filter.Status != nil && n.Status != *filter.Status {
			continue
		}
		if filter.OwnerRef != nil && n.OwnerRef != *filter.OwnerRef {
			continue
		}
		items = append(items, &n)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].Ref > items[j].Ref
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})

	total := int64(len(items))
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []*pc.ProjectNode{}, total, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], total, nil
}

type genesisStore struct{ db *DB }

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
	raw, err := marshalRaw(g)
	if err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.Genesis[refKey(tenantID, g.Ref)] = raw
	return s.db.persistLocked()
}

func (s *genesisStore) GetFull(tenantID string, ref pc.VRef) (*pc.GenesisUTXOFull, error) {
	s.db.mu.RLock()
	raw, ok := s.db.state.Genesis[refKey(tenantID, ref)]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("genesis not found: %s", ref)
	}
	var g pc.GenesisUTXOFull
	if err := unmarshalRaw(raw, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func (s *genesisStore) UpdateFull(tenantID string, g *pc.GenesisUTXOFull) error {
	if g == nil {
		return errors.New("nil genesis")
	}
	g.ProofHash = g.ComputeProofHash()
	raw, err := marshalRaw(g)
	if err != nil {
		return err
	}

	k := refKey(tenantID, g.Ref)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if _, ok := s.db.state.Genesis[k]; !ok {
		return fmt.Errorf("genesis not found: %s", g.Ref)
	}
	s.db.state.Genesis[k] = raw
	return s.db.persistLocked()
}

func (s *genesisStore) GetRemainingQuota(tenantID string, ref pc.VRef) (int64, error) {
	g, err := s.GetFull(tenantID, ref)
	if err != nil {
		return 0, err
	}
	return g.RemainingQuota(), nil
}

func (s *genesisStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*pc.GenesisUTXOFull, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*pc.GenesisUTXOFull, 0)
	for k, raw := range s.db.state.Genesis {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var g pc.GenesisUTXOFull
		if err := unmarshalRaw(raw, &g); err != nil {
			return nil, err
		}
		if g.ProjectRef == projectRef {
			out = append(out, &g)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

type contractStore struct{ db *DB }

func (s *contractStore) Create(tenantID string, c *store.Contract) error {
	if c == nil {
		return errors.New("nil contract")
	}
	if c.TenantID == "" {
		c.TenantID = tenantID
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now().UTC()
	}
	raw, err := marshalRaw(c)
	if err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.Contracts[refKey(tenantID, c.Ref)] = raw
	return s.db.persistLocked()
}

func (s *contractStore) Get(tenantID string, ref pc.VRef) (*store.Contract, error) {
	s.db.mu.RLock()
	raw, ok := s.db.state.Contracts[refKey(tenantID, ref)]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("contract not found: %s", ref)
	}
	var c store.Contract
	if err := unmarshalRaw(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *contractStore) Update(tenantID string, c *store.Contract) error {
	return s.Create(tenantID, c)
}

func (s *contractStore) List(tenantID string, f store.ContractFilter) ([]*store.Contract, int64, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	items := make([]*store.Contract, 0)
	for k, raw := range s.db.state.Contracts {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var c store.Contract
		if err := unmarshalRaw(raw, &c); err != nil {
			return nil, 0, err
		}
		if f.ProjectRef != nil && c.ProjectRef != *f.ProjectRef {
			continue
		}
		if f.BranchRef != nil && c.BranchRef != *f.BranchRef {
			continue
		}
		if f.Status != nil && c.Status != *f.Status {
			continue
		}
		if f.Kind != nil && c.ContractKind != *f.Kind {
			continue
		}
		items = append(items, &c)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })

	total := int64(len(items))
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []*store.Contract{}, total, nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], total, nil
}

func (s *contractStore) GetRemainingAmount(tenantID string, ref pc.VRef) (int64, error) {
	c, err := s.Get(tenantID, ref)
	if err != nil {
		return 0, err
	}
	if c.AmountWithTax < 0 {
		return 0, nil
	}
	return c.AmountWithTax, nil
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

type parcelStore struct{ db *DB }

func (s *parcelStore) Create(tenantID string, p *store.Parcel) error {
	if p == nil {
		return errors.New("nil parcel")
	}
	if p.TenantID == "" {
		p.TenantID = tenantID
	}
	if p.CreatedAt.IsZero() {
		p.CreatedAt = time.Now().UTC()
	}
	raw, err := marshalRaw(p)
	if err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.Parcels[refKey(tenantID, p.Ref)] = raw
	return s.db.persistLocked()
}

func (s *parcelStore) Get(tenantID string, ref pc.VRef) (*store.Parcel, error) {
	s.db.mu.RLock()
	raw, ok := s.db.state.Parcels[refKey(tenantID, ref)]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("parcel not found: %s", ref)
	}
	var p store.Parcel
	if err := unmarshalRaw(raw, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *parcelStore) Update(tenantID string, p *store.Parcel) error { return s.Create(tenantID, p) }

func (s *parcelStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.Parcel, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*store.Parcel, 0)
	for k, raw := range s.db.state.Parcels {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var p store.Parcel
		if err := unmarshalRaw(raw, &p); err != nil {
			return nil, err
		}
		if p.ProjectRef == projectRef {
			out = append(out, &p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *parcelStore) ListByContract(tenantID string, contractRef pc.VRef) ([]*store.Parcel, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*store.Parcel, 0)
	for k, raw := range s.db.state.Parcels {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var p store.Parcel
		if err := unmarshalRaw(raw, &p); err != nil {
			return nil, err
		}
		if p.ContractRef == contractRef {
			out = append(out, &p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

type utxoStore struct{ db *DB }

func (s *utxoStore) Create(tenantID string, u *store.UTXO) error {
	if u == nil {
		return errors.New("nil utxo")
	}
	if u.TenantID == "" {
		u.TenantID = tenantID
	}
	if u.CreatedAt.IsZero() {
		u.CreatedAt = time.Now().UTC()
	}
	raw, err := marshalRaw(u)
	if err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.UTXOs[refKey(tenantID, u.Ref)] = raw
	return s.db.persistLocked()
}

func (s *utxoStore) Get(tenantID string, ref pc.VRef) (*store.UTXO, error) {
	s.db.mu.RLock()
	raw, ok := s.db.state.UTXOs[refKey(tenantID, ref)]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("utxo not found: %s", ref)
	}
	var u store.UTXO
	if err := unmarshalRaw(raw, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *utxoStore) Update(tenantID string, u *store.UTXO) error { return s.Create(tenantID, u) }

func (s *utxoStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.UTXO, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*store.UTXO, 0)
	for k, raw := range s.db.state.UTXOs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var u store.UTXO
		if err := unmarshalRaw(raw, &u); err != nil {
			return nil, err
		}
		if u.ProjectRef == projectRef {
			out = append(out, &u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *utxoStore) ListByParcel(tenantID string, parcelRef pc.VRef) ([]*store.UTXO, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*store.UTXO, 0)
	for k, raw := range s.db.state.UTXOs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var u store.UTXO
		if err := unmarshalRaw(raw, &u); err != nil {
			return nil, err
		}
		if u.ParcelRef == parcelRef {
			out = append(out, &u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

type settlementStore struct{ db *DB }

func (s *settlementStore) Create(tenantID string, st *store.Settlement) error {
	if st == nil {
		return errors.New("nil settlement")
	}
	if st.TenantID == "" {
		st.TenantID = tenantID
	}
	if st.CreatedAt.IsZero() {
		st.CreatedAt = time.Now().UTC()
	}
	raw, err := marshalRaw(st)
	if err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.Settlements[refKey(tenantID, st.Ref)] = raw
	return s.db.persistLocked()
}

func (s *settlementStore) Get(tenantID string, ref pc.VRef) (*store.Settlement, error) {
	s.db.mu.RLock()
	raw, ok := s.db.state.Settlements[refKey(tenantID, ref)]
	s.db.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("settlement not found: %s", ref)
	}
	var st store.Settlement
	if err := unmarshalRaw(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *settlementStore) Update(tenantID string, st *store.Settlement) error {
	return s.Create(tenantID, st)
}

func (s *settlementStore) ListByProject(tenantID string, projectRef pc.VRef) ([]*store.Settlement, error) {
	prefix := tenantID + "|"
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	out := make([]*store.Settlement, 0)
	for k, raw := range s.db.state.Settlements {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var st store.Settlement
		if err := unmarshalRaw(raw, &st); err != nil {
			return nil, err
		}
		if st.ProjectRef == projectRef {
			out = append(out, &st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

type walletStore struct{ db *DB }

func (s *walletStore) GetOrCreate(tenantID string, ownerRef pc.VRef) (*store.Wallet, error) {
	k := ownerKey(tenantID, ownerRef)
	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	if w, ok := s.db.state.Wallets[k]; ok {
		clone := w
		return &clone, nil
	}
	w := store.Wallet{
		OwnerRef:  ownerRef,
		Balance:   0,
		TenantID:  tenantID,
		UpdatedAt: time.Now().UTC(),
	}
	s.db.state.Wallets[k] = w
	if err := s.db.persistLocked(); err != nil {
		return nil, err
	}
	clone := w
	return &clone, nil
}

func (s *walletStore) Credit(tenantID string, ownerRef pc.VRef, amount int64, note string) error {
	return s.applyDelta(tenantID, ownerRef, amount, note)
}

func (s *walletStore) Debit(tenantID string, ownerRef pc.VRef, amount int64, note string) error {
	return s.applyDelta(tenantID, ownerRef, -amount, note)
}

func (s *walletStore) GetBalance(tenantID string, ownerRef pc.VRef) (int64, error) {
	k := ownerKey(tenantID, ownerRef)
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if w, ok := s.db.state.Wallets[k]; ok {
		return w.Balance, nil
	}
	return 0, nil
}

func (s *walletStore) ListLedger(tenantID string, ownerRef pc.VRef, limit int) ([]*store.LedgerEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	k := ownerKey(tenantID, ownerRef)
	s.db.mu.RLock()
	entries := append([]store.LedgerEntry(nil), s.db.state.Ledgers[k]...)
	s.db.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].CreatedAt.After(entries[j].CreatedAt) })
	if len(entries) > limit {
		entries = entries[:limit]
	}
	out := make([]*store.LedgerEntry, 0, len(entries))
	for i := range entries {
		e := entries[i]
		out = append(out, &e)
	}
	return out, nil
}

func (s *walletStore) applyDelta(tenantID string, ownerRef pc.VRef, delta int64, note string) error {
	if delta == 0 {
		return errors.New("amount must not be zero")
	}
	if strings.TrimSpace(note) == "" {
		note = "n/a"
	}

	k := ownerKey(tenantID, ownerRef)
	now := time.Now().UTC()

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	w, ok := s.db.state.Wallets[k]
	if !ok {
		w = store.Wallet{
			OwnerRef:  ownerRef,
			Balance:   0,
			TenantID:  tenantID,
			UpdatedAt: now,
		}
	}
	if delta < 0 && w.Balance < -delta {
		return fmt.Errorf("insufficient balance")
	}
	w.Balance += delta
	w.UpdatedAt = now
	s.db.state.Wallets[k] = w

	s.db.state.Ledgers[k] = append(s.db.state.Ledgers[k], store.LedgerEntry{
		ID:        nextID("ledger"),
		OwnerRef:  ownerRef,
		Amount:    delta,
		Note:      note,
		CreatedAt: now,
	})
	return s.db.persistLocked()
}

type auditStore struct{ db *DB }

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

	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	s.db.state.Audits[evt.EventID] = evt
	if err := s.db.persistLocked(); err != nil {
		return "", err
	}
	return evt.EventID, nil
}

func (s *auditStore) RecordViolation(tenantID, rule string, evt store.AuditEvent, detail string) (string, error) {
	return s.RecordEvent(tenantID, store.AuditEvent{
		EventID:    nextID("vio"),
		TenantID:   tenantID,
		ProjectRef: evt.ProjectRef,
		ActorRef:   evt.ActorRef,
		Verb:       "VIOLATION:" + rule,
		Payload: map[string]interface{}{
			"rule":   rule,
			"detail": detail,
			"event":  evt,
		},
		ProofHash: evt.ProofHash,
		Timestamp: time.Now().UTC(),
	})
}

func (s *auditStore) QueryEvents(tenantID string, f store.AuditFilter) ([]store.AuditEvent, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	out := make([]store.AuditEvent, 0, limit)
	for _, evt := range s.db.state.Audits {
		if evt.TenantID != tenantID {
			continue
		}
		if f.ProjectRef != nil && evt.ProjectRef != *f.ProjectRef {
			continue
		}
		if f.ActorRef != nil && evt.ActorRef != *f.ActorRef {
			continue
		}
		if f.Verb != nil && evt.Verb != *f.Verb {
			continue
		}
		if f.From != nil && evt.Timestamp.Before(*f.From) {
			continue
		}
		if f.To != nil && evt.Timestamp.After(*f.To) {
			continue
		}
		out = append(out, evt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
