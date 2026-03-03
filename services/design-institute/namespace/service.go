package namespace

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

func ExtractNamespace(ref string) string {
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, "v://") {
		return ""
	}
	rest := strings.TrimPrefix(ref, "v://")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}
	return "v://" + parts[0]
}

func ExtractNamespaceFromExecutor(executorRef string) string {
	ns := ExtractNamespace(executorRef)
	if ns == "" {
		return ""
	}
	return ns
}

func IsSameNamespace(ref1, ref2 string) bool {
	return ExtractNamespace(ref1) == ExtractNamespace(ref2)
}

func IsChildOf(childRef, parentRef string) bool {
	childNS := ExtractNamespace(childRef)
	parentNS := ExtractNamespace(parentRef)
	if childNS == "" || parentNS == "" {
		return false
	}
	return childNS == parentNS
}

type Namespace struct {
	ID             int64     `json:"id"`
	Ref            string    `json:"ref"`
	ParentRef      *string   `json:"parent_ref,omitempty"`
	Name           string    `json:"name"`
	InheritedRules []string  `json:"inherited_rules"`
	OwnedGenesis   []string  `json:"owned_genesis"`
	TenantID       int       `json:"tenant_id"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Delegation struct {
	ID         int64     `json:"id"`
	FromRef    string    `json:"from_ref"`
	ToRef      string    `json:"to_ref"`
	ProjectRef string    `json:"project_ref,omitempty"`
	Action     string    `json:"action,omitempty"`
	Status     string    `json:"status"`
	TenantID   int       `json:"tenant_id"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Network struct {
	Root        *Namespace    `json:"root"`
	Namespaces  []*Namespace  `json:"namespaces"`
	Delegations []*Delegation `json:"delegations"`
}

type CreateNamespaceInput struct {
	Ref            string   `json:"ref"`
	ParentRef      *string  `json:"parent_ref"`
	Name           string   `json:"name"`
	InheritedRules []string `json:"inherited_rules"`
	OwnedGenesis   []string `json:"owned_genesis"`
}

type CreateDelegationInput struct {
	FromRef    string `json:"from_ref"`
	ToRef      string `json:"to_ref"`
	ProjectRef string `json:"project_ref"`
	Action     string `json:"action"`
	Status     string `json:"status"`
}

type RouteInput struct {
	FromRef    string `json:"from_ref"`
	ToRef      string `json:"to_ref"`
	ProjectRef string `json:"project_ref"`
	SPURef     string `json:"spu_ref"`
	Action     string `json:"action"`
}

type RouteDecision struct {
	Allowed           bool        `json:"allowed"`
	Reason            string      `json:"reason"`
	MatchedDelegation *Delegation `json:"matched_delegation,omitempty"`
}

type Store interface {
	UpsertNamespace(ctx context.Context, item *Namespace) error
	GetNamespace(ctx context.Context, tenantID int, ref string) (*Namespace, error)
	ListChildren(ctx context.Context, tenantID int, ref string) ([]*Namespace, error)
	ListNetworkNamespaces(ctx context.Context, tenantID int, rootRef string) ([]*Namespace, error)
	ListDelegationsByRefs(ctx context.Context, tenantID int, refs []string) ([]*Delegation, error)
	CreateDelegation(ctx context.Context, item *Delegation) error
	FindDelegation(ctx context.Context, tenantID int, fromRef, toRef, projectRef, action string) (*Delegation, error)
}

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

func (s *Service) Register(ctx context.Context, in CreateNamespaceInput) (*Namespace, error) {
	ref := strings.TrimSpace(in.Ref)
	if ref == "" {
		return nil, fmt.Errorf("ref is required")
	}
	if !strings.HasPrefix(ref, "v://") {
		return nil, fmt.Errorf("ref must start with v://")
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		name = ref
	}
	var parentRef *string
	if in.ParentRef != nil {
		v := strings.TrimSpace(*in.ParentRef)
		if v != "" {
			parentRef = &v
		}
	}
	item := &Namespace{
		Ref:            ref,
		ParentRef:      parentRef,
		Name:           name,
		InheritedRules: normalizeStrings(in.InheritedRules),
		OwnedGenesis:   normalizeStrings(in.OwnedGenesis),
		TenantID:       s.tenantID,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := s.store.UpsertNamespace(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) Get(ctx context.Context, ref string) (*Namespace, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("ref is required")
	}
	return s.store.GetNamespace(ctx, s.tenantID, ref)
}

func (s *Service) Children(ctx context.Context, ref string) ([]*Namespace, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, fmt.Errorf("ref is required")
	}
	return s.store.ListChildren(ctx, s.tenantID, ref)
}

func (s *Service) Network(ctx context.Context, ref string) (*Network, error) {
	root, err := s.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	nodes, err := s.store.ListNetworkNamespaces(ctx, s.tenantID, ref)
	if err != nil {
		return nil, err
	}
	refs := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		refs = append(refs, n.Ref)
	}
	delegations, err := s.store.ListDelegationsByRefs(ctx, s.tenantID, refs)
	if err != nil {
		return nil, err
	}
	return &Network{Root: root, Namespaces: nodes, Delegations: delegations}, nil
}

func (s *Service) Authorize(ctx context.Context, in CreateDelegationInput) (*Delegation, error) {
	fromRef := strings.TrimSpace(in.FromRef)
	toRef := strings.TrimSpace(in.ToRef)
	if fromRef == "" || toRef == "" {
		return nil, fmt.Errorf("from_ref and to_ref are required")
	}
	status := strings.ToUpper(strings.TrimSpace(in.Status))
	if status == "" {
		status = "ACTIVE"
	}
	if status != "ACTIVE" && status != "DISABLED" {
		return nil, fmt.Errorf("status must be ACTIVE or DISABLED")
	}
	item := &Delegation{
		FromRef:    fromRef,
		ToRef:      toRef,
		ProjectRef: strings.TrimSpace(in.ProjectRef),
		Action:     strings.TrimSpace(in.Action),
		Status:     status,
		TenantID:   s.tenantID,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := s.store.CreateDelegation(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) Route(ctx context.Context, in RouteInput) (*RouteDecision, error) {
	fromRef := strings.TrimSpace(in.FromRef)
	toRef := strings.TrimSpace(in.ToRef)
	if fromRef == "" || toRef == "" {
		return nil, fmt.Errorf("from_ref and to_ref are required")
	}
	item, err := s.store.FindDelegation(
		ctx,
		s.tenantID,
		fromRef,
		toRef,
		strings.TrimSpace(in.ProjectRef),
		strings.TrimSpace(in.Action),
	)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return &RouteDecision{
			Allowed: false,
			Reason:  "no active delegation matched",
		}, nil
	}
	return &RouteDecision{
		Allowed:           true,
		Reason:            "matched active delegation",
		MatchedDelegation: item,
	}, nil
}

func normalizeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
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

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) UpsertNamespace(ctx context.Context, item *Namespace) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO namespaces (
			ref, parent_ref, name, inherited_rules, owned_genesis,
			tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (tenant_id, ref) DO UPDATE SET
			parent_ref=EXCLUDED.parent_ref,
			name=EXCLUDED.name,
			inherited_rules=EXCLUDED.inherited_rules,
			owned_genesis=EXCLUDED.owned_genesis,
			updated_at=EXCLUDED.updated_at
		RETURNING id, created_at, updated_at
	`,
		item.Ref,
		item.ParentRef,
		item.Name,
		pq.Array(item.InheritedRules),
		pq.Array(item.OwnedGenesis),
		item.TenantID,
		item.CreatedAt,
		item.UpdatedAt,
	).Scan(&item.ID, &item.CreatedAt, &item.UpdatedAt)
}

func (s *PGStore) GetNamespace(ctx context.Context, tenantID int, ref string) (*Namespace, error) {
	item := &Namespace{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
		FROM namespaces
		WHERE tenant_id=$1 AND ref=$2
		LIMIT 1
	`, tenantID, ref).Scan(
		&item.ID,
		&item.Ref,
		&item.ParentRef,
		&item.Name,
		pq.Array(&item.InheritedRules),
		pq.Array(&item.OwnedGenesis),
		&item.TenantID,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	return item, err
}

func (s *PGStore) ListChildren(ctx context.Context, tenantID int, ref string) ([]*Namespace, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
		FROM namespaces
		WHERE tenant_id=$1 AND parent_ref=$2
		ORDER BY ref
	`, tenantID, ref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*Namespace, 0)
	for rows.Next() {
		item := &Namespace{}
		if err := rows.Scan(
			&item.ID,
			&item.Ref,
			&item.ParentRef,
			&item.Name,
			pq.Array(&item.InheritedRules),
			pq.Array(&item.OwnedGenesis),
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) ListNetworkNamespaces(ctx context.Context, tenantID int, rootRef string) ([]*Namespace, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE ns AS (
			SELECT id, ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
			FROM namespaces
			WHERE tenant_id=$1 AND ref=$2
			UNION ALL
			SELECT c.id, c.ref, c.parent_ref, c.name, c.inherited_rules, c.owned_genesis, c.tenant_id, c.created_at, c.updated_at
			FROM namespaces c
			INNER JOIN ns p ON c.parent_ref=p.ref
			WHERE c.tenant_id=$1
		)
		SELECT id, ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
		FROM ns
		ORDER BY ref
	`, tenantID, rootRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*Namespace, 0)
	for rows.Next() {
		item := &Namespace{}
		if err := rows.Scan(
			&item.ID,
			&item.Ref,
			&item.ParentRef,
			&item.Name,
			pq.Array(&item.InheritedRules),
			pq.Array(&item.OwnedGenesis),
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) ListDelegationsByRefs(ctx context.Context, tenantID int, refs []string) ([]*Delegation, error) {
	if len(refs) == 0 {
		return []*Delegation{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, from_ref, to_ref, project_ref, action, status, tenant_id, created_at, updated_at
		FROM namespace_delegations
		WHERE tenant_id=$1
		  AND (from_ref = ANY($2) OR to_ref = ANY($2))
		ORDER BY id DESC
	`, tenantID, pq.Array(refs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*Delegation, 0)
	for rows.Next() {
		item := &Delegation{}
		if err := rows.Scan(
			&item.ID,
			&item.FromRef,
			&item.ToRef,
			&item.ProjectRef,
			&item.Action,
			&item.Status,
			&item.TenantID,
			&item.CreatedAt,
			&item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *PGStore) CreateDelegation(ctx context.Context, item *Delegation) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO namespace_delegations (
			from_ref, to_ref, project_ref, action, status, tenant_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING id
	`,
		item.FromRef,
		item.ToRef,
		item.ProjectRef,
		item.Action,
		item.Status,
		item.TenantID,
		item.CreatedAt,
		item.UpdatedAt,
	).Scan(&item.ID)
}

func (s *PGStore) FindDelegation(ctx context.Context, tenantID int, fromRef, toRef, projectRef, action string) (*Delegation, error) {
	item := &Delegation{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id, from_ref, to_ref, project_ref, action, status, tenant_id, created_at, updated_at
		FROM namespace_delegations
		WHERE tenant_id=$1
		  AND from_ref=$2
		  AND to_ref=$3
		  AND status='ACTIVE'
		  AND (project_ref='' OR project_ref=$4 OR $4='')
		  AND (action='' OR action=$5 OR $5='')
		ORDER BY
		  CASE WHEN project_ref=$4 THEN 0 ELSE 1 END,
		  CASE WHEN action=$5 THEN 0 ELSE 1 END,
		  id DESC
		LIMIT 1
	`, tenantID, fromRef, toRef, projectRef, action).Scan(
		&item.ID,
		&item.FromRef,
		&item.ToRef,
		&item.ProjectRef,
		&item.Action,
		&item.Status,
		&item.TenantID,
		&item.CreatedAt,
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
