package project

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ══════════════════════════════════════════════════════════════
//  类型定义
// ══════════════════════════════════════════════════════════════

type Status string

const (
	StatusInitiated  Status = "INITIATED"
	StatusTendering  Status = "TENDERING"
	StatusContracted Status = "CONTRACTED"
	StatusInProgress Status = "IN_PROGRESS"
	StatusDelivered  Status = "DELIVERED"
	StatusSettled    Status = "SETTLED"
	StatusArchived   Status = "ARCHIVED"
)

type ProjectNode struct {
	ID            int64
	Ref           string    // v://zhongbei/project/{path}
	TenantID      int
	ParentID      *int64
	ParentRef     *string
	Depth         int       // 0=根节点
	Path          string    // 物化路径 /1/3/7/
	Name          string
	OwnerRef      string    // 业主
	ContractorRef string    // 承接方（总院）
	ExecutorRef   string    // 执行方（分院/个人）
	PlatformRef   string
	ContractRef   *string
	ProcurementRef *string
	GenesisRef    *string
	Status        Status
	ProofHash     string
	PrevHash      *string
	LegacyContractID *int64 // 对应旧系统 contract.id
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type CreateNodeInput struct {
	ParentRef     *string
	Name          string
	OwnerRef      string
	ContractorRef string
	ExecutorRef   string
	PlatformRef   string
	LegacyContractID *int64
	TenantID      int
}

type TreeNode struct {
	ProjectNode
	Children []*TreeNode
}

// ══════════════════════════════════════════════════════════════
//  Store 接口（PostgreSQL实现在 store_pg.go）
// ══════════════════════════════════════════════════════════════

type Store interface {
	Create(ctx context.Context, node *ProjectNode) error
	Get(ctx context.Context, ref string) (*ProjectNode, error)
	GetByID(ctx context.Context, id int64) (*ProjectNode, error)
	GetByLegacyContractID(ctx context.Context, contractID int64) (*ProjectNode, error)
	GetChildren(ctx context.Context, parentRef string) ([]*ProjectNode, error)
	GetAncestors(ctx context.Context, ref string) ([]*ProjectNode, error)
	GetTree(ctx context.Context, rootRef string, maxDepth int) (*TreeNode, error)
	UpdateStatus(ctx context.Context, ref string, status Status, proofHash string) error
	UpdateRefs(ctx context.Context, ref string, contractRef, genesisRef *string) error
	ListByTenant(ctx context.Context, tenantID int, status *Status, limit, offset int) ([]*ProjectNode, int, error)
	ListByExecutor(ctx context.Context, executorRef string) ([]*ProjectNode, error)
}

// ══════════════════════════════════════════════════════════════
//  Service
// ══════════════════════════════════════════════════════════════

type Service struct {
	store    Store
	tenantID int
}

func NewService(store Store, tenantID int) *Service {
	return &Service{store: store, tenantID: tenantID}
}

// ── 创建根节点（总院发起的项目） ──────────────────────────────
func (s *Service) CreateRoot(ctx context.Context, in CreateNodeInput) (*ProjectNode, error) {
	if in.OwnerRef == "" || in.ContractorRef == "" {
		return nil, fmt.Errorf("OwnerRef 和 ContractorRef 不能为空")
	}

	ref := s.genRef("root", in.Name)
	node := &ProjectNode{
		Ref:           ref,
		TenantID:      s.tenantID,
		Depth:         0,
		Path:          "/",
		Name:          in.Name,
		OwnerRef:      in.OwnerRef,
		ContractorRef: in.ContractorRef,
		ExecutorRef:   in.ExecutorRef,
		PlatformRef:   in.PlatformRef,
		Status:        StatusInitiated,
		LegacyContractID: in.LegacyContractID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	node.ProofHash = s.computeHash(node)

	if err := s.store.Create(ctx, node); err != nil {
		return nil, fmt.Errorf("创建根节点失败: %w", err)
	}
	return node, nil
}

// ── 创建子节点（委托链向下延伸） ──────────────────────────────
func (s *Service) CreateChild(ctx context.Context, in CreateNodeInput) (*ProjectNode, error) {
	if in.ParentRef == nil {
		return nil, fmt.Errorf("子节点必须指定 ParentRef")
	}

	parent, err := s.store.Get(ctx, *in.ParentRef)
	if err != nil {
		return nil, fmt.Errorf("父节点不存在: %w", err)
	}

	// 深度限制（防止无限嵌套）
	if parent.Depth >= 5 {
		return nil, fmt.Errorf("委托链最大深度为5，当前已达上限")
	}

	ref := s.genRef(fmt.Sprintf("d%d", parent.Depth+1), in.Name)
	prevHash := parent.ProofHash

	node := &ProjectNode{
		Ref:           ref,
		TenantID:      s.tenantID,
		ParentID:      &parent.ID,
		ParentRef:     in.ParentRef,
		Depth:         parent.Depth + 1,
		Path:          parent.Path + fmt.Sprintf("%s/", ref),
		Name:          in.Name,
		OwnerRef:      parent.ContractorRef, // 上层承接方成为下层业主
		ContractorRef: in.ContractorRef,
		ExecutorRef:   in.ExecutorRef,
		PlatformRef:   in.PlatformRef,
		Status:        StatusInitiated,
		PrevHash:      &prevHash,
		LegacyContractID: in.LegacyContractID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	node.ProofHash = s.computeHash(node)

	if err := s.store.Create(ctx, node); err != nil {
		return nil, fmt.Errorf("创建子节点失败: %w", err)
	}
	return node, nil
}

// ── 从旧合同数据重建项目树（迁移用） ─────────────────────────
func (s *Service) BuildFromLegacyContract(ctx context.Context, contractID int64,
	parentContractID *int64, contractName string, companyRef string) (*ProjectNode, error) {

	in := CreateNodeInput{
		Name:          contractName,
		ContractorRef: companyRef,
		ExecutorRef:   companyRef,
		LegacyContractID: &contractID,
		TenantID:      s.tenantID,
	}

	// 如果有父合同，找到对应的父节点
	if parentContractID != nil {
		parent, err := s.store.GetByLegacyContractID(ctx, *parentContractID)
		if err == nil && parent != nil {
			in.ParentRef = &parent.Ref
			in.OwnerRef = parent.ContractorRef
		}
	}

	if in.ParentRef == nil {
		in.OwnerRef = "v://zhongbei/executor/owner/default"
		return s.CreateRoot(ctx, in)
	}
	return s.CreateChild(ctx, in)
}

// ── 状态转移 ──────────────────────────────────────────────────
func (s *Service) Transition(ctx context.Context, ref string, to Status) error {
	node, err := s.store.Get(ctx, ref)
	if err != nil {
		return err
	}

	if err := s.validateTransition(node.Status, to); err != nil {
		return err
	}

	node.Status = to
	node.UpdatedAt = time.Now()
	hash := s.computeHash(node)

	return s.store.UpdateStatus(ctx, ref, to, hash)
}

func (s *Service) validateTransition(from, to Status) error {
	allowed := map[Status][]Status{
		StatusInitiated:  {StatusTendering},
		StatusTendering:  {StatusContracted, StatusInitiated},
		StatusContracted: {StatusInProgress},
		StatusInProgress: {StatusDelivered},
		StatusDelivered:  {StatusSettled, StatusInProgress},
		StatusSettled:    {StatusArchived},
	}
	for _, a := range allowed[from] {
		if a == to {
			return nil
		}
	}
	return fmt.Errorf("不允许的状态转移: %s → %s", from, to)
}

// ── 查询 ──────────────────────────────────────────────────────
func (s *Service) Get(ctx context.Context, ref string) (*ProjectNode, error) {
	return s.store.Get(ctx, ref)
}

func (s *Service) GetTree(ctx context.Context, rootRef string) (*TreeNode, error) {
	return s.store.GetTree(ctx, rootRef, 10)
}

func (s *Service) GetAncestors(ctx context.Context, ref string) ([]*ProjectNode, error) {
	return s.store.GetAncestors(ctx, ref)
}

func (s *Service) List(ctx context.Context, status *Status, limit, offset int) ([]*ProjectNode, int, error) {
	return s.store.ListByTenant(ctx, s.tenantID, status, limit, offset)
}

// ── 工具 ──────────────────────────────────────────────────────
func (s *Service) genRef(kind, name string) string {
	slug := strings.ToLower(strings.ReplaceAll(name, " ", "-"))
	if len(slug) > 20 {
		slug = slug[:20]
	}
	return fmt.Sprintf("v://zhongbei/project/%s/%s-%d", kind, slug, time.Now().UnixNano())
}

func (s *Service) computeHash(n *ProjectNode) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%s|%s|%s",
		n.Ref, n.OwnerRef, n.ContractorRef, n.ExecutorRef,
		n.Status, n.UpdatedAt.Format(time.RFC3339Nano))
	if n.PrevHash != nil {
		fmt.Fprintf(h, "|%s", *n.PrevHash)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ══════════════════════════════════════════════════════════════
//  PostgreSQL Store 实现
// ══════════════════════════════════════════════════════════════

type PGStore struct{ db *sql.DB }

func NewPGStore(db *sql.DB) Store { return &PGStore{db: db} }

func (s *PGStore) Create(ctx context.Context, n *ProjectNode) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO project_nodes (
			ref, tenant_id, parent_id, parent_ref, depth, path,
			name, owner_ref, contractor_ref, executor_ref, platform_ref,
			contract_ref, procurement_ref, genesis_ref,
			status, proof_hash, prev_hash, legacy_contract_id,
			created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
			$12,$13,$14,$15,$16,$17,$18,$19,$20
		) RETURNING id`,
		n.Ref, n.TenantID, n.ParentID, n.ParentRef, n.Depth, n.Path,
		n.Name, n.OwnerRef, n.ContractorRef, n.ExecutorRef, n.PlatformRef,
		n.ContractRef, n.ProcurementRef, n.GenesisRef,
		n.Status, n.ProofHash, n.PrevHash, n.LegacyContractID,
		n.CreatedAt, n.UpdatedAt,
	).Scan(&n.ID)
}

func (s *PGStore) Get(ctx context.Context, ref string) (*ProjectNode, error) {
	n := &ProjectNode{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		       name,owner_ref,contractor_ref,executor_ref,platform_ref,
		       contract_ref,procurement_ref,genesis_ref,
		       status,proof_hash,prev_hash,legacy_contract_id,
		       created_at,updated_at
		FROM project_nodes WHERE ref=$1`, ref,
	).Scan(
		&n.ID, &n.Ref, &n.TenantID, &n.ParentID, &n.ParentRef,
		&n.Depth, &n.Path, &n.Name,
		&n.OwnerRef, &n.ContractorRef, &n.ExecutorRef, &n.PlatformRef,
		&n.ContractRef, &n.ProcurementRef, &n.GenesisRef,
		&n.Status, &n.ProofHash, &n.PrevHash, &n.LegacyContractID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	return n, err
}

func (s *PGStore) GetByID(ctx context.Context, id int64) (*ProjectNode, error) {
	n := &ProjectNode{}
	err := s.db.QueryRowContext(ctx, `
		SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		       name,owner_ref,contractor_ref,executor_ref,platform_ref,
		       contract_ref,procurement_ref,genesis_ref,
		       status,proof_hash,prev_hash,legacy_contract_id,
		       created_at,updated_at
		FROM project_nodes WHERE id=$1`, id,
	).Scan(
		&n.ID, &n.Ref, &n.TenantID, &n.ParentID, &n.ParentRef,
		&n.Depth, &n.Path, &n.Name,
		&n.OwnerRef, &n.ContractorRef, &n.ExecutorRef, &n.PlatformRef,
		&n.ContractRef, &n.ProcurementRef, &n.GenesisRef,
		&n.Status, &n.ProofHash, &n.PrevHash, &n.LegacyContractID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	return n, err
}

func (s *PGStore) GetByLegacyContractID(ctx context.Context, contractID int64) (*ProjectNode, error) {
	n := &ProjectNode{}
	err := s.db.QueryRowContext(ctx,
		`SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		        name,owner_ref,contractor_ref,executor_ref,platform_ref,
		        contract_ref,procurement_ref,genesis_ref,
		        status,proof_hash,prev_hash,legacy_contract_id,
		        created_at,updated_at
		 FROM project_nodes WHERE legacy_contract_id=$1`, contractID,
	).Scan(
		&n.ID, &n.Ref, &n.TenantID, &n.ParentID, &n.ParentRef,
		&n.Depth, &n.Path, &n.Name,
		&n.OwnerRef, &n.ContractorRef, &n.ExecutorRef, &n.PlatformRef,
		&n.ContractRef, &n.ProcurementRef, &n.GenesisRef,
		&n.Status, &n.ProofHash, &n.PrevHash, &n.LegacyContractID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	return n, err
}

func (s *PGStore) GetChildren(ctx context.Context, parentRef string) ([]*ProjectNode, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		        name,owner_ref,contractor_ref,executor_ref,platform_ref,
		        contract_ref,procurement_ref,genesis_ref,
		        status,proof_hash,prev_hash,legacy_contract_id,
		        created_at,updated_at
		 FROM project_nodes WHERE parent_ref=$1 ORDER BY created_at`, parentRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

func (s *PGStore) GetAncestors(ctx context.Context, ref string) ([]*ProjectNode, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE ancestors AS (
			SELECT * FROM project_nodes WHERE ref = $1
			UNION ALL
			SELECT p.* FROM project_nodes p
			JOIN ancestors a ON p.ref = a.parent_ref
		)
		SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		       name,owner_ref,contractor_ref,executor_ref,platform_ref,
		       contract_ref,procurement_ref,genesis_ref,
		       status,proof_hash,prev_hash,legacy_contract_id,
		       created_at,updated_at
		FROM ancestors WHERE ref != $1 ORDER BY depth`, ref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

func (s *PGStore) GetTree(ctx context.Context, rootRef string, maxDepth int) (*TreeNode, error) {
	rows, err := s.db.QueryContext(ctx, `
		WITH RECURSIVE tree AS (
			SELECT * FROM project_nodes WHERE ref = $1
			UNION ALL
			SELECT p.* FROM project_nodes p
			JOIN tree t ON p.parent_ref = t.ref
			WHERE p.depth <= $2
		)
		SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		       name,owner_ref,contractor_ref,executor_ref,platform_ref,
		       contract_ref,procurement_ref,genesis_ref,
		       status,proof_hash,prev_hash,legacy_contract_id,
		       created_at,updated_at
		FROM tree ORDER BY depth, created_at`, rootRef, maxDepth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes, err := scanNodes(rows)
	if err != nil {
		return nil, err
	}
	return buildTree(nodes), nil
}

func (s *PGStore) UpdateStatus(ctx context.Context, ref string, status Status, proofHash string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE project_nodes SET status=$1, proof_hash=$2, updated_at=NOW() WHERE ref=$3`,
		status, proofHash, ref)
	return err
}

func (s *PGStore) UpdateRefs(ctx context.Context, ref string, contractRef, genesisRef *string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE project_nodes SET contract_ref=$1, genesis_ref=$2, updated_at=NOW() WHERE ref=$3`,
		contractRef, genesisRef, ref)
	return err
}

func (s *PGStore) ListByTenant(ctx context.Context, tenantID int, status *Status,
	limit, offset int) ([]*ProjectNode, int, error) {

	where := "tenant_id=$1"
	args := []any{tenantID}
	if status != nil {
		where += " AND status=$2"
		args = append(args, *status)
	}

	var total int
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM project_nodes WHERE "+where, args...).Scan(&total)

	args = append(args, limit, offset)
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		        name,owner_ref,contractor_ref,executor_ref,platform_ref,
		        contract_ref,procurement_ref,genesis_ref,
		        status,proof_hash,prev_hash,legacy_contract_id,
		        created_at,updated_at
		 FROM project_nodes WHERE `+where+
			fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`,
				len(args)-1, len(args)),
		args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	nodes, err := scanNodes(rows)
	return nodes, total, err
}

func (s *PGStore) ListByExecutor(ctx context.Context, executorRef string) ([]*ProjectNode, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,ref,tenant_id,parent_id,parent_ref,depth,path,
		        name,owner_ref,contractor_ref,executor_ref,platform_ref,
		        contract_ref,procurement_ref,genesis_ref,
		        status,proof_hash,prev_hash,legacy_contract_id,
		        created_at,updated_at
		 FROM project_nodes WHERE executor_ref=$1 ORDER BY created_at DESC`, executorRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNodes(rows)
}

// ── 工具函数 ──────────────────────────────────────────────────

func scanNodes(rows *sql.Rows) ([]*ProjectNode, error) {
	var nodes []*ProjectNode
	for rows.Next() {
		n := &ProjectNode{}
		if err := rows.Scan(
			&n.ID, &n.Ref, &n.TenantID, &n.ParentID, &n.ParentRef,
			&n.Depth, &n.Path, &n.Name,
			&n.OwnerRef, &n.ContractorRef, &n.ExecutorRef, &n.PlatformRef,
			&n.ContractRef, &n.ProcurementRef, &n.GenesisRef,
			&n.Status, &n.ProofHash, &n.PrevHash, &n.LegacyContractID,
			&n.CreatedAt, &n.UpdatedAt,
		); err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func buildTree(nodes []*ProjectNode) *TreeNode {
	if len(nodes) == 0 {
		return nil
	}
	nodeMap := make(map[string]*TreeNode)
	for _, n := range nodes {
		nodeMap[n.Ref] = &TreeNode{ProjectNode: *n}
	}
	var root *TreeNode
	for _, n := range nodeMap {
		if n.ParentRef == nil {
			root = n
		} else if parent, ok := nodeMap[*n.ParentRef]; ok {
			parent.Children = append(parent.Children, n)
		}
	}
	return root
}
