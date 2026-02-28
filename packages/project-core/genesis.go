// ============================================================
//  CoordOS — 阶段零核心协议定义
//
//  两个完整定义：
//  ① GenesisUTXO 裂变逻辑
//  ② 项目节点状态机
// ============================================================

package projectcore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// ══════════════════════════════════════════════════════════════
//
//	一、GenesisUTXO 裂变逻辑
//
//	核心不变式：
//	  子节点 GenesisUTXO 总和 ≤ 父节点 GenesisUTXO 剩余额度
//
//	裂变 = 父节点授权子节点消耗资源的行为
//	每次裂变都是一次不可逆的主权转移
//
// ══════════════════════════════════════════════════════════════

// GenesisUTXOStatus 额度状态
type GenesisUTXOStatus string

const (
	GenesisActive    GenesisUTXOStatus = "ACTIVE"    // 正常，可消耗
	GenesisExhausted GenesisUTXOStatus = "EXHAUSTED" // 额度耗尽
	GenesisFrozen    GenesisUTXOStatus = "FROZEN"    // 冻结（争议/违规）
	GenesisRevoked   GenesisUTXOStatus = "REVOKED"   // 撤销（合同解除）
	GenesisClosed    GenesisUTXOStatus = "CLOSED"    // 正常关闭（结算完成）
)

// GenesisUTXOFull 完整的 Genesis UTXO 定义
type GenesisUTXOFull struct {
	Ref       VRef   `json:"ref"`        // v://{tenant}/genesis/{id}
	ProjectRef VRef  `json:"project_ref"`
	ParentRef VRef   `json:"parent_ref"` // 父级 GenesisUTXO，空=根

	// ── 额度定义（裂变时锁定，不可变） ───────────────────────
	TotalQuota    int64  `json:"total_quota"`    // 总授权额度（分或工程量单位）
	QuotaUnit     string `json:"quota_unit"`     // 额度单位："CNY分/延米/㎡/项"
	UnitPrice     int64  `json:"unit_price"`     // 基准单价（分/单位）
	PriceTolerance float64 `json:"price_tolerance"` // 单价容差 0.05=5%

	// ── 消耗追踪（并发安全，每次 TRANSFORM 更新） ────────────
	mu              sync.Mutex `json:"-"`
	ConsumedQuota   int64      `json:"consumed_quota"`   // 已消耗额度
	AllocatedQuota  int64      `json:"allocated_quota"`  // 已分配给子节点
	FrozenQuota     int64      `json:"frozen_quota"`     // 冻结中（待结算）

	// ── 委托链上下文 ──────────────────────────────────────────
	Depth         int    `json:"depth"`          // 委托链深度，0=根
	AncestorRefs  []VRef `json:"ancestor_refs"`  // 祖先链，用于完整性校验
	ChildRefs     []VRef `json:"child_refs"`     // 已裂变的子 GenesisUTXO

	// ── 执行体约束（从 ProjectNode 快照，裂变时固化） ─────────
	AllowedExecutors []VRef           `json:"allowed_executors"` // 允许的执行体
	AllowedSkills    []string         `json:"allowed_skills"`    // 允许的 Skill 类型
	Constraint       ExecutorConstraint `json:"constraint"`

	// ── 质量标准（快照，不可变） ──────────────────────────────
	QualityStandard  string `json:"quality_standard"`
	QualityThreshold int    `json:"quality_threshold"` // 最低合格分（0-100）

	// ── 付款节点（合同付款条件的映射） ───────────────────────
	PaymentNodes []PaymentNode `json:"payment_nodes"`

	// ── 状态与存证 ────────────────────────────────────────────
	Status    GenesisUTXOStatus `json:"status"`
	CreatedAt time.Time         `json:"created_at"`
	LockedAt  *time.Time        `json:"locked_at"`  // 定标时刻
	ClosedAt  *time.Time        `json:"closed_at"`
	ProofHash string            `json:"proof_hash"` // SHA256(全部字段)
	PrevHash  string            `json:"prev_hash"`  // 链式存证
	TenantID  string            `json:"tenant_id"`
}

// RemainingQuota 剩余可用额度（线程安全）
func (g *GenesisUTXOFull) RemainingQuota() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.TotalQuota - g.ConsumedQuota - g.AllocatedQuota - g.FrozenQuota
}

// ComputeProofHash 计算防篡改哈希
func (g *GenesisUTXOFull) ComputeProofHash() string {
	data := map[string]interface{}{
		"ref":           g.Ref,
		"project_ref":   g.ProjectRef,
		"parent_ref":    g.ParentRef,
		"total_quota":   g.TotalQuota,
		"unit_price":    g.UnitPrice,
		"consumed":      g.ConsumedQuota,
		"allocated":     g.AllocatedQuota,
		"status":        g.Status,
		"created_at":    g.CreatedAt.Unix(),
		"prev_hash":     g.PrevHash,
	}
	b, _ := json.Marshal(data)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// ── 裂变请求 ────────────────────────────────────────────────

// FissionRequest 裂变请求：父节点授权子节点的完整参数
type FissionRequest struct {
	// 来源
	ParentGenesisRef VRef `json:"parent_genesis_ref"`
	ParentProjectRef VRef `json:"parent_project_ref"`

	// 目标子节点
	ChildProjectRef  VRef `json:"child_project_ref"`
	ChildExecutorRef VRef `json:"child_executor_ref"`

	// 裂变额度（必须 ≤ 父节点剩余额度）
	RequestedQuota   int64   `json:"requested_quota"`
	RequestedUnit    string  `json:"requested_unit"`
	NegotiatedPrice  int64   `json:"negotiated_price"`  // 谈判后单价
	PriceTolerance   float64 `json:"price_tolerance"`

	// 子节点允许的 Skills（必须是父节点 Skills 的子集）
	AllowedSkills    []string `json:"allowed_skills"`

	// 合同和招采锚点（RULE-003：裂变必须有合同支撑）
	ContractRef      VRef `json:"contract_ref"`
	ProcurementRef   VRef `json:"procurement_ref"`

	// 质量标准（不得低于父节点）
	QualityStandard  string `json:"quality_standard"`
	QualityThreshold int    `json:"quality_threshold"`

	// 付款节点
	PaymentNodes     []PaymentNode `json:"payment_nodes"`

	// 操作人（必须是父节点的 ContractorRef 或 PlatformRef）
	RequestedBy      VRef      `json:"requested_by"`
	RequestedAt      time.Time `json:"requested_at"`
}

// FissionResult 裂变结果
type FissionResult struct {
	ChildGenesis  *GenesisUTXOFull `json:"child_genesis"`
	ParentUpdated *GenesisUTXOFull `json:"parent_updated"`
	ProofEvent    *FissionEvent    `json:"proof_event"`
}

// FissionEvent 裂变存证事件（不可变记录）
type FissionEvent struct {
	EventID         string    `json:"event_id"`
	ParentGenesisRef VRef     `json:"parent_genesis_ref"`
	ChildGenesisRef  VRef     `json:"child_genesis_ref"`
	AllocatedQuota  int64     `json:"allocated_quota"`
	ParentRemaining int64     `json:"parent_remaining"`
	ContractRef     VRef      `json:"contract_ref"`
	ProcurementRef  VRef      `json:"procurement_ref"`
	RequestedBy     VRef      `json:"requested_by"`
	Timestamp       time.Time `json:"timestamp"`
	ProofHash       string    `json:"proof_hash"`
}

// ── 裂变引擎 ────────────────────────────────────────────────

// GenesisUTXOFullStore 裂变引擎专用存储接口（操作完整类型）
type GenesisUTXOFullStore interface {
	GetFull(ref VRef) (*GenesisUTXOFull, error)
	CreateFull(utxo *GenesisUTXOFull) error
	UpdateFull(utxo *GenesisUTXOFull) error
}

type FissionEngine struct {
	store  GenesisUTXOFullStore
	audit  AuditStore
	mu     sync.Mutex // 全局裂变锁，防止并发超分配
}

func NewFissionEngine(store GenesisUTXOFullStore, audit AuditStore) *FissionEngine {
	return &FissionEngine{store: store, audit: audit}
}

// Fission 执行裂变
// 这是委托链向下延伸的唯一入口
func (e *FissionEngine) Fission(req FissionRequest) (*FissionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// ── 校验父节点存在且状态正常 ─────────────────────────────
	parent, err := e.store.GetFull(req.ParentGenesisRef)
	if err != nil {
		return nil, fmt.Errorf("父节点不存在: %w", err)
	}
	if parent.Status != GenesisActive {
		return nil, &RuleViolationError{
			Rule:   "FISSION",
			Detail: fmt.Sprintf("父节点状态异常: %s，无法裂变", parent.Status),
		}
	}

	// ── RULE-004：子节点额度不超父节点剩余 ───────────────────
	remaining := parent.RemainingQuota()
	if req.RequestedQuota > remaining {
		return nil, &RuleViolationError{
			Rule: "RULE-004",
			Detail: fmt.Sprintf(
				"裂变额度超限：申请 %d，父节点剩余 %d（总量%d-已消耗%d-已分配%d-冻结%d）",
				req.RequestedQuota, remaining,
				parent.TotalQuota, parent.ConsumedQuota,
				parent.AllocatedQuota, parent.FrozenQuota,
			),
		}
	}

	// ── RULE-003：裂变必须有合同和招采记录 ───────────────────
	if req.ContractRef == "" {
		return nil, &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "裂变必须提供合同引用（contract_ref）",
		}
	}
	if req.ProcurementRef == "" {
		return nil, &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "裂变必须提供招采记录（procurement_ref），直接指派也需留痕",
		}
	}

	// ── Skills 必须是父节点的子集 ─────────────────────────────
	if err := validateSkillsSubset(req.AllowedSkills, parent.AllowedSkills); err != nil {
		return nil, &RuleViolationError{
			Rule:   "FISSION",
			Detail: "子节点 Skills 超出父节点允许范围: " + err.Error(),
		}
	}

	// ── 单价不得高于父节点单价（含容差） ─────────────────────
	maxPrice := float64(parent.UnitPrice) * (1 + parent.PriceTolerance)
	if float64(req.NegotiatedPrice) > maxPrice {
		return nil, &RuleViolationError{
			Rule:   "FISSION",
			Detail: fmt.Sprintf("子节点单价 %d 超出父节点允许上限 %.0f", req.NegotiatedPrice, maxPrice),
		}
	}

	// ── 质量标准不得低于父节点 ────────────────────────────────
	if req.QualityThreshold < parent.QualityThreshold {
		return nil, &RuleViolationError{
			Rule:   "FISSION",
			Detail: fmt.Sprintf("质量标准不得降低：父节点 %d，子节点申请 %d", parent.QualityThreshold, req.QualityThreshold),
		}
	}

	// ── 构建子 GenesisUTXO ────────────────────────────────────
	now := time.Now().UTC()
	childRef := VRef(fmt.Sprintf("%s/genesis/%s", req.ChildProjectRef, generateID()))

	// 祖先链 = 父节点祖先链 + 父节点自身
	ancestorRefs := append(parent.AncestorRefs, parent.Ref)

	child := &GenesisUTXOFull{
		Ref:              childRef,
		ProjectRef:       req.ChildProjectRef,
		ParentRef:        parent.Ref,
		TotalQuota:       req.RequestedQuota,
		QuotaUnit:        req.RequestedUnit,
		UnitPrice:        req.NegotiatedPrice,
		PriceTolerance:   req.PriceTolerance,
		ConsumedQuota:    0,
		AllocatedQuota:   0,
		FrozenQuota:      0,
		Depth:            parent.Depth + 1,
		AncestorRefs:     ancestorRefs,
		ChildRefs:        []VRef{},
		AllowedExecutors: []VRef{req.ChildExecutorRef},
		AllowedSkills:    req.AllowedSkills,
		QualityStandard:  req.QualityStandard,
		QualityThreshold: req.QualityThreshold,
		PaymentNodes:     req.PaymentNodes,
		Status:           GenesisActive,
		CreatedAt:        now,
		LockedAt:         &now,
		TenantID:         parent.TenantID,
		PrevHash:         parent.ProofHash,
	}
	child.ProofHash = child.ComputeProofHash()

	// ── 更新父节点已分配额度 ──────────────────────────────────
	parent.mu.Lock()
	parent.AllocatedQuota += req.RequestedQuota
	parent.ChildRefs = append(parent.ChildRefs, childRef)
	parent.ProofHash = parent.ComputeProofHash()
	parent.mu.Unlock()

	// ── 持久化 ────────────────────────────────────────────────
	if err := e.store.CreateFull(child); err != nil {
		return nil, fmt.Errorf("子 GenesisUTXO 创建失败: %w", err)
	}
	if err := e.store.UpdateFull(parent); err != nil {
		return nil, fmt.Errorf("父节点更新失败: %w", err)
	}

	// ── 存证裂变事件 ──────────────────────────────────────────
	fissionEvent := &FissionEvent{
		EventID:          generateID(),
		ParentGenesisRef: parent.Ref,
		ChildGenesisRef:  childRef,
		AllocatedQuota:   req.RequestedQuota,
		ParentRemaining:  parent.RemainingQuota(),
		ContractRef:      req.ContractRef,
		ProcurementRef:   req.ProcurementRef,
		RequestedBy:      req.RequestedBy,
		Timestamp:        now,
	}
	b, _ := json.Marshal(fissionEvent)
	h := sha256.Sum256(b)
	fissionEvent.ProofHash = hex.EncodeToString(h[:])

	return &FissionResult{
		ChildGenesis:  child,
		ParentUpdated: parent,
		ProofEvent:    fissionEvent,
	}, nil
}

// validateSkillsSubset 校验子集关系
func validateSkillsSubset(child, parent []string) error {
	parentSet := make(map[string]bool)
	for _, s := range parent {
		parentSet[s] = true
	}
	for _, s := range child {
		if !parentSet[s] {
			return fmt.Errorf("'%s' 不在父节点 Skills 范围内", s)
		}
	}
	return nil
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// ══════════════════════════════════════════════════════════════
//
//	二、项目节点状态机
//
//	管理线：控制"能做什么"
//	数据线：记录"做了什么"
//	两条线通过里程碑事件交汇
//
// ══════════════════════════════════════════════════════════════

// ── 状态转移表 ───────────────────────────────────────────────
//
// 每个状态定义：
//   进入条件    满足什么才能进入此状态
//   解锁操作    此状态下允许的操作动词
//   锁住操作    此状态下明确禁止的操作
//   退出条件    满足什么才能离开此状态

type StateDefinition struct {
	State       LifecycleStatus
	Description string

	// 进入条件（所有条件必须满足）
	EntryConditions []string

	// 此状态解锁的操作（白名单）
	AllowedVerbs []ProjectVerb

	// 此状态明确禁止的操作（黑名单，优先于白名单）
	ForbiddenVerbs []ProjectVerb

	// 可以转移到哪些状态
	ValidTransitions []LifecycleStatus

	// 自动触发转移的条件
	AutoTransitionTo *LifecycleStatus
	AutoTrigger      string
}

// StateTable 完整状态转移表
var StateTable = map[LifecycleStatus]StateDefinition{

	// ── INITIATED：立项 ──────────────────────────────────────
	StatusInitiated: {
		State:       StatusInitiated,
		Description: "项目已立项，正在定义资源需求",
		EntryConditions: []string{
			"ProjectNode.OwnerRef 不为空",
			"ProjectNode.ParentRef 已验证（非根节点时）",
			"RULE-004 通过（父节点剩余额度充足）",
		},
		AllowedVerbs: []ProjectVerb{
			VerbConfigure, // 配置标段、量价、执行体需求
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbTransform, // 禁止：未定标不能开工
			VerbPay,       // 禁止：未有合同不能付款
			VerbSettle,    // 禁止：未完成不能结算
		},
		ValidTransitions: []LifecycleStatus{
			StatusTendering, // 发布招标需求
		},
	},

	// ── TENDERING：招标 ──────────────────────────────────────
	StatusTendering: {
		State:       StatusTendering,
		Description: "招标进行中，寻找执行体",
		EntryConditions: []string{
			"Procurement 记录已创建（招采平台单号）",
			"资源需求已完整定义（Segment + 量价）",
		},
		AllowedVerbs: []ProjectVerb{
			VerbConfigure, // 执行体应标、评标
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbTransform, // 禁止：未定标不能开工
			VerbPay,       // 禁止：未签合同不能付款
			VerbSettle,    // 禁止：未完成不能结算
		},
		ValidTransitions: []LifecycleStatus{
			StatusContracted, // 定标完成
			StatusInitiated,  // 流标，退回立项
		},
	},

	// ── CONTRACTED：定标 ─────────────────────────────────────
	StatusContracted: {
		State:       StatusContracted,
		Description: "已定标，合同签署，GenesisUTXO锁定",
		EntryConditions: []string{
			"Contract.Status = ACTIVE",
			"Contract.ProcurementRef 不为空",
			"GenesisUTXO 已创建且 Status = ACTIVE",
			"四方绑定完成（Owner/Contractor/Executor/Platform）",
			"RULE-003 通过（合同有效）",
		},
		AllowedVerbs: []ProjectVerb{
			VerbConfigure, // 细化任务分解
			VerbTransform, // 可以开始执行
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbSettle, // 禁止：未完成不能结算
		},
		ValidTransitions: []LifecycleStatus{
			StatusInProgress, // 首个UTXO产出后自动转移
		},
		AutoTransitionTo: func() *LifecycleStatus { s := StatusInProgress; return &s }(),
		AutoTrigger:      "首个 UTXO Output 登记完成",
	},

	// ── IN_PROGRESS：实施 ────────────────────────────────────
	StatusInProgress: {
		State:       StatusInProgress,
		Description: "实施阶段，执行体消耗资源产出UTXO",
		EntryConditions: []string{
			"至少一个 UTXO Output 已登记",
		},
		AllowedVerbs: []ProjectVerb{
			VerbTransform, // 主要动作：执行体消耗资源产出UTXO
			VerbReview,    // 阶段性审查
			VerbPay,       // 阶段付款（必须引用合同+付款节点触发条件）
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbSettle, // 禁止：未验收不能最终结算
		},
		ValidTransitions: []LifecycleStatus{
			StatusDelivered, // 所有里程碑达成，触发交付
		},
	},

	// ── DELIVERED：交付验收 ──────────────────────────────────
	StatusDelivered: {
		State:       StatusDelivered,
		Description: "成果已交付，待质量验收",
		EntryConditions: []string{
			"所有必要里程碑已达成（Milestone.Status = REACHED）",
			"总院审图里程碑已完成（RULE-002 前置条件）",
			"交付物清单已核对",
		},
		AllowedVerbs: []ProjectVerb{
			VerbReview, // 质量验收、审图
			VerbPay,    // 验收款支付
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbTransform, // 禁止：验收后不能再产出UTXO
		},
		ValidTransitions: []LifecycleStatus{
			StatusSettled,    // 验收通过，触发结算
			StatusInProgress, // 验收不通过，退回实施
		},
	},

	// ── SETTLED：结算 ────────────────────────────────────────
	StatusSettled: {
		State:       StatusSettled,
		Description: "结算完成，钱包已到账",
		EntryConditions: []string{
			"RULE-002 通过（总院实质参与验证）",
			"RULE-005 通过（所有子节点已结算）",
			"质量验收通过（QualityScore >= QualityThreshold）",
			"所有付款节点已核销",
		},
		AllowedVerbs: []ProjectVerb{
			VerbPay, // 尾款、质保金释放
		},
		ForbiddenVerbs: []ProjectVerb{
			VerbTransform, // 禁止：不能再产出
			VerbConfigure, // 禁止：不能修改工程量
		},
		ValidTransitions: []LifecycleStatus{
			StatusArchived, // 质保期满，归档
		},
		AutoTransitionTo: func() *LifecycleStatus { s := StatusArchived; return &s }(),
		AutoTrigger:      "质保金释放完成",
	},

	// ── ARCHIVED：归档 ───────────────────────────────────────
	StatusArchived: {
		State:       StatusArchived,
		Description: "项目完结，存证链封存",
		EntryConditions: []string{
			"质保金已全部释放",
			"所有争议已解决",
			"存证链完整性校验通过",
		},
		AllowedVerbs:     []ProjectVerb{}, // 只读，任何操作都不允许
		ForbiddenVerbs:   []ProjectVerb{VerbConfigure, VerbTransform, VerbPay, VerbSettle, VerbReview},
		ValidTransitions: []LifecycleStatus{}, // 终态，不可转移
	},
}

// ── 状态机引擎 ───────────────────────────────────────────────

type StateMachine struct {
	store ProjectTreeStore
	audit AuditStore
}

func NewStateMachine(store ProjectTreeStore, audit AuditStore) *StateMachine {
	return &StateMachine{store: store, audit: audit}
}

// ValidateVerb 校验当前状态下是否允许此操作
func (sm *StateMachine) ValidateVerb(node *ProjectNode, verb ProjectVerb) error {
	def, ok := StateTable[node.Status]
	if !ok {
		return fmt.Errorf("未知状态: %s", node.Status)
	}

	// 先检查黑名单（禁止优先）
	for _, forbidden := range def.ForbiddenVerbs {
		if forbidden == verb {
			return &RuleViolationError{
				Rule:   "STATE_MACHINE",
				Detail: fmt.Sprintf("状态 [%s] 下禁止操作 [%s]", node.Status, verb),
			}
		}
	}

	// 再检查白名单
	for _, allowed := range def.AllowedVerbs {
		if allowed == verb {
			return nil
		}
	}

	return &RuleViolationError{
		Rule:   "STATE_MACHINE",
		Detail: fmt.Sprintf("状态 [%s] 下不允许操作 [%s]", node.Status, verb),
	}
}

// Transition 状态转移
// 转移前校验进入条件，转移后检查自动触发
func (sm *StateMachine) Transition(
	node *ProjectNode,
	targetStatus LifecycleStatus,
	operator VRef,
	ctx ProjectContext,
) error {
	currentDef := StateTable[node.Status]

	// ── 校验转移路径合法 ─────────────────────────────────────
	validTarget := false
	for _, valid := range currentDef.ValidTransitions {
		if valid == targetStatus {
			validTarget = true
			break
		}
	}
	if !validTarget {
		return &RuleViolationError{
			Rule:   "STATE_MACHINE",
			Detail: fmt.Sprintf("不允许从 [%s] 转移到 [%s]", node.Status, targetStatus),
		}
	}

	// ── 校验目标状态的进入条件 ────────────────────────────────
	if err := sm.validateEntryConditions(node, targetStatus, ctx); err != nil {
		return err
	}

	// ── 特殊状态的规则前置校验 ────────────────────────────────
	if targetStatus == StatusSettled {
		rules := NewProjectRules()
		if err := rules.EnforceRule002(node.Ref, ctx); err != nil {
			return err
		}
		if err := rules.EnforceRule005(node.Ref, ctx); err != nil {
			return err
		}
	}

	// ── 执行转移 ──────────────────────────────────────────────
	prevStatus := node.Status
	node.Status = targetStatus
	node.UpdatedAt = time.Now().UTC()

	// 更新存证哈希
	data := map[string]interface{}{
		"ref":         node.Ref,
		"prev_status": prevStatus,
		"new_status":  targetStatus,
		"operator":    operator,
		"timestamp":   node.UpdatedAt.Unix(),
		"prev_hash":   node.ProofHash,
	}
	b, _ := json.Marshal(data)
	h := sha256.Sum256(b)
	node.PrevHash = node.ProofHash
	node.ProofHash = hex.EncodeToString(h[:])

	if err := ctx.ProjectTree.UpdateStatus(node.Ref, targetStatus); err != nil {
		return fmt.Errorf("状态更新失败: %w", err)
	}

	// ── 记录转移存证 ──────────────────────────────────────────
	ctx.AuditStore.RecordEvent(ProjectEvent{
		EventID:    generateID(),
		ProjectRef: node.Ref,
		TenantID:   node.TenantID,
		Verb:       VerbConfigure,
		ActorRef:   operator,
		Timestamp:  node.UpdatedAt,
		Payload: map[string]interface{}{
			"transition":   fmt.Sprintf("%s → %s", prevStatus, targetStatus),
			"proof_hash":   node.ProofHash,
		},
	}, node.TenantID)

	return nil
}

// validateEntryConditions 校验目标状态的进入条件
// 这里将条件描述转化为实际校验逻辑
func (sm *StateMachine) validateEntryConditions(
	node *ProjectNode,
	target LifecycleStatus,
	ctx ProjectContext,
) error {
	switch target {

	case StatusTendering:
		if node.OwnerRef == "" {
			return &RuleViolationError{Rule: "STATE_ENTRY", Detail: "立项必须指定业主（owner_ref）"}
		}

	case StatusContracted:
		// 合同必须 ACTIVE
		contract, err := ctx.ContractStore.Get(node.ContractRef)
		if err != nil || contract.Status != "ACTIVE" {
			return &RuleViolationError{Rule: "STATE_ENTRY", Detail: "合同未生效，无法完成定标"}
		}
		// 招采记录必须存在
		if contract.ProcurementRef == "" {
			return &RuleViolationError{Rule: "STATE_ENTRY", Detail: "缺少招采记录，无法完成定标"}
		}
		// 四方绑定必须完整
		if node.ExecutorRef == "" || node.ContractorRef == "" {
			return &RuleViolationError{Rule: "STATE_ENTRY", Detail: "四方绑定不完整，无法完成定标"}
		}
		// GenesisUTXO 必须已创建
		genesis, err := ctx.GenesisStore.Get(node.GenesisUTXORef)
		if err != nil || genesis.Status != string(GenesisActive) {
			return &RuleViolationError{Rule: "STATE_ENTRY", Detail: "GenesisUTXO 未就绪，无法完成定标"}
		}

	case StatusSettled:
		// 子节点必须全部结算
		children, err := ctx.ProjectTree.GetChildren(node.Ref)
		if err != nil {
			return err
		}
		for _, child := range children {
			if child.Status != StatusSettled && child.Status != StatusArchived {
				return &RuleViolationError{
					Rule:   "RULE-005",
					Detail: fmt.Sprintf("子项目 %s 尚未完成结算", child.Ref),
				}
			}
		}
	}

	return nil
}

// ── 状态权限查询（UI 层用） ──────────────────────────────────

// AllowedActions 返回当前状态允许的所有操作（供前端渲染）
func AllowedActions(status LifecycleStatus) []ProjectVerb {
	def, ok := StateTable[status]
	if !ok {
		return nil
	}
	return def.AllowedVerbs
}

// CanTransitionTo 返回当前状态可以转移到的目标状态列表
func CanTransitionTo(status LifecycleStatus) []LifecycleStatus {
	def, ok := StateTable[status]
	if !ok {
		return nil
	}
	return def.ValidTransitions
}

// ── 完整状态转移图（文档用） ─────────────────────────────────
//
//  INITIATED ──────────────────────────────────────► TENDERING
//      │                                                  │
//      │                                     流标         │ 定标完成
//      │                                   ◄─────────────┘
//      │                                                  │
//      │                                             CONTRACTED
//      │                                                  │
//      │                                       首个UTXO产出（自动）
//      │                                                  ▼
//      │                                           IN_PROGRESS
//      │                                            │       │
//      │                              验收不通过    │       │ 所有里程碑达成
//      │                            ◄──────────────┘       ▼
//      │                                               DELIVERED
//      │                                                  │
//      │                                       验收通过    │
//      │                                                  ▼
//      │                                             SETTLED
//      │                                                  │
//      │                                      质保金释放（自动）
//      │                                                  ▼
//      └──────────────────────────────────────────► ARCHIVED
//
