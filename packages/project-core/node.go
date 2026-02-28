// ============================================================
//  CoordOS — ProjectTree 核心协议定义
//  以项目为核心的资源与执行体配置平台
//  
//  两个核心操作：
//    CONFIGURE  配置资源与执行体的绑定关系
//    TRANSFORM  执行体在约束内消耗资源产出UTXO
// ============================================================

package projectcore

import (
	"time"
)

// ── VRef：v:// 统一资源引用 ──────────────────────────────────
// 格式：v://{tenant}/{kind}/{id}
// 示例：v://zhongbei/project/highway-001/design/structure
type VRef string

// ── 生命周期状态（管理线） ───────────────────────────────────
type LifecycleStatus string

const (
	StatusInitiated  LifecycleStatus = "INITIATED"   // 立项：定义资源需求
	StatusTendering  LifecycleStatus = "TENDERING"   // 招标：寻找执行体
	StatusContracted LifecycleStatus = "CONTRACTED"  // 定标：约束锁定
	StatusInProgress LifecycleStatus = "IN_PROGRESS" // 实施：资源消耗
	StatusDelivered  LifecycleStatus = "DELIVERED"   // 验收：质量校验
	StatusSettled    LifecycleStatus = "SETTLED"     // 结算：兑现分账
	StatusArchived   LifecycleStatus = "ARCHIVED"    // 归档：完结存证
)

// ── 允许的操作动词（五动词协议扩展） ────────────────────────
type ProjectVerb string

const (
	VerbConfigure ProjectVerb = "CONFIGURE" // 配置资源与执行体绑定
	VerbTransform ProjectVerb = "TRANSFORM" // 执行体消耗资源产出UTXO
	VerbReview    ProjectVerb = "REVIEW"    // 质量审核
	VerbSettle    ProjectVerb = "SETTLE"    // 结算触发
	VerbPay       ProjectVerb = "PAY"       // 对外付款（必须引用合同）
)

// ══════════════════════════════════════════════════════════════
//  ProjectNode：项目树的基本节点
//  可递归：每个节点都可以成为子项目库的根
// ══════════════════════════════════════════════════════════════

type ProjectNode struct {
	Ref       VRef   `json:"ref"`        // v://tenant/project/path
	ParentRef VRef   `json:"parent_ref"` // 空 = 根节点（总院层）
	TenantID  string `json:"tenant_id"`

	// ── 四方绑定（定标时锁定，不可变） ───────────────────────
	OwnerRef      VRef `json:"owner_ref"`      // 业主：谁发起，谁付钱
	ContractorRef VRef `json:"contractor_ref"` // 承包商：谁签合同，谁受益
	ExecutorRef   VRef `json:"executor_ref"`   // 执行体：输入资源约束集合
	PlatformRef   VRef `json:"platform_ref"`   // 平台：撮合+存证+抽成

	// ── 合同锚点（RULE-003：所有对外付款必须引用合同） ────────
	ContractRef    VRef `json:"contract_ref"`    // 对应层级的合同
	ProcurementRef VRef `json:"procurement_ref"` // 招采平台单号（必填）

	// ── 资源约束（Genesis UTXO，定标后锁定） ─────────────────
	GenesisUTXORef VRef              `json:"genesis_utxo_ref"`
	Constraint     ExecutorConstraint `json:"constraint"` // 执行体输入资源约束

	// ── 项目树结构 ────────────────────────────────────────────
	Depth    int    `json:"depth"`    // 0=根，1=一级子项目，以此类推
	Path     string `json:"path"`     // 完整路径字符串，便于查询
	Children []VRef `json:"children"` // 子项目节点引用

	// ── 里程碑（生命周期事件记录） ────────────────────────────
	Milestones []MilestoneEvent `json:"milestones"`
	Status     LifecycleStatus  `json:"status"`

	// ── 存证 ─────────────────────────────────────────────────
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	ProofHash  string    `json:"proof_hash"`  // 状态哈希，防篡改
	PrevHash   string    `json:"prev_hash"`   // 链式存证
}

// ══════════════════════════════════════════════════════════════
//  ExecutorConstraint：执行体 = 输入资源的约束集合
//  招标锁定的不是"谁来做"，而是"允许消耗的资源边界"
// ══════════════════════════════════════════════════════════════

type ExecutorConstraint struct {
	// 能力（Capability）：资源种类的准入约束
	Capability Capability `json:"capability"`

	// 能源（Energy）：资源总量的上限约束
	Energy Energy `json:"energy"`

	// Skills：资源消耗的转化路径
	Skills []Skill `json:"skills"`
}

type Capability struct {
	QualificationLevel string   `json:"qualification_level"` // 甲级/乙级/丙级
	Specializations    []string `json:"specializations"`     // 公路/桥梁/市政...
	MaxProjectScale    int64    `json:"max_project_scale"`   // 可承接最大规模（分）
	CertExpiry         string   `json:"cert_expiry"`         // 资质到期日
}

type Energy struct {
	AvailableMandays int     `json:"available_mandays"` // 剩余可用工日
	CapitalReserve   int64   `json:"capital_reserve"`   // 可垫资能力（分）
	ActiveProjects   int     `json:"active_projects"`   // 当前在手项目数
	Utilization      float64 `json:"utilization"`       // 当前负载率 0~1
}

type Skill struct {
	Name       string `json:"name"`        // "桥梁施工图设计"
	Unit       string `json:"unit"`        // 计量单位："延米/㎡/项"
	UnitPrice  int64  `json:"unit_price"`  // 单价（分）
	Tolerance  float64 `json:"tolerance"` // 单价容差 0.05=5%
	OutputKind string `json:"output_kind"` // 产出UTXO类型
}

// ══════════════════════════════════════════════════════════════
//  GenesisUTXO：资源约束的链上快照
//  定标完成瞬间创建，锁定四样：执行体绑定、工程量、单价、质量标准
// ══════════════════════════════════════════════════════════════

type GenesisUTXO struct {
	Ref        VRef   `json:"ref"`
	ProjectRef VRef   `json:"project_ref"`
	ParentRef  VRef   `json:"parent_ref"` // 父级GenesisUTXO（委托链）
	TenantID   string `json:"tenant_id"`

	// 资源边界
	TotalQuota     int64   `json:"total_quota"`     // 总工程量
	ConsumedQuota  int64   `json:"consumed_quota"`  // 已消耗量
	UnitPrice      int64   `json:"unit_price"`      // 基准单价（分）
	PriceTolerance float64 `json:"price_tolerance"` // 单价容差

	// 质量标准快照（锁定后不可变）
	QualityStandard string `json:"quality_standard"`

	// 里程碑付款节点
	PaymentNodes []PaymentNode `json:"payment_nodes"`

	Status    string `json:"status"` // ACTIVE/EXHAUSTED/REVOKED
	CreatedAt time.Time `json:"created_at"`
	ProofHash string `json:"proof_hash"`
}

type PaymentNode struct {
	Name        string  `json:"name"`         // "初设完成款"
	Ratio       float64 `json:"ratio"`        // 占比 0.3=30%
	Amount      int64   `json:"amount"`       // 金额（分）
	Trigger     string  `json:"trigger"`      // 触发条件
	MilestoneID string  `json:"milestone_id"` // 关联里程碑
	Status      string  `json:"status"`       // PENDING/TRIGGERED/PAID
}

// ══════════════════════════════════════════════════════════════
//  MilestoneEvent：里程碑事件（管理线与数据线的交汇点）
// ══════════════════════════════════════════════════════════════

type MilestoneEvent struct {
	ID          string          `json:"id"`
	ProjectRef  VRef            `json:"project_ref"`
	Name        string          `json:"name"`        // "施工图设计完成"
	Status      string          `json:"status"`      // PENDING/REACHED/SKIPPED
	ReachedAt   *time.Time      `json:"reached_at"`
	UTXORef     VRef            `json:"utxo_ref"`    // 触发的产出UTXO
	SignedBy    VRef            `json:"signed_by"`   // 必须是总院执行体（RULE-002）
	ProofHash   string          `json:"proof_hash"`
}

// ══════════════════════════════════════════════════════════════
//  合同最小字段集（图3）
//  合同是项目的属性，不是主体
// ══════════════════════════════════════════════════════════════

type Contract struct {
	Ref        VRef   `json:"ref"`
	ProjectRef VRef   `json:"project_ref"` // 挂到哪个项目节点
	TenantID   string `json:"tenant_id"`

	// 必填最小字段集
	ContractNo      string `json:"contract_no"`       // 合同编号
	ContractName    string `json:"contract_name"`     // 合同名称
	PartyA          VRef   `json:"party_a"`           // 甲方主体
	PartyB          VRef   `json:"party_b"`           // 乙方主体
	BranchRef       VRef   `json:"branch_ref"`        // 分院归属
	ManagerRef      VRef   `json:"manager_ref"`       // 项目负责人

	AmountWithTax    int64   `json:"amount_with_tax"`    // 含税金额（分）
	AmountWithoutTax int64   `json:"amount_without_tax"` // 不含税金额（分）
	TaxRate          float64 `json:"tax_rate"`           // 税率

	SignDate      string `json:"sign_date"`      // 签署日期
	EffectiveDate string `json:"effective_date"` // 生效日期
	ExpiryDate    string `json:"expiry_date"`    // 终止日期

	PaymentNodes   []PaymentNode `json:"payment_nodes"` // 付款节点
	SealStatus     string        `json:"seal_status"`   // UNSIGNED/SIGNED/PARTIAL
	AttachmentRefs []VRef        `json:"attachment_refs"`

	// 招采来源（总院铁律：对外付款必须有合同+招采记录）
	ProcurementRef VRef `json:"procurement_ref"`

	// 合同类型
	ContractKind string `json:"contract_kind"` // EXTERNAL_MAIN/SUBCONTRACT/INTERNAL/PROCUREMENT

	// 合同状态机
	Status    string    `json:"status"` // DRAFT/REVIEW/APPROVED/ACTIVE/CLOSED
	CreatedAt time.Time `json:"created_at"`
	ProofHash string    `json:"proof_hash"`
}

// ══════════════════════════════════════════════════════════════
//  RULE 校验接口
// ══════════════════════════════════════════════════════════════

// RuleViolationError：规则违规错误
type RuleViolationError struct {
	Rule    string
	Detail  string
	EventID string
}

func (e *RuleViolationError) Error() string {
	return "[" + e.Rule + "] " + e.Detail
}

// ProjectRules：所有项目级规则的统一校验入口
type ProjectRules interface {
	// RULE-001：TRANSFORM 必须在执行体资源约束之内
	EnforceRule001(evt ProjectEvent, ctx ProjectContext) error

	// RULE-002：总院在每个项目必须有实质参与
	EnforceRule002(projectRef VRef, ctx ProjectContext) error

	// RULE-003：对外付款必须引用有效合同
	EnforceRule003(evt ProjectEvent, ctx ProjectContext) error

	// RULE-004：子项目资源约束不得超过父项目剩余额度
	EnforceRule004(child *ProjectNode, parent *ProjectNode) error

	// RULE-005：委托链最深层必须有实际UTXO产出才能向上结算
	EnforceRule005(projectRef VRef, ctx ProjectContext) error
}

// ProjectEvent：项目操作事件（进入规则校验的统一入口）
type ProjectEvent struct {
	EventID    string      `json:"event_id"`
	ProjectRef VRef        `json:"project_ref"`
	TenantID   string      `json:"tenant_id"`
	Verb       ProjectVerb `json:"verb"`
	ActorRef   VRef        `json:"actor_ref"`
	ContractRef VRef       `json:"contract_ref,omitempty"` // PAY时必填
	PlanRef    VRef        `json:"plan_ref,omitempty"`     // TRANSFORM时必填
	Timestamp  time.Time   `json:"timestamp"`
	Signature  string      `json:"signature"`
	Payload    map[string]interface{} `json:"payload"`
}

// ProjectContext：租户隔离的项目上下文
type ProjectContext struct {
	TenantID    string
	ProjectTree ProjectTreeStore
	GenesisStore GenesisUTXOStore
	ContractStore ContractStore
	AuditStore  AuditStore
}

// ══════════════════════════════════════════════════════════════
//  存储接口（RocksDB 实现注入）
// ══════════════════════════════════════════════════════════════

type ProjectTreeStore interface {
	GetNode(ref VRef) (*ProjectNode, error)
	GetChildren(ref VRef) ([]*ProjectNode, error)
	GetAncestors(ref VRef) ([]*ProjectNode, error)
	CreateNode(node *ProjectNode) error
	UpdateStatus(ref VRef, status LifecycleStatus) error
	// 校验子项目约束不变式
	ValidateChildConstraint(child *ProjectNode) error
}

type GenesisUTXOStore interface {
	Get(ref VRef) (*GenesisUTXO, error)
	Create(utxo *GenesisUTXO) error
	ConsumeQuota(ref VRef, amount int64) (*GenesisUTXO, error)
	GetRemainingQuota(ref VRef) (int64, error)
}

type ContractStore interface {
	Get(ref VRef) (*Contract, error)
	GetByProject(projectRef VRef) ([]*Contract, error)
	GetRemainingAmount(ref VRef) (int64, error)
	// RULE-003 校验：合同是否有效且余额充足
	ValidatePayment(contractRef VRef, amount int64) error
}

type AuditStore interface {
	RecordEvent(evt ProjectEvent, tenantID string) (string, error)
	RecordViolation(rule string, evt ProjectEvent, detail string) (string, error)
}

// ══════════════════════════════════════════════════════════════
//  v:// 路由规范
//  路径即委托链，深度无限，每层都是完整项目节点
// ══════════════════════════════════════════════════════════════

// v://{tenant}/project/{id}                    根项目
// v://{tenant}/project/{id}/{sub}              一级子项目
// v://{tenant}/project/{id}/{sub}/{subsub}     二级子项目
//
// 示例：
// v://zhongbei/project/highway-001
// v://zhongbei/project/highway-001/design
// v://zhongbei/project/highway-001/design/structure
// v://zhongbei/project/highway-001/design/structure/pile

func ProjectRefFromPath(tenant, path string) VRef {
	return VRef("v://" + tenant + "/project/" + path)
}

func ChildRef(parent VRef, childID string) VRef {
	return VRef(string(parent) + "/" + childID)
}
