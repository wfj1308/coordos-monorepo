// ============================================================
//  CoordOS — Resolver 执行体寻址服务
//  把"谁能干、谁能签、谁能收钱、谁能背责"变成可计算的问题
//
//  三个核心操作：
//    Verify  — 校验某个执行体在特定场景下是否合规（RULE-002落地）
//    Resolve — 给定任务约束，返回候选执行体列表
//    Occupied — 查询执行体当前资源占用（防挂靠超额）
// ============================================================

package resolver

import (
	"time"

	"coordos/vuri"
)

// ── 证书类型枚举 ─────────────────────────────────────────────

type CertType string

const (
	// 个人注册证书
	CertRegStruct CertType = "REG_STRUCT" // 注册结构工程师
	CertRegArch   CertType = "REG_ARCH"   // 注册建筑师
	CertRegElec   CertType = "REG_ELEC"   // 注册电气工程师
	CertRegGeo    CertType = "REG_GEO"    // 注册岩土工程师
	CertRegCivil  CertType = "REG_CIVIL"  // 注册土木工程师
	CertRegMech   CertType = "REG_MECH"   // 注册机械工程师
	CertSeniorEng CertType = "SENIOR_ENG" // 高级工程师（职称）
	CertChiefEng  CertType = "CHIEF_ENG"  // 总工程师（岗位资格）

	// 企业资质
	CertComprehA  CertType = "COMP_COMPREHENSIVE_A" // 工程设计综合甲级
	CertIndustryA CertType = "COMP_INDUSTRY_A"      // 行业甲级
	CertIndustryB CertType = "COMP_INDUSTRY_B"      // 行业乙级
	CertSpecialA  CertType = "COMP_SPECIAL_A"       // 专项甲级

	// 权力资源（不是证书，是授权）
	RightReviewStamp CertType = "RIGHT_REVIEW_STAMP" // 审图盖章权
	RightInvoice     CertType = "RIGHT_INVOICE"      // 开票权
	RightHeadOffice  CertType = "RIGHT_HEAD_OFFICE"  // 总院身份认证
)

// HolderType 证书/权限持有方类型
type HolderType string

const (
	HolderPerson  HolderType = "PERSON"  // 个人
	HolderCompany HolderType = "COMPANY" // 企业
)

// ── 证书/资质记录 ─────────────────────────────────────────────

// Credential 一条资质记录，对应 credentials 表的一行
type Credential struct {
	ID         int64
	HolderRef  vuri.VRef // executor_ref 或 company_ref
	HolderType HolderType
	CertType   CertType
	CertNumber string
	IssuedAt   *time.Time
	ExpiresAt  *time.Time // nil = 长期有效（企业资质类）
	Scope      []vuri.VRef   // 允许执行的 SPU ref 列表，空=不限
	Status     string     // ACTIVE / EXPIRED / REVOKED / SUSPENDED
	TenantID   int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (c *Credential) IsValid(at time.Time) bool {
	if c.Status != "ACTIVE" {
		return false
	}
	if c.ExpiresAt != nil && at.After(*c.ExpiresAt) {
		return false
	}
	return true
}

// ── Verify 输入输出 ───────────────────────────────────────────

// Action 执行体在某个场景下请求执行的动作
type Action string

const (
	ActionIssueReviewCert Action = "ISSUE_REVIEW_CERT" // 签发审图合格证（RULE-002核心）
	ActionIssueDelivery   Action = "ISSUE_DELIVERY"    // 签发交付证明
	ActionIssueInvoice    Action = "ISSUE_INVOICE"     // 开票
	ActionSignContract    Action = "SIGN_CONTRACT"     // 合同签署
	ActionApprovePayment  Action = "APPROVE_PAYMENT"   // 付款审批
	ActionExecuteSPU      Action = "EXECUTE_SPU"       // 执行某个 SPU
)

// VerifyInput 校验请求
type VerifyInput struct {
	ExecutorRef vuri.VRef // 要校验的执行体
	ProjectRef  vuri.VRef // 项目上下文（可为空，用于跨项目权限）
	SPURef      vuri.VRef // 关联 SPU（用于从 SPU 规格里读取资质要求）
	Action      Action    // 要执行的动作
	ValidOn     time.Time // 校验时间点，默认 time.Now()
	TenantID    int
}

// VerifyResult 校验结果
type VerifyResult struct {
	Pass    bool
	Reasons []VerifyReason // 每条资质要求的校验明细
	Summary string         // 可读摘要
}

// VerifyReason 单条校验明细
type VerifyReason struct {
	Requirement string // 要求描述，如"需要注册结构工程师证书"
	Pass        bool
	Evidence    *Credential // 满足要求的证书记录（Pass=true时有值）
	FailReason  string      // 不满足时的原因
}

func (r *VerifyResult) AddPass(req string, cred *Credential) {
	r.Reasons = append(r.Reasons, VerifyReason{
		Requirement: req,
		Pass:        true,
		Evidence:    cred,
	})
}

func (r *VerifyResult) AddFail(req, reason string) {
	r.Pass = false
	r.Reasons = append(r.Reasons, VerifyReason{
		Requirement: req,
		Pass:        false,
		FailReason:  reason,
	})
}

// ── Resolve 输入输出 ──────────────────────────────────────────

// ResolveInput 寻址请求：给定任务约束，找谁能做
type ResolveInput struct {
	TenantID       int
	ProjectRef     vuri.VRef // 项目上下文
	SPURef         vuri.VRef // 任务类型（从 SPU 规格读取资质要求）
	Action         Action
	NeedCertTypes  []CertType // 明确要求的证书类型
	HeadOfficeOnly bool       // 是否仅限总院执行体（RULE-002）
	ValidOn        time.Time
	Limit          int
}

// Candidate 候选执行体
type Candidate struct {
	ExecutorRef    vuri.VRef
	Name           string       // 显示名（从 employee/company 表读取）
	MatchedCreds   []Credential // 匹配的证书列表
	ActiveProjects int          // 当前在建项目数
	CapacityOK     bool         // 是否还有承接余量
	Score          float64      // 匹配分（0-1，越高越优先推荐）
}

// ── Occupied 输入输出 ─────────────────────────────────────────

// OccupiedState 执行体当前资源占用状态
type OccupiedState struct {
	ExecutorRef    vuri.VRef
	ActiveProjects int               // 当前 IN_PROGRESS 项目数
	ProjectLimit   int               // 上限（从证书类型推算，默认5）
	Available      bool              // 是否还能承接新项目
	Projects       []OccupiedProject // 占用明细
}

// OccupiedProject 单个在建项目的占用信息
type OccupiedProject struct {
	ProjectRef  vuri.VRef
	ProjectName string
	Role        string // 在项目里的角色（EXECUTOR/REVIEWER/etc）
	Since       time.Time
}
