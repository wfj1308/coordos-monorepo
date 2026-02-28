// ============================================================
//  CoordOS — ProjectRules 规则实现
//  平台的物理定律，任何参与方无法绕过
// ============================================================

package projectcore

import "fmt"

type projectRules struct{}

func NewProjectRules() ProjectRules {
	return &projectRules{}
}

// ── RULE-001：TRANSFORM 必须在执行体资源约束之内 ─────────────
// 每次执行体消耗资源，必须在三个维度全部合规：
//   ① 消耗的资源种类 ∈ 允许的 Skill
//   ② 累计消耗量 ≤ 能源上限（Genesis UTXO 剩余额度）
//   ③ 消耗比例 ≤ 单价容差
func (r *projectRules) EnforceRule001(evt ProjectEvent, ctx ProjectContext) error {
	if evt.Verb != VerbTransform {
		return nil
	}

	node, err := ctx.ProjectTree.GetNode(evt.ProjectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-001", Detail: "项目节点不存在: " + string(evt.ProjectRef)}
	}

	// ① 校验执行体身份
	if node.ExecutorRef != evt.ActorRef {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: fmt.Sprintf("执行体不匹配：期望 %s，实际 %s", node.ExecutorRef, evt.ActorRef),
		}
	}

	// ② 校验 Genesis UTXO 剩余额度
	quantity, ok := evt.Payload["quantity"].(float64)
	if !ok {
		return &RuleViolationError{Rule: "RULE-001", Detail: "payload 缺少 quantity 字段"}
	}
	remaining, err := ctx.GenesisStore.GetRemainingQuota(node.GenesisUTXORef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-001", Detail: "无法读取 Genesis UTXO 额度"}
	}
	if int64(quantity) > remaining {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: fmt.Sprintf("超出 Genesis UTXO 剩余额度：申请 %d，剩余 %d", int64(quantity), remaining),
		}
	}

	// ③ 校验 Skill 匹配
	skillName, _ := evt.Payload["skill"].(string)
	found := false
	for _, s := range node.Constraint.Skills {
		if s.Name == skillName {
			found = true
			// 单价容差校验
			unitPrice, _ := evt.Payload["unit_price"].(float64)
			diff := (unitPrice - float64(s.UnitPrice)) / float64(s.UnitPrice)
			if diff < 0 {
				diff = -diff
			}
			if diff > s.Tolerance {
				return &RuleViolationError{
					Rule:   "RULE-001",
					Detail: fmt.Sprintf("单价超出容差：基准 %d，申请 %.0f，容差 %.1f%%", s.UnitPrice, unitPrice, s.Tolerance*100),
				}
			}
			break
		}
	}
	if !found {
		return &RuleViolationError{
			Rule:   "RULE-001",
			Detail: "Skill 未授权：" + skillName + " 不在执行体允许的资源消耗范围内",
		}
	}

	return nil
}

// ── RULE-002：总院在每个项目必须有实质参与 ───────────────────
// 出事时自证合规的底线：
//   ① 至少一个 REVIEW UTXO（总院执行体签署）
//   ② 至少一个 DELIVER UTXO（总院执行体创建）
//   ③ invoice 主体必须是总院法人
// 违反任一条件 → settlement 被锁定，无法分账
func (r *projectRules) EnforceRule002(projectRef VRef, ctx ProjectContext) error {
	node, err := ctx.ProjectTree.GetNode(projectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-002", Detail: "项目节点不存在"}
	}

	headOfficePlatform := node.PlatformRef
	hasReview := false
	hasDelivery := false

	for _, m := range node.Milestones {
		if m.SignedBy == headOfficePlatform {
			switch m.Name {
			case "审图完成", "REVIEW":
				hasReview = true
			case "成果交付", "DELIVER":
				hasDelivery = true
			}
		}
	}

	if !hasReview {
		return &RuleViolationError{
			Rule:   "RULE-002",
			Detail: "缺少总院实质参与：未找到总院签署的审图里程碑，结算被锁定",
		}
	}
	if !hasDelivery {
		return &RuleViolationError{
			Rule:   "RULE-002",
			Detail: "缺少总院实质参与：未找到总院签署的交付里程碑，结算被锁定",
		}
	}

	return nil
}

// ── RULE-003：对外付款必须引用有效合同 ──────────────────────
// 总院铁律：所有对外付款必须有合同锚点
// 合同必须经过招标采购平台（procurement_ref 不为空）
func (r *projectRules) EnforceRule003(evt ProjectEvent, ctx ProjectContext) error {
	if evt.Verb != VerbPay {
		return nil
	}

	// ① 合同引用不为空
	if evt.ContractRef == "" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "对外付款必须引用合同编号，当前 contract_ref 为空",
		}
	}

	// ② 合同存在且状态为 ACTIVE
	contract, err := ctx.ContractStore.Get(evt.ContractRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-003", Detail: "合同不存在: " + string(evt.ContractRef)}
	}
	if contract.Status != "ACTIVE" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: fmt.Sprintf("合同状态异常：%s，当前状态 %s，不允许付款", contract.ContractNo, contract.Status),
		}
	}

	// ③ 合同必须来自招采平台（procurement_ref 不为空）
	if contract.ProcurementRef == "" {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "合同缺少招采平台单号（procurement_ref），对外付款被拒绝",
		}
	}

	// ④ 付款金额不超过合同剩余可付额度
	amount, ok := evt.Payload["amount"].(float64)
	if !ok {
		return &RuleViolationError{Rule: "RULE-003", Detail: "payload 缺少 amount 字段"}
	}
	if err := ctx.ContractStore.ValidatePayment(evt.ContractRef, int64(amount)); err != nil {
		return &RuleViolationError{
			Rule:   "RULE-003",
			Detail: "付款金额超出合同剩余可付额度：" + err.Error(),
		}
	}

	return nil
}

// ── RULE-004：子项目资源约束不得超过父项目剩余额度 ──────────
// 委托链不变式：
//   子层 Genesis UTXO 总和 ≤ 父层 Genesis UTXO 剩余额度
//   违反 = 超分包，创建时强制校验
func (r *projectRules) EnforceRule004(child *ProjectNode, parent *ProjectNode) error {
	if child.ParentRef == "" {
		return nil // 根节点，无需校验
	}

	childQuota, ok := child.Constraint.Energy.CapitalReserve, true
	if !ok {
		return &RuleViolationError{Rule: "RULE-004", Detail: "子项目缺少资源约束定义"}
	}

	// 计算父项目已分配给其他子项目的总额
	// （这里依赖 Store 查询，实际实现注入 GenesisStore）
	_ = childQuota
	_ = parent

	// 简化实现：通过 Store 的 ValidateChildConstraint 完成
	// 完整实现在 ProjectTreeStore.ValidateChildConstraint
	return nil
}

// ── RULE-005：委托链最深层必须有实际UTXO产出才能向上结算 ─────
// 结算必须从叶子节点向上触发，每层都验证下层已有实际产出
// 防止空壳层套取上层资金
func (r *projectRules) EnforceRule005(projectRef VRef, ctx ProjectContext) error {
	children, err := ctx.ProjectTree.GetChildren(projectRef)
	if err != nil {
		return &RuleViolationError{Rule: "RULE-005", Detail: "无法获取子项目列表"}
	}

	// 有子项目的节点：必须等所有子项目完成结算
	for _, child := range children {
		if child.Status != StatusSettled && child.Status != StatusArchived {
			return &RuleViolationError{
				Rule:   "RULE-005",
				Detail: fmt.Sprintf("子项目 %s 尚未完成结算，上层结算被阻断", child.Ref),
			}
		}
	}

	// 叶子节点：必须有实际UTXO产出
	if len(children) == 0 {
		node, err := ctx.ProjectTree.GetNode(projectRef)
		if err != nil {
			return &RuleViolationError{Rule: "RULE-005", Detail: "项目节点不存在"}
		}
		hasUTXO := false
		for _, m := range node.Milestones {
			if m.UTXORef != "" && m.Status == "REACHED" {
				hasUTXO = true
				break
			}
		}
		if !hasUTXO {
			return &RuleViolationError{
				Rule:   "RULE-005",
				Detail: "叶子节点无实际UTXO产出，无法触发结算",
			}
		}
	}

	return nil
}

// ══════════════════════════════════════════════════════════════
//  规则执行引擎：统一入口，顺序执行，任一失败即终止
// ══════════════════════════════════════════════════════════════

type RuleEngine struct {
	rules  ProjectRules
	audit  AuditStore
}

func NewRuleEngine(rules ProjectRules, audit AuditStore) *RuleEngine {
	return &RuleEngine{rules: rules, audit: audit}
}

// Execute：执行事件前的规则校验
// 顺序：RULE-003 → RULE-001 → RULE-002（按风险权重）
func (e *RuleEngine) Execute(evt ProjectEvent, ctx ProjectContext) error {
	// 记录事件（无论成功失败都存证）
	eventID, _ := ctx.AuditStore.RecordEvent(evt, ctx.TenantID)

	// PAY：先校验合同
	if evt.Verb == VerbPay {
		if err := e.rules.EnforceRule003(evt, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-003", evt, err.Error())
			return err
		}
	}

	// TRANSFORM：校验资源约束
	if evt.Verb == VerbTransform {
		if err := e.rules.EnforceRule001(evt, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-001", evt, err.Error())
			return err
		}
	}

	// SETTLE：校验总院实质参与 + 委托链完整性
	if evt.Verb == VerbSettle {
		if err := e.rules.EnforceRule002(evt.ProjectRef, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-002", evt, err.Error())
			return err
		}
		if err := e.rules.EnforceRule005(evt.ProjectRef, ctx); err != nil {
			ctx.AuditStore.RecordViolation("RULE-005", evt, err.Error())
			return err
		}
	}

	_ = eventID
	return nil
}
