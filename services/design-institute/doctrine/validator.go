// ============================================================
//  doctrine/validator.go
//  Container Doctrine v1 校验器
//
//  宪法在代码层的三处强制落地：
//  1. ValidateContainerKind  — 容器类型必须在七分类内
//  2. matchContainerCapability — Resolver 改为容器匹配
//  3. ValidateReceipt        — Receipt 强制 container_ref
//
//  所有违反都抛出 DoctrineViolation，携带 doctrine_ref
// ============================================================

package doctrine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// DoctrineRef 宪法的 UTXO ref
const DoctrineRef = "v://coordos/spec/container-doctrine@v1"

// ── 七类容器类型（宪法固定，不可扩展）──────────────────────

type ContainerKind string

const (
	KindVolume    ContainerKind = "VOLUME"
	KindEnergy    ContainerKind = "ENERGY"
	KindScheduler ContainerKind = "SCHEDULER"
	KindIO        ContainerKind = "IO"
	KindTransport ContainerKind = "TRANSPORT"
	KindLogic     ContainerKind = "LOGIC"
	KindCert      ContainerKind = "CERT"
)

var validKinds = map[ContainerKind]bool{
	KindVolume:    true,
	KindEnergy:    true,
	KindScheduler: true,
	KindIO:        true,
	KindTransport: true,
	KindLogic:     true,
	KindCert:      true,
}

// ── 错误类型 ──────────────────────────────────────────────────

type DoctrineViolation struct {
	Axiom      int    // 违反的公理编号
	Rule       string // 具体规则
	Detail     string // 详情
	DoctrineRef string
}

func (e *DoctrineViolation) Error() string {
	return fmt.Sprintf(
		"DOCTRINE VIOLATION [公理%d] %s: %s (doctrine: %s)",
		e.Axiom, e.Rule, e.Detail, e.DoctrineRef,
	)
}

func violation(axiom int, rule, detail string) *DoctrineViolation {
	return &DoctrineViolation{
		Axiom:       axiom,
		Rule:        rule,
		Detail:      detail,
		DoctrineRef: DoctrineRef,
	}
}

// ══════════════════════════════════════════════════════════════
//  1. ValidateContainerKind
//  公理一落地：能力必须挂在合法类型的容器上
// ══════════════════════════════════════════════════════════════

func ValidateContainerKind(kind string) error {
	if kind == "" {
		return violation(1,
			"Container is Capability Atom",
			"container.kind 不能为空",
		)
	}
	if !validKinds[ContainerKind(kind)] {
		return violation(1,
			"Container is Capability Atom",
			fmt.Sprintf(
				"container.kind [%s] 不在七分类内。"+
					"有效类型：VOLUME, ENERGY, SCHEDULER, IO, TRANSPORT, LOGIC, CERT",
				kind,
			),
		)
	}
	return nil
}

// ── ContainerSpec 容器规格 ────────────────────────────────────

type ContainerSpec struct {
	Ref          string        `json:"ref"`
	NamespaceRef string        `json:"namespace_ref"`
	Name         string        `json:"name"`
	Kind         ContainerKind `json:"kind"`
	CapTags      []string      `json:"cap_tags"`
	Skills       []string      `json:"skills"`
	CapLevel     float64       `json:"cap_level"`
	MaxParallel  int           `json:"max_parallel"`
	EnergyUnit   string        `json:"energy_unit"`
	EnergyBaseline float64     `json:"energy_baseline"`
	EnergyCoeffs map[string]float64 `json:"energy_coeffs"`
	ValidUntil   *time.Time    `json:"valid_until,omitempty"`
}

// Validate 校验容器规格（注册时调用）
func (c *ContainerSpec) Validate() error {
	if err := ValidateContainerKind(string(c.Kind)); err != nil {
		return err
	}
	if c.Ref == "" {
		return violation(1, "Container is Capability Atom", "container.ref 不能为空")
	}
	if !strings.HasPrefix(c.Ref, "v://") {
		return violation(1, "Container is Capability Atom",
			"container.ref 必须以 v:// 开头")
	}
	if len(c.CapTags) == 0 {
		return violation(1, "Container is Capability Atom",
			"container.cap_tags 不能为空，容器必须声明至少一个能力标签")
	}
	if c.MaxParallel <= 0 {
		return violation(1, "Container is Capability Atom",
			"container.max_parallel 必须大于0")
	}

	// CERT 类容器必须有 ValidUntil
	if c.Kind == KindCert && c.ValidUntil == nil {
		return violation(1, "Container is Capability Atom",
			"CERT 类容器必须声明 valid_until（资质证书有效期）")
	}
	return nil
}

// ══════════════════════════════════════════════════════════════
//  2. ContainerCapabilityMatcher
//  公理三落地：Trip 声明需求，ContainerResolver 匹配容器
//  替代原来的 matchExecutorCapability
// ══════════════════════════════════════════════════════════════

// ContainerRequirement Trip Step 声明的容器需求
type ContainerRequirement struct {
	Kind        ContainerKind `json:"kind,omitempty"`       // 可选，不指定则匹配所有类型
	CapTags     []string      `json:"cap_tags"`             // 必须包含这些标签
	Skills      []string      `json:"skills,omitempty"`     // 可选，必须包含这些技能
	MinCapLevel float64       `json:"min_cap_level"`        // 最低能力等级
}

// ContainerMatch 匹配结果
type ContainerMatch struct {
	ContainerRef   string  `json:"container_ref"`
	ContainerName  string  `json:"container_name"`
	ExecutorRef    string  `json:"executor_ref"`
	Kind           string  `json:"kind"`
	CapLevel       float64 `json:"cap_level"`
	AvailableSlots int     `json:"available_slots"`
	EnergyUnit     string  `json:"energy_unit"`
	EnergyBaseline float64 `json:"energy_baseline"`
}

// Matcher 容器能力匹配器
type Matcher struct {
	db *sql.DB
}

func NewMatcher(db *sql.DB) *Matcher {
	return &Matcher{db: db}
}

// MatchContainerCapability 给定需求，返回可用容器列表
// 这是 matchExecutorCapability 的替代，语义从"找执行体"变为"找容器"
func (m *Matcher) MatchContainerCapability(
	ctx context.Context,
	namespaceRef string,
	req ContainerRequirement,
) ([]ContainerMatch, error) {

	if len(req.CapTags) == 0 {
		return nil, violation(3,
			"Trip Declares Capability, Not Executor",
			"ContainerRequirement.cap_tags 不能为空，Trip 必须声明能力需求",
		)
	}

	query := `
		SELECT
			c.ref,
			c.name,
			ec.executor_ref,
			c.kind,
			c.cap_level,
			co.available_slots,
			COALESCE(c.energy_unit, ''),
			COALESCE(c.energy_baseline, 0)
		FROM containers c
		JOIN executor_containers ec ON ec.container_ref = c.ref
		JOIN container_occupancy co ON co.container_ref = c.ref
		WHERE c.namespace_ref = $1
		  AND c.status = 'ACTIVE'
		  AND co.available_slots > 0
		  AND c.cap_level >= $2
		  AND c.cap_tags @> $3
	`
	args := []any{namespaceRef, req.MinCapLevel, req.CapTags}
	argN := 4

	if req.Kind != "" {
		query += fmt.Sprintf(" AND c.kind = $%d", argN)
		args = append(args, string(req.Kind))
		argN++
	}

	if len(req.Skills) > 0 {
		query += fmt.Sprintf(" AND c.skills @> $%d", argN)
		args = append(args, req.Skills)
	}

	// CERT 类：过滤有效期
	query += " AND (c.kind != 'CERT' OR (c.spec_json->>'valid_until')::date >= CURRENT_DATE)"

	query += `
		ORDER BY
			c.cap_level DESC,
			co.available_slots DESC,
			c.energy_baseline ASC
		LIMIT 20
	`

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("容器匹配查询失败: %w", err)
	}
	defer rows.Close()

	var result []ContainerMatch
	for rows.Next() {
		var cm ContainerMatch
		err := rows.Scan(
			&cm.ContainerRef, &cm.ContainerName, &cm.ExecutorRef,
			&cm.Kind, &cm.CapLevel, &cm.AvailableSlots,
			&cm.EnergyUnit, &cm.EnergyBaseline,
		)
		if err != nil {
			continue
		}
		result = append(result, cm)
	}

	return result, nil
}

// LockContainer 锁定容器（Trip Step 开始执行时调用）
func (m *Matcher) LockContainer(
	ctx context.Context,
	containerRef, lockedBy string,
	expectedRelease *time.Time,
) error {
	// 先检查是否还有空位
	var available int
	err := m.db.QueryRowContext(ctx, `
		SELECT available_slots FROM container_occupancy
		WHERE container_ref = $1
	`, containerRef).Scan(&available)
	if err != nil || available <= 0 {
		return violation(3,
			"Trip Declares Capability, Not Executor",
			fmt.Sprintf("容器 [%s] 已无可用槽位，当前满负荷", containerRef),
		)
	}

	_, err = m.db.ExecContext(ctx, `
		INSERT INTO container_locks
		  (container_ref, locked_by, status, locked_at, expected_release, tenant_id)
		VALUES ($1, $2, 'LOCKED', NOW(), $3, 0)
	`, containerRef, lockedBy, expectedRelease)
	return err
}

// ReleaseContainer 释放容器锁（Trip Step 完成时调用）
func (m *Matcher) ReleaseContainer(
	ctx context.Context,
	containerRef, lockedBy string,
	energyUsed float64,
	energyUnit string,
) error {
	_, err := m.db.ExecContext(ctx, `
		UPDATE container_locks SET
			status       = 'RELEASED',
			released_at  = NOW(),
			energy_used  = $3,
			energy_unit  = $4
		WHERE container_ref = $1
		  AND locked_by     = $2
		  AND status        = 'LOCKED'
	`, containerRef, lockedBy, energyUsed, energyUnit)
	return err
}

// ══════════════════════════════════════════════════════════════
//  3. ValidateReceipt
//  公理五落地：Receipt 强制 container_ref
// ══════════════════════════════════════════════════════════════

// ReceiptDraft Receipt 草稿（写入前校验）
type ReceiptDraft struct {
	ReceiptRef    string            `json:"receipt_ref"`
	TripRef       string            `json:"trip_ref,omitempty"`
	StepName      string            `json:"step_name"`
	OperationID   string            `json:"operation_id,omitempty"`
	ContainerRef  string            `json:"container_ref"`   // 公理五：必填
	ExecutorRef   string            `json:"executor_ref"`    // 公理五：必填
	OperatorRef   string            `json:"operator_ref,omitempty"`
	InputRefs     []string          `json:"input_refs"`
	InputsHash    string            `json:"inputs_hash"`     // 公理五：必填
	OutputRef     string            `json:"output_ref,omitempty"`
	EnergyUnit    string            `json:"energy_unit,omitempty"`
	EnergyUsed    float64           `json:"energy_used"`
	EnergyCost    float64           `json:"energy_cost,omitempty"`
	QualityMetrics map[string]any   `json:"quality_metrics,omitempty"`
	ProofHash     string            `json:"proof_hash"`      // 公理五：必填
}

// ValidateReceipt 校验 Receipt（公理五落地）
func ValidateReceipt(r *ReceiptDraft) error {
	// container_ref 必填
	if r.ContainerRef == "" {
		return violation(5,
			"Receipt Locks Execution",
			"receipt.container_ref 不能为空。"+
				"无 container_ref 的 Receipt 为无效 Receipt，其产出的 UTXO 不被系统承认。",
		)
	}

	// executor_ref 必填
	if r.ExecutorRef == "" {
		return violation(5,
			"Receipt Locks Execution",
			"receipt.executor_ref 不能为空",
		)
	}

	// inputs_hash 必填
	if r.InputsHash == "" {
		return violation(5,
			"Receipt Locks Execution",
			"receipt.inputs_hash 不能为空。inputs_hash 防止输入资源事后被篡改。",
		)
	}

	// proof_hash 必填
	if r.ProofHash == "" {
		return violation(5,
			"Receipt Locks Execution",
			"receipt.proof_hash 不能为空",
		)
	}

	// receipt_ref 格式校验
	if !strings.HasPrefix(r.ReceiptRef, "v://") {
		return violation(5,
			"Receipt Locks Execution",
			"receipt.receipt_ref 必须以 v:// 开头",
		)
	}

	return nil
}

// ── 公理六落地：能力状态是统计结果，不是声明 ─────────────────
// 这在 capability/service.go 的 ComputeStats 里已经实现
// 这里只提供校验函数

// AssertCapabilityDerived 断言：能力等级必须来自执行历史，不接受直接设置
func AssertCapabilityDerived(capLevel float64, source string) error {
	if source != "COMPUTED" && source != "INITIAL" {
		return violation(6,
			"Capability State is Derived, Not Declared",
			fmt.Sprintf(
				"capability_level 的 source 必须是 COMPUTED（由执行历史统计）或 INITIAL（初始值），"+
					"不接受 [%s]。能力状态是统计结果，不是声明。",
				source,
			),
		)
	}
	_ = capLevel
	return nil
}

// ── 宪法摘要（对外展示）──────────────────────────────────────

type DoctrineSummary struct {
	Ref            string   `json:"ref"`
	Title          string   `json:"title"`
	Version        string   `json:"version"`
	ContainerTypes []string `json:"container_types"`
	AxiomCount     int      `json:"axiom_count"`
	Immutable      bool     `json:"immutable"`
	RatifiedAt     string   `json:"ratified_at"`
}

func GetSummary() *DoctrineSummary {
	return &DoctrineSummary{
		Ref:     DoctrineRef,
		Title:   "CoordOS Container Doctrine v1.0",
		Version: "1.0.0",
		ContainerTypes: []string{
			"VOLUME", "ENERGY", "SCHEDULER",
			"IO", "TRANSPORT", "LOGIC", "CERT",
		},
		AxiomCount: 7,
		Immutable:  true,
		RatifiedAt: "2026-03-01T00:00:00Z",
	}
}
