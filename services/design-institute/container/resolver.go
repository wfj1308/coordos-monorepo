// ============================================================
//  container/resolver.go
//  ContainerResolver — 公理三的落地实现
//
//  公理三：Trip Declares Capability, Not Executor
//  Trip Step 只声明 cap_tags + skills + min_level
//  ContainerResolver 在运行时匹配最优容器
//
//  从 matchExecutorCapability() 升级为 matchContainerCapability()
//  核心变化：能力匹配的单位从 Executor 变成 Container
// ============================================================

package container

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ── 容器类型枚举（Container Doctrine v1 固定，不可扩展）─────
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

// ValidKinds 全网固定，来自 Container Doctrine v1
// 任何不在此列表的 kind 必须被拒绝（AXIOM-1 + DOCTRINE_VIOLATION）
var ValidKinds = map[ContainerKind]bool{
	KindVolume:    true,
	KindEnergy:    true,
	KindScheduler: true,
	KindIO:        true,
	KindTransport: true,
	KindLogic:     true,
	KindCert:      true,
}

// DoctrineRef 宪法 UTXO 地址（全网唯一）
const DoctrineRef = "v://coordos/spec/container-doctrine@v1"

// ── 类型定义 ──────────────────────────────────────────────────

// ContainerSpec Trip Step 声明的容器需求
// 对应 AXIOM-3：Trip 只声明需求，不绑定具体容器
type ContainerSpec struct {
	Kind        ContainerKind `json:"kind,omitempty"`  // 可选，缩小范围
	CapTags     []string      `json:"cap_tags"`          // 必须包含这些标签
	Skills      []string      `json:"skills,omitempty"`  // 必须包含这些技能
	MinCapLevel float64       `json:"min_cap_level"`     // 最低能力等级
}

// ContainerMatch 匹配结果
type ContainerMatch struct {
	ContainerRef   string        `json:"container_ref"`
	ContainerName  string        `json:"container_name"`
	Kind           ContainerKind `json:"kind"`
	ExecutorRef    string        `json:"executor_ref"`
	CapLevel       float64       `json:"cap_level"`
	AvailableSlots int           `json:"available_slots"`
	EnergyUnit     string        `json:"energy_unit,omitempty"`
	EnergyBaseline float64       `json:"energy_baseline"`
	EnergyCoeffs   map[string]float64 `json:"energy_coeffs,omitempty"`
	// 为什么匹配（调试用）
	MatchReason    string        `json:"match_reason"`
}

// LockResult 占用容器的结果
type LockResult struct {
	LockID       int64     `json:"lock_id"`
	ContainerRef string    `json:"container_ref"`
	LockedBy     string    `json:"locked_by"`
	LockedAt     time.Time `json:"locked_at"`
}

// Receipt 执行存证（公理五：Receipt Locks Execution）
type Receipt struct {
	ReceiptRef     string             `json:"receipt_ref"`
	TripRef        string             `json:"trip_ref,omitempty"`
	StepName       string             `json:"step_name"`
	OperationID    string             `json:"operation_id,omitempty"`
	ContainerRef   string             `json:"container_ref"`   // LAW-3 必填
	ExecutorRef    string             `json:"executor_ref"`
	OperatorRef    string             `json:"operator_ref,omitempty"`
	InputRefs      []string           `json:"input_refs"`
	InputsHash     string             `json:"inputs_hash"`     // 锁定输入
	OutputRef      string             `json:"output_ref,omitempty"`
	EnergyUnit     string             `json:"energy_unit,omitempty"`
	EnergyUsed     float64            `json:"energy_used"`
	EnergyCost     float64            `json:"energy_cost"`
	QualityMetrics map[string]any     `json:"quality_metrics,omitempty"`
	ProofHash      string             `json:"proof_hash"`      // 必填
	ExecutedAt     time.Time          `json:"executed_at"`
	TenantID       int                `json:"tenant_id"`
}

// ── Service ───────────────────────────────────────────────────

type Service struct {
	db       *sql.DB
	tenantID int
}

func NewService(db *sql.DB, tenantID int) *Service {
	return &Service{db: db, tenantID: tenantID}
}

func (s *Service) TenantID() int { return s.tenantID }

// ══════════════════════════════════════════════════════════════
//  ValidateContainerKind
//  公理一的守卫：kind 必须 ∈ 七分类
//  任何容器注册前必须调用此函数
// ══════════════════════════════════════════════════════════════

func ValidateContainerKind(kind ContainerKind) error {
	if !ValidKinds[kind] {
		return fmt.Errorf(
			"DOCTRINE_VIOLATION: container kind %q is not in Container Doctrine v1 type set. "+
				"Valid kinds: VOLUME, ENERGY, SCHEDULER, IO, TRANSPORT, LOGIC, CERT. "+
				"Reference: %s",
			kind, DoctrineRef,
		)
	}
	return nil
}

// ══════════════════════════════════════════════════════════════
//  MatchContainerCapability（替代 matchExecutorCapability）
//  公理三的核心实现：
//    给定 ContainerSpec（cap_tags + skills + min_level）
//    返回命名空间内满足条件的可用容器列表
//    按 cap_level DESC, energy_baseline ASC 排序
// ══════════════════════════════════════════════════════════════

func (s *Service) MatchContainerCapability(
	ctx context.Context,
	nsRef string,
	spec ContainerSpec,
) ([]ContainerMatch, error) {

	// 构造查询
	conds := []string{
		"c.namespace_ref = $1",
		"c.status = 'ACTIVE'",
		"co.available_slots > 0",
		fmt.Sprintf("c.cap_level >= %f", spec.MinCapLevel),
	}
	args := []any{nsRef}
	argN := 2

	// kind 过滤（可选）
	if spec.Kind != "" {
		if err := ValidateContainerKind(spec.Kind); err != nil {
			return nil, err
		}
		conds = append(conds, fmt.Sprintf("c.kind = $%d", argN))
		args = append(args, string(spec.Kind))
		argN++
	}

	// cap_tags 包含关系（GIN 索引）
	if len(spec.CapTags) > 0 {
		conds = append(conds, fmt.Sprintf("c.cap_tags @> $%d::text[]", argN))
		args = append(args, pqArray(spec.CapTags))
		argN++
	}

	// skills 包含关系（可选）
	if len(spec.Skills) > 0 {
		conds = append(conds, fmt.Sprintf("c.skills @> $%d::text[]", argN))
		args = append(args, pqArray(spec.Skills))
		argN++
	}

	query := fmt.Sprintf(`
		SELECT
			c.ref,
			c.name,
			c.kind,
			ec.executor_ref,
			c.cap_level,
			co.available_slots,
			COALESCE(c.energy_unit,''),
			COALESCE(c.energy_baseline,0),
			COALESCE(c.energy_coeffs,'{}')
		FROM containers c
		JOIN executor_containers ec ON ec.container_ref = c.ref
		JOIN container_occupancy  co ON co.container_ref = c.ref
		WHERE %s
		  AND (ec.valid_until IS NULL OR ec.valid_until > NOW())
		ORDER BY c.cap_level DESC, co.available_slots DESC, c.energy_baseline ASC
		LIMIT 20
	`, strings.Join(conds, " AND "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("MatchContainerCapability query failed: %w", err)
	}
	defer rows.Close()

	var result []ContainerMatch
	for rows.Next() {
		var m ContainerMatch
		var coeffsJSON []byte
		err := rows.Scan(
			&m.ContainerRef, &m.ContainerName, (*string)(&m.Kind),
			&m.ExecutorRef, &m.CapLevel, &m.AvailableSlots,
			&m.EnergyUnit, &m.EnergyBaseline, &coeffsJSON,
		)
		if err != nil {
			continue
		}
		json.Unmarshal(coeffsJSON, &m.EnergyCoeffs)
		m.MatchReason = fmt.Sprintf(
			"cap_level=%.1f available=%d kind=%s",
			m.CapLevel, m.AvailableSlots, m.Kind,
		)
		result = append(result, m)
	}
	return result, nil
}

// ══════════════════════════════════════════════════════════════
//  LockContainer
//  Trip Step 占用容器（公理一：容器是能力的原子单元）
//  加锁后容器 available_slots - 1
// ══════════════════════════════════════════════════════════════

func (s *Service) LockContainer(
	ctx context.Context,
	containerRef string,
	lockedBy string,
	expectedRelease *time.Time,
) (*LockResult, error) {

	// 先检查还有没有空位
	var slots int
	err := s.db.QueryRowContext(ctx,
		`SELECT available_slots FROM container_occupancy WHERE container_ref=$1`,
		containerRef,
	).Scan(&slots)
	if err != nil || slots <= 0 {
		return nil, fmt.Errorf(
			"CONTAINER_FULL: container %s has no available slots. "+
				"Per AXIOM-1: container is the capability atom; when full, capability is unavailable.",
			containerRef,
		)
	}

	var lockID int64
	err = s.db.QueryRowContext(ctx, `
		INSERT INTO container_locks
		  (container_ref, locked_by, lock_scope, status,
		   locked_at, expected_release, tenant_id)
		VALUES ($1, $2, 'operation_id', 'LOCKED', NOW(), $3, $4)
		RETURNING id
	`, containerRef, lockedBy, expectedRelease, s.tenantID).Scan(&lockID)
	if err != nil {
		return nil, fmt.Errorf("LockContainer failed: %w", err)
	}

	return &LockResult{
		LockID:       lockID,
		ContainerRef: containerRef,
		LockedBy:     lockedBy,
		LockedAt:     time.Now(),
	}, nil
}

// ══════════════════════════════════════════════════════════════
//  ReleaseContainer
//  Trip Step 完成，释放容器锁
// ══════════════════════════════════════════════════════════════

func (s *Service) ReleaseContainer(ctx context.Context, lockID int64, energyUsed float64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE container_locks SET
			status      = 'RELEASED',
			released_at = NOW(),
			energy_used = $2
		WHERE id=$1 AND status='LOCKED'
	`, lockID, energyUsed)
	return err
}

// ══════════════════════════════════════════════════════════════
//  CreateReceipt
//  公理五：Receipt Locks Execution
//  LAW-3：Receipt Must Carry container_ref
//
//  校验：container_ref / inputs_hash / proof_hash 不能为空
//  数据库触发器也会校验，这里提前在应用层阻断
// ══════════════════════════════════════════════════════════════

func (s *Service) CreateReceipt(ctx context.Context, r Receipt) (*Receipt, error) {
	// ── 应用层强校验（LAW-3）─────────────────────────────────
	if r.ContainerRef == "" {
		return nil, fmt.Errorf(
			"DOCTRINE_VIOLATION: Receipt %s has no container_ref. "+
				"Per Container Doctrine v1 LAW-3: Receipt Must Carry container_ref. "+
				"Reference: %s",
			r.ReceiptRef, DoctrineRef,
		)
	}
	if r.InputsHash == "" {
		return nil, fmt.Errorf(
			"DOCTRINE_VIOLATION: Receipt %s has no inputs_hash. "+
				"inputs_hash locks execution inputs against tampering.",
			r.ReceiptRef,
		)
	}

	// ── 计算能耗成本 ─────────────────────────────────────────
	var energyCost float64
	s.db.QueryRowContext(ctx, `
		SELECT COALESCE(
			(SELECT energy_cost FROM calc_energy_cost($1, $2::jsonb)),
			0
		)
	`, r.ContainerRef, `{"unit":1}`).Scan(&energyCost)
	r.EnergyCost = energyCost

	// ── 计算 proof_hash ──────────────────────────────────────
	if r.ProofHash == "" {
		r.ProofHash = computeReceiptHash(r)
	}

	// ── 写入数据库 ───────────────────────────────────────────
	qualJSON, _ := json.Marshal(r.QualityMetrics)
	inputsJSON, _ := json.Marshal(r.InputRefs)

	var receiptID int64
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO execution_receipts (
			receipt_ref, trip_ref, step_name, operation_id,
			container_ref, executor_ref, operator_ref,
			input_refs, inputs_hash, output_ref,
			energy_unit, energy_used, energy_cost,
			quality_metrics, proof_hash, status,
			executed_at, tenant_id
		) VALUES (
			$1,$2,$3,$4,
			$5,$6,$7,
			$8,$9,$10,
			$11,$12,$13,
			$14,$15,'CONFIRMED',
			NOW(),$16
		) RETURNING id
	`,
		r.ReceiptRef, nullStr(r.TripRef), r.StepName, nullStr(r.OperationID),
		r.ContainerRef, r.ExecutorRef, nullStr(r.OperatorRef),
		inputsJSON, r.InputsHash, nullStr(r.OutputRef),
		nullStr(r.EnergyUnit), r.EnergyUsed, r.EnergyCost,
		qualJSON, r.ProofHash, s.tenantID,
	).Scan(&receiptID)
	if err != nil {
		return nil, fmt.Errorf("CreateReceipt failed: %w", err)
	}

	r.ExecutedAt = time.Now()
	return &r, nil
}

// ══════════════════════════════════════════════════════════════
//  GetDoctrineRef
//  返回 Container Doctrine v1 Genesis UTXO 的完整内容
//  任何组件可以调用此函数验证自己是否在宪法框架内
// ══════════════════════════════════════════════════════════════

func (s *Service) GetDoctrineRef(ctx context.Context) (map[string]any, error) {
	var constraintJSON []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT constraint_json FROM genesis_utxos
		WHERE ref = $1
	`, DoctrineRef).Scan(&constraintJSON)
	if err != nil {
		return nil, fmt.Errorf("Container Doctrine not found at %s: %w", DoctrineRef, err)
	}
	var doctrine map[string]any
	json.Unmarshal(constraintJSON, &doctrine)
	return doctrine, nil
}

// ── 工具函数 ──────────────────────────────────────────────────

func computeReceiptHash(r Receipt) string {
	data := fmt.Sprintf("%s|%s|%s|%s|%.4f|%s",
		r.ContainerRef, r.ExecutorRef,
		r.InputsHash, r.OutputRef,
		r.EnergyUsed,
		time.Now().Format(time.RFC3339),
	)
	h := sha256.Sum256([]byte(data))
	return fmt.Sprintf("sha256:%x", h)
}

func ComputeInputsHash(inputRefs []string) string {
	h := sha256.Sum256([]byte(strings.Join(inputRefs, "|")))
	return fmt.Sprintf("sha256:%x", h)
}

func pqArray(ss []string) string {
	if len(ss) == 0 {
		return "{}"
	}
	quoted := make([]string, len(ss))
	for i, s := range ss {
		quoted[i] = `"` + s + `"`
	}
	return "{" + strings.Join(quoted, ",") + "}"
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
