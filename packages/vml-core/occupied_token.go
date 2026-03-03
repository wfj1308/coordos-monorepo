// ============================================================
//  occupied_token.go
//  Package: vml-core
//
//  occupied_token 是容器承接 Trip 时产生的锁定凭证。
//  它代表：「这个容器的一份能力正在被消耗」
//
//  生命周期：ISSUED → ACTIVE → RELEASED | VOIDED
//
//  物理定律：
//    RULE-OCC-1  一个 Trip 只能产生一个 occupied_token
//    RULE-OCC-2  ACTIVE 时容器 occupied 不能超过 max_parallel
//    RULE-OCC-3  RELEASED 后 proof_hash 必须有值
//    RULE-OCC-4  VOIDED 时 proof_hash 为空，step_count 不计入 cap_level
//    RULE-OCC-5  deadline 超时系统自动 VOID
// ============================================================

package vmlcore

import (
	"fmt"
	"time"
)

// ── 状态枚举 ──────────────────────────────────────────────────

type OccupiedTokenStatus string

const (
	OccTokenIssued   OccupiedTokenStatus = "ISSUED"   // 产生，等待容器确认
	OccTokenActive   OccupiedTokenStatus = "ACTIVE"   // 执行中，容器锁定
	OccTokenReleased OccupiedTokenStatus = "RELEASED" // 完成，能力释放
	OccTokenVoided   OccupiedTokenStatus = "VOIDED"   // 中止，不计入业绩
)

type OccOutcome string

const (
	OutcomePending OccOutcome = "PENDING" // 执行中
	OutcomePass    OccOutcome = "PASS"    // 通过（proof_hash 有值）
	OutcomeFail    OccOutcome = "FAIL"    // 失败（执行但未通过验收）
	OutcomeVoid    OccOutcome = "VOID"    // 中止（不计分）
)

// ── 核心数据结构 ──────────────────────────────────────────────

// OccupiedToken 容器占用令牌
type OccupiedToken struct {
	// 协议地址
	TokenRef string `json:"token_ref"`
	// 格式：v://{ns}/occ-token/{trip_slug}@{seq}

	// 绑定关系（三元组，锁死来源）
	ContainerRef string `json:"container_ref"` // LAW-3
	TripRef      string `json:"trip_ref"`
	SPURef       string `json:"spu_ref"`

	// 执行信息
	ExecutorRef string `json:"executor_ref"`

	// 生命周期时间戳
	IssuedAt   time.Time  `json:"issued_at"`
	ActivatedAt *time.Time `json:"activated_at,omitempty"`
	ReleasedAt  *time.Time `json:"released_at,omitempty"`
	VoidedAt    *time.Time `json:"voided_at,omitempty"`
	Deadline    *time.Time `json:"deadline,omitempty"` // 超时自动 VOID

	// 状态
	Status  OccupiedTokenStatus `json:"status"`
	Outcome OccOutcome          `json:"outcome"`

	// 执行质量数据（cap_level 演化的输入）
	StepCount int     `json:"step_count"` // 完成的 Step 数
	SPUWeight float64 `json:"spu_weight"` // SPU 难度系数 [0.5, 3.0]

	// 因果锁定
	ProofHash string `json:"proof_hash,omitempty"` // rolling-hash-v1 结果
	RootHash  string `json:"root_hash,omitempty"`

	// 关联
	AchievementRef string `json:"achievement_ref,omitempty"` // 产出的业绩 UTXO
}

// ── 状态机 ────────────────────────────────────────────────────

// Issue 创建 token（Trip 开始时调用）
// RULE-OCC-1：一个 Trip 只能产生一个 token
func Issue(tokenRef, containerRef, tripRef, spuRef, executorRef string,
	spuWeight float64, deadline *time.Time) (*OccupiedToken, error) {

	if containerRef == "" {
		return nil, fmt.Errorf("occ-token: container_ref required (LAW-3)")
	}
	if tripRef == "" {
		return nil, fmt.Errorf("occ-token: trip_ref required (RULE-OCC-1)")
	}
	if spuRef == "" {
		return nil, fmt.Errorf("occ-token: spu_ref required")
	}
	if spuWeight <= 0 {
		spuWeight = 1.0 // 默认难度系数
	}

	now := time.Now()
	return &OccupiedToken{
		TokenRef:     tokenRef,
		ContainerRef: containerRef,
		TripRef:      tripRef,
		SPURef:       spuRef,
		ExecutorRef:  executorRef,
		IssuedAt:     now,
		Deadline:     deadline,
		Status:       OccTokenIssued,
		Outcome:      OutcomePending,
		SPUWeight:    spuWeight,
	}, nil
}

// Activate 激活 token（容器确认承接后）
// 容器 occupied + 1 由调用方负责（数据库层）
func (t *OccupiedToken) Activate() error {
	if t.Status != OccTokenIssued {
		return fmt.Errorf("occ-token: cannot activate from status %s", t.Status)
	}
	now := time.Now()
	t.Status = OccTokenActive
	t.ActivatedAt = &now
	return nil
}

// AddStep 记录一个 Step 完成（SIGNED）
func (t *OccupiedToken) AddStep() error {
	if t.Status != OccTokenActive {
		return fmt.Errorf("occ-token: cannot add step to token in status %s", t.Status)
	}
	t.StepCount++
	return nil
}

// Release 释放 token（Trip 完成，结果 PASS 或 FAIL）
// RULE-OCC-3：RELEASED 后 proof_hash 必须有值（PASS 情况下）
// 容器 occupied - 1 由调用方负责（数据库层）
func (t *OccupiedToken) Release(outcome OccOutcome, proofHash, rootHash string) error {
	if t.Status != OccTokenActive {
		return fmt.Errorf("occ-token: cannot release from status %s", t.Status)
	}
	if outcome == OutcomePass && proofHash == "" {
		return fmt.Errorf("occ-token: RULE-OCC-3 violation: PASS requires proof_hash")
	}
	if outcome == OutcomeVoid {
		return fmt.Errorf("occ-token: use Void() for void outcome")
	}

	now := time.Now()
	t.Status = OccTokenReleased
	t.Outcome = outcome
	t.ReleasedAt = &now
	t.ProofHash = proofHash
	t.RootHash = rootHash
	return nil
}

// Void 中止 token（Trip 被取消或超时）
// RULE-OCC-4：VOIDED 时 step_count 不计入 cap_level
// RULE-OCC-5：deadline 超时自动 VOID
func (t *OccupiedToken) Void(reason string) error {
	if t.Status == OccTokenReleased {
		return fmt.Errorf("occ-token: cannot void a released token")
	}
	if t.Status == OccTokenVoided {
		return nil // 幂等
	}
	now := time.Now()
	t.Status = OccTokenVoided
	t.Outcome = OutcomeVoid
	t.VoidedAt = &now
	t.ProofHash = "" // RULE-OCC-4
	return nil
}

// CheckDeadline 检查是否超时，超时则自动 VOID（RULE-OCC-5）
func (t *OccupiedToken) CheckDeadline() bool {
	if t.Deadline == nil {
		return false
	}
	if time.Now().After(*t.Deadline) && t.Status == OccTokenActive {
		_ = t.Void("deadline exceeded")
		return true
	}
	return false
}

// IsExpired 是否已超时
func (t *OccupiedToken) IsExpired() bool {
	if t.Deadline == nil {
		return false
	}
	return time.Now().After(*t.Deadline)
}

// CanContribute 是否可以贡献 cap_level 演化
// RULE-OCC-4：VOID 的 token 不贡献
func (t *OccupiedToken) CanContribute() bool {
	return t.Status == OccTokenReleased && t.Outcome == OutcomePass
}

// ExecutionDays 执行天数（从 Activate 到 Release）
func (t *OccupiedToken) ExecutionDays() float64 {
	if t.ActivatedAt == nil || t.ReleasedAt == nil {
		return 0
	}
	return t.ReleasedAt.Sub(*t.ActivatedAt).Hours() / 24
}

// ── 容器占用追踪 ──────────────────────────────────────────────

// ContainerOccupancy 容器当前占用状态
type ContainerOccupancy struct {
	ContainerRef string `json:"container_ref"`
	MaxParallel  int    `json:"max_parallel"`
	Occupied     int    `json:"occupied"`      // 当前 ACTIVE token 数
	Available    int    `json:"available"`     // max_parallel - occupied
}

// CanAccept RULE-OCC-2：新 Trip 能否被容器接受
func (o *ContainerOccupancy) CanAccept() error {
	if o.Occupied >= o.MaxParallel {
		return fmt.Errorf(
			"occ-token: RULE-OCC-2 violation: container %s at capacity (%d/%d)",
			o.ContainerRef, o.Occupied, o.MaxParallel,
		)
	}
	return nil
}

// ── Token Ref 生成规则 ─────────────────────────────────────────

// MakeTokenRef 生成标准 token_ref
// 格式：v://{ns}/occ-token/{trip_slug}@v{seq}
func MakeTokenRef(nsRef, tripSlug string, seq int) string {
	return fmt.Sprintf("%s/occ-token/%s@v%d", nsRef, tripSlug, seq)
}
