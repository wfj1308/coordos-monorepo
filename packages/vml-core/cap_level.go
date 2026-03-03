// ============================================================
//  cap_level.go
//  Package: vml-core
//
//  container cap_level 自动演化算法
//
//  核心原则：
//    cap_level 不是填的，是 occupied_token RELEASED 后自动算的。
//    每次释放一个 PASS token，cap_level 根据执行质量增长。
//    长期不执行，cap_level 缓慢衰减。
//
//  演化公式：
//    单次增量：
//      Δ = spu_weight × step_count × outcome × α
//      α = base_alpha × (1 - L_current / 12)   ← 随等级递减
//
//    衰减项：
//      decay = gap_days / 365 × β
//      β = 0.05
//
//    新等级：
//      L_new = clamp( L_current + Δ - decay, 0, 10 )
//
//  能力等级划分：
//    [0,  2)   NOVICE
//    [2,  4)   REGISTERED_ENGINEER
//    [4,  6)   SENIOR_ENGINEER
//    [6,  8)   EXPERT
//    [8, 10]   MASTER
// ============================================================

package vmlcore

import (
	"fmt"
	"math"
	"time"
)

// ── 常量 ──────────────────────────────────────────────────────

const (
	// 学习率基础值
	BaseAlpha = 0.15

	// 衰减率（每年衰减 5%）
	DecayBeta = 0.05

	// 等级上限
	CapLevelMax = 10.0
	CapLevelMin = 0.0

	// 学习率计算中的分母
	// α = BaseAlpha × (1 - L / AlphaDenominator)
	// AlphaDenominator = 12 使得 L=10 时 α ≈ 0.025（仍然有增长）
	AlphaDenominator = 12.0
)

// CapGrade 能力等级枚举
type CapGrade string

const (
	GradeNovice             CapGrade = "NOVICE"
	GradeRegisteredEngineer CapGrade = "REGISTERED_ENGINEER"
	GradeSeniorEngineer     CapGrade = "SENIOR_ENGINEER"
	GradeExpert             CapGrade = "EXPERT"
	GradeMaster             CapGrade = "MASTER"
)

// ── 核心输入/输出 ─────────────────────────────────────────────

// EvolutionInput cap_level 演化输入（来自一个 RELEASED OccupiedToken）
type EvolutionInput struct {
	CurrentLevel float64    // 当前 cap_level ∈ [0, 10]
	StepCount    int        // 本次执行完成的步骤数
	SPUWeight    float64    // SPU 难度系数 ∈ [0.5, 3.0]
	Outcome      OccOutcome // PASS / FAIL / VOID
	LastExecAt   *time.Time // 上次执行完成时间（nil 表示首次）
	Now          *time.Time // 当前时间（nil 使用 time.Now()，便于测试）
}

// EvolutionResult cap_level 演化结果
type EvolutionResult struct {
	PrevLevel  float64  `json:"prev_level"`
	NewLevel   float64  `json:"new_level"`
	Delta      float64  `json:"delta"`       // 本次增量（未减 decay）
	Decay      float64  `json:"decay"`       // 时间衰减量
	NetChange  float64  `json:"net_change"`  // delta - decay
	Alpha      float64  `json:"alpha"`       // 实际学习率
	GapDays    float64  `json:"gap_days"`    // 距上次执行天数
	PrevGrade  CapGrade `json:"prev_grade"`
	NewGrade   CapGrade `json:"new_grade"`
	GradeUp    bool     `json:"grade_up"`    // 是否升级
	Algorithm  string   `json:"algorithm"`   // "cap-evolution-v1"
}

// ── 核心演化函数 ──────────────────────────────────────────────

// Evolve 计算 cap_level 演化结果。
// 这是 cap_level 的唯一合法更新路径。
//
// 调用时机：occupied_token 状态变为 RELEASED 时。
// VOIDED token 不调用此函数（RULE-OCC-4）。
func Evolve(in EvolutionInput) (*EvolutionResult, error) {
	// 验证输入
	if in.CurrentLevel < CapLevelMin || in.CurrentLevel > CapLevelMax {
		return nil, fmt.Errorf("cap-evolution: current_level %.2f out of range [0,10]", in.CurrentLevel)
	}
	if in.SPUWeight <= 0 {
		in.SPUWeight = 1.0
	}

	now := time.Now()
	if in.Now != nil {
		now = *in.Now
	}

	// ── 计算 gap_days ─────────────────────────────────────────
	var gapDays float64
	if in.LastExecAt != nil {
		gapDays = now.Sub(*in.LastExecAt).Hours() / 24
		if gapDays < 0 {
			gapDays = 0
		}
	}

	// ── 计算学习率 α ──────────────────────────────────────────
	// α = BaseAlpha × (1 - L / AlphaDenominator)
	// 高等级时学习率递减，但永远 > 0（L=10 时 α = BaseAlpha/6 ≈ 0.025）
	alpha := BaseAlpha * (1.0 - in.CurrentLevel/AlphaDenominator)
	if alpha < 0.01 {
		alpha = 0.01 // 最低学习率保证，防止完全停止增长
	}

	// ── 计算本次增量 Δ ────────────────────────────────────────
	// Δ = spu_weight × step_count × outcome_factor × α
	outcomeFactor := 0.0
	switch in.Outcome {
	case OutcomePass:
		outcomeFactor = 1.0
	case OutcomeFail:
		outcomeFactor = 0.0 // 失败不增长，但 decay 继续（隐性惩罚）
	case OutcomeVoid:
		// VOID token 不应调用此函数，但防御性处理
		outcomeFactor = 0.0
	}

	delta := in.SPUWeight * float64(in.StepCount) * outcomeFactor * alpha

	// ── 计算衰减量 ────────────────────────────────────────────
	// decay = gap_days / 365 × β
	// 每年衰减 β = 0.05（5%的上限值，即0.5 cap_level）
	decay := (gapDays / 365.0) * DecayBeta * CapLevelMax
	// 衰减不超过本次增量的 50%（防止惩罚过重）
	maxDecay := math.Max(delta*0.5, 0.01)
	if decay > maxDecay {
		decay = maxDecay
	}

	// ── 计算新等级 ────────────────────────────────────────────
	newLevel := in.CurrentLevel + delta - decay
	newLevel = math.Max(CapLevelMin, math.Min(CapLevelMax, newLevel))
	// 精确到小数点后 4 位
	newLevel = math.Round(newLevel*10000) / 10000

	prevGrade := LevelToGrade(in.CurrentLevel)
	newGrade  := LevelToGrade(newLevel)

	return &EvolutionResult{
		PrevLevel: in.CurrentLevel,
		NewLevel:  newLevel,
		Delta:     math.Round(delta*10000) / 10000,
		Decay:     math.Round(decay*10000) / 10000,
		NetChange: math.Round((delta-decay)*10000) / 10000,
		Alpha:     math.Round(alpha*10000) / 10000,
		GapDays:   math.Round(gapDays*100) / 100,
		PrevGrade: prevGrade,
		NewGrade:  newGrade,
		GradeUp:   newGrade != prevGrade && newLevel > in.CurrentLevel,
		Algorithm: "cap-evolution-v1",
	}, nil
}

// EvolveFromToken 从一个已释放的 OccupiedToken 直接计算演化
func EvolveFromToken(token *OccupiedToken, currentLevel float64, lastExecAt *time.Time) (*EvolutionResult, error) {
	if token.Status != OccTokenReleased {
		return nil, fmt.Errorf("cap-evolution: token must be RELEASED, got %s", token.Status)
	}
	// RULE-OCC-4：VOID 不计入
	if token.Outcome == OutcomeVoid {
		return nil, fmt.Errorf("cap-evolution: RULE-OCC-4: VOID token does not contribute")
	}

	return Evolve(EvolutionInput{
		CurrentLevel: currentLevel,
		StepCount:    token.StepCount,
		SPUWeight:    token.SPUWeight,
		Outcome:      token.Outcome,
		LastExecAt:   lastExecAt,
	})
}

// ── 等级计算 ──────────────────────────────────────────────────

// LevelToGrade 数值等级 → 枚举等级
func LevelToGrade(level float64) CapGrade {
	switch {
	case level >= 8.0:
		return GradeMaster
	case level >= 6.0:
		return GradeExpert
	case level >= 4.0:
		return GradeSeniorEngineer
	case level >= 2.0:
		return GradeRegisteredEngineer
	default:
		return GradeNovice
	}
}

// GradeThreshold 等级对应的最低 cap_level
func GradeThreshold(grade CapGrade) float64 {
	switch grade {
	case GradeMaster:             return 8.0
	case GradeExpert:             return 6.0
	case GradeSeniorEngineer:     return 4.0
	case GradeRegisteredEngineer: return 2.0
	default:                      return 0.0
	}
}

// ── 批量演化（历史数据迁移用）────────────────────────────────

// BatchEvolveInput 批量演化的单条记录
type BatchEvolveInput struct {
	StepCount  int
	SPUWeight  float64
	Outcome    OccOutcome
	ExecutedAt time.Time
}

// BatchEvolve 从零开始，按时间顺序重算 cap_level
// 用于历史数据迁移或验证
func BatchEvolve(records []BatchEvolveInput) (float64, []*EvolutionResult, error) {
	if len(records) == 0 {
		return 0, nil, nil
	}

	level := 0.0
	results := make([]*EvolutionResult, 0, len(records))
	var lastAt *time.Time

	for i, rec := range records {
		execAt := rec.ExecutedAt
		result, err := Evolve(EvolutionInput{
			CurrentLevel: level,
			StepCount:    rec.StepCount,
			SPUWeight:    rec.SPUWeight,
			Outcome:      rec.Outcome,
			LastExecAt:   lastAt,
			Now:          &execAt,
		})
		if err != nil {
			return level, results, fmt.Errorf("batch evolve record[%d]: %w", i, err)
		}
		level = result.NewLevel
		results = append(results, result)
		lastAt = &execAt
	}

	return level, results, nil
}

// ── 预测 ──────────────────────────────────────────────────────

// StepsToNextGrade 估算达到下一等级需要多少次执行
// 用于给工程师展示成长路径
func StepsToNextGrade(currentLevel float64, avgStepCount int, avgSPUWeight float64) int {
	currentGrade := LevelToGrade(currentLevel)
	var targetLevel float64

	switch currentGrade {
	case GradeNovice:             targetLevel = 2.0
	case GradeRegisteredEngineer: targetLevel = 4.0
	case GradeSeniorEngineer:     targetLevel = 6.0
	case GradeExpert:             targetLevel = 8.0
	case GradeMaster:             return 0 // 已是最高级
	}

	if avgStepCount <= 0 { avgStepCount = 3 }
	if avgSPUWeight <= 0 { avgSPUWeight = 1.0 }

	count := 0
	level := currentLevel
	for level < targetLevel && count < 10000 {
		result, err := Evolve(EvolutionInput{
			CurrentLevel: level,
			StepCount:    avgStepCount,
			SPUWeight:    avgSPUWeight,
			Outcome:      OutcomePass,
			LastExecAt:   nil, // 忽略衰减，只算增长
		})
		if err != nil {
			break
		}
		level = result.NewLevel
		count++
	}
	return count
}
