// utxo_ingest.go
// SPU系统打入UTXO的接收端点
// POST /api/utxo/ingest
// 这是桥梁SPU系统和设计院管理系统之间的唯一接口

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	vmlcore "coordos/vml-core"
)

// ── 请求结构 ──────────────────────────────────────────────────

type UTXOIngestRequest struct {
	SPURef       string            `json:"spu_ref"`       // v://zhongbei/spu/bridge/pile_foundation_drawing@v1
	ProjectRef   string            `json:"project_ref"`   // v://zhongbei/project/highway-001/design/structure
	ContainerRef string            `json:"container_ref"` // 执行体能力容器引用
	Steps        []json.RawMessage `json:"steps"`         // Trip 步骤列表，用于计算 rolling-hash
	Payload      json.RawMessage   `json:"payload"`       // 产出内容（图纸引用/文件hash等）
	ProofHash    string            `json:"proof_hash"`    // rolling-hash-v1 证明
	StepUTXOs    map[string]string `json:"step_utxos"`    // 各步骤产出的UTXO引用
	IngestedAt   string            `json:"ingested_at"`   // ISO8601
}

type UTXOIngestResponse struct {
	UTXORef       string  `json:"utxo_ref"`
	AchievementID int64   `json:"achievement_id"`
	ContractID    *int64  `json:"contract_id,omitempty"`   // 自动匹配到的合同
	SettleTrigger bool    `json:"settle_trigger"`          // 是否触发了结算检查
	Status        string  `json:"status"`
}

// ── Handler ───────────────────────────────────────────────────

type UTXOIngestHandler struct {
	achievementStore AchievementStore
	contractMatcher  ContractMatcher
	settleEngine     SettleEngine
}

func (h *UTXOIngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. 解析请求
	var req UTXOIngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// 2. 校验必要字段
	if req.SPURef == "" || req.ProjectRef == "" || req.ContainerRef == "" || len(req.Steps) == 0 {
		http.Error(w, "spu_ref, project_ref, container_ref, and non-empty steps are required", http.StatusBadRequest)
		return
	}

	// 3. 验证 proof_hash (使用 rolling-hash-v1)
	steps := make([]any, len(req.Steps))
	for i, s := range req.Steps {
		var stepData any
		if err := json.Unmarshal(s, &stepData); err != nil {
			http.Error(w, fmt.Sprintf("invalid json in steps[%d]: %v", i, err), http.StatusBadRequest)
			return
		}
		steps[i] = stepData
	}

	result, err := vmlcore.ComputeRollingHash(steps, req.ContainerRef, req.SPURef)
	if err != nil {
		http.Error(w, "failed to compute proof_hash: "+err.Error(), http.StatusInternalServerError)
		return
	}
	expected := result.ProofHash

	if req.ProofHash != expected {
		http.Error(w, "proof_hash mismatch", http.StatusBadRequest)
		return
	}

	// 4. 生成 UTXO ref
	utxoRef := fmt.Sprintf("v://zhongbei/utxo/%s/%d",
		sanitizeRef(req.ProjectRef), time.Now().UnixNano())

	// 5. 写入业绩库
	achievement := &AchievementUTXO{
		UTXORef:     utxoRef,
		SPURef:      req.SPURef,
		ProjectRef:  req.ProjectRef,
		ExecutorRef: req.ContainerRef, // 使用 ContainerRef 填充
		Steps:       req.Steps,        // 存储完整步骤，用于第三方验证
		Payload:     req.Payload,
		ProofHash:   req.ProofHash,
		StepUTXOs:   req.StepUTXOs,
		Status:      "PENDING",
		Source:      "SPU_INGEST",
		IngestedAt:  time.Now(),
	}

	id, err := h.achievementStore.Create(r.Context(), achievement)
	if err != nil {
		http.Error(w, "failed to store achievement", http.StatusInternalServerError)
		return
	}

	// 6. 自动匹配合同（根据 project_ref 找对应合同）
	contractID, _ := h.contractMatcher.Match(r.Context(), req.ProjectRef, req.ContainerRef)
	if contractID != nil {
		h.achievementStore.SetContract(r.Context(), id, *contractID)
	}

	// 7. 检查结算条件（RULE-005：有产出才能向上结算）
	settleTrigger := false
	if contractID != nil {
		triggered, err := h.settleEngine.CheckAndTrigger(r.Context(), *contractID, utxoRef)
		if err == nil {
			settleTrigger = triggered
		}
	}

	// 8. 返回
	resp := UTXOIngestResponse{
		UTXORef:       utxoRef,
		AchievementID: id,
		ContractID:    contractID,
		SettleTrigger: settleTrigger,
		Status:        "INGESTED",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

// ── 合同自动匹配（project_ref → contract） ───────────────────

type ContractMatcher interface {
	// 根据 project_ref 找对应合同
	// project_ref: v://zhongbei/project/highway-001/design/structure
	// 策略：先精确匹配 project_ref 字段，再模糊匹配合同名/编号
	Match(ctx interface{}, projectRef, executorRef string) (*int64, error)
}

// ── 结算引擎 ──────────────────────────────────────────────────

type SettleEngine interface {
	// 检查合同是否满足结算条件，满足则触发
	// RULE-005：叶子节点有实际产出才能向上结算
	CheckAndTrigger(ctx interface{}, contractID int64, utxoRef string) (bool, error)
}

// ── 业绩存储接口 ───────────────────────────────────────────────

type AchievementStore interface {
	Create(ctx interface{}, a *AchievementUTXO) (int64, error)
	SetContract(ctx interface{}, id int64, contractID int64) error
	Get(ctx interface{}, id int64) (*AchievementUTXO, error)
	ListByExecutor(ctx interface{}, executorRef string) ([]*AchievementUTXO, error)
	ListByProject(ctx interface{}, projectRef string) ([]*AchievementUTXO, error)
}

// ── 数据结构 ──────────────────────────────────────────────────

type AchievementUTXO struct {
	ID          int64
	UTXORef     string
	SPURef      string
	ProjectRef  string
	ExecutorRef string
	GenesisRef  string
	ContractID  *int64
	Payload     json.RawMessage
	Steps       []json.RawMessage `json:"steps,omitempty"` // 增加了 steps 字段，用于验证
	ProofHash   string
	StepUTXOs   map[string]string
	Status      string // PENDING/SETTLED/DISPUTED/LEGACY
	Source      string // SPU_INGEST/LEGACY_IMPORT/MANUAL
	IngestedAt  time.Time
	SettledAt   *time.Time
}

// ── 工具函数 ──────────────────────────────────────────────────

func sanitizeRef(ref string) string {
	// v://zhongbei/project/highway-001/design/structure
	// → zhongbei/project/highway-001/design/structure
	if len(ref) > 5 && ref[:5] == "v://" {
		return ref[5:]
	}
	return ref
}
