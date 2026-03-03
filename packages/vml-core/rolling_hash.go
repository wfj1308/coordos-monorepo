// ============================================================
//  rolling_hash.go
//  Algorithm: rolling-hash-v1
//
//  目标：锁定整个 Trip 链，使每一步的哈希依赖前序所有步骤。
//
//  精确定义：
//    h_1 = SHA256( C(S_1) )
//    h_i = SHA256( h_{i-1} || C(S_i) )    i > 1
//
//    root_hash = h_n
//
//    proof_hash = SHA256(
//        root_hash  ||
//        UTF8(container_ref) ||
//        UTF8(spu_ref)       ||
//        UTF8("v1")
//    )
//
//  其中：
//    C(S_i) = CanonicalBytes(S_i)
//    ||      = raw bytes 拼接（非字符串拼接）
//
//  防重放：
//    proof_hash 绑定 container_ref + spu_ref，
//    同一条 Trip 链无法被重用到其他容器或 SPU。
// ============================================================

package vmlcore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ── 公开类型 ──────────────────────────────────────────────────

// RollingHasher 滚动哈希计算器。
// 顺序调用 Add，最后调用 Finalize。
type RollingHasher struct {
	prev  []byte // 上一步的 h_{i-1}，len=32 或 nil（初始）
	steps int    // 已累积步骤数
}

// StepResult 单步哈希结果
type StepResult struct {
	StepSeq  int    // 步骤序号（1-based）
	StepHash string // hex(h_i)，32字节
	Inputs   []byte // C(S_i)，canonical bytes
}

// ProofHashResult 最终 proof_hash 计算结果
type ProofHashResult struct {
	RootHash     string // hex(root_hash = h_n)
	ProofHash    string // hex(proof_hash)，含容器绑定
	ContainerRef string
	SPURef       string
	StepCount    int
	Algorithm    string // "rolling-hash-v1"
}

// ── RollingHasher 方法 ────────────────────────────────────────

// NewRollingHasher 创建新的哈希器
func NewRollingHasher() *RollingHasher {
	return &RollingHasher{}
}

// Add 添加一个步骤，返回本步的哈希。
//
// step 可以是：
//   - map[string]any（已解析的 JSON 树）
//   - 任何可被 CanonicalBytes 处理的类型
//
// 内部逻辑：
//   h_1 = SHA256( C(S_1) )
//   h_i = SHA256( h_{i-1} || C(S_i) )
func (r *RollingHasher) Add(step any) (*StepResult, error) {
	// Step A：规范化
	cb, err := CanonicalBytes(step)
	if err != nil {
		return nil, fmt.Errorf("rolling-hash: step %d canonical: %w", r.steps+1, err)
	}

	// Step B：计算 h_i
	var h [32]byte
	if r.prev == nil {
		// 第一步：h_1 = SHA256( C(S_1) )
		h = sha256.Sum256(cb)
	} else {
		// 后续步骤：h_i = SHA256( h_{i-1} || C(S_i) )
		// raw bytes 拼接，不是字符串拼接
		combined := concat(r.prev, cb)
		h = sha256.Sum256(combined)
	}

	r.prev = h[:]
	r.steps++

	return &StepResult{
		StepSeq:  r.steps,
		StepHash: hex.EncodeToString(h[:]),
		Inputs:   cb,
	}, nil
}

// Finalize 计算最终 proof_hash，绑定 container_ref 和 spu_ref。
//
// proof_hash = SHA256(
//     root_hash       ||   -- 32 bytes
//     UTF8(container_ref) ||
//     UTF8(spu_ref)        ||
//     UTF8("v1")
// )
//
// 防重放保证：
//   任何试图把同一条 Trip 链移到不同容器/SPU 的操作，
//   都会产生不同的 proof_hash，验证立即失败。
func (r *RollingHasher) Finalize(containerRef, spuRef string) (*ProofHashResult, error) {
	if r.steps == 0 {
		return nil, fmt.Errorf("rolling-hash: no steps added")
	}
	if containerRef == "" {
		return nil, fmt.Errorf("rolling-hash: container_ref required (LAW-3)")
	}
	if spuRef == "" {
		return nil, fmt.Errorf("rolling-hash: spu_ref required")
	}

	rootHash := r.prev // h_n

	// proof_hash = SHA256( root_hash || container_ref || spu_ref || "v1" )
	combined := concat(
		rootHash,
		[]byte(containerRef),
		[]byte(spuRef),
		[]byte("v1"),
	)
	ph := sha256.Sum256(combined)

	return &ProofHashResult{
		RootHash:     hex.EncodeToString(rootHash),
		ProofHash:    hex.EncodeToString(ph[:]),
		ContainerRef: containerRef,
		SPURef:       spuRef,
		StepCount:    r.steps,
		Algorithm:    "rolling-hash-v1",
	}, nil
}

// ── 独立函数入口 ──────────────────────────────────────────────

// ComputeRollingHash 一次性计算整条 Trip 链的 proof_hash。
// steps 按顺序传入，必须至少有一个元素。
func ComputeRollingHash(steps []any, containerRef, spuRef string) (*ProofHashResult, error) {
	if len(steps) == 0 {
		return nil, fmt.Errorf("rolling-hash: steps must not be empty")
	}
	h := NewRollingHasher()
	for i, s := range steps {
		if _, err := h.Add(s); err != nil {
			return nil, fmt.Errorf("rolling-hash: step[%d]: %w", i, err)
		}
	}
	return h.Finalize(containerRef, spuRef)
}

// VerifyProofHash 第三方验证算法（完全确定性）。
//
// 输入：
//   steps        []any    -- 原始 Trip 步骤列表（顺序一致）
//   containerRef string   -- 绑定的容器引用
//   spuRef       string   -- 绑定的 SPU 引用
//   expected     string   -- 待验证的 proof_hash（hex string）
//
// 算法：
//   1. 重算 rolling-hash
//   2. 计算 proof_hash
//   3. 与 expected 对比
//   4. 完全一致 → true，否则 false
//
// 没有模糊空间。任何一个字节不一致都返回 false。
func VerifyProofHash(steps []any, containerRef, spuRef, expected string) (bool, error) {
	result, err := ComputeRollingHash(steps, containerRef, spuRef)
	if err != nil {
		return false, err
	}
	return result.ProofHash == expected, nil
}

// VerifyStepHash 验证单步哈希（用于增量验证）。
//
// prevHash：上一步的 StepHash（第一步传 ""）
// step：当前步骤数据
// expected：期望的 StepHash
func VerifyStepHash(prevHash string, step any, expected string) (bool, error) {
	cb, err := CanonicalBytes(step)
	if err != nil {
		return false, err
	}

	var h [32]byte
	if prevHash == "" {
		h = sha256.Sum256(cb)
	} else {
		prev, err := hex.DecodeString(prevHash)
		if err != nil {
			return false, fmt.Errorf("rolling-hash: invalid prevHash hex: %w", err)
		}
		if len(prev) != 32 {
			return false, fmt.Errorf("rolling-hash: prevHash must be 32 bytes")
		}
		h = sha256.Sum256(concat(prev, cb))
	}

	return hex.EncodeToString(h[:]) == expected, nil
}

// ── 工具 ──────────────────────────────────────────────────────

// concat raw bytes 拼接（非字符串）
func concat(parts ...[]byte) []byte {
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]byte, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// StepHashFromCanonical 从已有的 canonical bytes 计算单步哈希
// 用于不需要反序列化的场景
func StepHashFromCanonical(prevHashHex string, canonicalBytes []byte) (string, error) {
	var h [32]byte
	if prevHashHex == "" {
		h = sha256.Sum256(canonicalBytes)
	} else {
		prev, err := hex.DecodeString(prevHashHex)
		if err != nil {
			return "", err
		}
		h = sha256.Sum256(concat(prev, canonicalBytes))
	}
	return hex.EncodeToString(h[:]), nil
}
