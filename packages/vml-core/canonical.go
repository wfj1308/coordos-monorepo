// ============================================================
//  canonical.go
//  Profile: v://vml/metering/canonical@v1
//
//  目标：同一份逻辑数据，在任何语言、任何节点，
//        产生完全一致的字节序列。
//
//  规则摘要：
//    - UTF-8，无 BOM
//    - JSON，无注释，无尾逗号，无多余空格
//    - 对象字段按 ASCII 字典序升序递归排序
//    - 不允许 null / undefined / 空字符串可选字段
//    - 整数不加引号，不允许科学计数法
//    - 布尔只用 true / false
//    - 哈希计算前必须排除 "sign" 字段
// ============================================================

package vmlcore

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// CanonicalProfile 规范化版本标识
const CanonicalProfile = "v://vml/metering/canonical@v1"

// ── 公开入口 ──────────────────────────────────────────────────

// CanonicalBytes 把任意 Go 值规范化为字节序列。
// 这是所有哈希计算的唯一入口。
//
// 调用者保证：
//   - obj 是 map[string]any / []any / string / float64 / bool / nil
//     （即 json.Unmarshal 的自然产物）
//   - 或任何可被 json.Marshal 序列化的结构体
//
// 函数保证：
//   - 排除 "sign" 字段（哈希循环依赖防护）
//   - 字段按 ASCII 字典序递归排序
//   - null / 空字符串 / 空数组 移除（可选字段约定）
//   - 输出是 minified JSON，UTF-8，无 BOM
func CanonicalBytes(obj any) ([]byte, error) {
	// Step 1：统一转成 map[string]any / []any 的树
	tree, err := toTree(obj)
	if err != nil {
		return nil, fmt.Errorf("canonical: toTree: %w", err)
	}

	// Step 2：排除 sign 字段
	stripped := stripSign(tree)

	// Step 3：规范化（排序 + 清理 null）
	normalized := normalize(stripped)

	// Step 4：序列化为 minified JSON
	b, err := marshalCanonical(normalized)
	if err != nil {
		return nil, fmt.Errorf("canonical: marshal: %w", err)
	}

	// Step 5：验证 UTF-8（防御性检查）
	if !utf8.Valid(b) {
		return nil, fmt.Errorf("canonical: output is not valid UTF-8")
	}

	return b, nil
}

// CanonicalBytesKeepSign 和 CanonicalBytes 相同，但保留 sign 字段。
// 仅用于签名验证，不用于哈希计算。
func CanonicalBytesKeepSign(obj any) ([]byte, error) {
	tree, err := toTree(obj)
	if err != nil {
		return nil, err
	}
	normalized := normalize(tree)
	return marshalCanonical(normalized)
}

// ── Step 1: toTree ─────────────────────────────────────────────
// 把任意类型转成 JSON 树（map[string]any / []any / scalar）

func toTree(obj any) (any, error) {
	switch v := obj.(type) {
	case map[string]any, []any, string, float64, bool, nil:
		return v, nil
	case []string:
		out := make([]any, len(v))
		for i, s := range v {
			out[i] = s
		}
		return out, nil
	case map[string]string:
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = val
		}
		return out, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var tree any
		if err := json.Unmarshal(b, &tree); err != nil {
			return nil, err
		}
		return tree, nil
	}
}

// ── Step 2: stripSign ─────────────────────────────────────────
// 递归排除 "sign" 字段

func stripSign(v any) any {
	switch node := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(node))
		for k, val := range node {
			if k == "sign" {
				continue
			}
			out[k] = stripSign(val)
		}
		return out
	case []any:
		out := make([]any, len(node))
		for i, item := range node {
			out[i] = stripSign(item)
		}
		return out
	default:
		return v
	}
}

// ── Step 3: normalize ─────────────────────────────────────────
// 递归清理 null / 空字符串 / 空数组（可选字段约定）
// 注意：必填字段为空会在上层验证，此处只清理

func normalize(v any) any {
	switch node := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(node))
		for k, val := range node {
			cleaned := normalize(val)
			if isAbsent(cleaned) {
				continue // 删除空可选字段
			}
			out[k] = cleaned
		}
		return out
	case []any:
		out := make([]any, 0, len(node))
		for _, item := range node {
			cleaned := normalize(item)
			if !isAbsent(cleaned) {
				out = append(out, cleaned)
			}
		}
		if len(out) == 0 {
			return nil // 空数组视为缺失
		}
		return out
	default:
		return v
	}
}

func isAbsent(v any) bool {
	if v == nil {
		return true
	}
	if s, ok := v.(string); ok && s == "" {
		return true
	}
	return false
}

// ── Step 4: marshalCanonical ──────────────────────────────────
// 手写序列化器：保证字段 ASCII 升序、数值不科学计数

func marshalCanonical(v any) ([]byte, error) {
	var sb strings.Builder
	if err := writeValue(&sb, v); err != nil {
		return nil, err
	}
	return []byte(sb.String()), nil
}

func writeValue(sb *strings.Builder, v any) error {
	switch node := v.(type) {
	case nil:
		sb.WriteString("null")

	case bool:
		if node {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}

	case int:
		sb.WriteString(strconv.FormatInt(int64(node), 10))

	case int64:
		sb.WriteString(strconv.FormatInt(node, 10))

	case float64:
		if err := writeNumber(sb, node); err != nil {
			return err
		}

	case string:
		writeString(sb, node)

	case []string:
		sb.WriteByte('[')
		for i, item := range node {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeString(sb, item)
		}
		sb.WriteByte(']')

	case []any:
		sb.WriteByte('[')
		for i, item := range node {
			if i > 0 {
				sb.WriteByte(',')
			}
			if err := writeValue(sb, item); err != nil {
				return err
			}
		}
		sb.WriteByte(']')

	case map[string]any:
		keys := make([]string, 0, len(node))
		for k := range node {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		sb.WriteByte('{')
		first := true
		for _, k := range keys {
			val := node[k]
			if val == nil {
				continue
			}
			if !first {
				sb.WriteByte(',')
			}
			first = false
			writeString(sb, k)
			sb.WriteByte(':')
			if err := writeValue(sb, val); err != nil {
				return err
			}
		}
		sb.WriteByte('}')

	default:
		return fmt.Errorf("canonical: unsupported type %T", v)
	}
	return nil
}

// writeNumber 规范化数值输出：
//   - 整数：不加小数点
//   - 浮点：保留精度，不用科学计数法
//   - 不允许 NaN / Inf
func writeNumber(sb *strings.Builder, f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("canonical: NaN/Inf not allowed")
	}
	// 判断是否为整数
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		sb.WriteString(strconv.FormatInt(int64(f), 10))
		return nil
	}
	// 浮点：使用 'f' 格式，保留完整精度
	s := strconv.FormatFloat(f, 'f', -1, 64)
	sb.WriteString(s)
	return nil
}

// writeString 输出 JSON 字符串，手动转义
func writeString(sb *strings.Builder, s string) {
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 {
				// 控制字符用 \uXXXX
				sb.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
}
