# VML 因果物理层算法规范 v1

**状态：LOCKED**  
**版本：v1**  
**生效日期：2026-03-02**  
**实现参考：`coordos/packages/vml-core`**

---

## 概述

本文档定义 VML（v:// Metering Layer）因果物理层的两个底层算法：

1. **canonical@v1** — 规范化序列化规则
2. **rolling-hash-v1** — Trip 链锁定算法

这两个算法是整个 CoordOS 信任体系的数学基础。

**核心保证：**
> 同一份逻辑数据，在任何语言、任何节点、任何时间，
> 经过这两个算法处理后，产生**完全一致**的字节序列和哈希值。

不满足此保证的实现，不得声称兼容 `canonical@v1` 或 `rolling-hash-v1`。

---

## 一、canonical@v1 规范化算法

**Profile 标识：**

```
v://vml/metering/canonical@v1
```

### 1.1 目标

将任意结构化数据转换为唯一、确定性的字节序列（Canonical Bytes），
使得 SHA-256(Canonical Bytes) 在所有兼容实现中产生相同结果。

### 1.2 规则（全部必须满足）

#### R-1 编码
- 输出必须是 **UTF-8** 编码
- **禁止** BOM（Byte Order Mark）

#### R-2 格式
- 使用 **JSON** 表达
- **禁止** 注释
- **禁止** 尾逗号
- **禁止** 额外空格或换行（输出是 minified JSON）

#### R-3 字段排序
对象内的字段按 **ASCII 字典序升序** 递归排列。

```
正确：{"a":1,"b":2,"z":3}
错误：{"z":3,"a":1,"b":2}
```

嵌套对象同样递归排序：

```
正确：{"inner":{"a":1,"z":2},"outer":{"b":3,"y":4}}
```

#### R-4 空值清理
- **禁止** `null` 出现在输出中（删除对应字段）
- **禁止** 空字符串 `""` 出现在输出中（删除对应字段）
- **禁止** `undefined`
- 可选字段不存在时，直接省略，不写 `null` 或 `""`

#### R-5 数值格式
- 整数值（无小数部分）：**不加小数点**，不加引号
- **禁止** 科学计数法（不得出现 `1e5`、`1.23e-4`）
- 浮点数：保留原始精度，不补零

```
正确：{"n":42}      （整数）
正确：{"n":3.14}    （浮点）
错误：{"n":42.0}    （整数不加小数点）
错误：{"n":1e5}     （禁止科学计数法）
```

#### R-6 布尔值
- 只允许 `true` / `false`
- **禁止** 字符串形式（`"true"` / `"false"`）

#### R-7 哈希排除字段
计算哈希前，**必须排除** `"sign"` 字段。

**原因：** `sign` 字段本身依赖哈希计算结果，若纳入计算会形成循环依赖。

### 1.3 伪代码

```
function canonical_bytes(obj):
    obj = deep_copy(obj)
    remove_field(obj, "sign")           # R-7
    obj = clean_nulls(obj)              # R-4：递归删除 null/空字符串
    obj = sort_keys_ascii(obj)          # R-3：递归按 ASCII 升序排序
    bytes = minified_json_encode(obj)   # R-1,R-2,R-5,R-6
    assert is_valid_utf8(bytes)         # R-1
    return bytes
```

### 1.4 权威参考向量

以下向量由 Go 参考实现生成，**任何兼容实现必须产生完全相同的结果**。

---

**向量 V1-1：基础字段排序**

```
输入：
{
  "output_name":   "桩基施工图",
  "output_type":   "DESIGN_DOC",
  "step_seq":      1,
  "executor_ref":  "v://cn.zhongbei/executor/person/cyp4310@v1",
  "container_ref": "v://cn.zhongbei/container/cert/reg-structure/cyp4310@v1"
}

Canonical Bytes（minified JSON，UTF-8）：
{"container_ref":"v://cn.zhongbei/container/cert/reg-structure/cyp4310@v1","executor_ref":"v://cn.zhongbei/executor/person/cyp4310@v1","output_name":"桩基施工图","output_type":"DESIGN_DOC","step_seq":1}

SHA-256(Canonical Bytes)：
56a39d5e10df3e00db5017d1826f920e4738c03426b5e69f404eb7f9d17227f1
```

---

**向量 V1-2：null 清理**

```
输入：{"a":"value","b":null,"c":""}
Canonical Bytes：{"a":"value"}
```

---

**向量 V1-3：sign 字段排除**

```
输入：{"a":1,"sign":"abc","z":2}
Canonical Bytes：{"a":1,"z":2}
```

---

**向量 V1-4：数值格式**

```
输入：{"f":3.14,"i":42,"neg":-7,"zero":0}
Canonical Bytes：{"f":3.14,"i":42,"neg":-7,"zero":0}
```

---

## 二、rolling-hash-v1 链锁定算法

### 2.1 目标

对整条 Trip 链（由有序的 Step 序列组成）计算一个**因果哈希**，
使得：
- 任意步骤内容的改变，导致 proof_hash 改变
- 步骤顺序的改变，导致 proof_hash 改变
- 相同步骤换到不同容器或 SPU，导致 proof_hash 改变（防重放）

### 2.2 精确定义

设：
- `S_i` 为第 i 个 Trip Step（i 从 1 开始）
- `C(S_i)` 为 `canonical_bytes(S_i)`（按 canonical@v1 计算）
- `H(x)` 为 `SHA-256(x)`
- `||` 为 **raw bytes 拼接**（非字符串拼接）

**逐步哈希：**

```
h_1 = H( C(S_1) )
h_i = H( h_{i-1} || C(S_i) )      for i > 1
```

**链根：**

```
root_hash = h_n    （n 为总步骤数）
```

**最终 proof_hash（绑定容器和 SPU，防重放）：**

```
proof_hash = H(
    root_hash           ||    # 32 bytes, raw
    UTF8(container_ref) ||    # variable length UTF-8 bytes
    UTF8(spu_ref)       ||    # variable length UTF-8 bytes
    UTF8("v1")                # 2 bytes: 0x76 0x31
)
```

### 2.3 关键约束

#### 字节拼接，非字符串拼接

```
正确：SHA256( bytes(h_{i-1}) + bytes(C(S_i)) )
错误：SHA256( hex(h_{i-1}) + string(C(S_i)) )
```

`h_{i-1}` 是 **32字节 raw bytes**，不是 64字符 hex 字符串。

#### 为什么绑定 container_ref 和 spu_ref

若不绑定，攻击者可以：
1. 取出合法的 Trip 链（步骤序列）
2. 换一个容器引用
3. 重新声称为另一个执行体的业绩

绑定后，`proof_hash` 在数学上锁定了：
- **谁做的**（container_ref → 具体执行体的能力证书）
- **做的是什么**（spu_ref → 业绩规格类型）

这是「能力绑定」专利的核心技术机制。

### 2.4 伪代码

```
function compute_proof_hash(steps, container_ref, spu_ref):
    assert len(steps) > 0
    assert container_ref != ""
    assert spu_ref != ""

    prev = nil
    for i, step in enumerate(steps):
        cb = canonical_bytes(step)
        if i == 0:
            h = SHA256(cb)
        else:
            h = SHA256(prev + cb)    # raw bytes concat
        prev = h

    root_hash = prev    # h_n, 32 bytes

    combined = root_hash + UTF8(container_ref) + UTF8(spu_ref) + UTF8("v1")
    proof_hash = SHA256(combined)

    return hex(proof_hash)
```

```
function verify_proof_hash(steps, container_ref, spu_ref, expected_hex):
    computed = compute_proof_hash(steps, container_ref, spu_ref)
    return computed == expected_hex    # 严格等值，无模糊空间
```

### 2.5 权威参考向量

---

**向量 V2-1：三步 Trip 链**

```
Steps（按顺序）：
  S1 = {"output_type":"SURVEY_REPORT","step_seq":1}
  S2 = {"output_type":"CALC_REPORT","step_seq":2}
  S3 = {"output_type":"DESIGN_DOC","step_seq":3}

container_ref = "v://cn.zhongbei/container/cert/reg-structure/cyp4310@v1"
spu_ref       = "v://cn.zhongbei/spu/bridge/pile_foundation_drawing@v1"

中间结果：
  C(S1) = {"output_type":"SURVEY_REPORT","step_seq":1}
  h_1   = SHA256(C(S1))

  C(S2) = {"output_type":"CALC_REPORT","step_seq":2}
  h_2   = SHA256(h_1 || C(S2))

  C(S3) = {"output_type":"DESIGN_DOC","step_seq":3}
  h_3   = SHA256(h_2 || C(S3))

最终结果：
  root_hash  = 86bb81e4c3022c6ad92a3ba335f520cba3213b4fc7862baa1b4ef5c90ef4170b
  proof_hash = 66af1f8c059c6fd3cde04dfbf1c2769e5b8db4ac81e074c6522e85b62192a1a2
```

任何兼容实现输入相同数据，**必须**输出相同的 `proof_hash`。

---

**向量 V2-2：顺序敏感性验证**

```
Steps [S1, S2] 的 proof_hash ≠ Steps [S2, S1] 的 proof_hash
```

---

**向量 V2-3：防重放验证**

```
相同 Steps，不同 container_ref：proof_hash 不同
相同 Steps，不同 spu_ref：proof_hash 不同
```

---

## 三、第三方验证协议

任何外部节点（不需要访问平台数据库）可以通过以下步骤独立验证一条业绩：

```
function verify_achievement(receipt, steps):

    # Step 1：验证步骤签名（可选，需公钥）
    for step in steps:
        if step.sign:
            verify_signature(step.sign, canonical_bytes(step))

    # Step 2：重算 rolling-hash
    computed_proof = compute_proof_hash(
        steps,
        receipt.container_ref,
        receipt.spu_ref
    )

    # Step 3：比对
    if computed_proof != receipt.proof_hash:
        return INVALID, "proof_hash mismatch"

    # Step 4：与链上锚定对比（可选，需访问区块链）
    if receipt.anchor.tx_hash:
        on_chain = query_blockchain(receipt.anchor.tx_hash)
        if on_chain.proof_hash != receipt.proof_hash:
            return INVALID, "on-chain anchor mismatch"

    return VALID
```

**结论：一致 → 合法，不一致 → 非法。没有模糊空间。**

---

## 四、完整物理闭环

```
Trip 执行
    ↓
Step i 完成
    ↓
canonical_bytes(S_i)    ← canonical@v1
    ↓
h_i = SHA256(h_{i-1} || C(S_i))    ← rolling-hash-v1
    ↓
所有步骤完成
    ↓
proof_hash = SHA256(root_hash || container_ref || spu_ref || "v1")
    ↓
OccupiedToken.Release(proof_hash)    ← 锁定，不可更改
    ↓
cap_level 演化    ← 能力增长
    ↓
Achievement UTXO    ← 业绩产出，携带 proof_hash
    ↓
任何人可独立验证    ← 不依赖平台
```

---

## 五、实现兼容性要求

声称兼容本规范的实现，必须：

1. 通过所有权威参考向量（向量编号 V1-1 到 V2-3）
2. 对 `TestReferenceVectors` 测试产生相同的 `proof_hash`：
   ```
   root_hash  = 86bb81e4c3022c6ad92a3ba335f520cba3213b4fc7862baa1b4ef5c90ef4170b
   proof_hash = 66af1f8c059c6fd3cde04dfbf1c2769e5b8db4ac81e074c6522e85b62192a1a2
   ```
3. 对 V1-1 向量产生相同的 SHA-256：
   ```
   56a39d5e10df3e00db5017d1826f920e4738c03426b5e69f404eb7f9d17227f1
   ```

---

## 六、当前完整度

| 模块 | 状态 |
|------|------|
| canonical@v1 | ✅ LOCKED |
| rolling-hash-v1 | ✅ LOCKED |
| occupied_token 生命周期 | ✅ 已实现 |
| container cap_level 演化 | ✅ 已实现 |
| SPU 与 Trip 类型强约束 | 🔲 待定义 |
| 去中心化 mint v2 | 🔲 待设计 |

---

*本文档一经锁定不可修改。如需更新，发布新版本（canonical@v2 等）。*  
*参考实现：`coordos/packages/vml-core/canonical.go` + `rolling_hash.go`*
