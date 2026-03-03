# 投标-业绩闭环

## 核心概念
项目投标需要业绩，中标项目又会产出新的业绩，这构成完整闭环：

1. 投标阶段引用企业资质、人员资质、历史业绩。
2. 中标后创建项目与合同锚点，进入执行阶段。
3. 执行阶段逐步产出 Achievement UTXO。
4. 结算完成后，业绩入池，作为下一次投标可验证输入。

## 闭环流程

### 1. 投标（消耗/引用资源）
输入资源：
- 企业资质 UTXO（资格证明）
- 人员资质 UTXO（执行能力）
- 历史业绩 UTXO（经验证明）

输出：
- `BID_DOCUMENT_UTXO`
- 包含输入资源 `proof_hash`
- 状态流转：`DRAFT -> SUBMITTED -> AWARDED/FAILED`

### 2. 中标触发
`BID_DOCUMENT_UTXO.status = AWARDED` 时自动触发：
- 创建 `project_nodes` 根节点
- 创建合同锚点
- 人员资源从 `REFERENCED` 切换为 `OCCUPIED`
- 激活执行链路

### 3. 执行产出
执行阶段逐步写入 UTXO，例如：
- `pile_foundation_drawing`
- `pier_rebar_drawing`
- `review_certificate`
- `settlement_cert`

### 4. 结算归档
`settlement_cert.status = SETTLED` 时：
- 释放占用人员资源
- 项目标记 `SETTLED`
- 业绩 UTXO 入业绩池

## 关键数据表

### bid_documents
- `bid_ref`：投标引用
- `project_type`：项目类型
- `status`：投标状态
- `proof_hash`：可验证摘要
- `project_ref`、`contract_id`：中标后关联

### bid_resources
- `resource_type`：`QUAL_COMPANY` / `QUAL_PERSON` / `ACHIEVEMENT`
- `resource_ref`：资源引用
- `consume_mode`：`REFERENCE` / `OCCUPY`
- `consume_status`：`PENDING/REFERENCED/OCCUPIED/RELEASED`

## 推荐触发器
- `fn_bid_awarded`：中标后自动创建项目/合同并占用人员
- `fn_project_settled`：结算后释放人员并归档项目

## 验证接口
- `POST /api/v1/bid/validate`
- `POST /api/v1/bid`
- `POST /api/v1/bid/{id}/submit`
- `POST /api/v1/bid/{id}/award`
- `GET /api/v1/bid/pool/{namespace}`

## 结论
闭环后，业绩不是“人工填报”，而是由执行与结算自动产出并可验证，能显著降低造假与挂靠风险。
