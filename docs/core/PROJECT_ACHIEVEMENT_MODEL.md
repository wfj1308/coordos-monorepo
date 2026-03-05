# 业绩与项目树模型

## 核心定义
在 CoordOS 中，业绩不是人工填报，而是项目执行过程中产出的 Achievement UTXO。

- 项目树定义执行上下文
- SPU 执行产生 UTXO
- UTXO 通过 `proof_hash` 保证可验证

## 项目树与业绩关系
- `project_ref`：业绩归属项目
- `spu_ref`：业绩对应产出类型
- `executor_ref`：业绩对应执行体
- `status=SETTLED`：业绩可用于对外证明

## Achievement UTXO 结构（建议）
- `utxo_ref`
- `spu_ref`
- `project_ref`
- `executor_ref`
- `proof_hash`
- `status` (`PENDING/SETTLED/DISPUTED/LEGACY`)
- `source` (`SPU_INGEST/LEGACY_IMPORT/MANUAL`)

## 三层业绩视图
1. 个人业绩：按 `executor_ref` 查询。
2. 项目业绩：按 `project_ref` 查询。
3. 企业业绩：按 `namespace_ref` 聚合查询。

三层共享同一条 UTXO 数据，只是视图不同。

## 校验规则
- 项目节点必须存在。
- 执行体必须在项目授权范围内。
- SPU 类型必须符合项目类型约束。
- `proof_hash` 必须可复算一致。

## 历史业绩迁移原则
- 允许 `LEGACY_IMPORT` 导入历史项目。
- 导入后仍需形成标准 UTXO 结构。
- 与项目树建立关联，保留可追溯信息。

## 结论
项目树是上下文，UTXO 是证据。二者结合后，业绩天然可验证、可追踪、可复用。
