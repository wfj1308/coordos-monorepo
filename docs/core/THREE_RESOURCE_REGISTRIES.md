# 三类资源注册系统

## 系统本质
设计院管理系统本质上围绕三类资源注册与治理：
1. 资质资源（Qualification）
2. 人员资源（Executor/Employee）
3. 业绩资源（Achievement UTXO）

## 资源表建议

### 1) 资质资源
`qualifications`
- `qual_type`
- `holder_type` (`COMPANY/PERSON`)
- `holder_ref`
- `cert_no`
- `valid_until`
- `status`

### 2) 人员资源
`employees` + `executors`
- 人员基础信息
- 执行体引用 `executor_ref`
- 能力等级与技能标签

### 3) 业绩资源
`achievement_utxos`
- `utxo_ref`
- `spu_ref`
- `project_ref`
- `executor_ref`
- `proof_hash`
- `status`

## 资源绑定关系
`resource_bindings`
用于记录“某条业绩消耗了哪些资质，由谁执行”。

推荐关键字段：
- `achievement_utxo_id`
- `credential_id`
- `executor_ref`
- `project_ref`
- `status`

## 典型能力
- 投标资质审查
- 人员可执行性匹配（Resolver）
- 业绩可验证证明
- 超额占用检测
- 资质申报材料聚合导出

## 设计原则
- 三类资源统一纳入可查询、可审计体系
- 绑定关系显式化，不依赖人工口径
- 输出以 UTXO 与 proof_hash 为准，减少争议
