# 招投标作为执行体寻址

## 核心概念
招投标在系统中不是简单的信息发布，而是“为目标 SPU 寻找可执行执行体”的过程。

目标：
- 给定 SPU 与约束条件
- 自动筛选满足资质、可用性、历史业绩的执行体
- 输出可验证的候选名单与匹配得分

## 典型流程
1. 招标方定义目标 `spu_ref` 与约束。
2. 投标方提交能力声明（资质、人员、业绩快照）。
3. Resolver 计算匹配结果并排序。
4. 中标后进入执行链路。

## 推荐接口

### 1) 投标前校验
`POST /api/v1/bid/verify`
- 输入：`executor_ref`、`target_spu`、`namespace_ref`、`project_type`
- 输出：`pass`、`matching_score`、`constraint_checks`

### 2) 候选推荐
`GET /api/v1/bid/recommend/{namespace}`
- 输入：`spu_ref`、`limit`
- 输出：候选执行体、能力等级、可用性、匹配得分

### 3) 业绩匹配
`GET /api/v1/bid/match-achievements/{executor_ref}`
- 输入：`project_type`、`within_years`
- 输出：可引用的业绩 UTXO 与匹配分

### 4) 能力快照
`GET /api/v1/bid/capability/{namespace}`
- 输出企业资质、人员结构、业绩摘要与综合评分

## 数据模型建议
为 `bid_documents` 增加字段：
- `executor_ref`
- `credential_snapshot` (JSONB)
- `achievement_snapshot` (JSONB)
- `matching_score`
- `won_at` / `lost_at`

## 平台差异
传统平台：发布信息 -> 人工判断。  
CoordOS：发布约束 -> 自动匹配 -> 可验证寻址。

## 结论
“招投标即执行体寻址”使选择过程可计算、可追溯、可审计，是协议化协作网络的关键能力。
