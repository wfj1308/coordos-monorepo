# 资源批次与约束模型

## 批次（Resource Batch）
批次用于表达“同类资源在不同来源下的可用边界”。

典型差异：
- 来源不同（自有、借用、待激活）
- 有效期不同
- 可用项目范围不同

## 约束（Constraint）三维
1. 时间约束：`valid_from` / `valid_until`
2. 数量约束：`max_concurrent_projects` / `max_per_project`
3. 范围约束：项目类型、区域、金额阈值

## Genesis UTXO 表达建议
关键字段：
- `resource_type`
- `batch_source`
- `holders` (JSONB)
- `quantity`
- `constraints` (JSONB)
- `remaining`
- `consumed_by` (JSONB)

## 消耗判定
一次资源消耗需同时满足：
- 当前时间在有效期内
- 并发与单项目占用不超限
- 项目属性满足范围约束

任一条件失败即拒绝执行。

## 数据库建议
`genesis_utxos` 中保留统一 `constraints` JSONB 字段，避免分散多表造成规则不一致。

## 实践收益
- 资源约束可配置化
- Resolver 可直接基于 SQL 过滤候选
- 规则执行一致，审计可追溯
