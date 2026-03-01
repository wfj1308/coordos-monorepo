# CoordOS STATUS

更新日期: 2026-03-01

## 总结
- 当前仓库主线以 PostgreSQL 为唯一业务数据库基线。
- design-institute 与 vault-service 可编译、可联调。
- UI 已支持从后端读取现有 PG 数据并回显到看板。
- 历史审计/外部对比文档已合并到本文件，不再分散维护。

## 已落地能力
1. 设计院主流程
- 项目、合同、人员、资质、业绩、回款、发票、结算接口均已接入 design-institute。
- 项目资源聚合接口已可用: `GET /api/v1/projects/{ref}/resources`。

2. 协议侧能力
- vault-service `POST /api/utxo/ingest` 已接主路由并落库。
- 结算回写链路已具备对 design-institute 的回调能力。

3. 执行体寻址 v0.1
- `POST /api/v1/resolve/executor`
- `POST /api/v1/verify/executor`
- `GET /api/v1/projects/{ref}/resources`

4. 资质与资源命名
- 资源寻址规则文档: `docs/RESOURCE_ADDRESSING_V0_1.md`。
- `v://` 资源命名与项目资源聚合已在接口输出中体现。

## 本轮清理策略
1. 文档
- 保留: `docs/STATUS.md`, `docs/RESOURCE_ADDRESSING_V0_1.md`
- 已并入并建议删除: 历史审计快照、外部对比快照文档

2. 数据与日志
- `migration_*.log`, `tmp_logs/` 为本地运行日志，不入库。
- `coordos_vault_rocks_native.json` 为本地 Rocks 数据文件，不入库。

3. 测试与脚本
- 按当前仓库策略不保留测试文件和测试脚本到提交集合。

## 最小验收命令
```bash
go build ./services/design-institute ./services/vault-service
```

## 备注
- 如需恢复历史审计细节，请从 Git 历史记录查看已合并文档版本。
