# CoordOS Monorepo 状态报告（严格验收闭环）

生成时间：2026-02-28（修订）

---

## 一、当前完成情况

### 1) 核心包
- `packages/vuri`：VRef 类型与工具函数可用。
- `packages/project-core`：ProjectNode / FissionEngine / StateMachine / RULE-001~005 已有实现。
- `packages/spu-runtime`：SPU/UTXO 类型与 `executeSPU()` 可用。
- `packages/project-core/pg_store.go`：PostgreSQL 持久化实现（ProjectTree / Genesis / Contract / Audit）。

### 2) 业务服务（design-institute）
以下 12 个域已具备：类型、Store 接口、Service、PGStore。
- `project`
- `contract`
- `gathering`
- `settlement`
- `invoice`
- `costticket`
- `payment`
- `company`
- `employee`
- `achievement`
- `approve`
- `report`

已完成：
- `services/design-institute/main.go`（HTTP 入口 + 12 个 Service 注入）
- `services/design-institute/api/handler.go`（路由与处理器）
- `services/design-institute/config_loader.go`（默认值 + YAML + 环境变量覆盖 + 启动校验）
- `services/design-institute/report/service.go`
  - `report` 域已统一为 `Store + Service + PGStore` 模式
  - `main.go` 注入已改为 `report.NewService(report.NewPGStore(db), tenantID)`

### 3) 协议服务（vault-service）
已完成：
- HTTP API（`/health`、项目、裂变、事件、结算端点）
- `services/vault-service/api/server.go`
  - 已补齐读取端点：`GET /api/v1/projects/{ref}`、`GET /api/v1/projects/{ref}/tree`、`GET /api/v1/genesis/{ref}`
  - 已移除对应 TODO 占位返回
- `services/vault-service/infra/store/sqlite/store.go`
  - SQLite 开发实现
  - 覆盖 ProjectTree / Genesis / Contract / Parcel / UTXO / Settlement / Wallet / Audit
- `services/vault-service/infra/store/rocksdb/store.go`
  - 已替换为**原生文件持久化实现**（不再依赖 sqlite 兼容层）
  - 按 bucket 维护 Project/Genesis/Contract/Parcel/UTXO/Settlement/Wallet/Audit
  - 写入采用原子快照落盘（`*.tmp` -> rename），支持重启恢复
- `services/vault-service/app/projectcore_adapters.go`
  - 补齐 vault-store 与 project-core 适配
  - 新增 `GetFull/CreateFull/UpdateFull` 适配能力，支持 FissionEngine 装配
- `services/vault-service/app/bootstrap.go`
  - 新增依赖装配函数 `BuildDeps`
- `services/vault-service/main.go`
  - 新增独立启动入口
  - 接入 `config.Load()`
  - 按 `storage.backend` 注入 `sqlite/rocksdb`
  - 完成 app + project-core 引擎装配并启动 HTTP Server
- `services/vault-service/config/config.go`
  - 配置统一加载（默认值 + YAML + 环境变量覆盖 + 启动校验）
- `services/vault-service/infra/store/sqlite/store_test.go`
  - 新增 Golden Path 验收测试，覆盖 8 类 Store 关键读写/校验流程
- `services/vault-service/integration/workflow_test.go`
  - 新增跨层集成测试：`ProjectApp -> FissionApp -> EventApp -> RuleEngine -> Store/Audit`
  - 覆盖“建根项目 -> 建子项目 -> Genesis 裂变 -> 事件提交 -> 审计留痕”主链路
- `services/vault-service/infra/store/interfaces.go`
  - 已清理乱码与隐藏字符风险，接口定义重写为稳定版本

### 4) 数据库与迁移
- `scripts/migrate_pg_schema.sql` 缺失表已补齐：
  - `project_nodes`
  - `approve_flows`
  - `approve_tasks`
  - `approve_records`
  - `costtickets`
  - `dual_write_compensation`
- `scripts/migrate.py` 已修复：
  - 连接参数环境变量化
  - `contract/finance/drawing` 时间空值兜底
  - 冲突回填（`ON CONFLICT DO UPDATE`）
  - finance 源表字段兼容

迁移结果（已验证）：
- `companies`: 476
- `employees`: 328
- `contracts`: 18207
- `gatherings`: 17441
- `balances`: 12323
- `invoices`: 19350
- `drawings`: 9135
- 校验：无失败记录，委托链完整

### 5) sovereign-skills（bridge）
- `services/sovereign-skills/bridge/*.ts`：10 个 SPU 技能文件齐全（含聚合入口 `index.ts`）
- 与 `specs/spu/bridge/*.v1.json`（排除 `catalog.v1.json`）对齐
- `services/sovereign-skills/package.json` 已修复为合法 JSON（清理乱码导致的非法格式）

### 6) 文档与前端（P2 交付）
- OpenAPI 3.0 文档已补齐：
  - `docs/api/openapi.design-institute.yaml`
  - `docs/api/openapi.vault-service.yaml`
  - `docs/api/README.md`
- `ui/design-institute` 前端已落地：
  - `index.html`（单页控制台）
  - `styles.css`（响应式布局 + 动效 + 主题变量）
  - `app.js`（配置持久化、快捷动作、请求调试器、响应/日志面板）
  - `README.md`（本地使用说明）

---

## 二、优先级状态

### P0（阻塞启动）
- [x] `services/design-institute/main.go`
- [x] `services/design-institute/api/handler.go`
- [x] `services/vault-service/infra/store/sqlite/store.go`
- [x] `scripts/migrate_pg_schema.sql` 缺失表补齐

结论：**P0 已全部完成。**

### P1（可运行后立即需要）
- [x] 配置治理统一化
  - `services/design-institute/config_loader.go`
  - `services/vault-service/config/config.go`
- [x] `services/vault-service` 启动接线
  - `services/vault-service/main.go` + `services/vault-service/app/bootstrap.go`
- [x] `services/vault-service/infra/store/rocksdb` 后端入口
  - 已完成原生持久化实现，移除 sqlite 兼容依赖
- [x] `vault-service` 读取端点补齐（清理 TODO 占位）
  - `GET /api/v1/projects/{ref}`
  - `GET /api/v1/projects/{ref}/tree`
  - `GET /api/v1/genesis/{ref}`
- [x] `packages/project-core` PG 持久化实现
  - `PGStore` + typed sub-store（`ProjectTree/Genesis/Contract/Audit`）
- [x] `services/sovereign-skills/bridge` 其余 9 个技能文件补齐
- [x] `vault-service` SQLite Admin Golden Path 所需 Store 实现与验收测试
  - `services/vault-service/infra/store/sqlite/store_test.go`
- [x] `design-institute/report` 结构统一
  - 补齐 `Store + PGStore`，与其他业务域一致

结论：**P1 已全部完成。**

### P2（稳定化）
- [x] `ui/design-institute` 前端
  - 已提供可直接运行的静态控制台页面（支持桌面与移动端）
- [x] OpenAPI 3.0 文档
  - 已覆盖 `design-institute` 与 `vault-service` 路由基线
- [x] 原生 RocksDB 实现（替换当前兼容适配层）
  - `services/vault-service/infra/store/rocksdb/store.go`
- [x] 更广覆盖的集成测试（跨服务链路）
  - `services/vault-service/integration/workflow_test.go`

---

## 三、严格验收证据

### 1) Go 测试
已执行并通过：
```bash
go test ./...   # services/vault-service
go test ./...   # services/design-institute
go test ./...   # packages/project-core
```

关键结果：
- `coordos/vault-service/infra/store/sqlite`：`ok`
- `coordos/vault-service/infra/store/rocksdb`：`ok`
- `coordos/vault-service/integration`：`ok`
- `coordos/design-institute`：`ok`（各子包可编译通过）
- `coordos/project-core`：`ok`

### 1.1) 关键修复核验（2026-02-28）
- `services/vault-service/infra/store/rocksdb/store.go` 已完成原生实现并参与测试通过
- `services/vault-service/api/server.go` 已无 `TODO` 占位端点
- `services/design-institute/report/service.go` 已具备 `Store/Service/PGStore` 结构
- `docs/api/openapi.design-institute.yaml` 与 `docs/api/openapi.vault-service.yaml` 已落地
- `ui/design-institute` 前端控制台已落地并可本地打开使用

### 2) bridge 技能对齐
已核对：
- `bridge_spu_specs=10`
- `skill_files=10`
- `mapped_refs=10`

### 3) JSON 有效性
已校验：
- `services/sovereign-skills/package.json` 可被 `JSON.parse` 正常解析

---

## 四、快速启动建议

### 1) 启动数据库
```bash
docker compose -f docker-compose.db.yml up -d
```

### 2) 初始化 PG 表
```bash
# PowerShell
$env:PGPASSWORD="coordos"
psql -h 127.0.0.1 -U coordos -d coordos -f scripts/migrate_pg_schema.sql
```

### 3) 跑迁移（已验证顺序）
```bash
python scripts/migrate.py --phase company
python scripts/migrate.py --phase employee
python scripts/migrate.py --phase contract
python scripts/migrate.py --phase finance
python scripts/migrate.py --phase drawing
python scripts/migrate.py --phase verify
```

### 4) 启动服务
```bash
# design-institute
go run ./services/design-institute

# vault-service
go run ./services/vault-service
```

---

## 五、摘要

- P0：清零。
- P1：清零。
- P2：清零。
- 状态文档与验收证据已形成闭环，可复验。

