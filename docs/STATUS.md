# CoordOS Monorepo — 现状评估 + 补充计划 + 迁移指南

生成时间：2026-02-27

---

## 一、已完成清单（✅ 可直接使用）

### packages/vuri
- ✅ VRef 类型定义、Parse / New / Child / Tenant / Depth / IsAncestor
- **状态**：完整可用，无依赖

### packages/project-core
- ✅ ProjectNode、ExecutorConstraint、GenesisUTXO（node.go）
- ✅ FissionEngine 裂变引擎（含并发锁、配额校验、存证事件）
- ✅ StateMachine 状态机（7个状态、完整转移图）
- ✅ RULE-001 ~ RULE-005 规则引擎（rules.go）
- **状态**：协议层完整，但 Store 接口只定义了类型，无 PG 实现

### packages/spu-runtime（TypeScript）
- ✅ SPU / UTXO / Executor / Skill 完整类型（types.ts）
- ✅ executeSPU() 通用执行引擎（execute.ts）
- **状态**：完整可用

### services/vault-service
- ✅ HTTP Server + 10 个协议端点（api/server.go）
- ✅ POST /api/utxo/ingest SPU 对接入口（api/utxo_ingest.go）
- ✅ ProjectApp / FissionApp / EventApp / SettleApp（app/app.go）
- ✅ 8 个 Store 接口定义（infra/store/interfaces.go）
- ✅ MySQL↔PG 双写中间件（infra/dualwrite/dual_write.go）
- **状态**：接口层完整，Store 无实现（sqlite/rocksdb 目录为空）

### services/design-institute（12个业务服务）
每个服务均包含：类型定义 + Store 接口 + Service 方法 + PGStore 实现
- ✅ project/      ProjectService（骨架）
- ✅ contract/     ContractService
- ✅ gathering/    GatheringService
- ✅ settlement/   SettlementService
- ✅ invoice/      InvoiceService
- ✅ costticket/   CostTicketService
- ✅ payment/      PaymentService（含 RULE-003 拦截）
- ✅ company/      CompanyService
- ✅ employee/     EmployeeService
- ✅ achievement/  AchievementService
- ✅ approve/      ApproveService（含回调钩子）
- ✅ report/       ReportService（5张报表）
- **状态**：Service + PGStore 代码完整，但无 main.go、无 HTTP handler、无 wire/DI

### services/sovereign-skills（TypeScript）
- ✅ 桩基施工图 5 个技能实现（pile_foundation_skills.ts）
- **状态**：技能实现完整，接入点为 stub（等待接入 1.8M 行系统）

### specs/spu/bridge（10个SPU规格）
- ✅ 桥墩完整闭合链（设计→施工→竣工→结算）
- ✅ catalog.v1.json（闭合链顺序 + DisputePRC 举证地图）
- **状态**：规格文件完整，可直接被 spu-runtime 加载

### scripts/
- ✅ migrate_pg_schema.sql（PostgreSQL 建表，8张核心表）
- ✅ migrate.py（6个phase，拓扑排序，补偿队列）
- **状态**：可直接执行，需配置 DB 连接

---

## 二、缺失清单（❌ 需要补充）

### 优先级 P0（不补就跑不起来）

```
❌ services/design-institute/main.go
   HTTP Server 入口，注入所有 12 个 PGStore + Service
   大约 150 行

❌ services/design-institute/api/handler.go
   12 个服务的 HTTP 路由（CRUD端点）
   大约 600~800 行

❌ services/vault-service/infra/store/sqlite/store.go
   vault-service 的 Store 实现（单机开发用 SQLite）
   project-core 的 ProjectTreeStore / GenesisStore 接口实现
   大约 400 行

❌ scripts/migrate_pg_schema.sql 缺失的表
   approve_flows / approve_tasks / approve_records
   costtickets（新表）
   dual_write_compensation（双写补偿）
   project_nodes（协议层节点表）
   大约 80 行 DDL
```

### 优先级 P1（跑起来后立刻需要）

```
❌ 配置文件
   services/design-institute/config.yaml
   services/vault-service/config.yaml
   （DB连接、端口、tenantID、密钥）

❌ services/sovereign-skills/bridge/
   其余 9 个 SPU 的技能实现（pile_cap / pier_rebar / ...）
   现在只有 pile_foundation

❌ packages/project-core 的 PG Store 实现
   project-core 定义了 ProjectTreeStore / GenesisUTXOStore 接口
   但 vault-service 的 sqlite/rocksdb 实现为空

❌ vault-service/infra/store/sqlite/
   Admin Golden Path 验收测试需要的 Store 实现
```

### 优先级 P2（稳定后）

```
❌ ui/design-institute/
   前端（React + TailwindCSS）

❌ 完整测试覆盖
   每个 Service 的单元测试
   RULE-001~005 边界测试
   迁移脚本的 dry-run 测试

❌ docs/api/
   OpenAPI 3.0 规格文件
```

---

## 三、两层冲突需要解决

### 冲突 1：ProjectNode 定义了两份

```
packages/project-core/node.go      → ProjectNode（协议层，含UTXO/Genesis）
services/design-institute/project/ → ProjectNode（业务层，含legacy_contract_id）
```

**解决方案**：
- project-core 的 ProjectNode 是协议定义（权威）
- design-institute/project 的 ProjectNode 是数据库映射层
- design-institute/project 改为 embed project-core 的类型，加业务字段

### 冲突 2：Store 接口定义了两套

```
vault-service/infra/store/interfaces.go     → 8 个协议层 Store
design-institute/*/service.go               → 12 × 1 个业务层 Store
```

**解决方案**：
- 两套 Store 各自独立，不合并
- vault-service Store = 协议操作（ProjectNode / GenesisUTXO / UTXO / Wallet）
- design-institute Store = 业务操作（合同 / 发票 / 结算 / 员工）

---

## 四、现有可用模块迁移进来的步骤

### 迁移对象

你现有的系统（需要迁移进 monorepo 的部分）：
```
1. 1.8M 行桥梁设计计算系统（DTO 升级为 UTXO 出口）
2. 历史 iCRM MySQL 数据（631家分公司 / 19500条合同 / ...）
3. sovereign-guard（已有的主权网关，之前单独部署）
```

---

### 步骤 A：补齐 P0 缺口（先让骨架能跑）

**A1. 补 migrate_pg_schema.sql 缺失的表**

```sql
-- 补到 scripts/migrate_pg_schema.sql 末尾

-- 协议层节点表（project-core 使用）
CREATE TABLE IF NOT EXISTS project_nodes (
    id              BIGSERIAL PRIMARY KEY,
    ref             VARCHAR(500) NOT NULL UNIQUE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    parent_id       BIGINT REFERENCES project_nodes(id),
    parent_ref      VARCHAR(500),
    depth           INT NOT NULL DEFAULT 0,
    path            TEXT NOT NULL DEFAULT '/',
    name            VARCHAR(255) NOT NULL,
    owner_ref       VARCHAR(500),
    contractor_ref  VARCHAR(500),
    executor_ref    VARCHAR(500),
    platform_ref    VARCHAR(500),
    contract_ref    VARCHAR(500),
    procurement_ref VARCHAR(500),
    genesis_ref     VARCHAR(500),
    status          VARCHAR(50) NOT NULL DEFAULT 'INITIATED',
    proof_hash      VARCHAR(255),
    prev_hash       VARCHAR(255),
    legacy_contract_id BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 审批流表（approve 服务使用）
CREATE TABLE IF NOT EXISTS approve_flows (
    id          BIGSERIAL PRIMARY KEY,
    legacy_id   BIGINT,
    tenant_id   INT NOT NULL DEFAULT 10000,
    biz_type    VARCHAR(50) NOT NULL,
    biz_id      BIGINT NOT NULL,
    biz_ref     VARCHAR(500),
    title       VARCHAR(500),
    applicant   VARCHAR(255),
    state       VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    flow_id     BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS approve_tasks (
    id           BIGSERIAL PRIMARY KEY,
    flow_id      BIGINT NOT NULL REFERENCES approve_flows(id),
    seq          INT NOT NULL,
    approver_ref VARCHAR(255) NOT NULL,
    state        VARCHAR(50) NOT NULL DEFAULT 'WAITING',
    comment      TEXT,
    acted_at     TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS approve_records (
    id         BIGSERIAL PRIMARY KEY,
    flow_id    BIGINT NOT NULL REFERENCES approve_flows(id),
    task_id    BIGINT NOT NULL REFERENCES approve_tasks(id),
    action     VARCHAR(50) NOT NULL,
    actor      VARCHAR(255) NOT NULL,
    comment    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 费用单（costticket 服务使用）
CREATE TABLE IF NOT EXISTS costtickets (
    id                  BIGSERIAL PRIMARY KEY,
    legacy_id           BIGINT UNIQUE,
    tenant_id           INT NOT NULL DEFAULT 10000,
    cost_ticket_number  VARCHAR(255),
    balance_type        SMALLINT,
    bank_money          DECIMAL(19,2),
    cash_money          DECIMAL(19,2),
    bank_settlement     DECIMAL(19,2),
    cash_settlement     DECIMAL(19,2),
    vat_rate            VARCHAR(50),
    vat_sum             DECIMAL(19,2),
    deduct_rate         VARCHAR(50),
    deduct_sum          DECIMAL(19,2),
    management_cost_sum DECIMAL(19,2),
    cost_ticket_sum     DECIMAL(19,2),
    total_invoice       DECIMAL(19,2),
    no_ticket_sum       DECIMAL(19,2),
    state               VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    pay_date            TIMESTAMPTZ,
    employee_id         BIGINT REFERENCES employees(id),
    bank_id             BIGINT,
    pay_employee_id     BIGINT,
    contract_id         BIGINT REFERENCES contracts(id),
    project_ref         VARCHAR(500),
    note                TEXT,
    draft               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 双写补偿队列
CREATE TABLE IF NOT EXISTS dual_write_compensation (
    id          BIGSERIAL PRIMARY KEY,
    table_name  VARCHAR(100) NOT NULL,
    legacy_id   BIGINT NOT NULL,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retries     INT NOT NULL DEFAULT 0,
    resolved    BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (table_name, legacy_id)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_project_nodes_tenant  ON project_nodes(tenant_id);
CREATE INDEX IF NOT EXISTS idx_project_nodes_parent  ON project_nodes(parent_ref);
CREATE INDEX IF NOT EXISTS idx_project_nodes_executor ON project_nodes(executor_ref);
CREATE INDEX IF NOT EXISTS idx_project_nodes_legacy  ON project_nodes(legacy_contract_id);
CREATE INDEX IF NOT EXISTS idx_approve_flows_biz     ON approve_flows(biz_type, biz_id);
CREATE INDEX IF NOT EXISTS idx_approve_tasks_flow    ON approve_tasks(flow_id);
CREATE INDEX IF NOT EXISTS idx_costtickets_contract  ON costtickets(contract_id);
```

**A2. 补 design-institute main.go**

```
位置：services/design-institute/main.go
内容：连接 PG → 初始化 12 个 PGStore → 初始化 12 个 Service
     → 注册 HTTP 路由 → 启动服务
约 150 行
```

**A3. 补 design-institute HTTP handler**

```
位置：services/design-institute/api/handler.go
内容：每个服务约 5~6 个端点（List / Get / Create / Update / 状态变更）
     12 个服务 × 5 = 约 60 个端点
约 700 行
```

---

### 步骤 B：接入 1.8M 行桥梁设计系统

**B1. DTO → UTXO 改造原则（计算逻辑一行不改）**

```go
// 原来（DTO，用完即丢）
result := calcPileDimension(geology, load)   // 返回 PileDTO
drawingEngine.Generate(result)                // DTO进去，DWG出来

// 改造后（UTXO，永久存证）
result := calcPileDimension(geology, load)   // 计算逻辑不变
utxo := wrapAsUTXO(result, spuRef, executorRef, genesisRef)
store.SaveUTXO(utxo)                          // 存证
drawingEngine.Generate(utxo.Payload)          // Payload就是原来的DTO
```

**B2. 接入点（pile_foundation_skills.ts 里的 stub）**

```typescript
// 现在是 stub
async function callDrawingEngine(params: DrawingParams): Promise<DrawingOutput> {
    // TODO: 接入 1.8M 行系统
    throw new Error("not implemented")
}

// 改成（通过 HTTP / gRPC 调用现有系统）
async function callDrawingEngine(params: DrawingParams): Promise<DrawingOutput> {
    const resp = await fetch("http://your-existing-system/api/draw", {
        method: "POST",
        body: JSON.stringify(params)
    })
    return resp.json()
}
```

**B3. 其余 9 个 SPU 的技能文件**

```
services/sovereign-skills/bridge/
  pile_foundation_skills.ts    ✅ 已有
  pile_cap_skills.ts           ❌ 待补
  pier_rebar_skills.ts         ❌ 待补
  superstructure_skills.ts     ❌ 待补
  concealed_acceptance_skills.ts ❌ 待补
  prestress_skills.ts          ❌ 待补
  concrete_strength_skills.ts  ❌ 待补
  coordinate_proof_skills.ts   ❌ 待补
  review_certificate_skills.ts ❌ 待补
  settlement_cert_skills.ts    ❌ 待补
```

每个技能文件约 200~300 行，结构与 pile_foundation_skills.ts 完全一致。

---

### 步骤 C：iCRM 历史数据迁移

```bash
# 1. 建表
psql -d coordos -f scripts/migrate_pg_schema.sql

# 2. 配置连接
vim scripts/migrate.py
# MYSQL_CONFIG["password"] = "..."
# PG_CONFIG["password"]    = "..."

# 3. 按序执行
python3 scripts/migrate.py --phase company    # 631家，~1分钟
python3 scripts/migrate.py --phase employee   # 435人，~10秒
python3 scripts/migrate.py --phase contract   # 19500条，含委托链拓扑排序，~5分钟
python3 scripts/migrate.py --phase finance    # 收款+结算+发票，~10分钟
python3 scripts/migrate.py --phase drawing    # 9263条，~3分钟
python3 scripts/migrate.py --phase verify     # 校验+报告

# 4. 启动双写（新业务同时写新旧库）
# 修改应用层配置：WriteMySQL=true, WritePostgres=true

# 5. 3周后切换
# WriteMySQL=false → MySQL 只读备份
```

---

### 步骤 D：sovereign-guard 合并

sovereign-guard 之前是独立部署的主权网关（S-API），现在合并进 vault-service：

```
原 sovereign-guard 的功能          目标位置
─────────────────────────         ──────────────────────────────
Genesis UTXO 配额强制              vault-service/app/app.go（已有）
DID 认证                          vault-service/api/auth/（待补）
SCV 成本规约                      vault-service/infra/store/（待补）
Admin 平面 HMAC 鉴权              vault-service/api/middleware/（待补）
RocksDB 存储适配器                vault-service/infra/store/rocksdb/（待补）
```

迁移方式：直接复制 sovereign-guard 的源码到对应目录，调整 import 路径。

---

## 五、总结

| 层次 | 完成度 | 可直接用 | 需要补充 |
|------|--------|----------|----------|
| 协议层（vuri + project-core） | 95% | ✅ | PG Store 实现 |
| SPU 运行时（spu-runtime） | 90% | ✅ | — |
| 主权服务（vault-service） | 70% | 接口层✅ | Store 实现 + 配置文件 |
| 业务服务（design-institute） | 75% | Service✅ | main.go + HTTP handler |
| 技能库（sovereign-skills） | 10% | pile_foundation✅ | 其余 9 个 SPU |
| SPU 规格（specs/spu/bridge） | 100% | ✅ | — |
| 迁移脚本（scripts） | 80% | ✅ | 补缺失建表 SQL |
| 前端（ui） | 0% | — | 全部待开发 |

**最短路径跑起来第一个闭环：**

```
A1（补建表SQL）→ A2（main.go）→ A3（handler）
→ 迁移脚本跑通 company + contract
→ 手动创建一个 ProjectNode
→ 手动触发 GenesisUTXO 裂变
→ 第一个 e2e 测试通过
```

预估工作量：3~4 天
