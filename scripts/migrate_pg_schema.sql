-- ============================================================
-- iCRM → CoordOS 双写迁移方案
-- MySQL 5.7 (icrm) → PostgreSQL 14 (coordos)
-- 策略：双写过渡，新旧并行，零停机
-- ============================================================
--
-- 数据规模：~19500合同 / ~631分公司 / ~18291收款 / ~12630结算
-- 迁移周期：建议4周
--
-- 第一周：PostgreSQL建表 + 存量数据导入 + 双写开关
-- 第二周：50家分公司数据校验 + 结算链验证
-- 第三周：新业务全走新库 + 旧库只读
-- 第四周：旧库下线
-- ============================================================


-- ============================================================
-- PHASE 0: PostgreSQL 目标库建表
-- ============================================================

-- 扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── 0-1. 分公司/执行体 ──────────────────────────────────────
CREATE TABLE companies (
    id              SERIAL PRIMARY KEY,
    legacy_id       INT UNIQUE,                    -- 对应 MySQL company.id
    name            VARCHAR(255) NOT NULL,
    company_type    SMALLINT NOT NULL DEFAULT 2,   -- 1总公司 2分公司 3合伙人
    parent_id       INT REFERENCES companies(id),  -- 上级公司
    executor_ref    VARCHAR(500),                  -- v://zhongbei/executor/{id}
    code            VARCHAR(50),
    license_num     VARCHAR(255),                  -- 统一信用代码
    charger         VARCHAR(255),
    charger_phone   VARCHAR(255),
    finance_charger VARCHAR(255),
    bank_account    VARCHAR(255),
    area_id         INT,
    zone_id         BIGINT,
    address         VARCHAR(255),
    note            VARCHAR(255),
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增字段
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (migrate_status IN ('PENDING','MAPPED','LEGACY')),
    genesis_ref     VARCHAR(500)                   -- 关联GenesisUTXO（后期填充）
);

-- ── 0-2. 员工 ─────────────────────────────────────────────
CREATE TABLE employees (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    name            VARCHAR(255),
    phone           VARCHAR(255),
    account         VARCHAR(255),
    company_id      INT REFERENCES companies(id),
    department_id   INT,
    user_id         BIGINT,
    position        VARCHAR(255),
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    executor_ref    VARCHAR(500),                  -- v://zhongbei/executor/person/{id}
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- ── 0-3. 合同（核心表，最复杂） ───────────────────────────
CREATE TABLE contracts (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,                 -- 对应 MySQL contract.id
    num             VARCHAR(255),                  -- 合同编号
    contract_name   VARCHAR(2000),
    contract_balance DECIMAL(19,2),                -- 合同金额
    manage_ratio    DECIMAL(19,2),                 -- 管理费比率
    signing_subject VARCHAR(255),                  -- 签约主体
    signing_time    TIMESTAMPTZ,
    contract_date   TIMESTAMPTZ,
    pay_type        SMALLINT,                      -- 1总价 2费率 3单价 4框架
    contract_type   VARCHAR(255),                  -- 中标/挂靠
    state           VARCHAR(255),                  -- 审批状态
    store_state     SMALLINT DEFAULT 2,            -- 1已作废 2执行中
    company_id      INT REFERENCES companies(id),
    customer_id     BIGINT,
    employee_id     BIGINT REFERENCES employees(id),
    parent_id       BIGINT REFERENCES contracts(id), -- 父级合同（委托链）
    owner_id        BIGINT,
    catalog         SMALLINT DEFAULT 1,
    totle_balance   DECIMAL(19,2),                 -- 累计结算金额
    totle_gathering DECIMAL(19,2),                 -- 累计收款金额
    totle_invoice   DECIMAL(19,2),                 -- 累计开票金额
    note            TEXT,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    draft           SMALLINT DEFAULT 0,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增字段（双写后填充）
    project_ref     VARCHAR(500),                  -- v://zhongbei/project/{path}
    genesis_ref     VARCHAR(500),                  -- GenesisUTXO引用
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (migrate_status IN ('PENDING','MAPPED','LEGACY'))
);

-- ── 0-4. 收款单 ───────────────────────────────────────────
CREATE TABLE gatherings (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    gathering_number VARCHAR(255),
    gathering_money DECIMAL(19,2),
    gathering_date  VARCHAR(255),
    gathering_state VARCHAR(255),
    gathering_type  VARCHAR(255),
    gathering_person VARCHAR(255),
    contract_id     BIGINT REFERENCES contracts(id),
    company_id      INT REFERENCES companies(id),
    employee_id     BIGINT REFERENCES employees(id),
    balance_id      BIGINT,                        -- 关联结算单
    bank_info_id    BIGINT,
    state           VARCHAR(255),
    before_money    DECIMAL(19,2),
    after_money     DECIMAL(19,2),
    manage_ratio    DECIMAL(19,2),
    need_manage_fee DECIMAL(19,2),
    note            VARCHAR(255),
    draft           SMALLINT DEFAULT 0,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增
    project_ref     VARCHAR(500),
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- ── 0-5. 结算单 ───────────────────────────────────────────
CREATE TABLE balances (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    balance_number  VARCHAR(255),
    contract_name   VARCHAR(255),
    customer_name   VARCHAR(255),
    gathering_money DECIMAL(19,2),
    bank_money      DECIMAL(19,2),
    cash_money      DECIMAL(19,2),
    bank_settlement DECIMAL(19,2),
    cash_settlement DECIMAL(19,2),
    vat_rate        VARCHAR(255),
    vat_sum         DECIMAL(19,2),
    deduct_rate     VARCHAR(255),
    deduct_sum      DECIMAL(19,2),
    management_cost_sum DECIMAL(19,2),
    cost_ticket_sum DECIMAL(19,2),
    total_invoice   DECIMAL(19,2),
    balance_type    SMALLINT,
    state           VARCHAR(255),
    pay_date        TIMESTAMPTZ,
    gathering_id    BIGINT REFERENCES gatherings(id),
    employee_id     BIGINT REFERENCES employees(id),
    bank_id         BIGINT,
    pay_employee_id BIGINT,
    note            VARCHAR(500),
    draft           SMALLINT DEFAULT 0,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增
    project_ref     VARCHAR(500),
    genesis_ref     VARCHAR(500),                  -- 消耗的GenesisUTXO
    utxo_ref        VARCHAR(500),                  -- 产出的UTXO
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- ── 0-6. 发票 ─────────────────────────────────────────────
CREATE TABLE invoices (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    invoice_code    VARCHAR(255),
    invoice_number  VARCHAR(255),
    invoice_type    VARCHAR(255),
    invoice_state   VARCHAR(255),
    invoice_date    VARCHAR(255),
    invoice_content VARCHAR(255),
    invoice_person  VARCHAR(255),
    cur_amount      DECIMAL(19,2),
    money           DECIMAL(19,2),
    used_money      DECIMAL(19,2),
    contract_id     BIGINT REFERENCES contracts(id),
    customer_id     BIGINT,
    employee_id     BIGINT REFERENCES employees(id),
    state           VARCHAR(255),
    draft           SMALLINT DEFAULT 0,
    note            TEXT,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增
    project_ref     VARCHAR(500),
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- ── 0-7. 图纸（双写后对接SPU系统） ──────────────────────
CREATE TABLE drawings (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    num             VARCHAR(255),
    major           VARCHAR(255),                  -- 专业
    state           VARCHAR(255),
    handle_status   SMALLINT NOT NULL DEFAULT 0,
    result_status   SMALLINT NOT NULL DEFAULT 0,
    contract_id     BIGINT REFERENCES contracts(id),
    company_id      INT REFERENCES companies(id),
    employee_id     BIGINT REFERENCES employees(id),
    reviewer        VARCHAR(255),
    remarks         VARCHAR(255),
    draft           SMALLINT DEFAULT 0,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS新增：对接SPU系统
    project_ref     VARCHAR(500),
    spu_ref         VARCHAR(500),                  -- 对应哪个SPU
    utxo_ref        VARCHAR(500),                  -- SPU系统打入的UTXO引用
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

-- ── 0-8. 业绩库（SPU系统打入的UTXO落地表） ──────────────
CREATE TABLE achievement_utxos (
    id              BIGSERIAL PRIMARY KEY,
    utxo_ref        VARCHAR(500) NOT NULL UNIQUE,  -- v://zhongbei/utxo/{id}
    spu_ref         VARCHAR(500) NOT NULL,
    project_ref     VARCHAR(500) NOT NULL,
    executor_ref    VARCHAR(500) NOT NULL,          -- 分院/个人
    genesis_ref     VARCHAR(500),                  -- 关联GenesisUTXO
    contract_id     BIGINT REFERENCES contracts(id), -- 关联合同（人工或自动匹配）
    payload         JSONB,                         -- 产出内容
    proof_hash      VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','SETTLED','DISPUTED','LEGACY')),
    source          VARCHAR(20) NOT NULL DEFAULT 'SPU_INGEST'
                    CHECK (source IN ('SPU_INGEST','LEGACY_IMPORT','MANUAL')),
    tenant_id       INT NOT NULL DEFAULT 10000,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at      TIMESTAMPTZ
);

-- ── 0-9. 迁移日志（追踪每条记录的迁移状态） ─────────────
CREATE TABLE migration_log (
    id              BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100) NOT NULL,
    legacy_id       BIGINT NOT NULL,
    new_id          BIGINT,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','SUCCESS','FAILED','SKIPPED')),
    error_msg       TEXT,
    migrated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (table_name, legacy_id)
);

-- ── 0-10. 索引 ────────────────────────────────────────────
CREATE INDEX idx_contracts_company    ON contracts(company_id);
CREATE INDEX idx_contracts_parent     ON contracts(parent_id);
CREATE INDEX idx_contracts_project    ON contracts(project_ref);
CREATE INDEX idx_contracts_migrate    ON contracts(migrate_status);
CREATE INDEX idx_gatherings_contract  ON gatherings(contract_id);
CREATE INDEX idx_balances_gathering   ON balances(gathering_id);
CREATE INDEX idx_balances_project     ON balances(project_ref);
CREATE INDEX idx_invoices_contract    ON invoices(contract_id);
CREATE INDEX idx_drawings_contract    ON drawings(contract_id);
CREATE INDEX idx_drawings_utxo        ON drawings(utxo_ref);
CREATE INDEX idx_achievement_executor ON achievement_utxos(executor_ref);
CREATE INDEX idx_achievement_project  ON achievement_utxos(project_ref);
CREATE INDEX idx_achievement_status   ON achievement_utxos(status);
CREATE INDEX idx_migration_log_table  ON migration_log(table_name, status);

-- ============================================================
-- PHASE 0 EXTENSION: Missing tables for design-institute/vault-service
-- ============================================================

-- Protocol-layer project tree (project-core)
CREATE TABLE IF NOT EXISTS project_nodes (
    id                 BIGSERIAL PRIMARY KEY,
    ref                VARCHAR(500) NOT NULL UNIQUE,
    tenant_id          INT NOT NULL DEFAULT 10000,
    parent_id          BIGINT REFERENCES project_nodes(id),
    parent_ref         VARCHAR(500),
    depth              INT NOT NULL DEFAULT 0,
    path               TEXT NOT NULL DEFAULT '/',
    name               VARCHAR(255) NOT NULL,
    owner_ref          VARCHAR(500),
    contractor_ref     VARCHAR(500),
    executor_ref       VARCHAR(500),
    platform_ref       VARCHAR(500),
    contract_ref       VARCHAR(500),
    procurement_ref    VARCHAR(500),
    genesis_ref        VARCHAR(500),
    status             VARCHAR(50) NOT NULL DEFAULT 'INITIATED',
    proof_hash         VARCHAR(255),
    prev_hash          VARCHAR(255),
    legacy_contract_id BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Approve service
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

-- Cost ticket service
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

-- Payment service (used by design-institute/payment)
CREATE TABLE IF NOT EXISTS payments (
    id           BIGSERIAL PRIMARY KEY,
    legacy_id    BIGINT UNIQUE,
    amount       DECIMAL(19,2) NOT NULL,
    pay_date     TIMESTAMPTZ,
    contract_id  BIGINT NOT NULL REFERENCES contracts(id),
    contract_ref VARCHAR(255),
    project_ref  VARCHAR(500),
    bank_id      BIGINT,
    employee_id  BIGINT REFERENCES employees(id),
    state        VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    note         TEXT,
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dual-write compensation queue
CREATE TABLE IF NOT EXISTS dual_write_compensation (
    id         BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    legacy_id  BIGINT NOT NULL,
    error_msg  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    retries    INT NOT NULL DEFAULT 0,
    resolved   BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (table_name, legacy_id)
);

CREATE INDEX IF NOT EXISTS idx_project_nodes_tenant   ON project_nodes(tenant_id);
CREATE INDEX IF NOT EXISTS idx_project_nodes_parent   ON project_nodes(parent_ref);
CREATE INDEX IF NOT EXISTS idx_project_nodes_executor ON project_nodes(executor_ref);
CREATE INDEX IF NOT EXISTS idx_project_nodes_legacy   ON project_nodes(legacy_contract_id);
CREATE INDEX IF NOT EXISTS idx_approve_flows_biz      ON approve_flows(biz_type, biz_id);
CREATE INDEX IF NOT EXISTS idx_approve_tasks_flow     ON approve_tasks(flow_id);
CREATE INDEX IF NOT EXISTS idx_costtickets_contract   ON costtickets(contract_id);
CREATE INDEX IF NOT EXISTS idx_payments_contract      ON payments(contract_id);
CREATE INDEX IF NOT EXISTS idx_payments_project       ON payments(project_ref);
