-- ============================================================
-- iCRM -> CoordOS migration schema
-- Source: MySQL 5.7 (icrm)
-- Target: PostgreSQL 14 (coordos)
-- Strategy: dual-write transition, side-by-side rollout, zero downtime
-- ============================================================

-- ============================================================
-- PHASE 0: PostgreSQL target schema
-- ============================================================

-- Extensions
--
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

--
CREATE TABLE IF NOT EXISTS companies (
    id              SERIAL PRIMARY KEY,
    legacy_id       INT UNIQUE,
    name            VARCHAR(255) NOT NULL,
    company_type    SMALLINT NOT NULL DEFAULT 2,   -- 1 HQ, 2 branch, 3 partner
    parent_id       INT REFERENCES companies(id),  -- parent company
    executor_ref    VARCHAR(500),                  -- v://zhongbei/executor/{id}
    code            VARCHAR(50),
    license_num     VARCHAR(255),
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
--
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (migrate_status IN ('PENDING','MAPPED','LEGACY')),
    genesis_ref     VARCHAR(500)
);

--
CREATE TABLE IF NOT EXISTS employees (
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

-- 0-3. Contracts
CREATE TABLE IF NOT EXISTS contracts (
    id               BIGSERIAL PRIMARY KEY,
    legacy_id        BIGINT UNIQUE,                 -- MySQL contract.id
    num              VARCHAR(255),                  -- contract number
    contract_name    VARCHAR(2000),
    contract_balance DECIMAL(19,2),                 -- contract amount
    manage_ratio     DECIMAL(19,2),                 -- management fee ratio
    signing_subject  VARCHAR(255),                  -- signing subject
    signing_time     TIMESTAMPTZ,
    contract_date    TIMESTAMPTZ,
    pay_type         SMALLINT,                      -- 1 total 2 rate 3 unit 4 framework
    contract_type    VARCHAR(255),                  -- bid / attachment
    state            VARCHAR(255),                  -- approval state
    store_state      SMALLINT DEFAULT 2,            -- 1 voided 2 executing
    company_id       INT REFERENCES companies(id),
    customer_id      BIGINT,
    employee_id      BIGINT REFERENCES employees(id),
    parent_id        BIGINT REFERENCES contracts(id), -- parent contract in delegation chain
    owner_id         BIGINT,
    catalog          SMALLINT DEFAULT 1,
    totle_balance    DECIMAL(19,2),                 -- accumulated settlement amount
    totle_gathering  DECIMAL(19,2),                 -- accumulated gathering amount
    totle_invoice    DECIMAL(19,2),                 -- accumulated invoice amount
    note             TEXT,
    deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    draft            SMALLINT DEFAULT 0,
    tenant_id        INT NOT NULL DEFAULT 10000,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS extensions
    project_ref      VARCHAR(500),                  -- v://zhongbei/project/{path}
    ref              VARCHAR(500),                  -- v://{tenant}/finance/contract/{id}@v1
    genesis_ref      VARCHAR(500),                  -- linked GenesisUTXO
    migrate_status   VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                     CHECK (migrate_status IN ('PENDING','MAPPED','LEGACY'))
);

-- 0-4. Gatherings
CREATE TABLE IF NOT EXISTS gatherings (
    id               BIGSERIAL PRIMARY KEY,
    legacy_id        BIGINT UNIQUE,
    gathering_number VARCHAR(255),
    gathering_money  DECIMAL(19,2),
    gathering_date   VARCHAR(255),
    gathering_state  VARCHAR(255),
    gathering_type   VARCHAR(255),
    gathering_person VARCHAR(255),
    contract_id      BIGINT REFERENCES contracts(id),
    company_id       INT REFERENCES companies(id),
    employee_id      BIGINT REFERENCES employees(id),
    balance_id       BIGINT,                        -- linked balance
    bank_info_id     BIGINT,
    state            VARCHAR(255),
    before_money     DECIMAL(19,2),
    after_money      DECIMAL(19,2),
    manage_ratio     DECIMAL(19,2),
    need_manage_fee  DECIMAL(19,2),
    note             VARCHAR(255),
    draft            SMALLINT DEFAULT 0,
    deleted          BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id        INT NOT NULL DEFAULT 10000,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- CoordOS extensions
    project_ref      VARCHAR(500),
    migrate_status   VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

--
CREATE TABLE IF NOT EXISTS balances (
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
    contract_id     BIGINT REFERENCES contracts(id),
    employee_id     BIGINT REFERENCES employees(id),
    bank_id         BIGINT,
    pay_employee_id BIGINT,
    note            VARCHAR(500),
    draft           SMALLINT DEFAULT 0,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
--
    project_ref     VARCHAR(500),
    genesis_ref     VARCHAR(500),
    utxo_ref        VARCHAR(500),
    settlement_ref  VARCHAR(500),                  -- v://{tenant}/finance/settlement/{id}@v1
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

--
CREATE TABLE IF NOT EXISTS invoices (
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
--
    project_ref     VARCHAR(500),
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

--
CREATE TABLE IF NOT EXISTS drawings (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT UNIQUE,
    num             VARCHAR(255),
    major           VARCHAR(255),
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
--
    project_ref     VARCHAR(500),
    spu_ref         VARCHAR(500),
    utxo_ref        VARCHAR(500),
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    
    -- 版本链：图纸版本追踪
    drawing_no      VARCHAR(255),              -- 图纸编号（唯一）
    version         INT NOT NULL DEFAULT 1,   -- 版本号
    prev_version_id BIGINT REFERENCES drawings(id), -- 前一版本
    status          VARCHAR(20) NOT NULL DEFAULT 'DRAFT' 
                    CHECK (status IN ('DRAFT','REVIEWING','SEALED','PUBLISHED','SUPERSEDED')),
    
    -- 审图证引用：每张图纸必须有审图合格证才能出版
    review_cert_utxo_ref VARCHAR(500),        -- 关联的审图合格证 UTXO
    review_cert_id  BIGINT REFERENCES achievement_utxos(id),
    sealed_at       TIMESTAMPTZ,              -- 盖章时间
    sealed_by       VARCHAR(500),             -- 盖章人
    
    -- 出版记录
    published_at    TIMESTAMPTZ,              -- 出版时间
    published_by    VARCHAR(500),             -- 出版人
    proof_hash      VARCHAR(255)              -- 出版证明hash
);

-- 图纸编号唯一索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_drawings_no_version 
    ON drawings(tenant_id, drawing_no, version);
CREATE INDEX IF NOT EXISTS idx_drawings_status ON drawings(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_drawings_review_cert ON drawings(review_cert_utxo_ref);

--
CREATE TABLE IF NOT EXISTS achievement_utxos (
    id              BIGSERIAL PRIMARY KEY,
    utxo_ref        VARCHAR(500) NOT NULL UNIQUE,  -- v://zhongbei/utxo/{id}
    spu_ref         VARCHAR(500) NOT NULL,
    project_ref     VARCHAR(500) NOT NULL,
    executor_ref    VARCHAR(500) NOT NULL,
    genesis_ref     VARCHAR(500),
    contract_id     BIGINT REFERENCES contracts(id),
    payload         JSONB,
    proof_hash      VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING','SETTLED','DISPUTED','LEGACY')),
    source          VARCHAR(20) NOT NULL DEFAULT 'SPU_INGEST'
                    CHECK (source IN ('SPU_INGEST','LEGACY_IMPORT','MANUAL')),
    experience_ref  VARCHAR(500),                  -- v://{tenant}/experience/project/{project_ref}/{utxo_ref}@v1
    tenant_id       INT NOT NULL DEFAULT 10000,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at      TIMESTAMPTZ
);

--
CREATE TABLE IF NOT EXISTS migration_log (
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

--
CREATE INDEX IF NOT EXISTS idx_contracts_company    ON contracts(company_id);
CREATE INDEX IF NOT EXISTS idx_contracts_parent     ON contracts(parent_id);
CREATE INDEX IF NOT EXISTS idx_contracts_project    ON contracts(project_ref);
CREATE INDEX IF NOT EXISTS idx_contracts_migrate    ON contracts(migrate_status);
CREATE INDEX IF NOT EXISTS idx_gatherings_contract  ON gatherings(contract_id);
CREATE INDEX IF NOT EXISTS idx_balances_gathering   ON balances(gathering_id);
CREATE INDEX IF NOT EXISTS idx_balances_contract    ON balances(contract_id);
CREATE INDEX IF NOT EXISTS idx_balances_project     ON balances(project_ref);
CREATE INDEX IF NOT EXISTS idx_invoices_contract    ON invoices(contract_id);
CREATE INDEX IF NOT EXISTS idx_drawings_contract    ON drawings(contract_id);
CREATE INDEX IF NOT EXISTS idx_drawings_utxo        ON drawings(utxo_ref);
CREATE INDEX IF NOT EXISTS idx_achievement_executor ON achievement_utxos(executor_ref);
CREATE INDEX IF NOT EXISTS idx_achievement_project  ON achievement_utxos(project_ref);
CREATE INDEX IF NOT EXISTS idx_achievement_status   ON achievement_utxos(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_contracts_ref_uq
    ON contracts(ref) WHERE ref IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_balances_settlement_ref_uq
    ON balances(settlement_ref) WHERE settlement_ref IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_achievement_experience_ref_uq
    ON achievement_utxos(experience_ref) WHERE experience_ref IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_migration_log_table  ON migration_log(table_name, status);

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
    legacy_catalog INT,
    legacy_hierarchy INT,
    legacy_oid  BIGINT,
    legacy_user_id BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS approve_tasks (
    id           BIGSERIAL PRIMARY KEY,
    legacy_id    BIGINT,
    flow_id      BIGINT NOT NULL REFERENCES approve_flows(id),
    seq          INT NOT NULL,
    legacy_catalog INT,
    legacy_oid   BIGINT,
    legacy_style INT,
    legacy_user_id BIGINT,
    approver_ref VARCHAR(255) NOT NULL,
    state        VARCHAR(50) NOT NULL DEFAULT 'WAITING',
    comment      TEXT,
    acted_at     TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS approve_records (
    id         BIGSERIAL PRIMARY KEY,
    legacy_id  BIGINT,
    flow_id    BIGINT NOT NULL REFERENCES approve_flows(id),
    task_id    BIGINT NOT NULL REFERENCES approve_tasks(id),
    legacy_hierarchy INT,
    legacy_state INT,
    legacy_user_id BIGINT,
    action     VARCHAR(50) NOT NULL,
    actor      VARCHAR(255) NOT NULL,
    comment    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS approve_flow_approvals (
    id             BIGSERIAL PRIMARY KEY,
    legacy_id      BIGINT UNIQUE,
    tenant_id      INT NOT NULL DEFAULT 10000,
    flow_id        BIGINT REFERENCES approve_flows(id) ON DELETE CASCADE,
    task_id        BIGINT REFERENCES approve_tasks(id) ON DELETE SET NULL,
    legacy_flow_id BIGINT,
    hierarchy      INT,
    legacy_user_id BIGINT,
    actor_ref      VARCHAR(255),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw            JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_approve_flow_approvals_flow
ON approve_flow_approvals(flow_id, hierarchy);

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
    flow_id             BIGINT,
    invoice_id          BIGINT,
    record_id           BIGINT,
    tax_expenses_sum    DECIMAL(19,2),
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
    legacy_balance_id BIGINT,
    serial_number VARCHAR(255),
    source_table VARCHAR(64) NOT NULL DEFAULT 'balance_payment',
    bank_id      BIGINT,
    employee_id  BIGINT REFERENCES employees(id),
    state        VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    note         TEXT,
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS costticket_items (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    costticket_id         BIGINT NOT NULL REFERENCES costtickets(id) ON DELETE CASCADE,
    amount                DECIMAL(19,2),
    balance_type          INT,
    bank_name             VARCHAR(255),
    bank_no               VARCHAR(255),
    invoice_amount        DECIMAL(19,2),
    invoice_type          INT,
    management_amount     DECIMAL(19,2),
    management_rate       DECIMAL(10,4),
    money                 DECIMAL(19,2),
    rate                  INT,
    settlement_type       INT,
    tax_expenses          DECIMAL(10,4),
    ticket_date           TIMESTAMPTZ,
    ticket_number         VARCHAR(255),
    unit                  VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS payment_items (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    payment_id            BIGINT NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    balance_invoice_legacy_id BIGINT,
    deduction_amount      DECIMAL(19,2),
    payment_amount        DECIMAL(19,2),
    remark                TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS payment_attachments (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    payment_id            BIGINT NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    filename              VARCHAR(255),
    url                   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS balance_invoices (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    balance_id            BIGINT REFERENCES balances(id) ON DELETE SET NULL,
    balance_legacy_id     BIGINT,
    contract_id           BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    amount                DECIMAL(19,2),
    money                 DECIMAL(19,2),
    invoice               DECIMAL(19,2),
    management            DECIMAL(19,2),
    file_bond_money       DECIMAL(19,2),
    fast_money            DECIMAL(19,2),
    management_rate       DECIMAL(10,4),
    rate                  INT,
    tax_expenses          DECIMAL(10,4),
    bank_name             VARCHAR(255),
    bank_no               VARCHAR(255),
    unit                  VARCHAR(255),
    balance_type          INT,
    invoice_type          INT,
    settlement_type       INT,
    fast_type             INT,
    file_num              VARCHAR(255),
    bond_type_check       INT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_balance_invoices_balance
ON balance_invoices(balance_id);

CREATE TABLE IF NOT EXISTS gathering_items (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    gathering_id          BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
    gathering_legacy_id   BIGINT,
    contract_id           BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    contract_legacy_id    BIGINT,
    invoice_id            BIGINT REFERENCES invoices(id) ON DELETE SET NULL,
    invoice_legacy_id     BIGINT,
    relation_state        INT,
    money                 DECIMAL(19,2),
    invoice_money         DECIMAL(19,2),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gathering_items_gathering
ON gathering_items(gathering_id);

CREATE TABLE IF NOT EXISTS customers (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    company_id            BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    name                  VARCHAR(500) NOT NULL,
    state                 VARCHAR(50),
    address               TEXT,
    telephone             VARCHAR(100),
    phone                 VARCHAR(100),
    mail                  VARCHAR(255),
    charger_name          VARCHAR(255),
    charger_phone         VARCHAR(100),
    charger_position      VARCHAR(100),
    bank_name             VARCHAR(255),
    bank_no               VARCHAR(255),
    bank_account          VARCHAR(255),
    deposit_bank          VARCHAR(255),
    taxpayer_no           VARCHAR(255),
    card_number           VARCHAR(255),
    job                   VARCHAR(255),
    principal             VARCHAR(255),
    extra                 TEXT,
    deleted               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_customers_company
ON customers(company_id);

CREATE TABLE IF NOT EXISTS balance_records (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    balance_id            BIGINT REFERENCES balances(id) ON DELETE SET NULL,
    balance_legacy_id     BIGINT,
    money                 DECIMAL(19,2),
    before_money          DECIMAL(19,2),
    after_money           DECIMAL(19,2),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_balance_records_balance
ON balance_records(balance_id);

CREATE TABLE IF NOT EXISTS gathering_records (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    gathering_id          BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
    gathering_legacy_id   BIGINT,
    money                 DECIMAL(19,2),
    before_money          DECIMAL(19,2),
    after_money           DECIMAL(19,2),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_gathering_records_gathering
ON gathering_records(gathering_id);

CREATE TABLE IF NOT EXISTS invoice_records (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    invoice_id            BIGINT REFERENCES invoices(id) ON DELETE SET NULL,
    invoice_legacy_id     BIGINT,
    money                 DECIMAL(19,2),
    before_money          DECIMAL(19,2),
    after_money           DECIMAL(19,2),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_invoice_records_invoice
ON invoice_records(invoice_id);

CREATE TABLE IF NOT EXISTS contract_creations (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    company_id            BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    employee_id           BIGINT REFERENCES employees(id) ON DELETE SET NULL,
    legacy_parent_id      BIGINT,
    parent_id             BIGINT REFERENCES contract_creations(id) ON DELETE SET NULL,
    name                  TEXT,
    contract_number       VARCHAR(255),
    contract_type         INT,
    signing_type          INT,
    zb_wt                 VARCHAR(100),
    state                 VARCHAR(100),
    store_state           INT,
    leader                VARCHAR(255),
    leader_phone          VARCHAR(100),
    contacts              VARCHAR(255),
    contacts_phone        VARCHAR(100),
    size                  TEXT,
    note                  TEXT,
    contract_money        DECIMAL(19,2),
    investment_money      DECIMAL(19,2),
    sign_date             TEXT,
    confirm_date          TEXT,
    flow_id               BIGINT,
    owner_legacy_id       BIGINT,
    user_legacy_id        BIGINT,
    draft                 BOOLEAN NOT NULL DEFAULT FALSE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS contract_creation_attachments (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_creation_id  BIGINT REFERENCES contract_creations(id) ON DELETE CASCADE,
    contract_creation_legacy_id BIGINT,
    filename              VARCHAR(500),
    url                   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_contract_creation_attachments_creation
ON contract_creation_attachments(contract_creation_id);

CREATE TABLE IF NOT EXISTS contract_extras (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_id           BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    contract_creation_id  BIGINT REFERENCES contract_creations(id) ON DELETE SET NULL,
    contract_creation_legacy_id BIGINT,
    state                 VARCHAR(100),
    payment_type          VARCHAR(100),
    binding_style         VARCHAR(100),
    sender                VARCHAR(255),
    receiver              VARCHAR(255),
    submitter             VARCHAR(255),
    stamper               VARCHAR(255),
    printer               VARCHAR(255),
    contact               VARCHAR(255),
    mailing_address       TEXT,
    express_number        VARCHAR(255),
    express_file          TEXT,
    express_date          TEXT,
    stamp_date            TEXT,
    application_time      TEXT,
    received_date         TEXT,
    stamp_require         TEXT,
    note                  TEXT,
    publish_num           INT,
    sealed                TEXT,
    mailed                TEXT,
    received              TEXT,
    plan_receiver         TEXT,
    real_receiver         TEXT,
    legacy_user_id        BIGINT,
    sender_user_id        BIGINT,
    receiver_user_id      BIGINT,
    submitter_id          BIGINT,
    stamper_user_id       BIGINT,
    receiver_id           BIGINT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_contract_extras_contract
ON contract_extras(contract_id);

CREATE TABLE IF NOT EXISTS contract_extra_attachments (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_extra_id     BIGINT REFERENCES contract_extras(id) ON DELETE CASCADE,
    contract_extra_legacy_id BIGINT,
    filename              VARCHAR(500),
    url                   TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_contract_extra_attachments_extra
ON contract_extra_attachments(contract_extra_id);

CREATE TABLE IF NOT EXISTS bid_assures (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    company_id            BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    employee_id           BIGINT REFERENCES employees(id) ON DELETE SET NULL,
    approve_task_id       BIGINT REFERENCES approve_tasks(id) ON DELETE SET NULL,
    legacy_user_id        BIGINT,
    assure_number         VARCHAR(255),
    project               TEXT,
    purpose               TEXT,
    state                 VARCHAR(100),
    state_back            VARCHAR(100),
    state_return          VARCHAR(100),
    pay_type              INT,
    assure_type           INT,
    partner_type          INT,
    payee                 VARCHAR(255),
    payer                 VARCHAR(255),
    assure_payee          VARCHAR(255),
    partner               VARCHAR(255),
    other                 VARCHAR(255),
    other_phone           VARCHAR(100),
    piao_hao              VARCHAR(255),
    assure_fund           DECIMAL(19,2),
    import_money          DECIMAL(19,2),
    money_back            DECIMAL(19,2),
    return_money          DECIMAL(19,2),
    assure_fund_chinese   VARCHAR(255),
    pay_date              TEXT,
    import_date           TEXT,
    money_back_date       TEXT,
    return_pay_date       TEXT,
    time_end              TEXT,
    bank_name             VARCHAR(255),
    bank_account          VARCHAR(255),
    assure_bank_name      VARCHAR(255),
    assure_bank_account   VARCHAR(255),
    return_bank_name      VARCHAR(255),
    return_bank_account   VARCHAR(255),
    return_payee          VARCHAR(255),
    return_payer          VARCHAR(255),
    return_zhuanyuan      VARCHAR(255),
    tou_zhuanyuan         VARCHAR(255),
    pay_voucher           TEXT,
    return_file           TEXT,
    bid_file              TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_bid_assures_company
ON bid_assures(company_id);

CREATE TABLE IF NOT EXISTS bid_assure_flows (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    bid_assure_id         BIGINT REFERENCES bid_assures(id) ON DELETE CASCADE,
    bid_assure_legacy_id  BIGINT,
    company_id            BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    employee_id           BIGINT REFERENCES employees(id) ON DELETE SET NULL,
    bankflow_entry_id     BIGINT REFERENCES bankflow_entries(id) ON DELETE SET NULL,
    legacy_bankflow_id    BIGINT,
    legacy_user_id        BIGINT,
    project               TEXT,
    note                  TEXT,
    opposite_name         VARCHAR(255),
    payee                 VARCHAR(255),
    assure_payee          VARCHAR(255),
    piao_hao              VARCHAR(255),
    assure_fund           DECIMAL(19,2),
    import_money          DECIMAL(19,2),
    money_back            DECIMAL(19,2),
    return_money          DECIMAL(19,2),
    pay_date              TEXT,
    import_date           TEXT,
    money_back_date       TEXT,
    return_pay_date       TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_bid_assure_flows_assure
ON bid_assure_flows(bid_assure_id);

CREATE TABLE IF NOT EXISTS contract_details (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_id           BIGINT REFERENCES contracts(id) ON DELETE CASCADE,
    invoice_id            BIGINT REFERENCES invoices(id),
    investment            DECIMAL(19,2),
    money                 DECIMAL(19,2),
    note                  TEXT,
    pay_type              INT,
    program_type          VARCHAR(255),
    rate                  VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS contract_attributes (
    id                    BIGSERIAL PRIMARY KEY,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_id           BIGINT NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    name                  VARCHAR(128) NOT NULL,
    attr                  TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (tenant_id, contract_id, name)
);

CREATE TABLE IF NOT EXISTS contract_attachments (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT NOT NULL,
    source_table          VARCHAR(64) NOT NULL,
    tenant_id             INT NOT NULL DEFAULT 10000,
    contract_id           BIGINT REFERENCES contracts(id) ON DELETE CASCADE,
    related_legacy_id     BIGINT,
    attachment_type       VARCHAR(255),
    name                  VARCHAR(500),
    path                  TEXT,
    url                   TEXT,
    note                  TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_table, legacy_id)
);

CREATE TABLE IF NOT EXISTS invoice_items (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    invoice_id            BIGINT REFERENCES invoices(id) ON DELETE CASCADE,
    money                 DECIMAL(19,2),
    program_type          VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS drawing_attachments (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT NOT NULL,
    source_table          VARCHAR(64) NOT NULL,
    tenant_id             INT NOT NULL DEFAULT 10000,
    drawing_id            BIGINT REFERENCES drawings(id) ON DELETE CASCADE,
    approve_date          TIMESTAMPTZ,
    name                  VARCHAR(500),
    remarks               TEXT,
    state                 INT,
    url                   TEXT,
    version               VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_table, legacy_id)
);

CREATE TABLE IF NOT EXISTS bankflow_entries (
    id                    BIGSERIAL PRIMARY KEY,
    legacy_id             BIGINT UNIQUE,
    tenant_id             INT NOT NULL DEFAULT 10000,
    bank_type_legacy_id   BIGINT,
    balance_money         DECIMAL(19,2),
    business_no           VARCHAR(255),
    card_number           VARCHAR(255),
    credit_amount         DECIMAL(19,2),
    currency              VARCHAR(64),
    debit_amount          DECIMAL(19,2),
    guanlian_type         INT,
    note                  TEXT,
    opposite_account      VARCHAR(255),
    opposite_name         VARCHAR(255),
    transaction_time      TIMESTAMPTZ,
    voucher_number        VARCHAR(255),
    voucher_type          VARCHAR(255),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                   JSONB NOT NULL DEFAULT '{}'::jsonb
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_flows_legacy_id ON approve_flows(legacy_id);
CREATE INDEX IF NOT EXISTS idx_approve_tasks_flow     ON approve_tasks(flow_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_tasks_legacy_id
    ON approve_tasks(legacy_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_records_legacy_id
    ON approve_records(legacy_id);
CREATE INDEX IF NOT EXISTS idx_costtickets_contract   ON costtickets(contract_id);
CREATE INDEX IF NOT EXISTS idx_payments_contract      ON payments(contract_id);
CREATE INDEX IF NOT EXISTS idx_payments_project       ON payments(project_ref);
CREATE INDEX IF NOT EXISTS idx_payments_legacy_balance_id ON payments(legacy_balance_id);
CREATE INDEX IF NOT EXISTS idx_costticket_items_costticket ON costticket_items(costticket_id);
CREATE INDEX IF NOT EXISTS idx_payment_items_payment ON payment_items(payment_id);
CREATE INDEX IF NOT EXISTS idx_payment_attachments_payment ON payment_attachments(payment_id);
CREATE INDEX IF NOT EXISTS idx_contract_details_contract ON contract_details(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_details_invoice ON contract_details(invoice_id);
CREATE INDEX IF NOT EXISTS idx_contract_attachments_contract ON contract_attachments(contract_id);
CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice ON invoice_items(invoice_id);
CREATE INDEX IF NOT EXISTS idx_drawing_attachments_drawing ON drawing_attachments(drawing_id);
CREATE INDEX IF NOT EXISTS idx_bankflow_entries_time ON bankflow_entries(transaction_time DESC);

-- ============================================================
-- PHASE 1 EXTENSION: resolver + qualification + profile
-- ============================================================

-- Resolver credential ledger (shared by resolver/qualification flows).
CREATE TABLE IF NOT EXISTS credentials (
    id           BIGSERIAL PRIMARY KEY,
    holder_ref   TEXT NOT NULL,
    holder_type  TEXT NOT NULL CHECK (holder_type IN ('PERSON', 'COMPANY')),
    cert_type    TEXT NOT NULL,
    cert_number  TEXT,
    issued_at    DATE,
    expires_at   DATE,
    scope        TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE', 'EXPIRED', 'REVOKED', 'SUSPENDED')),
    ref          VARCHAR(500),
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_credentials_holder
    ON credentials (holder_ref, status, expires_at);
CREATE INDEX IF NOT EXISTS idx_credentials_type
    ON credentials (tenant_id, cert_type, status, expires_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_credentials_ref_uq
    ON credentials(ref) WHERE ref IS NOT NULL;

-- Qualification certificates (company/person).
CREATE TABLE IF NOT EXISTS qualifications (
    id              BIGSERIAL PRIMARY KEY,
    holder_type     VARCHAR(10) NOT NULL CHECK (holder_type IN ('COMPANY','PERSON')),
    holder_id       BIGINT NOT NULL,
    holder_name     VARCHAR(255) NOT NULL,
    executor_ref    VARCHAR(500),
    qual_type       VARCHAR(50) NOT NULL,
    cert_no         VARCHAR(255) NOT NULL,
    issued_by       VARCHAR(255),
    issued_at       TIMESTAMPTZ,
    valid_from      TIMESTAMPTZ,
    valid_until     TIMESTAMPTZ,
    status          VARCHAR(20) NOT NULL DEFAULT 'VALID'
                    CHECK (status IN ('VALID','EXPIRED','EXPIRE_SOON','APPLYING','REVOKED')),
    specialty       VARCHAR(255),
    level           VARCHAR(50),
    scope           TEXT,
    attachment_url  TEXT,
    note            TEXT,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    ref             VARCHAR(500),
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qual_holder       ON qualifications(holder_type, holder_id);
CREATE INDEX IF NOT EXISTS idx_qual_executor     ON qualifications(executor_ref);
CREATE INDEX IF NOT EXISTS idx_qual_type_status  ON qualifications(qual_type, status);
CREATE INDEX IF NOT EXISTS idx_qual_valid_until  ON qualifications(valid_until) WHERE deleted=FALSE;
CREATE UNIQUE INDEX IF NOT EXISTS idx_qualifications_ref_uq
    ON qualifications(ref) WHERE ref IS NOT NULL;

-- Qualification assignment ledger: bind one qualification to one active project.
CREATE TABLE IF NOT EXISTS qualification_assignments (
    id               BIGSERIAL PRIMARY KEY,
    qualification_id BIGINT NOT NULL REFERENCES qualifications(id),
    executor_ref     VARCHAR(500) NOT NULL,
    project_ref      VARCHAR(500) NOT NULL,
    status           VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
                     CHECK (status IN ('ACTIVE','RELEASED')),
    tenant_id        INT NOT NULL DEFAULT 10000,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_qa_project_active
    ON qualification_assignments(tenant_id, project_ref, status);
CREATE INDEX IF NOT EXISTS idx_qa_qualification_active
    ON qualification_assignments(tenant_id, qualification_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_qa_uniq_active_qual
    ON qualification_assignments(qualification_id) WHERE status='ACTIVE';

-- Regulation document registry.
CREATE TABLE IF NOT EXISTS regulation_documents (
    id              BIGSERIAL PRIMARY KEY,
    legacy_id       BIGINT,
    doc_no          VARCHAR(128),
    title           VARCHAR(500) NOT NULL,
    doc_type        VARCHAR(100) NOT NULL DEFAULT 'REGULATION',
    jurisdiction    VARCHAR(100) NOT NULL DEFAULT 'CN',
    publisher       VARCHAR(255),
    status          VARCHAR(20) NOT NULL DEFAULT 'EFFECTIVE'
                    CHECK (status IN ('DRAFT','EFFECTIVE','SUPERSEDED','REPEALED','ARCHIVED')),
    category        VARCHAR(100),
    keywords        TEXT,
    summary         TEXT,
    source_url      TEXT,
    project_ref     VARCHAR(500),
    executor_ref    VARCHAR(500),
    ref             VARCHAR(500),
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_docs_ref_uq
    ON regulation_documents(ref) WHERE ref IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_docs_doc_no_uq
    ON regulation_documents(tenant_id, doc_no)
    WHERE doc_no IS NOT NULL AND doc_no <> '' AND deleted=FALSE;
CREATE INDEX IF NOT EXISTS idx_reg_docs_status
    ON regulation_documents(tenant_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_reg_docs_title
    ON regulation_documents USING gin(to_tsvector('simple', title));

-- Regulation document version timeline.
CREATE TABLE IF NOT EXISTS regulation_versions (
    id              BIGSERIAL PRIMARY KEY,
    document_id     BIGINT NOT NULL REFERENCES regulation_documents(id) ON DELETE CASCADE,
    version_no      INT NOT NULL DEFAULT 1,
    effective_from  TIMESTAMPTZ,
    effective_to    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    content_hash    VARCHAR(128),
    content_text    TEXT,
    attachment_url  TEXT,
    source_note     TEXT,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (document_id, version_no)
);

CREATE INDEX IF NOT EXISTS idx_reg_versions_effective
    ON regulation_versions(tenant_id, effective_from DESC, effective_to DESC);
CREATE INDEX IF NOT EXISTS idx_reg_versions_published
    ON regulation_versions(tenant_id, published_at DESC);

-- Achievement profile root table for bid/qualification materials.
CREATE TABLE IF NOT EXISTS achievement_profiles (
    id              BIGSERIAL PRIMARY KEY,
    project_name    VARCHAR(500) NOT NULL,
    project_type    VARCHAR(30) NOT NULL DEFAULT 'OTHER',
    building_unit   VARCHAR(255),
    location        VARCHAR(255),
    start_date      TIMESTAMPTZ,
    end_date        TIMESTAMPTZ,
    our_scope       TEXT,
    contract_amount DECIMAL(19,2) DEFAULT 0,
    our_amount      DECIMAL(19,2) DEFAULT 0,
    scale_metrics   JSONB DEFAULT '{}',
    contract_id     BIGINT REFERENCES contracts(id),
    project_ref     VARCHAR(500),
    utxo_ref        VARCHAR(500),
    status          VARCHAR(20) NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT','COMPLETE','SUBMITTED')),
    company_id      INT REFERENCES companies(id),
    source          VARCHAR(20) NOT NULL DEFAULT 'MANUAL'
                    CHECK (source IN ('UTXO_AUTO','MANUAL')),
    note            TEXT,
    deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS achievement_profile_personnel (
    id              BIGSERIAL PRIMARY KEY,
    profile_id      BIGINT NOT NULL REFERENCES achievement_profiles(id) ON DELETE CASCADE,
    employee_id     BIGINT REFERENCES employees(id),
    employee_name   VARCHAR(255) NOT NULL,
    executor_ref    VARCHAR(500),
    role            VARCHAR(100) NOT NULL,
    specialty       VARCHAR(100),
    qual_type       VARCHAR(50),
    cert_no         VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS achievement_profile_attachments (
    id              BIGSERIAL PRIMARY KEY,
    profile_id      BIGINT NOT NULL REFERENCES achievement_profiles(id) ON DELETE CASCADE,
    kind            VARCHAR(30) NOT NULL DEFAULT 'OTHER'
                    CHECK (kind IN ('CONTRACT','REVIEW_CERT','COMPLETION','OTHER')),
    name            VARCHAR(255) NOT NULL,
    url             TEXT NOT NULL,
    utxo_ref        VARCHAR(500),
    note            TEXT
);

CREATE INDEX IF NOT EXISTS idx_profile_company    ON achievement_profiles(company_id);
CREATE INDEX IF NOT EXISTS idx_profile_type       ON achievement_profiles(project_type);
CREATE INDEX IF NOT EXISTS idx_profile_status     ON achievement_profiles(status);
CREATE INDEX IF NOT EXISTS idx_profile_utxo       ON achievement_profiles(utxo_ref);
CREATE INDEX IF NOT EXISTS idx_profile_end_date   ON achievement_profiles(end_date DESC);
CREATE INDEX IF NOT EXISTS idx_profile_name       ON achievement_profiles USING gin(to_tsvector('simple', project_name));
CREATE INDEX IF NOT EXISTS idx_profile_personnel  ON achievement_profile_personnel(profile_id);
CREATE INDEX IF NOT EXISTS idx_profile_person_emp ON achievement_profile_personnel(employee_id);
CREATE INDEX IF NOT EXISTS idx_profile_attach     ON achievement_profile_attachments(profile_id);

-- Rights resource ledger (review/sign/invoice authority).
CREATE TABLE IF NOT EXISTS rights (
    id           BIGSERIAL PRIMARY KEY,
    ref          VARCHAR(500) NOT NULL UNIQUE,
    right_type   VARCHAR(50) NOT NULL,
    holder_ref   VARCHAR(500) NOT NULL,
    scope        TEXT NOT NULL DEFAULT '',
    status       VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE','REVOKED','EXPIRED','DISABLED')),
    valid_from   TIMESTAMPTZ,
    valid_until  TIMESTAMPTZ,
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rights_holder ON rights(tenant_id, holder_ref, status);
CREATE INDEX IF NOT EXISTS idx_rights_type   ON rights(tenant_id, right_type, status);

-- ============================================================
-- PHASE 1 EXTENSION: namespace protocol + publishing center
-- ============================================================

CREATE TABLE IF NOT EXISTS namespaces (
    id              BIGSERIAL PRIMARY KEY,
    ref             VARCHAR(500) NOT NULL,
    parent_ref      VARCHAR(500),
    name            VARCHAR(255) NOT NULL,
    inherited_rules TEXT[] NOT NULL DEFAULT '{}',
    owned_genesis   TEXT[] NOT NULL DEFAULT '{}',
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, ref)
);

CREATE INDEX IF NOT EXISTS idx_namespaces_parent
    ON namespaces(tenant_id, parent_ref);

CREATE TABLE IF NOT EXISTS namespace_delegations (
    id         BIGSERIAL PRIMARY KEY,
    from_ref   VARCHAR(500) NOT NULL,
    to_ref     VARCHAR(500) NOT NULL,
    project_ref VARCHAR(500) NOT NULL DEFAULT '',
    action     VARCHAR(100) NOT NULL DEFAULT '',
    status     VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
               CHECK (status IN ('ACTIVE','DISABLED')),
    tenant_id  INT NOT NULL DEFAULT 10000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_namespace_delegations_match
    ON namespace_delegations(tenant_id, from_ref, to_ref, status, project_ref, action);

CREATE TABLE IF NOT EXISTS review_certificates (
    id           BIGSERIAL PRIMARY KEY,
    cert_ref     VARCHAR(500) NOT NULL,
    project_ref  VARCHAR(500) NOT NULL,
    drawing_no   VARCHAR(255) NOT NULL,
    executor_ref VARCHAR(500) NOT NULL,
    payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, cert_ref)
);

CREATE INDEX IF NOT EXISTS idx_review_certificates_project
    ON review_certificates(tenant_id, project_ref, drawing_no, created_at DESC);

CREATE TABLE IF NOT EXISTS drawing_versions (
    id              BIGSERIAL PRIMARY KEY,
    drawing_no      VARCHAR(255) NOT NULL,
    version_no      INT NOT NULL,
    project_ref     VARCHAR(500) NOT NULL,
    review_cert_ref VARCHAR(500) NOT NULL,
    file_hash       VARCHAR(255),
    publisher_ref   VARCHAR(500),
    status          VARCHAR(20) NOT NULL DEFAULT 'CURRENT'
                    CHECK (status IN ('CURRENT','SUPERSEDED')),
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, drawing_no, version_no)
);

CREATE INDEX IF NOT EXISTS idx_drawing_versions_current
    ON drawing_versions(tenant_id, drawing_no, status, version_no DESC);
CREATE INDEX IF NOT EXISTS idx_drawing_versions_project
    ON drawing_versions(tenant_id, project_ref, drawing_no, version_no DESC);

-- ============================================================
-- PHASE 1 EXTENSION: bidding + compliance + resource bindings
-- ============================================================

CREATE TABLE IF NOT EXISTS bid_profiles (
    id              BIGSERIAL PRIMARY KEY,
    ref             VARCHAR(500) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    project_ref     VARCHAR(500) NOT NULL,
    spu_ref         VARCHAR(500) NOT NULL,
    profile_ids     JSONB NOT NULL DEFAULT '[]'::jsonb,
    requirements    JSONB NOT NULL DEFAULT '{}'::jsonb,
    package_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status          VARCHAR(20) NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT','PUBLISHED','ARCHIVED')),
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bid_profiles_project
    ON bid_profiles(tenant_id, project_ref, status);

CREATE TABLE IF NOT EXISTS violation_records (
    id           BIGSERIAL PRIMARY KEY,
    executor_ref VARCHAR(500) NOT NULL,
    violation_type VARCHAR(100),
    project_ref  VARCHAR(500) NOT NULL,
    utxo_ref     VARCHAR(500),
    rule_code    VARCHAR(100) NOT NULL,
    severity     VARCHAR(20) NOT NULL
                 CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL','MINOR','MAJOR')),
    description  TEXT,
    message      TEXT NOT NULL DEFAULT '',
    penalty      NUMERIC NOT NULL DEFAULT 0,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_violation_executor
    ON violation_records(tenant_id, executor_ref, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_violation_executor_recorded
    ON violation_records(tenant_id, executor_ref, recorded_at DESC);

CREATE TABLE IF NOT EXISTS executor_stats (
    id               BIGSERIAL PRIMARY KEY,
    executor_ref     VARCHAR(500) NOT NULL,
    spu_pass_rate    NUMERIC NOT NULL DEFAULT 0,
    total_projects   INT NOT NULL DEFAULT 0,
    total_utxos      INT NOT NULL DEFAULT 0,
    violation_count  INT NOT NULL DEFAULT 0,
    total_violations INT NOT NULL DEFAULT 0,
    last_violation_at TIMESTAMPTZ,
    score            INT NOT NULL DEFAULT 0,
    capability_level_num NUMERIC NOT NULL DEFAULT 0,
    capability_level VARCHAR(20) NOT NULL DEFAULT 'RISK',
    specialty_spus   TEXT[] NOT NULL DEFAULT '{}',
    last_computed_at TIMESTAMPTZ,
    tenant_id        INT NOT NULL DEFAULT 10000,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, executor_ref)
);

CREATE OR REPLACE FUNCTION capability_grade(level NUMERIC)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    IF level >= 4.5 THEN
        RETURN 'CHIEF_ENGINEER';
    ELSIF level >= 4 THEN
        RETURN 'SENIOR_ENGINEER';
    ELSIF level >= 3 THEN
        RETURN 'REGISTERED_ENGINEER';
    ELSIF level >= 2 THEN
        RETURN 'LEAD_ENGINEER';
    ELSE
        RETURN 'ASSISTANT';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION compute_capability_level(
    base_level NUMERIC,
    pass_rate NUMERIC,
    utxo_count INT,
    violation_count INT,
    penalty NUMERIC
)
RETURNS NUMERIC
LANGUAGE plpgsql
AS $$
DECLARE
    level NUMERIC := COALESCE(base_level, 2);
BEGIN
    IF COALESCE(utxo_count, 0) >= 20 AND COALESCE(pass_rate, 0) >= 0.95 AND COALESCE(violation_count, 0) = 0 THEN
        level := level + 0.5;
    END IF;
    IF COALESCE(utxo_count, 0) >= 50 THEN
        level := level + 0.2;
    END IF;
    level := level + ((COALESCE(pass_rate, 0) - 0.8) * 0.8) + COALESCE(penalty, 0);
    IF level < 0 THEN
        level := 0;
    END IF;
    IF level > 5 THEN
        level := 5;
    END IF;
    RETURN level;
END;
$$;

CREATE TABLE IF NOT EXISTS resource_bindings (
    id            BIGSERIAL PRIMARY KEY,
    resource_ref  VARCHAR(500) NOT NULL,
    resource_type VARCHAR(100) NOT NULL,
    project_ref   VARCHAR(500) NOT NULL,
    executor_ref  VARCHAR(500) NOT NULL DEFAULT '',
    spu_ref       VARCHAR(500) NOT NULL DEFAULT '',
    status        VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
                  CHECK (status IN ('ACTIVE','RELEASED')),
    note          TEXT NOT NULL DEFAULT '',
    tenant_id     INT NOT NULL DEFAULT 10000,
    bound_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    -- 三类资源显式绑定（用证留痕）
    achievement_utxo_id BIGINT REFERENCES achievement_utxos(id),
    credential_id       BIGINT REFERENCES qualifications(id)
);

CREATE INDEX IF NOT EXISTS idx_resource_bindings_project
    ON resource_bindings(tenant_id, project_ref, status, bound_at DESC);
CREATE INDEX IF NOT EXISTS idx_resource_bindings_executor
    ON resource_bindings(tenant_id, executor_ref, status, bound_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_bindings_active_unique
    ON resource_bindings(tenant_id, resource_ref) WHERE status='ACTIVE';

-- 用证留痕视图：三条JOIN一次性查清
CREATE OR REPLACE VIEW credential_trace AS
SELECT 
    a.id AS achievement_id,
    a.utxo_ref,
    a.spu_ref,
    a.project_ref,
    a.executor_ref,
    a.proof_hash,
    a.status AS achievement_status,
    a.settled_at,
    
    -- 使用的资质
    q.id AS credential_id,
    q.qual_type,
    q.cert_no,
    q.holder_name AS credential_holder,
    q.status AS credential_status,
    
    -- 执行人员
    a.executor_ref AS actual_executor,
    emp.name AS executor_name,
    COALESCE(es.capability_level, 'RISK') AS capability_level,
    
    -- 绑定信息
    rb.id AS binding_id,
    rb.bound_at,
    rb.status AS binding_status,
    rb.tenant_id
    
FROM achievement_utxos a
LEFT JOIN resource_bindings rb ON rb.achievement_utxo_id = a.id
LEFT JOIN qualifications q ON q.id = rb.credential_id
LEFT JOIN executor_stats es ON es.executor_ref = a.executor_ref AND es.tenant_id = a.tenant_id
LEFT JOIN employees emp ON emp.executor_ref = a.executor_ref
ORDER BY a.settled_at DESC NULLS LAST;

-- 执行体证书仓库视图：以 executor_ref 为主键视角的资质聚合底座
CREATE OR REPLACE VIEW credential_vault AS
SELECT
    q.tenant_id,
    COALESCE(q.executor_ref, '') AS executor_ref,
    q.id AS qualification_id,
    q.qual_type,
    COALESCE(q.holder_name, '') AS holder_name,
    COALESCE(q.cert_no, '') AS cert_no,
    q.status AS qualification_status,
    q.valid_until,
    q.updated_at,
    COALESCE(qa.assignment_count, 0) AS assignment_count,
    qa.last_assignment_at
FROM qualifications q
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)::INT AS assignment_count,
        MAX(created_at) AS last_assignment_at
    FROM qualification_assignments a
    WHERE a.tenant_id = q.tenant_id
      AND a.qualification_id = q.id
) qa ON TRUE
WHERE COALESCE(q.deleted, FALSE) = FALSE;

-- 资质消耗追踪：查某资质被哪些项目消耗
CREATE OR REPLACE VIEW credential_consumption AS
SELECT 
    q.id AS credential_id,
    q.qual_type,
    q.cert_no,
    q.holder_name AS holder_ref,
    q.status AS credential_status,
    q.valid_until,
    
    a.id AS achievement_id,
    a.utxo_ref,
    a.project_ref,
    a.proof_hash,
    a.settled_at,
    
    rb.bound_at,
    rb.status AS binding_status,
    
    CASE WHEN q.status = 'VALID' AND q.valid_until > NOW() THEN true ELSE false END AS is_valid_at_use
    
FROM qualifications q
LEFT JOIN resource_bindings rb ON rb.credential_id = q.id
LEFT JOIN achievement_utxos a ON a.id = rb.achievement_utxo_id
ORDER BY rb.bound_at DESC NULLS LAST;

-- 人员用证汇总：查某人用了哪些证
CREATE OR REPLACE VIEW executor_credential_usage AS
SELECT 
    a.executor_ref,
    emp.name AS executor_name,
    
    COUNT(DISTINCT a.id) AS total_achievements,
    COUNT(DISTINCT q.id) AS credentials_used,
    COUNT(DISTINCT CASE WHEN a.status = 'SETTLED' THEN a.id END) AS settled_count,
    
    json_agg(DISTINCT json_build_object(
        'credential_id', q.id,
        'qual_type', q.qual_type,
        'cert_no', q.cert_no,
        'used_in_project', a.project_ref,
        'used_at', rb.bound_at
    )) FILTER (WHERE q.id IS NOT NULL) AS credential_usage,
    
    a.tenant_id
    
FROM achievement_utxos a
LEFT JOIN resource_bindings rb ON rb.achievement_utxo_id = a.id
LEFT JOIN qualifications q ON q.id = rb.credential_id
LEFT JOIN employees emp ON emp.executor_ref = a.executor_ref
GROUP BY a.executor_ref, emp.name, a.tenant_id;

-- Ref alias mapping for legacy -> canonical namespace compatibility.
CREATE TABLE IF NOT EXISTS ref_aliases (
    id             BIGSERIAL PRIMARY KEY,
    tenant_id      INT NOT NULL DEFAULT 10000,
    alias_ref      VARCHAR(500) NOT NULL,
    canonical_ref  VARCHAR(500) NOT NULL,
    ref_type       VARCHAR(64) NOT NULL DEFAULT 'GENERIC',
    status         VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE','INACTIVE')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ref_aliases_tenant_alias UNIQUE (tenant_id, alias_ref)
);

CREATE INDEX IF NOT EXISTS idx_ref_aliases_tenant_canonical
    ON ref_aliases(tenant_id, canonical_ref);

-- ============================================================
-- PHASE 14 EXTENSION: additional non-log business tables
-- ============================================================

CREATE TABLE IF NOT EXISTS contract_archives (
    id                 BIGSERIAL PRIMARY KEY,
    legacy_id          BIGINT UNIQUE,
    tenant_id          INT NOT NULL DEFAULT 10000,
    contract_id        BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    contract_legacy_id BIGINT,
    archive_date       TIMESTAMPTZ,
    archive_note       TEXT,
    archive_operator   VARCHAR(255),
    check_date         TIMESTAMPTZ,
    check_note         TEXT,
    check_operator     VARCHAR(255),
    signing_time       TIMESTAMPTZ,
    create_by          VARCHAR(255),
    update_by          VARCHAR(255),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_contract_archives_contract
    ON contract_archives(contract_id);

CREATE TABLE IF NOT EXISTS gathering_attachments (
    id                  BIGSERIAL PRIMARY KEY,
    legacy_id           BIGINT UNIQUE,
    tenant_id           INT NOT NULL DEFAULT 10000,
    gathering_id        BIGINT REFERENCES gatherings(id) ON DELETE SET NULL,
    gathering_legacy_id BIGINT,
    filename            VARCHAR(500),
    url                 TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                 JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_gathering_attachments_gathering
    ON gathering_attachments(gathering_id);

CREATE TABLE IF NOT EXISTS filebonds (
    id                       BIGSERIAL PRIMARY KEY,
    legacy_id                BIGINT UNIQUE,
    tenant_id                INT NOT NULL DEFAULT 10000,
    company_id               BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    employee_id              BIGINT REFERENCES employees(id) ON DELETE SET NULL,
    contract_id              BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    balance_invoice_id       BIGINT REFERENCES balance_invoices(id) ON DELETE SET NULL,
    balance_invoice_legacy_id BIGINT,
    user_legacy_id           BIGINT,
    state                    VARCHAR(100),
    bond_fund                DECIMAL(19,2),
    bond_type                INT,
    bond_number              VARCHAR(255),
    partner_type             INT,
    return_file              TEXT,
    return_pay_date          TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                      JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_filebonds_contract
    ON filebonds(contract_id);
CREATE INDEX IF NOT EXISTS idx_filebonds_balance_invoice
    ON filebonds(balance_invoice_id);

CREATE TABLE IF NOT EXISTS project_file_uploads (
    id                 BIGSERIAL PRIMARY KEY,
    legacy_id          BIGINT UNIQUE,
    tenant_id          INT NOT NULL DEFAULT 10000,
    employee_id        BIGINT REFERENCES employees(id) ON DELETE SET NULL,
    user_legacy_id     BIGINT,
    name               TEXT,
    note               TEXT,
    leader             VARCHAR(255),
    sign_date          TIMESTAMPTZ,
    category_legacy_id BIGINT,
    industry_legacy_id BIGINT,
    contract_money     DECIMAL(19,2),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS project_files (
    id                          BIGSERIAL PRIMARY KEY,
    legacy_id                   BIGINT UNIQUE,
    tenant_id                   INT NOT NULL DEFAULT 10000,
    project_file_upload_id      BIGINT REFERENCES project_file_uploads(id) ON DELETE SET NULL,
    project_file_upload_legacy_id BIGINT,
    filename                    VARCHAR(500),
    url                         TEXT,
    state                       VARCHAR(50),
    project_file_type           VARCHAR(255),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                         JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_project_files_upload
    ON project_files(project_file_upload_id);

CREATE TABLE IF NOT EXISTS contract_cancels (
    id                 BIGSERIAL PRIMARY KEY,
    legacy_id          BIGINT UNIQUE,
    tenant_id          INT NOT NULL DEFAULT 10000,
    contract_id        BIGINT REFERENCES contracts(id) ON DELETE SET NULL,
    contract_legacy_id BIGINT,
    cancel_note        TEXT,
    extra              TEXT,
    deleted            BOOLEAN NOT NULL DEFAULT FALSE,
    create_by          VARCHAR(255),
    update_by          VARCHAR(255),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw                JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_contract_cancels_contract
    ON contract_cancels(contract_id);

CREATE TABLE IF NOT EXISTS project_partners (
    id                BIGSERIAL PRIMARY KEY,
    legacy_id         BIGINT UNIQUE,
    tenant_id         INT NOT NULL DEFAULT 10000,
    company_id        BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    company_legacy_id BIGINT,
    name              VARCHAR(255),
    id_card           VARCHAR(255),
    id_card_scanning  TEXT,
    rel_cert_scanning TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw               JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_project_partners_company
    ON project_partners(company_id);

CREATE TABLE IF NOT EXISTS company_contracts (
    id                BIGSERIAL PRIMARY KEY,
    legacy_id         BIGINT UNIQUE,
    tenant_id         INT NOT NULL DEFAULT 10000,
    company_id        BIGINT REFERENCES companies(id) ON DELETE SET NULL,
    company_legacy_id BIGINT,
    user_legacy_id    BIGINT,
    name              VARCHAR(500),
    filename          VARCHAR(500),
    url               TEXT,
    state             VARCHAR(100),
    start_date        TIMESTAMPTZ,
    end_date          TIMESTAMPTZ,
    upload_time       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw               JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_company_contracts_company
    ON company_contracts(company_id);

-- Generic archive of system/log tables to keep full-table traceability.
CREATE TABLE IF NOT EXISTS legacy_source_rows (
    id           BIGSERIAL PRIMARY KEY,
    batch_id     BIGINT NOT NULL,
    source_table TEXT NOT NULL,
    legacy_id    BIGINT,
    source_pk    JSONB,
    row_hash     TEXT NOT NULL,
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw          JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (batch_id, source_table, row_hash)
);
CREATE INDEX IF NOT EXISTS idx_legacy_source_rows_table_legacy
    ON legacy_source_rows(source_table, legacy_id);

