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
    migrate_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING'
);

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
    project_ref  VARCHAR(500) NOT NULL,
    rule_code    VARCHAR(100) NOT NULL,
    severity     VARCHAR(20) NOT NULL
                 CHECK (severity IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    message      TEXT NOT NULL DEFAULT '',
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id    INT NOT NULL DEFAULT 10000,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_violation_executor
    ON violation_records(tenant_id, executor_ref, occurred_at DESC);

CREATE TABLE IF NOT EXISTS executor_stats (
    id               BIGSERIAL PRIMARY KEY,
    executor_ref     VARCHAR(500) NOT NULL,
    total_projects   INT NOT NULL DEFAULT 0,
    total_utxos      INT NOT NULL DEFAULT 0,
    total_violations INT NOT NULL DEFAULT 0,
    last_violation_at TIMESTAMPTZ,
    score            INT NOT NULL DEFAULT 0,
    capability_level VARCHAR(20) NOT NULL DEFAULT 'RISK',
    tenant_id        INT NOT NULL DEFAULT 10000,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, executor_ref)
);

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
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resource_bindings_project
    ON resource_bindings(tenant_id, project_ref, status, bound_at DESC);
CREATE INDEX IF NOT EXISTS idx_resource_bindings_executor
    ON resource_bindings(tenant_id, executor_ref, status, bound_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_resource_bindings_active_unique
    ON resource_bindings(tenant_id, resource_ref) WHERE status='ACTIVE';

