\set ON_ERROR_STOP on

BEGIN;

-- ============================================================
-- Approval traceability extension
-- ============================================================
ALTER TABLE approve_flows
    ADD COLUMN IF NOT EXISTS legacy_catalog INT,
    ADD COLUMN IF NOT EXISTS legacy_hierarchy INT,
    ADD COLUMN IF NOT EXISTS legacy_oid BIGINT,
    ADD COLUMN IF NOT EXISTS legacy_user_id BIGINT;

ALTER TABLE approve_tasks
    ADD COLUMN IF NOT EXISTS legacy_id BIGINT,
    ADD COLUMN IF NOT EXISTS legacy_catalog INT,
    ADD COLUMN IF NOT EXISTS legacy_oid BIGINT,
    ADD COLUMN IF NOT EXISTS legacy_style INT,
    ADD COLUMN IF NOT EXISTS legacy_user_id BIGINT;

ALTER TABLE approve_records
    ADD COLUMN IF NOT EXISTS legacy_id BIGINT,
    ADD COLUMN IF NOT EXISTS legacy_hierarchy INT,
    ADD COLUMN IF NOT EXISTS legacy_state INT,
    ADD COLUMN IF NOT EXISTS legacy_user_id BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_tasks_legacy_id
    ON approve_tasks(legacy_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_records_legacy_id
    ON approve_records(legacy_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_approve_flows_legacy_id
    ON approve_flows(legacy_id);

-- ============================================================
-- Cost/payment domain extension
-- ============================================================
ALTER TABLE costtickets
    ADD COLUMN IF NOT EXISTS flow_id BIGINT,
    ADD COLUMN IF NOT EXISTS invoice_id BIGINT,
    ADD COLUMN IF NOT EXISTS record_id BIGINT,
    ADD COLUMN IF NOT EXISTS tax_expenses_sum DECIMAL(19,2);

ALTER TABLE payments
    ADD COLUMN IF NOT EXISTS legacy_balance_id BIGINT,
    ADD COLUMN IF NOT EXISTS serial_number VARCHAR(255),
    ADD COLUMN IF NOT EXISTS source_table VARCHAR(64) NOT NULL DEFAULT 'balance_payment';

CREATE INDEX IF NOT EXISTS idx_payments_legacy_balance_id
    ON payments(legacy_balance_id);

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

CREATE INDEX IF NOT EXISTS idx_costticket_items_costticket
    ON costticket_items(costticket_id);

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

CREATE INDEX IF NOT EXISTS idx_payment_items_payment
    ON payment_items(payment_id);

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

CREATE INDEX IF NOT EXISTS idx_payment_attachments_payment
    ON payment_attachments(payment_id);

-- ============================================================
-- Contract / invoice / drawing / bankflow details + attachments
-- ============================================================
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

CREATE INDEX IF NOT EXISTS idx_contract_details_contract
    ON contract_details(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_details_invoice
    ON contract_details(invoice_id);

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

CREATE INDEX IF NOT EXISTS idx_contract_attachments_contract
    ON contract_attachments(contract_id);

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

CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice
    ON invoice_items(invoice_id);

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

CREATE INDEX IF NOT EXISTS idx_drawing_attachments_drawing
    ON drawing_attachments(drawing_id);

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

CREATE INDEX IF NOT EXISTS idx_bankflow_entries_time
    ON bankflow_entries(transaction_time DESC);

COMMIT;
