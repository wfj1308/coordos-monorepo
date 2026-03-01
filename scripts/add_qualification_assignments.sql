-- Qualification assignment ledger for project-level credential binding.
-- Safe to run multiple times.

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
