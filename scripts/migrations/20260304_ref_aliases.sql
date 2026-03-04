BEGIN;

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

COMMIT;
