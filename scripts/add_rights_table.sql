-- Rights resource table (review/sign/invoice authority)
-- Addressing format:
--   v://{tenant}/right/{right_type}/{holder_ref}@v1

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

CREATE INDEX IF NOT EXISTS idx_rights_holder
    ON rights(tenant_id, holder_ref, status);
CREATE INDEX IF NOT EXISTS idx_rights_type
    ON rights(tenant_id, right_type, status);
