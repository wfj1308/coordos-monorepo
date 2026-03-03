-- Achievement library schema (compatible with both legacy and new model)
-- Safe to run on existing databases created by migrate_pg_schema.sql.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Base tables (superset schema)
CREATE TABLE IF NOT EXISTS achievement_utxos (
    id                    BIGSERIAL PRIMARY KEY,
    utxo_ref              TEXT UNIQUE,
    ref                   TEXT UNIQUE,
    namespace_ref         TEXT,
    spu_ref               TEXT,
    project_ref           TEXT,
    executor_ref          TEXT,
    genesis_ref           TEXT,
    contract_id           BIGINT REFERENCES contracts(id),
    payload               JSONB,
    project_name          TEXT,
    project_type          TEXT,
    owner_name            TEXT,
    region                TEXT,
    scale                 TEXT,
    contract_amount       NUMERIC(18,2),
    completed_year        INT,
    completed_at          DATE,
    qual_ref              TEXT,
    attachments           JSONB DEFAULT '[]'::jsonb,
    inputs_hash           TEXT,
    proof_hash            TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'ACTIVE',
    source                TEXT NOT NULL DEFAULT 'HISTORICAL_IMPORT',
    experience_ref        TEXT,
    contract_genesis_ref  TEXT,
    tender_genesis_ref    TEXT,
    step_count            INT DEFAULT 0,
    review_ref            TEXT,
    settled_amount        NUMERIC(18,2),
    layer                 INT DEFAULT 4,
    tenant_id             INT NOT NULL DEFAULT 1,
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    settled_at            TIMESTAMPTZ,
    created_at            TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS engineer_achievement_receipts (
    id              BIGSERIAL PRIMARY KEY,
    ref             TEXT NOT NULL UNIQUE,
    achievement_ref TEXT NOT NULL,
    executor_ref    TEXT NOT NULL,
    engineer_name   TEXT NOT NULL DEFAULT '',
    engineer_id     TEXT NOT NULL,
    container_ref   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'PARTICIPANT',
    contribution    TEXT,
    step_refs       TEXT[] DEFAULT '{}',
    step_count      INT DEFAULT 0,
    inputs_hash     TEXT NOT NULL DEFAULT '',
    proof_hash      TEXT NOT NULL DEFAULT '',
    source          TEXT NOT NULL DEFAULT 'HISTORICAL_IMPORT',
    status          TEXT NOT NULL DEFAULT 'ACTIVE',
    tenant_id       INT NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- 2) Compatibility: add missing columns on pre-existing tables
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS utxo_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS namespace_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS project_name TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS project_type TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS owner_name TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS region TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS scale TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS contract_amount NUMERIC(18,2);
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS completed_year INT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS completed_at DATE;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS qual_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS attachments JSONB DEFAULT '[]'::jsonb;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS inputs_hash TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS contract_genesis_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS tender_genesis_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS step_count INT DEFAULT 0;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS review_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS settled_amount NUMERIC(18,2);
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS layer INT DEFAULT 4;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

ALTER TABLE engineer_achievement_receipts ADD COLUMN IF NOT EXISTS step_refs TEXT[] DEFAULT '{}';
ALTER TABLE engineer_achievement_receipts ADD COLUMN IF NOT EXISTS step_count INT DEFAULT 0;

-- 3) Backfill key fields for legacy rows
UPDATE achievement_utxos
SET ref = COALESCE(ref, utxo_ref)
WHERE ref IS NULL;

UPDATE achievement_utxos
SET utxo_ref = COALESCE(utxo_ref, ref)
WHERE utxo_ref IS NULL;

UPDATE achievement_utxos
SET namespace_ref = substring(COALESCE(ref, utxo_ref) from '^(v://[^/]+)')
WHERE namespace_ref IS NULL
  AND COALESCE(ref, utxo_ref) IS NOT NULL;

UPDATE achievement_utxos
SET namespace_ref = 'v://legacy'
WHERE namespace_ref IS NULL;

UPDATE achievement_utxos
SET project_name = COALESCE(project_name, NULLIF(payload->>'project_name', ''), project_ref, 'legacy_project')
WHERE project_name IS NULL;

UPDATE achievement_utxos
SET project_type = COALESCE(project_type, NULLIF(payload->>'project_type', ''), 'OTHER')
WHERE project_type IS NULL;

UPDATE achievement_utxos
SET owner_name = COALESCE(owner_name, NULLIF(payload->>'owner_name', ''), '')
WHERE owner_name IS NULL;

UPDATE achievement_utxos
SET contract_amount = COALESCE(
        contract_amount,
        CASE
            WHEN COALESCE(payload->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (payload->>'amount')::numeric
            ELSE NULL
        END,
        0
    )
WHERE contract_amount IS NULL;

UPDATE achievement_utxos
SET completed_year = COALESCE(completed_year, EXTRACT(YEAR FROM COALESCE(ingested_at, created_at, NOW()))::INT)
WHERE completed_year IS NULL;

UPDATE achievement_utxos
SET source = COALESCE(NULLIF(source, ''), 'HISTORICAL_IMPORT')
WHERE source IS NULL OR source = '';

UPDATE achievement_utxos
SET status = COALESCE(NULLIF(status, ''), 'ACTIVE')
WHERE status IS NULL OR status = '';

UPDATE achievement_utxos
SET created_at = COALESCE(created_at, ingested_at, NOW()),
    updated_at = COALESCE(updated_at, created_at, NOW());

UPDATE achievement_utxos
SET proof_hash = 'sha256:' || encode(digest(COALESCE(ref, utxo_ref, id::text), 'sha256'), 'hex')
WHERE proof_hash IS NULL OR proof_hash = '';

UPDATE engineer_achievement_receipts
SET source = COALESCE(NULLIF(source, ''), 'HISTORICAL_IMPORT')
WHERE source IS NULL OR source = '';

UPDATE engineer_achievement_receipts
SET status = COALESCE(NULLIF(status, ''), 'ACTIVE')
WHERE status IS NULL OR status = '';

-- 4) Constraint compatibility: replace restrictive legacy checks
DO $$
DECLARE c RECORD;
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'achievement_utxos'::regclass
          AND conname = 'achievement_utxos_status_check'
    ) THEN
        ALTER TABLE achievement_utxos DROP CONSTRAINT achievement_utxos_status_check;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'achievement_utxos'::regclass
          AND conname = 'achievement_utxos_source_check'
    ) THEN
        ALTER TABLE achievement_utxos DROP CONSTRAINT achievement_utxos_source_check;
    END IF;

    FOR c IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'achievement_utxos'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) ILIKE '%status IN (%'
              OR pg_get_constraintdef(oid) ILIKE '%source IN (%'
          )
    LOOP
        EXECUTE format('ALTER TABLE achievement_utxos DROP CONSTRAINT %I', c.conname);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'achievement_utxos'::regclass
          AND conname = 'ck_achievement_utxos_status_all'
    ) THEN
        ALTER TABLE achievement_utxos
            ADD CONSTRAINT ck_achievement_utxos_status_all
            CHECK (
                status IN (
                    'PENDING','SETTLED','DISPUTED','LEGACY',
                    'ACTIVE','EXPIRED','VOIDED'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'achievement_utxos'::regclass
          AND conname = 'ck_achievement_utxos_source_all'
    ) THEN
        ALTER TABLE achievement_utxos
            ADD CONSTRAINT ck_achievement_utxos_source_all
            CHECK (
                source IN (
                    'SPU_INGEST','LEGACY_IMPORT','MANUAL',
                    'HISTORICAL_IMPORT','TRIP_DERIVED'
                )
            );
    END IF;
END $$;

DO $$
DECLARE c RECORD;
BEGIN
    FOR c IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'engineer_achievement_receipts'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) ILIKE '%status IN (%'
              OR pg_get_constraintdef(oid) ILIKE '%source IN (%'
          )
    LOOP
        EXECUTE format('ALTER TABLE engineer_achievement_receipts DROP CONSTRAINT %I', c.conname);
    END LOOP;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'engineer_achievement_receipts'::regclass
          AND conname = 'ck_engineer_achievement_receipts_status_all'
    ) THEN
        ALTER TABLE engineer_achievement_receipts
            ADD CONSTRAINT ck_engineer_achievement_receipts_status_all
            CHECK (
                status IN (
                    'ACTIVE','VOIDED',
                    'PENDING','SETTLED','DISPUTED','LEGACY'
                )
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'engineer_achievement_receipts'::regclass
          AND conname = 'ck_engineer_achievement_receipts_source_all'
    ) THEN
        ALTER TABLE engineer_achievement_receipts
            ADD CONSTRAINT ck_engineer_achievement_receipts_source_all
            CHECK (
                source IN (
                    'SPU_INGEST','LEGACY_IMPORT','MANUAL',
                    'HISTORICAL_IMPORT','TRIP_DERIVED'
                )
            );
    END IF;
END $$;

-- 5) FK + indexes for library queries
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_achievement_utxos_ref'
          AND conrelid = 'achievement_utxos'::regclass
    ) THEN
        ALTER TABLE achievement_utxos
            ADD CONSTRAINT uq_achievement_utxos_ref UNIQUE (ref);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_engineer_achievement_receipts_achievement_ref'
          AND conrelid = 'engineer_achievement_receipts'::regclass
    ) THEN
        ALTER TABLE engineer_achievement_receipts
            ADD CONSTRAINT fk_engineer_achievement_receipts_achievement_ref
            FOREIGN KEY (achievement_ref)
            REFERENCES achievement_utxos(ref)
            ON UPDATE CASCADE ON DELETE CASCADE
            NOT VALID;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_achievement_ref_uq
    ON achievement_utxos(ref)
    WHERE ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_achievement_ns
    ON achievement_utxos(namespace_ref);
CREATE INDEX IF NOT EXISTS idx_achievement_type
    ON achievement_utxos(project_type);
CREATE INDEX IF NOT EXISTS idx_achievement_year
    ON achievement_utxos(completed_year DESC);
CREATE INDEX IF NOT EXISTS idx_achievement_amount
    ON achievement_utxos(contract_amount DESC);
CREATE INDEX IF NOT EXISTS idx_achievement_source
    ON achievement_utxos(source);
CREATE INDEX IF NOT EXISTS idx_achievement_status
    ON achievement_utxos(status);

CREATE INDEX IF NOT EXISTS idx_eng_receipt_achievement
    ON engineer_achievement_receipts(achievement_ref);
CREATE INDEX IF NOT EXISTS idx_eng_receipt_executor
    ON engineer_achievement_receipts(executor_ref);
CREATE INDEX IF NOT EXISTS idx_eng_receipt_engineer_id
    ON engineer_achievement_receipts(engineer_id);
CREATE INDEX IF NOT EXISTS idx_eng_receipt_container
    ON engineer_achievement_receipts(container_ref);

-- 6) Read models used by API
CREATE OR REPLACE VIEW achievement_pool AS
SELECT
    COALESCE(a.ref, a.utxo_ref) AS ref,
    COALESCE(a.namespace_ref, substring(COALESCE(a.ref, a.utxo_ref) from '^(v://[^/]+)')) AS namespace_ref,
    COALESCE(a.project_name, NULLIF(a.payload->>'project_name', ''), a.project_ref, 'legacy_project') AS project_name,
    COALESCE(a.project_type, NULLIF(a.payload->>'project_type', ''), 'OTHER') AS project_type,
    COALESCE(a.owner_name, NULLIF(a.payload->>'owner_name', ''), '') AS owner_name,
    COALESCE(a.region, '') AS region,
    COALESCE(a.scale, '') AS scale,
    COALESCE(
        a.contract_amount,
        CASE
            WHEN COALESCE(a.payload->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (a.payload->>'amount')::numeric
            ELSE NULL
        END,
        0
    ) AS contract_amount,
    COALESCE(a.completed_year, EXTRACT(YEAR FROM COALESCE(a.ingested_at, a.created_at, NOW()))::INT) AS completed_year,
    a.qual_ref,
    COALESCE(a.source, 'HISTORICAL_IMPORT') AS source,
    COALESCE(a.proof_hash, '') AS proof_hash,
    COALESCE(a.status, 'ACTIVE') AS status,
    (
        COALESCE(a.completed_year, EXTRACT(YEAR FROM COALESCE(a.ingested_at, a.created_at, NOW()))::INT)
        >= EXTRACT(YEAR FROM NOW())::INT - 3
    ) AS within_3years,
    (
        COALESCE(a.completed_year, EXTRACT(YEAR FROM COALESCE(a.ingested_at, a.created_at, NOW()))::INT)
        >= EXTRACT(YEAR FROM NOW())::INT - 5
    ) AS within_5years,
    COUNT(e.id) AS engineer_count,
    MAX(CASE WHEN e.role = 'LEAD_ENGINEER' THEN e.engineer_name END) AS lead_engineer
FROM achievement_utxos a
LEFT JOIN engineer_achievement_receipts e
    ON e.achievement_ref = COALESCE(a.ref, a.utxo_ref)
   AND e.status = 'ACTIVE'
WHERE COALESCE(a.status, 'ACTIVE') = 'ACTIVE'
GROUP BY a.id, a.ref, a.utxo_ref, a.namespace_ref, a.project_name, a.project_type,
         a.owner_name, a.region, a.scale, a.contract_amount, a.completed_year,
         a.qual_ref, a.source, a.proof_hash, a.status, a.payload, a.project_ref,
         a.ingested_at, a.created_at;

CREATE OR REPLACE VIEW engineer_achievement_pool AS
SELECT
    e.ref,
    e.achievement_ref,
    e.executor_ref,
    e.engineer_id,
    e.engineer_name,
    e.container_ref,
    e.role,
    e.contribution,
    e.source,
    e.proof_hash,
    COALESCE(a.project_name, NULLIF(a.payload->>'project_name', ''), a.project_ref, 'legacy_project') AS project_name,
    COALESCE(a.project_type, NULLIF(a.payload->>'project_type', ''), 'OTHER') AS project_type,
    COALESCE(a.owner_name, NULLIF(a.payload->>'owner_name', ''), '') AS owner_name,
    COALESCE(a.region, '') AS region,
    COALESCE(
        a.contract_amount,
        CASE
            WHEN COALESCE(a.payload->>'amount', '') ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN (a.payload->>'amount')::numeric
            ELSE NULL
        END,
        0
    ) AS contract_amount,
    COALESCE(a.completed_year, EXTRACT(YEAR FROM COALESCE(a.ingested_at, a.created_at, NOW()))::INT) AS completed_year,
    (
        COALESCE(a.completed_year, EXTRACT(YEAR FROM COALESCE(a.ingested_at, a.created_at, NOW()))::INT)
        >= EXTRACT(YEAR FROM NOW())::INT - 3
    ) AS within_3years
FROM engineer_achievement_receipts e
JOIN achievement_utxos a
    ON COALESCE(a.ref, a.utxo_ref) = e.achievement_ref
WHERE e.status = 'ACTIVE'
  AND COALESCE(a.status, 'ACTIVE') = 'ACTIVE';

-- 7) Maintenance function
CREATE OR REPLACE FUNCTION fn_expire_old_achievements()
RETURNS INT AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE achievement_utxos
    SET status = 'EXPIRED',
        updated_at = NOW()
    WHERE status = 'ACTIVE'
      AND COALESCE(completed_year, EXTRACT(YEAR FROM COALESCE(ingested_at, created_at, NOW()))::INT)
          < EXTRACT(YEAR FROM NOW())::INT - 8;

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

COMMIT;

-- quick verification output
SELECT 'achievement_utxos' AS table_name, COUNT(*) AS row_count FROM achievement_utxos
UNION ALL
SELECT 'engineer_achievement_receipts' AS table_name, COUNT(*) AS row_count FROM engineer_achievement_receipts;
