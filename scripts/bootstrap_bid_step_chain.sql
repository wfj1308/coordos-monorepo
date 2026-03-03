-- Bootstrap minimal bid -> step -> settlement -> achievement chain.
-- Safe to rerun.
-- Recommended order:
--   1) scripts/add_achievement_lib.sql
--   2) scripts/bootstrap_bid_step_chain.sql

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Bid documents for /api/v1/bid
CREATE TABLE IF NOT EXISTS bid_documents (
    id                 BIGSERIAL PRIMARY KEY,
    bid_ref            VARCHAR(255) UNIQUE NOT NULL,
    tenant_id          INT NOT NULL DEFAULT 10000,
    namespace_ref      VARCHAR(255) NOT NULL,
    tender_genesis_ref VARCHAR(500),
    project_name       VARCHAR(500) NOT NULL,
    project_type       VARCHAR(50) NOT NULL,
    owner_name         VARCHAR(255),
    estimated_amount   NUMERIC(15,2),
    bid_deadline       TIMESTAMPTZ,
    our_bid_amount     NUMERIC(15,2),
    bid_package_ref    VARCHAR(255),
    status             VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    proof_hash         VARCHAR(255),
    resource_count     INT NOT NULL DEFAULT 0,
    project_ref        VARCHAR(500),
    contract_id        BIGINT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at       TIMESTAMPTZ,
    awarded_at         TIMESTAMPTZ,
    failed_at          TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS tender_genesis_ref VARCHAR(500);
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS project_ref VARCHAR(500);
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS contract_id BIGINT;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS awarded_at TIMESTAMPTZ;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_bid_documents_namespace ON bid_documents(namespace_ref);
CREATE INDEX IF NOT EXISTS idx_bid_documents_status ON bid_documents(status);
CREATE INDEX IF NOT EXISTS idx_bid_documents_tenant_status ON bid_documents(tenant_id, status);

CREATE TABLE IF NOT EXISTS bid_resources (
    id             BIGSERIAL PRIMARY KEY,
    bid_id         BIGINT NOT NULL REFERENCES bid_documents(id) ON DELETE CASCADE,
    tenant_id      INT NOT NULL DEFAULT 10000,
    resource_type  VARCHAR(50) NOT NULL,
    resource_ref   VARCHAR(500) NOT NULL,
    consume_mode   VARCHAR(20) NOT NULL DEFAULT 'REFERENCE',
    consume_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    resource_name  VARCHAR(255),
    resource_data  JSONB,
    valid_from     TIMESTAMPTZ,
    valid_until    TIMESTAMPTZ,
    verify_url     VARCHAR(500),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bid_resources_bid ON bid_resources(bid_id);
CREATE INDEX IF NOT EXISTS idx_bid_resources_type ON bid_resources(resource_type);
CREATE INDEX IF NOT EXISTS idx_bid_resources_ref ON bid_resources(resource_ref);
CREATE INDEX IF NOT EXISTS idx_bid_resources_consume ON bid_resources(consume_status);

-- Project node compatibility
ALTER TABLE project_nodes ADD COLUMN IF NOT EXISTS project_type VARCHAR(50);
ALTER TABLE project_nodes ADD COLUMN IF NOT EXISTS namespace_ref VARCHAR(500);

-- Step achievement table for /api/v1/step-achievements
CREATE TABLE IF NOT EXISTS step_achievement_utxos (
    id              BIGSERIAL PRIMARY KEY,
    ref             TEXT NOT NULL UNIQUE,
    namespace_ref   TEXT NOT NULL,
    project_ref     TEXT NOT NULL,
    spu_ref         TEXT,
    trip_ref        TEXT,
    step_seq        INT NOT NULL DEFAULT 0,
    executor_ref    TEXT NOT NULL,
    container_ref   TEXT NOT NULL,
    input_refs      TEXT[] DEFAULT '{}',
    output_type     TEXT NOT NULL DEFAULT 'DESIGN_DOC',
    output_name     TEXT,
    output_payload  JSONB DEFAULT '{}',
    quota_consumed  NUMERIC(18,2) DEFAULT 0,
    quota_unit      TEXT DEFAULT 'unit',
    inputs_hash     TEXT NOT NULL,
    proof_hash      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'DRAFT',
    signed_by       TEXT,
    signed_at       TIMESTAMPTZ,
    source          TEXT NOT NULL DEFAULT 'TRIP_DERIVED',
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_step_ach_project ON step_achievement_utxos(project_ref);
CREATE INDEX IF NOT EXISTS idx_step_ach_executor ON step_achievement_utxos(executor_ref);
CREATE INDEX IF NOT EXISTS idx_step_ach_status ON step_achievement_utxos(status);

-- Achievement compatibility columns used by settlement trigger
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS namespace_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS project_name TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS project_type TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS owner_name TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS contract_amount NUMERIC(18,2);
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS completed_year INT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS contract_genesis_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS tender_genesis_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS step_count INT DEFAULT 0;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS review_ref TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS settled_amount NUMERIC(18,2);
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS layer INT DEFAULT 4;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS inputs_hash TEXT;
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE achievement_utxos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

UPDATE achievement_utxos
SET ref = COALESCE(ref, utxo_ref)
WHERE ref IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='achievement_utxos'::regclass
          AND conname='achievement_utxos_status_check'
    ) THEN
        ALTER TABLE achievement_utxos DROP CONSTRAINT achievement_utxos_status_check;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='achievement_utxos'::regclass
          AND conname='achievement_utxos_source_check'
    ) THEN
        ALTER TABLE achievement_utxos DROP CONSTRAINT achievement_utxos_source_check;
    END IF;
END $$;

ALTER TABLE achievement_utxos DROP CONSTRAINT IF EXISTS ck_achievement_utxos_status_all;
ALTER TABLE achievement_utxos DROP CONSTRAINT IF EXISTS ck_achievement_utxos_source_all;
ALTER TABLE achievement_utxos
    ADD CONSTRAINT ck_achievement_utxos_status_all
    CHECK (status IN ('PENDING','SETTLED','DISPUTED','LEGACY','ACTIVE','EXPIRED','VOIDED'));
ALTER TABLE achievement_utxos
    ADD CONSTRAINT ck_achievement_utxos_source_all
    CHECK (source IN ('SPU_INGEST','LEGACY_IMPORT','MANUAL','HISTORICAL_IMPORT','TRIP_DERIVED'));

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname='uq_achievement_utxos_ref'
          AND conrelid='achievement_utxos'::regclass
    ) THEN
        ALTER TABLE achievement_utxos
            ADD CONSTRAINT uq_achievement_utxos_ref UNIQUE (ref);
    END IF;
END $$;

-- Engineer receipt compatibility
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
    tenant_id       INT NOT NULL DEFAULT 10000,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

DO $$
BEGIN
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

CREATE INDEX IF NOT EXISTS idx_eng_receipt_achievement ON engineer_achievement_receipts(achievement_ref);
CREATE INDEX IF NOT EXISTS idx_eng_receipt_executor ON engineer_achievement_receipts(executor_ref);

-- Award trigger: bid -> project/contract genesis
CREATE OR REPLACE FUNCTION fn_bid_awarded()
RETURNS TRIGGER AS $$
DECLARE
    v_ns          TEXT;
    v_year        INT;
    v_seq         TEXT;
    v_project_ref TEXT;
    v_contract_ref TEXT;
    v_tender_ref  TEXT;
    v_amount      BIGINT;
    v_constraints JSONB;
BEGIN
    IF NEW.status <> 'AWARDED' OR OLD.status = 'AWARDED' THEN
        RETURN NEW;
    END IF;

    v_ns := TRIM(COALESCE(NEW.namespace_ref, ''));
    IF v_ns = '' THEN
        v_ns := 'v://unknown';
    ELSIF v_ns NOT LIKE 'v://%' THEN
        v_ns := 'v://' || v_ns;
    END IF;

    v_year := EXTRACT(YEAR FROM NOW())::INT;
    v_seq := LPAD(NEW.id::TEXT, 4, '0');
    v_project_ref := v_ns || '/project/bid-' || v_year || '-' || v_seq;
    v_contract_ref := v_ns || '/genesis/contract/' || v_year || '/' || v_seq;
    v_tender_ref := COALESCE(NEW.tender_genesis_ref, v_ns || '/genesis/tender/' || v_year || '/' || v_seq);

    v_amount := GREATEST(0, COALESCE((NEW.our_bid_amount * 10000)::BIGINT, (NEW.estimated_amount * 10000)::BIGINT, 0));
    v_constraints := jsonb_build_object(
        'project_name', NEW.project_name,
        'project_type', NEW.project_type,
        'owner_name', COALESCE(NEW.owner_name, ''),
        'bid_ref', COALESCE(NEW.bid_ref, ''),
        'tender_ref', v_tender_ref,
        'source', 'BID_AWARD'
    );

    INSERT INTO genesis_utxos (
        ref, resource_type, name,
        total_amount, available_amount, unit,
        constraints, status, tenant_id, created_at, updated_at
    ) VALUES (
        v_contract_ref, 'CONTRACT_FUND', COALESCE(NEW.project_name, 'contract_fund'),
        v_amount, v_amount, 'CNY',
        v_constraints, 'ACTIVE', NEW.tenant_id, NOW(), NOW()
    ) ON CONFLICT (ref) DO NOTHING;

    INSERT INTO project_nodes (
        ref, tenant_id, name, project_type, namespace_ref,
        contract_ref, status, depth, path, created_at, updated_at
    ) VALUES (
        v_project_ref, NEW.tenant_id, COALESCE(NEW.project_name, 'project'),
        COALESCE(NULLIF(NEW.project_type,''), 'OTHER'), v_ns,
        v_contract_ref, 'CONTRACTED', 0, v_project_ref, NOW(), NOW()
    ) ON CONFLICT (ref) DO NOTHING;

    UPDATE bid_resources
    SET consume_status='OCCUPIED', updated_at=NOW()
    WHERE bid_id=NEW.id
      AND resource_type IN ('QUAL_PERSON','REG_ENGINEER')
      AND consume_status IN ('REFERENCED','PENDING');

    NEW.project_ref := v_project_ref;
    NEW.awarded_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bid_awarded ON bid_documents;
CREATE TRIGGER trg_bid_awarded
    BEFORE UPDATE OF status ON bid_documents
    FOR EACH ROW
    EXECUTE FUNCTION fn_bid_awarded();

-- Settlement trigger: final signed step -> TRIP_DERIVED achievement + receipts
CREATE OR REPLACE FUNCTION fn_project_settled()
RETURNS TRIGGER AS $$
DECLARE
    v_ns            TEXT;
    v_year          INT;
    v_project_type  TEXT;
    v_project_name  TEXT;
    v_owner_name    TEXT;
    v_contract_ref  TEXT;
    v_tender_ref    TEXT;
    v_bid_amount    NUMERIC;
    v_step_count    INT;
    v_step_refs     TEXT[];
    v_review_ref    TEXT;
    v_ach_ref       TEXT;
    v_inputs_hash   TEXT;
    v_proof_hash    TEXT;
    v_seq_n         INT := 0;
    r_eng           RECORD;
    v_eng_id        TEXT;
    v_eng_name      TEXT;
    v_eng_ref       TEXT;
    v_eng_inputs    TEXT;
    v_eng_proof     TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.status = OLD.status AND NEW.output_type = OLD.output_type THEN
        RETURN NEW;
    END IF;
    IF NEW.status <> 'SIGNED' THEN
        RETURN NEW;
    END IF;
    IF COALESCE(NEW.spu_ref,'') NOT ILIKE '%settlement%' AND COALESCE(NEW.output_type,'') NOT IN ('SETTLEMENT','FINAL_DELIVERY') THEN
        RETURN NEW;
    END IF;

    SELECT
        COALESCE(pn.namespace_ref, substring(NEW.project_ref from '^(v://[^/]+)')),
        COALESCE(NULLIF(pn.project_type,''), 'OTHER'),
        COALESCE(NULLIF(pn.name,''), 'project'),
        COALESCE(bd.owner_name, ''),
        COALESCE(pn.contract_ref, ''),
        COALESCE(bd.tender_genesis_ref, ''),
        COALESCE(gu.total_amount::numeric / 10000.0, 0)
    INTO
        v_ns, v_project_type, v_project_name,
        v_owner_name, v_contract_ref, v_tender_ref, v_bid_amount
    FROM project_nodes pn
    LEFT JOIN bid_documents bd ON bd.project_ref = pn.ref
    LEFT JOIN genesis_utxos gu ON gu.ref = pn.contract_ref
    WHERE pn.ref = NEW.project_ref
    LIMIT 1;

    IF v_ns IS NULL OR v_ns = '' THEN
        v_ns := substring(NEW.project_ref from '^(v://[^/]+)');
    END IF;
    IF v_ns IS NULL OR v_ns = '' THEN
        RETURN NEW;
    END IF;

    v_year := EXTRACT(YEAR FROM NOW())::INT;

    SELECT COUNT(*), COALESCE(ARRAY_AGG(ref ORDER BY step_seq), '{}')
    INTO v_step_count, v_step_refs
    FROM step_achievement_utxos
    WHERE project_ref = NEW.project_ref
      AND status = 'SIGNED'
      AND output_type <> 'SETTLEMENT';

    IF v_step_count <= 0 THEN
        RETURN NEW;
    END IF;

    SELECT sa.ref
    INTO v_review_ref
    FROM step_achievement_utxos sa
    WHERE sa.project_ref = NEW.project_ref
      AND sa.status = 'SIGNED'
      AND (
        sa.output_type = 'REVIEW_CERT'
        OR COALESCE(sa.spu_ref, '') ILIKE '%review_certificate%'
      )
    ORDER BY sa.signed_at DESC NULLS LAST, sa.step_seq DESC
    LIMIT 1;

    -- Hard gate: no signed review cert -> do not derive achievement.
    IF v_review_ref IS NULL OR v_review_ref = '' THEN
        RETURN NEW;
    END IF;

    v_ach_ref := v_ns || '/utxo/achievement/' || lower(COALESCE(v_project_type, 'other')) || '/' || v_year || '/' || LPAD(NEW.id::TEXT, 4, '0');
    v_inputs_hash := 'sha256:' || encode(digest(
        COALESCE(v_contract_ref,'') || '|' ||
        COALESCE(v_tender_ref,'') || '|' ||
        COALESCE(NEW.proof_hash,'') || '|' ||
        array_to_string(v_step_refs, ',') || '|' ||
        COALESCE(v_bid_amount::TEXT, '0'),
        'sha256'
    ), 'hex');
    v_proof_hash := 'sha256:' || encode(digest(
        v_ach_ref || '|' || v_inputs_hash || '|TRIP_DERIVED',
        'sha256'
    ), 'hex');

    INSERT INTO achievement_utxos (
        utxo_ref, ref, spu_ref, project_ref, executor_ref,
        namespace_ref, project_name, project_type, owner_name,
        contract_amount, completed_year, contract_genesis_ref, tender_genesis_ref,
        step_count, review_ref, settled_amount, layer, inputs_hash, proof_hash,
        status, source, tenant_id, ingested_at, created_at, updated_at
    ) VALUES (
        v_ach_ref, v_ach_ref,
        COALESCE(NULLIF(NEW.spu_ref,''), v_ns || '/spu/bridge/settlement_cert@v1'),
        NEW.project_ref,
        COALESCE(NULLIF(NEW.executor_ref,''), v_ns || '/executor/system/settlement@v1'),
        v_ns, v_project_name, v_project_type, v_owner_name,
        v_bid_amount, v_year, NULLIF(v_contract_ref,''), NULLIF(v_tender_ref,''),
        v_step_count, COALESCE(v_review_ref, NEW.ref), v_bid_amount, 4, v_inputs_hash, v_proof_hash,
        'ACTIVE', 'TRIP_DERIVED', NEW.tenant_id, NOW(), NOW(), NOW()
    ) ON CONFLICT (ref) DO UPDATE SET
        step_count=EXCLUDED.step_count,
        proof_hash=EXCLUDED.proof_hash,
        review_ref=EXCLUDED.review_ref,
        updated_at=NOW();

    FOR r_eng IN
        SELECT sa.executor_ref, sa.container_ref,
               ARRAY_AGG(sa.ref ORDER BY sa.step_seq) AS step_refs,
               COUNT(*) AS step_count,
               CASE WHEN bool_or(sa.signed_by = sa.executor_ref) THEN 'DESIGN_LEAD' ELSE 'PARTICIPANT' END AS role
        FROM step_achievement_utxos sa
        WHERE sa.project_ref = NEW.project_ref
          AND sa.status = 'SIGNED'
          AND sa.output_type <> 'SETTLEMENT'
        GROUP BY sa.executor_ref, sa.container_ref
    LOOP
        v_seq_n := v_seq_n + 1;
        v_eng_id := regexp_replace(r_eng.executor_ref, '.*/executor/person/(.+?)(@v1)?$', '\1');
        IF v_eng_id = '' OR v_eng_id = r_eng.executor_ref THEN
            v_eng_id := 'unknown' || v_seq_n::text;
        END IF;
        SELECT COALESCE(name, v_eng_id) INTO v_eng_name
        FROM employees WHERE executor_ref=r_eng.executor_ref LIMIT 1;
        v_eng_ref := v_ns || '/receipt/achievement/' || v_eng_id || '/' || v_year || '/' || LPAD(NEW.id::TEXT,4,'0') || LPAD(v_seq_n::TEXT,2,'0');
        v_eng_inputs := 'sha256:' || encode(digest(v_ach_ref || '|' || r_eng.executor_ref || '|' || r_eng.container_ref || '|' || r_eng.role, 'sha256'), 'hex');
        v_eng_proof := 'sha256:' || encode(digest(v_eng_ref || '|' || v_eng_inputs || '|TRIP_DERIVED', 'sha256'), 'hex');

        INSERT INTO engineer_achievement_receipts (
            ref, achievement_ref, executor_ref, engineer_name, engineer_id,
            container_ref, role, step_refs, step_count,
            inputs_hash, proof_hash, source, status, tenant_id, created_at
        ) VALUES (
            v_eng_ref, v_ach_ref, r_eng.executor_ref, COALESCE(v_eng_name, v_eng_id), v_eng_id,
            r_eng.container_ref, r_eng.role, r_eng.step_refs, r_eng.step_count,
            v_eng_inputs, v_eng_proof, 'TRIP_DERIVED', 'ACTIVE', NEW.tenant_id, NOW()
        ) ON CONFLICT (ref) DO UPDATE SET
            step_count=EXCLUDED.step_count,
            proof_hash=EXCLUDED.proof_hash;
    END LOOP;

    UPDATE bid_resources br
    SET consume_status='RELEASED', updated_at=NOW()
    FROM bid_documents bd
    WHERE bd.project_ref = NEW.project_ref
      AND br.bid_id = bd.id
      AND br.resource_type IN ('QUAL_PERSON','REG_ENGINEER')
      AND br.consume_status='OCCUPIED';

    IF to_regclass('public.containers') IS NOT NULL THEN
        UPDATE containers
        SET occupied = GREATEST(0, occupied - 1), updated_at = NOW()
        WHERE linked_executor_ref IN (
            SELECT DISTINCT executor_ref
            FROM step_achievement_utxos
            WHERE project_ref=NEW.project_ref
              AND status='SIGNED'
              AND output_type <> 'SETTLEMENT'
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_settled ON step_achievement_utxos;
CREATE TRIGGER trg_project_settled
    AFTER INSERT OR UPDATE ON step_achievement_utxos
    FOR EACH ROW
    EXECUTE FUNCTION fn_project_settled();

COMMIT;
