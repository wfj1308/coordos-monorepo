-- Capability engine migration (design-institute)

CREATE TABLE IF NOT EXISTS violation_records (
    id             BIGSERIAL PRIMARY KEY,
    executor_ref   VARCHAR(500) NOT NULL,
    violation_type VARCHAR(100) NOT NULL,
    severity       VARCHAR(20) NOT NULL,
    project_ref    VARCHAR(500),
    utxo_ref       VARCHAR(500),
    description    TEXT,
    penalty        NUMERIC NOT NULL DEFAULT 0,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id      INT NOT NULL DEFAULT 10000
);

ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS violation_type VARCHAR(100);
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS utxo_ref VARCHAR(500);
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS penalty NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS rule_code VARCHAR(100) NOT NULL DEFAULT '';
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS message TEXT NOT NULL DEFAULT '';
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE violation_records ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE violation_records
SET violation_type = COALESCE(NULLIF(violation_type, ''), rule_code),
    description = COALESCE(NULLIF(description, ''), message),
    recorded_at = COALESCE(recorded_at, occurred_at, created_at)
WHERE violation_type IS NULL
   OR description IS NULL
   OR recorded_at IS NULL;

UPDATE violation_records
SET rule_code = COALESCE(NULLIF(rule_code, ''), violation_type),
    message = COALESCE(NULLIF(message, ''), description),
    occurred_at = COALESCE(occurred_at, recorded_at, created_at),
    created_at = COALESCE(created_at, recorded_at, occurred_at)
WHERE rule_code = '' OR message = '';

ALTER TABLE violation_records DROP CONSTRAINT IF EXISTS violation_records_severity_check;
ALTER TABLE violation_records
    ADD CONSTRAINT violation_records_severity_check
    CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL', 'MINOR', 'MAJOR'));

CREATE INDEX IF NOT EXISTS idx_violation_executor_recorded
    ON violation_records(tenant_id, executor_ref, recorded_at DESC);

CREATE TABLE IF NOT EXISTS executor_stats (
    executor_ref      TEXT PRIMARY KEY,
    spu_pass_rate     NUMERIC NOT NULL DEFAULT 0,
    total_utxos       INT NOT NULL DEFAULT 0,
    violation_count   INT NOT NULL DEFAULT 0,
    capability_level  NUMERIC NOT NULL DEFAULT 0,
    specialty_spus    TEXT[] NOT NULL DEFAULT '{}',
    last_computed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tenant_id         INT NOT NULL DEFAULT 10000
);

ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS id BIGSERIAL;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS spu_pass_rate NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS total_projects INT NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS total_violations INT NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS violation_count INT NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS last_violation_at TIMESTAMPTZ;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS score INT NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS capability_level_num NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS capability_level VARCHAR(20) NOT NULL DEFAULT 'RISK';
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS specialty_spus TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE executor_stats ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

UPDATE executor_stats
SET violation_count = COALESCE(violation_count, total_violations, 0),
    spu_pass_rate = COALESCE(spu_pass_rate, 0),
    specialty_spus = COALESCE(specialty_spus, '{}'::text[]),
    last_computed_at = COALESCE(last_computed_at, updated_at, NOW());

CREATE UNIQUE INDEX IF NOT EXISTS idx_executor_stats_tenant_ref
    ON executor_stats(tenant_id, executor_ref);

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
