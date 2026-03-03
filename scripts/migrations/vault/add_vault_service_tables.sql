-- ============================================================
-- add_vault_service_tables.sql
--
-- Core tables for the Vault Service, which acts as the
-- Trip Runner Engine.
-- ============================================================

BEGIN;

-- ══════════════════════════════════════════════════════════════
-- 1. trips - 业务流程实例
-- ══════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS trips (
    id              BIGSERIAL PRIMARY KEY,
    trip_ref        VARCHAR(255) UNIQUE NOT NULL, -- v://cn.zhongbei/trip/project/bid-1-1677654321/1
    tenant_id       INT NOT NULL,
    project_ref     VARCHAR(255) NOT NULL,        -- The project this trip belongs to
    entry_spu_ref   VARCHAR(255) NOT NULL,        -- The SPU that initiated this trip

    -- Current state
    current_spu_ref VARCHAR(255),
    current_step    INT,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PAUSED')),

    -- Context and data
    inputs          JSONB,                        -- Initial UTXOs that started the trip
    outputs         JSONB,                        -- Final UTXOs produced by the trip
    context         JSONB,                        -- Additional trip-specific context

    -- Timestamps
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trips_project_ref ON trips(project_ref);
CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);
CREATE INDEX IF NOT EXISTS idx_trips_tenant_id ON trips(tenant_id);

COMMENT ON TABLE trips IS 'Represents an instance of a business process (a "Trip"), executing a chain of SPUs.';
COMMENT ON COLUMN trips.trip_ref IS 'Unique, addressable reference for this trip instance.';
COMMENT ON COLUMN trips.entry_spu_ref IS 'The first SPU in the chain that this trip is executing.';
COMMENT ON COLUMN trips.current_spu_ref IS 'The SPU currently being processed in the chain.';
COMMENT ON COLUMN trips.current_step IS 'The sequence number of the current step within the current SPU.';

-- ══════════════════════════════════════════════════════════════
-- 2. trip_log - Trip 状态变更和关键事件日志
-- ══════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS trip_log (
    id          BIGSERIAL PRIMARY KEY,
    trip_id     BIGINT NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    tenant_id   INT NOT NULL,
    message     TEXT NOT NULL,
    details     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;