-- ============================================================
--  Upgrade to Decentralized Resolver Framework
--  Implements the database schema changes for the 3-layer
--  decentralized v:// architecture.
--
--  Layer 0: Adds `proof_anchors` table for blockchain anchoring.
--  Layer 1: Adds `resolver_url` to `namespaces` table.
--  Layer 2: Adds helper views for query gateways.
-- ============================================================

BEGIN;

-- ── Layer 1: Resolver Network Support ────────────────────────
-- Add resolver URL to namespaces table. This is the entrypoint for a
-- Layer 1 resolver node responsible for a given namespace.

ALTER TABLE namespaces
    ADD COLUMN IF NOT EXISTS resolver_url VARCHAR(500);

COMMENT ON COLUMN namespaces.resolver_url IS 'Layer 1 VRP resolver endpoint for this namespace (e.g., https://coordos.zhongbei.com/vlink)';

-- Backfill existing namespaces with placeholder URLs
UPDATE namespaces
SET resolver_url = 'https://gateway.coordos.io/vlink'
WHERE ref = 'v://coordos' AND resolver_url IS NULL;

UPDATE namespaces
SET resolver_url = 'https://coordos.cn.zhongbei.com/vlink'
WHERE ref = 'v://cn.zhongbei' AND resolver_url IS NULL;


-- ── Layer 0: Root Anchor Support ─────────────────────────────
-- Create a central table to store blockchain anchor information for any proof_hash.
-- This decouples the anchoring mechanism from the resource tables themselves.

CREATE TABLE IF NOT EXISTS proof_anchors (
    id               BIGSERIAL PRIMARY KEY,
    proof_hash       VARCHAR(255) NOT NULL,
    ref              VARCHAR(500) NOT NULL, -- The v:// ref of the anchored item
    anchor_chain     VARCHAR(50),           -- e.g., 'polygon_amoy'
    anchor_tx_hash   VARCHAR(255),
    anchor_block     BIGINT,
    anchored_at      TIMESTAMPTZ,
    status           VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'CONFIRMED', 'FAILED')),
    tenant_id        INT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (proof_hash)
);

CREATE INDEX IF NOT EXISTS idx_proof_anchors_ref ON proof_anchors(ref);
CREATE INDEX IF NOT EXISTS idx_proof_anchors_status ON proof_anchors(status, tenant_id);
CREATE INDEX IF NOT EXISTS idx_proof_anchors_tx ON proof_anchors(anchor_chain, anchor_tx_hash);

COMMENT ON TABLE proof_anchors IS 'Stores Layer 0 blockchain anchor data for any given proof_hash.';
COMMENT ON COLUMN proof_anchors.proof_hash IS 'The sha256 hash of the resource payload, linking it to the anchor.';
COMMENT ON COLUMN proof_anchors.ref IS 'The v:// address of the resource being anchored.';
COMMENT ON COLUMN proof_anchors.anchor_chain IS 'The blockchain where the anchor transaction was recorded (e.g., polygon_amoy).';
COMMENT ON COLUMN proof_anchors.anchor_tx_hash IS 'The transaction hash of the anchor.';


-- ── Layer 2: Query Gateway Support ───────────────────────────
-- Create helper views to simplify the implementation of resolver endpoints.
-- These views join resources with their anchor information.

CREATE OR REPLACE VIEW resolved_achievements AS
SELECT
    a.id,
    a.ref,
    a.utxo_ref,
    a.namespace_ref,
    a.project_ref,
    a.executor_ref,
    a.payload,
    a.proof_hash,
    a.status,
    a.source,
    a.tenant_id,
    a.created_at,
    a.updated_at,
    pa.anchor_chain,
    pa.anchor_tx_hash,
    pa.anchor_block,
    pa.anchored_at,
    pa.status AS anchor_status
FROM achievement_utxos a
LEFT JOIN proof_anchors pa ON a.proof_hash = pa.proof_hash;

COMMENT ON VIEW resolved_achievements IS 'View joining achievement_utxos with their blockchain anchor data for easy resolution.';

CREATE OR REPLACE VIEW resolved_genesis_utxos AS
SELECT
    g.*,
    pa.anchor_chain,
    pa.anchor_tx_hash,
    pa.anchor_block,
    pa.anchored_at,
    pa.status AS anchor_status
FROM genesis_utxos g
LEFT JOIN proof_anchors pa ON g.proof_hash = pa.proof_hash;

COMMENT ON VIEW resolved_genesis_utxos IS 'View joining genesis_utxos with their blockchain anchor data for easy resolution.';

COMMIT;