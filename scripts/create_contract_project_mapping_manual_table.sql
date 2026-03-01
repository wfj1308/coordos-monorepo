-- Manual mapping queue for contract <-> project binding backfill.
-- Safe-by-default: each mapping is reviewed/applied with status tracking.

CREATE TABLE IF NOT EXISTS contract_project_mapping_manual (
  id BIGSERIAL PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  contract_id BIGINT NOT NULL,
  project_ref VARCHAR(500) NOT NULL,
  source TEXT NOT NULL DEFAULT 'MANUAL',
  note TEXT NOT NULL DEFAULT '',
  status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
  last_result TEXT NOT NULL DEFAULT '',
  created_by TEXT NOT NULL DEFAULT CURRENT_USER,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ NULL,
  CONSTRAINT chk_contract_project_mapping_manual_status
    CHECK (status IN ('PENDING', 'APPLIED', 'SKIPPED'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_contract_project_mapping_manual_tenant_contract
  ON contract_project_mapping_manual(tenant_id, contract_id);

CREATE INDEX IF NOT EXISTS idx_contract_project_mapping_manual_tenant_status
  ON contract_project_mapping_manual(tenant_id, status, id);

COMMENT ON TABLE contract_project_mapping_manual IS
  'Manual queue to bind contracts.project_ref and project_nodes.contract_ref.';
COMMENT ON COLUMN contract_project_mapping_manual.last_result IS
  'apply result: applied / skipped reason';

