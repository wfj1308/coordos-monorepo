-- Apply manual mapping queue.
-- pgAdmin/psql friendly script (pure SQL).
--
-- Usage:
--   1) Ensure queue table exists:
--      psql "$DATABASE_URL" -f scripts/create_contract_project_mapping_manual_table.sql
--   2) Insert mappings into contract_project_mapping_manual (status defaults to PENDING)
--   3) Execute this script.
--
-- Optional:
--   Edit tenant/apply_limit below.

BEGIN;
SET LOCAL coordos.tenant_id = '10000';
SET LOCAL coordos.apply_limit = '500';

WITH pending AS (
  SELECT
    m.id,
    m.tenant_id,
    m.contract_id,
    m.project_ref,
    format('v://%s/contract/%s', m.tenant_id, m.contract_id) AS expected_contract_ref
  FROM contract_project_mapping_manual m
  WHERE m.tenant_id = current_setting('coordos.tenant_id')::int
    AND m.status = 'PENDING'
  ORDER BY m.id ASC
  LIMIT current_setting('coordos.apply_limit')::int
),
checked AS (
  SELECT
    p.*,
    c.id AS c_id,
    c.deleted AS c_deleted,
    c.project_ref AS c_project_ref,
    pn.id AS pn_id,
    pn.contract_ref AS pn_contract_ref,
    CASE
      WHEN c.id IS NULL THEN 'contract_not_found'
      WHEN c.deleted THEN 'contract_deleted'
      WHEN pn.id IS NULL THEN 'project_not_found'
      WHEN COALESCE(c.project_ref, '') <> '' AND c.project_ref <> p.project_ref THEN 'contract_already_bound_other_project'
      WHEN COALESCE(pn.contract_ref, '') <> '' AND pn.contract_ref <> p.expected_contract_ref THEN 'project_already_bound_other_contract'
      ELSE 'ok'
    END AS reason
  FROM pending p
  LEFT JOIN contracts c
    ON c.id = p.contract_id
   AND c.tenant_id = p.tenant_id
  LEFT JOIN project_nodes pn
    ON pn.ref = p.project_ref
   AND pn.tenant_id = p.tenant_id
),
contract_upd AS (
  UPDATE contracts c
  SET project_ref = ch.project_ref,
      updated_at = NOW()
  FROM checked ch
  WHERE ch.reason = 'ok'
    AND c.tenant_id = ch.tenant_id
    AND c.id = ch.contract_id
    AND COALESCE(c.project_ref, '') = ''
  RETURNING c.id
),
project_upd AS (
  UPDATE project_nodes pn
  SET contract_ref = ch.expected_contract_ref,
      updated_at = NOW()
  FROM checked ch
  WHERE ch.reason = 'ok'
    AND pn.tenant_id = ch.tenant_id
    AND pn.ref = ch.project_ref
    AND COALESCE(pn.contract_ref, '') = ''
  RETURNING pn.id
),
applied AS (
  UPDATE contract_project_mapping_manual m
  SET status = 'APPLIED',
      last_result = CASE
        WHEN COALESCE(ch.c_project_ref, '') = ch.project_ref
         AND COALESCE(ch.pn_contract_ref, '') = ch.expected_contract_ref
          THEN 'applied_already_consistent'
        ELSE 'applied'
      END,
      applied_at = NOW(),
      updated_at = NOW()
  FROM checked ch
  WHERE m.id = ch.id
    AND ch.reason = 'ok'
  RETURNING m.id
),
skipped AS (
  UPDATE contract_project_mapping_manual m
  SET status = 'SKIPPED',
      last_result = ch.reason,
      updated_at = NOW()
  FROM checked ch
  WHERE m.id = ch.id
    AND ch.reason <> 'ok'
  RETURNING m.id
)
SELECT
  (SELECT COUNT(*) FROM pending) AS pending_count,
  (SELECT COUNT(*) FROM checked WHERE reason = 'ok') AS eligible_count,
  (SELECT COUNT(*) FROM contract_upd) AS contracts_updated,
  (SELECT COUNT(*) FROM project_upd) AS projects_updated,
  (SELECT COUNT(*) FROM applied) AS applied_rows,
  (SELECT COUNT(*) FROM skipped) AS skipped_rows;

SELECT
  m.id,
  m.contract_id,
  m.project_ref,
  m.status,
  m.last_result,
  m.updated_at
FROM contract_project_mapping_manual m
WHERE m.tenant_id = current_setting('coordos.tenant_id')::int
ORDER BY m.id DESC
LIMIT 30;

COMMIT;

