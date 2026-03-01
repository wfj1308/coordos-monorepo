-- Helper query for manual contract-project mapping.
-- Replace tenant_id in WHERE clauses if needed.

-- 1) Unbound contracts (legacy first)
SELECT
  c.id AS contract_id,
  c.legacy_id,
  c.num,
  c.contract_name,
  c.signing_time,
  c.contract_balance,
  c.migrate_status,
  c.created_at
FROM contracts c
WHERE c.tenant_id = 10000
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') = ''
ORDER BY
  CASE WHEN c.migrate_status = 'LEGACY' THEN 0 ELSE 1 END,
  c.created_at DESC
LIMIT 200;

-- 2) Project nodes without contract_ref
SELECT
  pn.id AS project_id,
  pn.ref AS project_ref,
  pn.name,
  pn.status,
  pn.legacy_contract_id,
  pn.created_at
FROM project_nodes pn
WHERE pn.tenant_id = 10000
  AND COALESCE(pn.contract_ref, '') = ''
ORDER BY pn.created_at DESC
LIMIT 200;

-- 3) Current queue status
SELECT
  m.id,
  m.tenant_id,
  m.contract_id,
  m.project_ref,
  m.status,
  m.last_result,
  m.note,
  m.updated_at
FROM contract_project_mapping_manual m
WHERE m.tenant_id = 10000
ORDER BY m.id DESC
LIMIT 200;

