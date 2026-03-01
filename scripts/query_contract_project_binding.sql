-- Query templates for contract <-> project binding verification.
-- Replace the literals (10000 / 54637 / 16) with your target values.

-- A) By contract ID: find linked project node.
SELECT
  c.id AS contract_id,
  c.legacy_id,
  c.num,
  c.contract_name,
  c.tenant_id,
  c.project_ref,
  pn.id AS project_id,
  pn.ref AS project_ref_node,
  pn.contract_ref AS project_contract_ref
FROM contracts c
LEFT JOIN project_nodes pn
  ON pn.tenant_id = c.tenant_id
 AND pn.ref = c.project_ref
WHERE c.tenant_id = 10000
  AND c.id = 54637;

-- B) By project ID: find linked contract.
SELECT
  pn.id AS project_id,
  pn.ref AS project_ref,
  pn.tenant_id,
  pn.contract_ref,
  c.id AS contract_id,
  c.legacy_id,
  c.num,
  c.contract_name,
  c.project_ref AS contract_project_ref
FROM project_nodes pn
LEFT JOIN contracts c
  ON c.tenant_id = pn.tenant_id
 AND pn.contract_ref = format('v://%s/contract/%s', c.tenant_id, c.id)
WHERE pn.tenant_id = 10000
  AND pn.id = 16;

-- C) By project ref string.
SELECT
  pn.id AS project_id,
  pn.ref AS project_ref,
  pn.contract_ref,
  c.id AS contract_id,
  c.legacy_id,
  c.num
FROM project_nodes pn
LEFT JOIN contracts c
  ON c.tenant_id = pn.tenant_id
 AND pn.contract_ref = format('v://%s/contract/%s', c.tenant_id, c.id)
WHERE pn.tenant_id = 10000
  AND pn.ref = 'v://zhongbei/project/root/verify-fix-202602281-1772267697873574500';

-- D) List mismatched pairs (should be 0).
SELECT
  c.id AS contract_id,
  c.project_ref,
  pn.id AS project_id,
  pn.contract_ref,
  format('v://%s/contract/%s', c.tenant_id, c.id) AS expected_contract_ref
FROM contracts c
JOIN project_nodes pn
  ON pn.tenant_id = c.tenant_id
 AND pn.ref = c.project_ref
WHERE c.tenant_id = 10000
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') <> ''
  AND COALESCE(pn.contract_ref, '') <> format('v://%s/contract/%s', c.tenant_id, c.id)
ORDER BY c.id DESC;

