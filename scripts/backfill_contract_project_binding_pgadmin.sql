-- pgAdmin-friendly version (pure SQL, no psql meta command).
-- Change tenant id by editing the value in SET LOCAL below.

BEGIN;
SET LOCAL coordos.tenant_id = '10000';

SELECT
  'before_stats' AS stage,
  current_setting('coordos.tenant_id')::int AS tenant_id,
  (SELECT COUNT(*) FROM contracts c WHERE c.tenant_id = current_setting('coordos.tenant_id')::int AND c.deleted = FALSE) AS contracts_total,
  (SELECT COUNT(*) FROM contracts c WHERE c.tenant_id = current_setting('coordos.tenant_id')::int AND c.deleted = FALSE AND COALESCE(c.project_ref, '') <> '') AS contracts_bound,
  (SELECT COUNT(*) FROM project_nodes pn WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int) AS project_nodes_total,
  (SELECT COUNT(*) FROM project_nodes pn WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int AND COALESCE(pn.contract_ref, '') <> '') AS project_nodes_bound;

WITH parsed_refs AS (
  SELECT
    pn.ref AS project_ref,
    pn.tenant_id,
    (m[1])::int AS tenant_id_from_ref,
    (m[2])::bigint AS contract_id
  FROM project_nodes pn
  CROSS JOIN LATERAL regexp_match(COALESCE(pn.contract_ref, ''), '^v://([0-9]+)/contract/([0-9]+)$') m
  WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int
),
consistent_refs AS (
  SELECT project_ref, tenant_id, contract_id
  FROM parsed_refs
  WHERE tenant_id = tenant_id_from_ref
),
unique_candidates AS (
  SELECT tenant_id, contract_id, MIN(project_ref) AS project_ref
  FROM consistent_refs
  GROUP BY tenant_id, contract_id
  HAVING COUNT(*) = 1
),
updated AS (
  UPDATE contracts c
  SET project_ref = uc.project_ref,
      updated_at = NOW()
  FROM unique_candidates uc
  WHERE c.tenant_id = uc.tenant_id
    AND c.id = uc.contract_id
    AND c.tenant_id = current_setting('coordos.tenant_id')::int
    AND c.deleted = FALSE
    AND COALESCE(c.project_ref, '') = ''
  RETURNING c.id
)
SELECT 'step_a_pn_contract_ref_to_contracts_project_ref' AS step, COUNT(*) AS updated_count
FROM updated;

WITH unique_candidates AS (
  SELECT
    c.id AS contract_id,
    MIN(pn.ref) AS project_ref
  FROM contracts c
  JOIN project_nodes pn
    ON pn.tenant_id = c.tenant_id
   AND pn.legacy_contract_id = c.legacy_id
  WHERE c.tenant_id = current_setting('coordos.tenant_id')::int
    AND c.deleted = FALSE
    AND c.legacy_id IS NOT NULL
    AND COALESCE(c.project_ref, '') = ''
  GROUP BY c.id
  HAVING COUNT(*) = 1
),
updated AS (
  UPDATE contracts c
  SET project_ref = uc.project_ref,
      updated_at = NOW()
  FROM unique_candidates uc
  WHERE c.id = uc.contract_id
    AND c.tenant_id = current_setting('coordos.tenant_id')::int
    AND c.deleted = FALSE
    AND COALESCE(c.project_ref, '') = ''
  RETURNING c.id
)
SELECT 'step_b_legacy_contract_id_to_contracts_project_ref' AS step, COUNT(*) AS updated_count
FROM updated;

WITH candidates AS (
  SELECT
    pn.id AS project_node_id,
    format('v://%s/contract/%s', c.tenant_id, c.id) AS contract_ref
  FROM contracts c
  JOIN project_nodes pn
    ON pn.tenant_id = c.tenant_id
   AND pn.ref = c.project_ref
  WHERE c.tenant_id = current_setting('coordos.tenant_id')::int
    AND c.deleted = FALSE
    AND COALESCE(c.project_ref, '') <> ''
    AND COALESCE(pn.contract_ref, '') = ''
),
updated AS (
  UPDATE project_nodes pn
  SET contract_ref = c.contract_ref,
      updated_at = NOW()
  FROM candidates c
  WHERE pn.id = c.project_node_id
  RETURNING pn.id
)
SELECT 'step_c_contracts_project_ref_to_pn_contract_ref' AS step, COUNT(*) AS updated_count
FROM updated;

WITH raw_candidates AS (
  SELECT
    pn.id AS project_node_id,
    format('v://%s/contract/%s', c.tenant_id, c.id) AS contract_ref
  FROM project_nodes pn
  JOIN contracts c
    ON c.tenant_id = pn.tenant_id
   AND c.legacy_id = pn.legacy_contract_id
  WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int
    AND pn.legacy_contract_id IS NOT NULL
    AND c.deleted = FALSE
    AND COALESCE(pn.contract_ref, '') = ''
),
unique_candidates AS (
  SELECT project_node_id, MIN(contract_ref) AS contract_ref
  FROM raw_candidates
  GROUP BY project_node_id
  HAVING COUNT(*) = 1
),
updated AS (
  UPDATE project_nodes pn
  SET contract_ref = uc.contract_ref,
      updated_at = NOW()
  FROM unique_candidates uc
  WHERE pn.id = uc.project_node_id
  RETURNING pn.id
)
SELECT 'step_d_legacy_contract_id_to_pn_contract_ref' AS step, COUNT(*) AS updated_count
FROM updated;

SELECT
  'after_stats' AS stage,
  current_setting('coordos.tenant_id')::int AS tenant_id,
  (SELECT COUNT(*) FROM contracts c WHERE c.tenant_id = current_setting('coordos.tenant_id')::int AND c.deleted = FALSE) AS contracts_total,
  (SELECT COUNT(*) FROM contracts c WHERE c.tenant_id = current_setting('coordos.tenant_id')::int AND c.deleted = FALSE AND COALESCE(c.project_ref, '') <> '') AS contracts_bound,
  (SELECT COUNT(*) FROM project_nodes pn WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int) AS project_nodes_total,
  (SELECT COUNT(*) FROM project_nodes pn WHERE pn.tenant_id = current_setting('coordos.tenant_id')::int AND COALESCE(pn.contract_ref, '') <> '') AS project_nodes_bound,
  (
    SELECT COUNT(*)
    FROM contracts c
    JOIN project_nodes pn
      ON pn.tenant_id = c.tenant_id
     AND pn.ref = c.project_ref
    WHERE c.tenant_id = current_setting('coordos.tenant_id')::int
      AND c.deleted = FALSE
      AND COALESCE(c.project_ref, '') <> ''
      AND COALESCE(pn.contract_ref, '') <> format('v://%s/contract/%s', c.tenant_id, c.id)
  ) AS mismatched_pairs;

COMMIT;

