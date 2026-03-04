\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

-- Purpose:
--   Converge legacy refs to v://cn.zhongbei and fill deterministic executor_ref/project_ref gaps.
--
-- Usage:
--   psql "$DATABASE_URL" -v tenant_id=10000 -f scripts/backfill_protocol_cn_zhongbei.sql

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

-- ---------------------------------------------------------------------------
-- 0) Before snapshot
-- ---------------------------------------------------------------------------
SELECT 'before' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(executor_ref, '') = '') AS employees_executor_missing
FROM employees
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

SELECT 'before' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(executor_ref, '') = '') AS companies_executor_missing
FROM companies
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

SELECT 'before' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(project_ref, '') = '') AS contracts_project_missing
FROM contracts
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

-- ---------------------------------------------------------------------------
-- 1) Fill companies.executor_ref and employees.executor_ref
-- ---------------------------------------------------------------------------
WITH normalized AS (
    SELECT
        c.id,
        COALESCE(
            NULLIF(
                lower(trim(both '-.' FROM regexp_replace(COALESCE(c.code, ''), '[^a-zA-Z0-9._-]+', '-', 'g'))),
                ''
            ),
            'legacy-' || COALESCE(c.legacy_id::text, c.id::text)
        ) AS slug
    FROM companies c
    WHERE c.tenant_id = :tenant_id::int
      AND c.deleted = FALSE
      AND COALESCE(c.executor_ref, '') = ''
)
UPDATE companies c
SET executor_ref = 'v://cn.zhongbei/executor/org/' || n.slug || '@v1'
FROM normalized n
WHERE c.id = n.id;

UPDATE companies
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') <> ''
  AND executor_ref LIKE 'v://zhongbei/%';

UPDATE companies
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

WITH normalized AS (
    SELECT
        e.id,
        COALESCE(
            NULLIF(lower(regexp_replace(COALESCE(e.id_card, ''), '[^0-9xX]+', '', 'g')), ''),
            NULLIF(lower(trim(both '-.' FROM regexp_replace(COALESCE(e.account, ''), '[^a-zA-Z0-9._-]+', '-', 'g'))), ''),
            'legacy-' || COALESCE(e.legacy_id::text, e.id::text)
        ) AS slug
    FROM employees e
    WHERE e.tenant_id = :tenant_id::int
      AND e.deleted = FALSE
      AND COALESCE(e.executor_ref, '') = ''
)
UPDATE employees e
SET executor_ref = 'v://cn.zhongbei/executor/person/' || n.slug || '@v1'
FROM normalized n
WHERE e.id = n.id;

UPDATE employees
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') <> ''
  AND executor_ref LIKE 'v://zhongbei/%';

UPDATE employees
SET executor_ref = regexp_replace(executor_ref, '^v://person/([^/]+)/executor$', 'v://cn.zhongbei/executor/person/\1@v1')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') <> ''
  AND executor_ref LIKE 'v://person/%/executor';

UPDATE employees
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

UPDATE employees e
SET company_ref = c.executor_ref
FROM companies c
WHERE e.tenant_id = :tenant_id::int
  AND e.deleted = FALSE
  AND e.company_id = c.id
  AND COALESCE(e.company_ref, '') = '';

-- Keep qualification/executor mappings aligned.
UPDATE qualifications
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE qualifications
SET executor_ref = regexp_replace(executor_ref, '^v://person/([^/]+)/executor$', 'v://cn.zhongbei/executor/person/\1@v1')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://person/%/executor';

UPDATE qualifications
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

UPDATE qualification_assignments
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE qualification_assignments
SET executor_ref = regexp_replace(executor_ref, '^v://person/([^/]+)/executor$', 'v://cn.zhongbei/executor/person/\1@v1')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://person/%/executor';

UPDATE qualification_assignments
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

UPDATE violation_records
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE violation_records
SET executor_ref = regexp_replace(executor_ref, '^v://person/([^/]+)/executor$', 'v://cn.zhongbei/executor/person/\1@v1')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://person/%/executor';

UPDATE violation_records
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

-- ---------------------------------------------------------------------------
-- 2) Normalize canonical namespace prefixes
-- ---------------------------------------------------------------------------
UPDATE contracts
SET ref = regexp_replace(ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(ref, '') LIKE 'v://10000/%';

UPDATE balances
SET settlement_ref = regexp_replace(settlement_ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(settlement_ref, '') LIKE 'v://10000/%';

UPDATE credentials
SET ref = regexp_replace(ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(ref, '') LIKE 'v://10000/%';

UPDATE qualifications
SET ref = regexp_replace(ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(ref, '') LIKE 'v://10000/%';

UPDATE achievement_utxos
SET ref = regexp_replace(ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(ref, '') LIKE 'v://10000/%';

UPDATE achievement_utxos
SET utxo_ref = regexp_replace(utxo_ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(utxo_ref, '') LIKE 'v://10000/%';

UPDATE achievement_utxos
SET experience_ref = regexp_replace(experience_ref, '^v://10000', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(experience_ref, '') LIKE 'v://10000/%';

-- ---------------------------------------------------------------------------
-- 3) Backfill contracts.project_ref and align project_nodes
-- ---------------------------------------------------------------------------
UPDATE contracts
SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';

-- Existing project nodes old->new prefix
UPDATE project_nodes p
SET ref = regexp_replace(p.ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE p.tenant_id = :tenant_id::int
  AND COALESCE(p.ref, '') LIKE 'v://zhongbei/%'
  AND NOT EXISTS (
      SELECT 1 FROM project_nodes q
      WHERE q.ref = regexp_replace(p.ref, '^v://zhongbei', 'v://cn.zhongbei')
  );

UPDATE project_nodes
SET parent_ref = regexp_replace(parent_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(parent_ref, '') LIKE 'v://zhongbei/%';

UPDATE project_nodes
SET path = replace(path, 'v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(path, '') LIKE '%v://zhongbei%';

UPDATE project_nodes
SET namespace_ref = 'v://cn.zhongbei'
WHERE tenant_id = :tenant_id::int
  AND (COALESCE(namespace_ref, '') = '' OR namespace_ref IN ('v://zhongbei', 'v://10000'));

UPDATE project_nodes
SET owner_ref = regexp_replace(owner_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(owner_ref, '') LIKE 'v://zhongbei/%';

UPDATE project_nodes
SET contractor_ref = regexp_replace(contractor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(contractor_ref, '') LIKE 'v://zhongbei/%';

UPDATE project_nodes
SET executor_ref = regexp_replace(executor_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE project_nodes
SET contract_ref = regexp_replace(contract_ref, '^v://10000/contract/', 'v://cn.zhongbei/contract/')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(contract_ref, '') LIKE 'v://10000/contract/%';

-- Fill project_ref from legacy mapping first.
WITH candidates AS (
    SELECT
        c.id AS contract_id,
        regexp_replace(MIN(pn.ref), '^v://zhongbei', 'v://cn.zhongbei') AS project_ref
    FROM contracts c
    JOIN project_nodes pn
      ON pn.tenant_id = c.tenant_id
     AND pn.legacy_contract_id = c.legacy_id
    WHERE c.tenant_id = :tenant_id::int
      AND c.deleted = FALSE
      AND COALESCE(c.project_ref, '') = ''
    GROUP BY c.id
)
UPDATE contracts c
SET project_ref = ca.project_ref,
    updated_at = NOW()
FROM candidates ca
WHERE c.id = ca.contract_id
  AND COALESCE(c.project_ref, '') = '';

-- Fallback to deterministic synthetic project_ref.
UPDATE contracts c
SET project_ref = 'v://cn.zhongbei/project/legacy-contract/' || c.id::text,
    updated_at = NOW()
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') = '';

-- Ensure every contract project_ref has a project node.
INSERT INTO project_nodes (
    ref, tenant_id, parent_id, parent_ref, depth, path, name,
    owner_ref, contractor_ref, executor_ref, platform_ref, contract_ref,
    procurement_ref, genesis_ref, status, proof_hash, prev_hash,
    legacy_contract_id, created_at, updated_at, project_type, namespace_ref
)
SELECT
    c.project_ref,
    c.tenant_id,
    NULL::bigint,
    NULL::text,
    1,
    '/legacy',
    LEFT(COALESCE(NULLIF(c.contract_name, ''), NULLIF(c.num, ''), 'legacy-contract-' || c.id::text), 255),
    co.executor_ref,
    co.executor_ref,
    e.executor_ref,
    'v://cn.zhongbei/platform/default@v1',
    'v://cn.zhongbei/contract/' || c.id::text,
    NULL::text,
    NULL::text,
    'CONTRACTED',
    ''::text,
    ''::text,
    c.legacy_id,
    COALESCE(c.created_at, NOW()),
    COALESCE(c.updated_at, NOW()),
    'LEGACY',
    'v://cn.zhongbei'
FROM contracts c
LEFT JOIN companies co ON co.id = c.company_id
LEFT JOIN employees e ON e.id = c.employee_id
LEFT JOIN project_nodes pn ON pn.ref = c.project_ref
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') <> ''
  AND pn.id IS NULL;

-- ---------------------------------------------------------------------------
-- 4) Propagate contract project_ref to dependent business tables
-- ---------------------------------------------------------------------------
UPDATE gatherings g
SET project_ref = c.project_ref,
    updated_at = NOW()
FROM contracts c
WHERE g.tenant_id = :tenant_id::int
  AND g.deleted = FALSE
  AND g.contract_id = c.id
  AND COALESCE(g.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE invoices i
SET project_ref = c.project_ref,
    updated_at = NOW()
FROM contracts c
WHERE i.tenant_id = :tenant_id::int
  AND i.deleted = FALSE
  AND i.contract_id = c.id
  AND COALESCE(i.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE balances b
SET contract_id = g.contract_id,
    updated_at = NOW()
FROM gatherings g
WHERE b.tenant_id = :tenant_id::int
  AND b.deleted = FALSE
  AND b.contract_id IS NULL
  AND b.gathering_id = g.id
  AND g.contract_id IS NOT NULL;

UPDATE balances b
SET project_ref = c.project_ref,
    updated_at = NOW()
FROM contracts c
WHERE b.tenant_id = :tenant_id::int
  AND b.deleted = FALSE
  AND b.contract_id = c.id
  AND COALESCE(b.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE drawings d
SET project_ref = c.project_ref,
    updated_at = NOW()
FROM contracts c
WHERE d.tenant_id = :tenant_id::int
  AND d.deleted = FALSE
  AND d.contract_id = c.id
  AND COALESCE(d.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE payments p
SET project_ref = c.project_ref
FROM contracts c
WHERE p.tenant_id = :tenant_id::int
  AND p.contract_id = c.id
  AND COALESCE(p.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE costtickets ct
SET project_ref = c.project_ref
FROM contracts c
WHERE ct.tenant_id = :tenant_id::int
  AND ct.contract_id = c.id
  AND COALESCE(ct.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

UPDATE achievement_profiles ap
SET project_ref = c.project_ref,
    updated_at = NOW()
FROM contracts c
WHERE ap.tenant_id = :tenant_id::int
  AND ap.deleted = FALSE
  AND ap.contract_id = c.id
  AND COALESCE(ap.project_ref, '') = ''
  AND COALESCE(c.project_ref, '') <> '';

-- ---------------------------------------------------------------------------
-- 5) Normalize remaining project/namespace refs to v://cn.zhongbei
-- ---------------------------------------------------------------------------
UPDATE contracts SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE gatherings SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE invoices SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE balances SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE drawings SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE payments SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE costtickets SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE achievement_profiles SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND deleted = FALSE AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE achievement_utxos SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE step_achievement_utxos SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE bid_documents SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';
UPDATE qualification_assignments SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';

UPDATE achievement_utxos
SET namespace_ref = 'v://cn.zhongbei'
WHERE tenant_id = :tenant_id::int
  AND namespace_ref IN ('v://zhongbei', 'v://10000');

UPDATE step_achievement_utxos
SET namespace_ref = 'v://cn.zhongbei'
WHERE tenant_id = :tenant_id::int
  AND namespace_ref IN ('v://zhongbei', 'v://10000');

UPDATE bid_documents
SET namespace_ref = 'v://cn.zhongbei'
WHERE tenant_id = :tenant_id::int
  AND namespace_ref IN ('v://zhongbei', 'v://10000');

UPDATE achievement_utxos
SET ref = regexp_replace(ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(ref, '') LIKE 'v://zhongbei/%';

UPDATE achievement_utxos
SET utxo_ref = regexp_replace(utxo_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(utxo_ref, '') LIKE 'v://zhongbei/%';

-- ---------------------------------------------------------------------------
-- 6) After snapshot
-- ---------------------------------------------------------------------------
SELECT 'after' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(executor_ref, '') = '') AS employees_executor_missing
FROM employees
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

SELECT 'after' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(executor_ref, '') = '') AS companies_executor_missing
FROM companies
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

SELECT 'after' AS stage,
       COUNT(*) FILTER (WHERE COALESCE(project_ref, '') = '') AS contracts_project_missing
FROM contracts
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE;

SELECT
    COALESCE(namespace_ref, 'NULL') AS namespace_ref,
    COUNT(*)::bigint AS cnt
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
GROUP BY COALESCE(namespace_ref, 'NULL')
ORDER BY cnt DESC, namespace_ref;

COMMIT;
