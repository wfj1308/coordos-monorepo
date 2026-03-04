\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

-- Rebuild-compatible protocol backfill:
-- - no dependency on employees.id_card / project_nodes.namespace_ref / bid tables
-- - converge refs/executor/project namespace to v://cn.zhongbei

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

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

-- Ensure namespace roots exist (compatible namespaces schema).
INSERT INTO namespaces (
    ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
)
SELECT
    'v://cn', NULL, 'China Root',
    ARRAY['RULE-001','RULE-002','RULE-003','RULE-004','RULE-005']::varchar[],
    ARRAY[]::varchar[],
    :tenant_id::int, NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM namespaces n WHERE n.ref = 'v://cn' AND n.tenant_id = :tenant_id::int
);

INSERT INTO namespaces (
    ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
)
SELECT
    'v://cn.zhongbei', 'v://cn', 'cn.zhongbei',
    ARRAY['RULE-001','RULE-002','RULE-003','RULE-004','RULE-005']::varchar[],
    ARRAY[]::varchar[],
    :tenant_id::int, NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM namespaces n WHERE n.ref = 'v://cn.zhongbei' AND n.tenant_id = :tenant_id::int
);

-- 1) Fill companies.executor_ref
WITH normalized AS (
    SELECT
        c.id,
        COALESCE(
            NULLIF(lower(trim(both '-.' FROM regexp_replace(COALESCE(c.code, ''), '[^a-zA-Z0-9._-]+', '-', 'g'))), ''),
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
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE companies
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

-- 2) Fill employees.executor_ref
WITH normalized AS (
    SELECT
        e.id,
        COALESCE(
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
  AND COALESCE(executor_ref, '') LIKE 'v://zhongbei/%';

UPDATE employees
SET executor_ref = regexp_replace(executor_ref, '^v://person/([^/]+)/executor$', 'v://cn.zhongbei/executor/person/\1@v1')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') LIKE 'v://person/%/executor';

UPDATE employees
SET executor_ref = executor_ref || '@v1'
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') LIKE 'v://cn.zhongbei/executor/%'
  AND executor_ref !~ '@v[0-9]+$';

-- Keep qualification-executor references aligned.
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

-- 3) Normalize canonical ref prefixes.
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

-- 4) Backfill contracts.project_ref and align project_nodes.
UPDATE contracts
SET project_ref = regexp_replace(project_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(project_ref, '') LIKE 'v://zhongbei/%';

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

UPDATE contracts c
SET project_ref = 'v://cn.zhongbei/project/legacy-contract/' || c.id::text,
    updated_at = NOW()
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') = '';

INSERT INTO project_nodes (
    ref, tenant_id, parent_id, parent_ref, depth, path, name,
    owner_ref, contractor_ref, executor_ref, platform_ref, contract_ref,
    procurement_ref, genesis_ref, status, proof_hash, prev_hash,
    legacy_contract_id, created_at, updated_at
)
SELECT
    c.project_ref,
    c.tenant_id,
    NULL::bigint,
    NULL::varchar,
    1,
    '/legacy',
    LEFT(COALESCE(NULLIF(c.contract_name, ''), NULLIF(c.num, ''), 'legacy-contract-' || c.id::text), 255),
    co.executor_ref,
    co.executor_ref,
    e.executor_ref,
    'v://cn.zhongbei/platform/default@v1',
    'v://cn.zhongbei/contract/' || c.id::text,
    NULL::varchar,
    NULL::varchar,
    'CONTRACTED',
    ''::varchar,
    ''::varchar,
    c.legacy_id,
    COALESCE(c.created_at, NOW()),
    COALESCE(c.updated_at, NOW())
FROM contracts c
LEFT JOIN companies co ON co.id = c.company_id
LEFT JOIN employees e ON e.id = c.employee_id
LEFT JOIN project_nodes pn ON pn.ref = c.project_ref
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') <> ''
  AND pn.id IS NULL;

-- 5) Propagate project_ref into dependent tables.
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

-- 6) Normalize remaining project_ref / namespace_ref prefixes.
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

UPDATE achievement_utxos
SET namespace_ref = 'v://cn.zhongbei'
WHERE tenant_id = :tenant_id::int
  AND (COALESCE(namespace_ref, '') = '' OR namespace_ref IN ('v://zhongbei', 'v://10000'));

UPDATE achievement_utxos
SET ref = regexp_replace(ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(ref, '') LIKE 'v://zhongbei/%';

UPDATE achievement_utxos
SET utxo_ref = regexp_replace(utxo_ref, '^v://zhongbei', 'v://cn.zhongbei')
WHERE tenant_id = :tenant_id::int
  AND COALESCE(utxo_ref, '') LIKE 'v://zhongbei/%';

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
