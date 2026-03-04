\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

-- Usage:
--   psql "$DATABASE_URL" -v tenant_id=10000 -f scripts/verify_cutover_cn_zhongbei.sql

BEGIN;

SELECT 'basic_counts' AS section;
SELECT 'companies' AS table_name, COUNT(*)::bigint AS row_count
FROM companies
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'employees', COUNT(*)::bigint
FROM employees
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'contracts', COUNT(*)::bigint
FROM contracts
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'gatherings', COUNT(*)::bigint
FROM gatherings
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'balances', COUNT(*)::bigint
FROM balances
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'invoices', COUNT(*)::bigint
FROM invoices
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'drawings', COUNT(*)::bigint
FROM drawings
WHERE tenant_id = :tenant_id::int AND deleted = FALSE
UNION ALL
SELECT 'approve_flows', COUNT(*)::bigint
FROM approve_flows
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'approve_tasks', COUNT(*)::bigint
FROM approve_tasks t
JOIN approve_flows f ON f.id = t.flow_id
WHERE f.tenant_id = :tenant_id::int
UNION ALL
SELECT 'approve_records', COUNT(*)::bigint
FROM approve_records r
JOIN approve_flows f ON f.id = r.flow_id
WHERE f.tenant_id = :tenant_id::int
UNION ALL
SELECT 'costtickets', COUNT(*)::bigint
FROM costtickets
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'costticket_items', COUNT(*)::bigint
FROM costticket_items
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'payments', COUNT(*)::bigint
FROM payments
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'payment_items', COUNT(*)::bigint
FROM payment_items
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'payment_attachments', COUNT(*)::bigint
FROM payment_attachments
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'contract_details', COUNT(*)::bigint
FROM contract_details
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'contract_attributes', COUNT(*)::bigint
FROM contract_attributes
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'contract_attachments', COUNT(*)::bigint
FROM contract_attachments
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'invoice_items', COUNT(*)::bigint
FROM invoice_items
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'drawing_attachments', COUNT(*)::bigint
FROM drawing_attachments
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'bankflow_entries', COUNT(*)::bigint
FROM bankflow_entries
WHERE tenant_id = :tenant_id::int
UNION ALL
SELECT 'achievement_utxos', COUNT(*)::bigint
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int;

SELECT 'migration_failures' AS section;
SELECT table_name, COUNT(*)::bigint AS failed_count
FROM migration_log
WHERE status = 'FAILED'
GROUP BY table_name
ORDER BY failed_count DESC, table_name;

SELECT 'namespace_check' AS section;
SELECT
  ref,
  short_code,
  org_type,
  parent_ref,
  depth,
  status,
  tenant_id
FROM namespaces
WHERE tenant_id = :tenant_id::int
  AND ref IN ('v://cn', 'v://cn.zhongbei', 'v://zhongbei')
ORDER BY ref;

SELECT 'protocol_ref_completeness' AS section;
SELECT
  'contracts_ref_missing' AS metric,
  COUNT(*)::bigint AS cnt
FROM contracts
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(ref, '') = ''
UNION ALL
SELECT
  'contracts_project_ref_missing',
  COUNT(*)::bigint
FROM contracts
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(project_ref, '') = ''
UNION ALL
SELECT
  'employees_executor_ref_missing',
  COUNT(*)::bigint
FROM employees
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') = ''
UNION ALL
SELECT
  'companies_executor_ref_missing',
  COUNT(*)::bigint
FROM companies
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
  AND COALESCE(executor_ref, '') = ''
UNION ALL
SELECT
  'achievement_namespace_ref_missing',
  COUNT(*)::bigint
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
  AND COALESCE(namespace_ref, '') = ''
UNION ALL
SELECT
  'achievement_executor_ref_missing',
  COUNT(*)::bigint
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
  AND COALESCE(executor_ref, '') = '';

SELECT 'traceability_integrity' AS section;
SELECT
  'approve_flows_missing_legacy_oid_non_catalog8' AS metric,
  COUNT(*)::bigint AS cnt
FROM approve_flows
WHERE tenant_id = :tenant_id::int
  AND legacy_id IS NOT NULL
  AND legacy_oid IS NULL
  AND COALESCE(legacy_catalog, -1) <> 8
UNION ALL
SELECT
  'approve_tasks_missing_legacy_id',
  COUNT(*)::bigint
FROM approve_tasks t
JOIN approve_flows f ON f.id = t.flow_id
WHERE f.tenant_id = :tenant_id::int
  AND t.legacy_id IS NULL
UNION ALL
SELECT
  'approve_records_missing_legacy_id',
  COUNT(*)::bigint
FROM approve_records r
JOIN approve_flows f ON f.id = r.flow_id
WHERE f.tenant_id = :tenant_id::int
  AND r.legacy_id IS NULL
UNION ALL
SELECT
  'costtickets_missing_contract',
  COUNT(*)::bigint
FROM costtickets
WHERE tenant_id = :tenant_id::int
  AND legacy_id IS NOT NULL
  AND contract_id IS NULL
UNION ALL
SELECT
  'payments_missing_contract',
  COUNT(*)::bigint
FROM payments
WHERE tenant_id = :tenant_id::int
  AND legacy_id IS NOT NULL
  AND contract_id IS NULL
UNION ALL
SELECT
  'contract_attachments_missing_contract_but_mappable',
  COUNT(*)::bigint
FROM contract_attachments ca
WHERE ca.tenant_id = :tenant_id::int
  AND ca.contract_id IS NULL
  AND (ca.raw->>'contract_id') ~ '^[0-9]+$'
  AND EXISTS (
      SELECT 1
      FROM contracts c
      WHERE c.tenant_id = ca.tenant_id
        AND c.legacy_id = (ca.raw->>'contract_id')::bigint
  )
UNION ALL
SELECT
  'invoice_items_missing_invoice',
  COUNT(*)::bigint
FROM invoice_items
WHERE tenant_id = :tenant_id::int
  AND invoice_id IS NULL
UNION ALL
SELECT
  'drawing_attachments_missing_drawing',
  COUNT(*)::bigint
FROM drawing_attachments
WHERE tenant_id = :tenant_id::int
  AND drawing_id IS NULL;

SELECT 'traceability_orphans' AS section;
SELECT
  'contract_attachments_missing_contract_total' AS metric,
  COUNT(*)::bigint AS cnt
FROM contract_attachments ca
WHERE ca.tenant_id = :tenant_id::int
  AND ca.contract_id IS NULL
UNION ALL
SELECT
  'contract_attachments_missing_contract_but_mappable',
  COUNT(*)::bigint
FROM contract_attachments ca
WHERE ca.tenant_id = :tenant_id::int
  AND ca.contract_id IS NULL
  AND (ca.raw->>'contract_id') ~ '^[0-9]+$'
  AND EXISTS (
      SELECT 1
      FROM contracts c
      WHERE c.tenant_id = ca.tenant_id
        AND c.legacy_id = (ca.raw->>'contract_id')::bigint
  )
UNION ALL
SELECT
  'contract_attachments_orphan_legacy_unmapped',
  COUNT(*)::bigint
FROM contract_attachments ca
WHERE ca.tenant_id = :tenant_id::int
  AND ca.contract_id IS NULL
  AND (ca.raw->>'contract_id') ~ '^[0-9]+$'
  AND NOT EXISTS (
      SELECT 1
      FROM contracts c
      WHERE c.tenant_id = ca.tenant_id
        AND c.legacy_id = (ca.raw->>'contract_id')::bigint
  );

SELECT 'contract_project_binding_check' AS section;
SELECT
  COUNT(*)::bigint AS mismatched_pairs
FROM contracts c
JOIN project_nodes pn
  ON pn.tenant_id = c.tenant_id
 AND pn.ref = c.project_ref
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.project_ref, '') <> ''
  AND COALESCE(pn.contract_ref, '') <> ''
  AND COALESCE(substring(pn.contract_ref from '/contract/([0-9]+)'), '') <> c.id::text;

SELECT 'top_namespace_distribution' AS section;
SELECT
  COALESCE(namespace_ref, 'NULL') AS namespace_ref,
  COUNT(*)::bigint AS cnt
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
GROUP BY COALESCE(namespace_ref, 'NULL')
ORDER BY cnt DESC, namespace_ref
LIMIT 20;

SELECT 'sample_recent_contracts' AS section;
SELECT
  id, num, contract_name, project_ref, ref, company_id, employee_id, created_at
FROM contracts
WHERE tenant_id = :tenant_id::int
  AND deleted = FALSE
ORDER BY created_at DESC
LIMIT 20;

SELECT 'sample_recent_achievements' AS section;
SELECT
  id,
  COALESCE(NULLIF(ref, ''), utxo_ref) AS ref_or_utxo,
  namespace_ref,
  project_ref,
  executor_ref,
  status,
  source,
  ingested_at
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
ORDER BY ingested_at DESC NULLS LAST, id DESC
LIMIT 20;

ROLLBACK;
