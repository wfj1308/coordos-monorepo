\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

-- Usage:
--   psql "$DATABASE_URL" -v tenant_id=10000 -f scripts/cleanup_orphan_contract_attachments_cn_zhongbei.sql
--
-- Safe default:
--   This script ends with ROLLBACK.
--   After reviewing output, replace the last ROLLBACK with COMMIT to apply deletion.

BEGIN;

CREATE TEMP TABLE _orphan_contract_attachments AS
SELECT
    ca.id,
    ca.source_table,
    ca.name,
    ca.path,
    ca.raw ->> 'contract_id' AS legacy_contract_id
FROM contract_attachments ca
WHERE ca.tenant_id = :tenant_id::int
  AND ca.contract_id IS NULL
  AND (ca.raw ->> 'contract_id') ~ '^[0-9]+$'
  AND NOT EXISTS (
      SELECT 1
      FROM contracts c
      WHERE c.tenant_id = ca.tenant_id
        AND c.legacy_id = (ca.raw ->> 'contract_id')::bigint
  );

SELECT 'orphan_contract_attachments_summary' AS section;
SELECT
    source_table,
    COUNT(*)::bigint AS orphan_count
FROM _orphan_contract_attachments
GROUP BY source_table
ORDER BY orphan_count DESC, source_table;

SELECT 'orphan_contract_attachments_total' AS section;
SELECT COUNT(*)::bigint AS orphan_total
FROM _orphan_contract_attachments;

SELECT 'orphan_contract_attachments_sample' AS section;
SELECT
    id,
    source_table,
    legacy_contract_id,
    name,
    path
FROM _orphan_contract_attachments
ORDER BY id
LIMIT 50;

-- Apply delete (still wrapped in transaction; only persists if COMMIT at end):
DELETE FROM contract_attachments ca
USING _orphan_contract_attachments o
WHERE ca.id = o.id;

SELECT 'deleted_rows' AS section;
SELECT COUNT(*)::bigint AS deleted_count
FROM _orphan_contract_attachments;

-- Safety default
ROLLBACK;
