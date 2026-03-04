\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

BEGIN;

WITH orphan_ids AS (
    SELECT ca.id
    FROM contract_attachments ca
    WHERE ca.tenant_id = :tenant_id::int
      AND ca.contract_id IS NULL
      AND (ca.raw ->> 'contract_id') ~ '^[0-9]+$'
      AND NOT EXISTS (
          SELECT 1
          FROM contracts c
          WHERE c.tenant_id = ca.tenant_id
            AND c.legacy_id = (ca.raw ->> 'contract_id')::bigint
      )
)
DELETE FROM contract_attachments ca
USING orphan_ids o
WHERE ca.id = o.id;

COMMIT;
