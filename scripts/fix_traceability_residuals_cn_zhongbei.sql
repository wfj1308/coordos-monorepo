\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

BEGIN;

-- 1) Fill missing company executor_ref with deterministic fallback refs.
UPDATE companies c
SET executor_ref = format('v://cn.zhongbei/executor/org/company-%s@v1', c.id),
    updated_at = NOW()
WHERE c.tenant_id = :tenant_id::int
  AND c.deleted = FALSE
  AND COALESCE(c.executor_ref, '') = '';

-- 2) Backfill missing namespace_ref for achievement_utxos from project/ref prefixes.
UPDATE achievement_utxos a
SET namespace_ref = CASE
        WHEN a.project_ref LIKE 'v://cn.zhongbei/%' THEN 'v://cn.zhongbei'
        WHEN a.project_ref LIKE 'v://zhongbei/%' THEN 'v://cn.zhongbei'
        WHEN COALESCE(NULLIF(a.ref, ''), a.utxo_ref) LIKE 'v://cn.zhongbei/%' THEN 'v://cn.zhongbei'
        WHEN COALESCE(NULLIF(a.ref, ''), a.utxo_ref) LIKE 'v://zhongbei/%' THEN 'v://cn.zhongbei'
        ELSE a.namespace_ref
    END,
    updated_at = NOW()
WHERE a.tenant_id = :tenant_id::int
  AND COALESCE(a.namespace_ref, '') = '';

COMMIT;
