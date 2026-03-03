-- Auto-settle existing Zhongbei achievement UTXOs.
-- This script does not create new achievements; it only settles existing
-- pending rows for Zhongbei executors.

WITH updated AS (
    UPDATE achievement_utxos
    SET status = 'SETTLED',
        settled_at = COALESCE(settled_at, NOW())
    WHERE tenant_id = 10000
      AND status = 'PENDING'
      AND executor_ref LIKE 'v://zhongbei/executor/person/%'
    RETURNING id
)
SELECT COUNT(*) AS settled_rows FROM updated;

SELECT
    COUNT(*) FILTER (WHERE status = 'SETTLED') AS settled_total,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_total
FROM achievement_utxos
WHERE tenant_id = 10000
  AND executor_ref LIKE 'v://zhongbei/executor/person/%';

