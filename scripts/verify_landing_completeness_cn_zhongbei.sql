\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

/*
Purpose:
  Verify "real business data landed" by comparing target tables with
  latest successful raw landing batch (icrm_raw.landing_rows).

Usage:
  psql "$DATABASE_URL" -v tenant_id=10000 -f scripts/verify_landing_completeness_cn_zhongbei.sql
*/

WITH latest_batch AS (
    SELECT batch_id
    FROM icrm_raw.landing_batches
    WHERE status = 'SUCCESS'
    ORDER BY batch_id DESC
    LIMIT 1
)
SELECT 'latest_success_batch' AS item, batch_id::text AS value
FROM latest_batch;

-- Contract: separate test/demo exclusions from real missing rows.
WITH latest_batch AS (
    SELECT batch_id
    FROM icrm_raw.landing_batches
    WHERE status = 'SUCCESS'
    ORDER BY batch_id DESC
    LIMIT 1
),
src_contract AS (
    SELECT
        NULLIF(r.row_data->>'id', '')::bigint AS legacy_id,
        COALESCE(r.row_data->>'contractName', '') AS contract_name,
        COALESCE(r.row_data->>'num', '') AS contract_num
    FROM icrm_raw.landing_rows r
    JOIN latest_batch b ON b.batch_id = r.batch_id
    WHERE r.table_name = 'contract'
),
excluded_contract AS (
    SELECT legacy_id
    FROM src_contract
    WHERE legacy_id IS NOT NULL
      AND (
          lower(contract_name) ~ '(test|demo|sample|mock)'
          OR lower(contract_num) ~ '(test|demo|sample|mock)'
          OR contract_name ~ '(测试|演示|示例|样例)'
          OR contract_num ~ '(测试|演示|示例|样例)'
      )
),
missing_real_contract AS (
    SELECT s.legacy_id
    FROM src_contract s
    LEFT JOIN contracts c ON c.legacy_id = s.legacy_id
    LEFT JOIN excluded_contract e ON e.legacy_id = s.legacy_id
    WHERE s.legacy_id IS NOT NULL
      AND c.id IS NULL
      AND e.legacy_id IS NULL
)
SELECT
    'contract_summary' AS item,
    (SELECT COUNT(*) FROM src_contract WHERE legacy_id IS NOT NULL) AS source_total,
    (SELECT COUNT(*) FROM excluded_contract) AS excluded_test_demo,
    (SELECT COUNT(*) FROM contracts WHERE tenant_id = :tenant_id::int) AS target_total,
    (SELECT COUNT(*) FROM missing_real_contract) AS missing_real;

WITH latest_batch AS (
    SELECT batch_id
    FROM icrm_raw.landing_batches
    WHERE status = 'SUCCESS'
    ORDER BY batch_id DESC
    LIMIT 1
),
src_contract AS (
    SELECT
        NULLIF(r.row_data->>'id', '')::bigint AS legacy_id,
        COALESCE(r.row_data->>'contractName', '') AS contract_name,
        COALESCE(r.row_data->>'num', '') AS contract_num
    FROM icrm_raw.landing_rows r
    JOIN latest_batch b ON b.batch_id = r.batch_id
    WHERE r.table_name = 'contract'
),
excluded_contract AS (
    SELECT legacy_id
    FROM src_contract
    WHERE legacy_id IS NOT NULL
      AND (
          lower(contract_name) ~ '(test|demo|sample|mock)'
          OR lower(contract_num) ~ '(test|demo|sample|mock)'
          OR contract_name ~ '(测试|演示|示例|样例)'
          OR contract_num ~ '(测试|演示|示例|样例)'
      )
)
SELECT
    s.legacy_id,
    s.contract_num,
    s.contract_name
FROM src_contract s
LEFT JOIN contracts c ON c.legacy_id = s.legacy_id
LEFT JOIN excluded_contract e ON e.legacy_id = s.legacy_id
WHERE s.legacy_id IS NOT NULL
  AND c.id IS NULL
  AND e.legacy_id IS NULL
ORDER BY s.legacy_id;

-- Gathering: if linked contract is excluded test/demo, treat as excluded too.
WITH latest_batch AS (
    SELECT batch_id
    FROM icrm_raw.landing_batches
    WHERE status = 'SUCCESS'
    ORDER BY batch_id DESC
    LIMIT 1
),
src_contract AS (
    SELECT
        NULLIF(r.row_data->>'id', '')::bigint AS legacy_id,
        COALESCE(r.row_data->>'contractName', '') AS contract_name,
        COALESCE(r.row_data->>'num', '') AS contract_num
    FROM icrm_raw.landing_rows r
    JOIN latest_batch b ON b.batch_id = r.batch_id
    WHERE r.table_name = 'contract'
),
excluded_contract AS (
    SELECT legacy_id
    FROM src_contract
    WHERE legacy_id IS NOT NULL
      AND (
          lower(contract_name) ~ '(test|demo|sample|mock)'
          OR lower(contract_num) ~ '(test|demo|sample|mock)'
          OR contract_name ~ '(测试|演示|示例|样例)'
          OR contract_num ~ '(测试|演示|示例|样例)'
      )
),
src_gathering AS (
    SELECT
        NULLIF(r.row_data->>'id', '')::bigint AS legacy_id,
        NULLIF(r.row_data->>'contract_id', '')::bigint AS contract_legacy_id,
        COALESCE(r.row_data->>'gatheringnumber', '') AS gathering_number
    FROM icrm_raw.landing_rows r
    JOIN latest_batch b ON b.batch_id = r.batch_id
    WHERE r.table_name = 'gathering'
),
excluded_gathering AS (
    SELECT g.legacy_id
    FROM src_gathering g
    LEFT JOIN excluded_contract ec ON ec.legacy_id = g.contract_legacy_id
    WHERE g.legacy_id IS NOT NULL
      AND (
          lower(g.gathering_number) ~ '(test|demo|sample|mock)'
          OR g.gathering_number ~ '(测试|演示|示例|样例)'
          OR ec.legacy_id IS NOT NULL
      )
),
missing_real_gathering AS (
    SELECT g.legacy_id
    FROM src_gathering g
    LEFT JOIN gatherings t ON t.legacy_id = g.legacy_id
    LEFT JOIN excluded_gathering eg ON eg.legacy_id = g.legacy_id
    WHERE g.legacy_id IS NOT NULL
      AND t.id IS NULL
      AND eg.legacy_id IS NULL
)
SELECT
    'gathering_summary' AS item,
    (SELECT COUNT(*) FROM src_gathering WHERE legacy_id IS NOT NULL) AS source_total,
    (SELECT COUNT(*) FROM excluded_gathering) AS excluded_test_demo,
    (SELECT COUNT(*) FROM gatherings WHERE tenant_id = :tenant_id::int) AS target_total,
    (SELECT COUNT(*) FROM missing_real_gathering) AS missing_real;

WITH latest_batch AS (
    SELECT batch_id
    FROM icrm_raw.landing_batches
    WHERE status = 'SUCCESS'
    ORDER BY batch_id DESC
    LIMIT 1
),
src_flow AS (
    SELECT
        NULLIF(r.row_data->>'id', '')::bigint AS legacy_id,
        NULLIF(r.row_data->>'oid', '')::bigint AS legacy_oid
    FROM icrm_raw.landing_rows r
    JOIN latest_batch b ON b.batch_id = r.batch_id
    WHERE r.table_name = 'approve_flow'
)
SELECT
    'approve_flow_summary' AS item,
    COUNT(*) FILTER (WHERE legacy_id IS NOT NULL) AS source_total,
    COUNT(*) FILTER (WHERE legacy_oid IS NULL) AS source_oid_null,
    COUNT(*) FILTER (
        WHERE legacy_oid IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM approve_flows af WHERE af.legacy_oid = src_flow.legacy_oid
          )
    ) AS missing_by_oid_nonnull,
    COUNT(*) FILTER (
        WHERE legacy_oid IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM approve_flows af WHERE af.legacy_id = src_flow.legacy_id
          )
    ) AS oid_null_and_missing_by_legacy_id
FROM src_flow;

