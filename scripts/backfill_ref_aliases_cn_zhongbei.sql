\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

BEGIN;

CREATE TABLE IF NOT EXISTS ref_aliases (
  id           BIGSERIAL PRIMARY KEY,
  tenant_id    INT NOT NULL DEFAULT 10000,
  alias_ref    VARCHAR(500) NOT NULL,
  canonical_ref VARCHAR(500) NOT NULL,
  ref_type     VARCHAR(64) NOT NULL DEFAULT 'GENERIC',
  status       VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_ref_aliases_tenant_alias UNIQUE (tenant_id, alias_ref)
);

CREATE INDEX IF NOT EXISTS idx_ref_aliases_tenant_canonical
  ON ref_aliases(tenant_id, canonical_ref);

WITH raw AS (
  SELECT
    :tenant_id::int AS tenant_id,
    regexp_replace(a.utxo_ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei') AS alias_ref,
    a.utxo_ref AS canonical_ref,
    'achievement_utxo_ref'::varchar(64) AS ref_type,
    1 AS priority
  FROM achievement_utxos a
  WHERE a.tenant_id = :tenant_id::int
    AND COALESCE(a.utxo_ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(a.ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    a.ref,
    'achievement_ref'::varchar(64),
    2
  FROM achievement_utxos a
  WHERE a.tenant_id = :tenant_id::int
    AND COALESCE(a.ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(sa.ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    sa.ref,
    'step_achievement_ref'::varchar(64),
    3
  FROM step_achievement_utxos sa
  WHERE sa.tenant_id = :tenant_id::int
    AND COALESCE(sa.ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(er.ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    er.ref,
    'engineer_receipt_ref'::varchar(64),
    4
  FROM engineer_achievement_receipts er
  WHERE er.tenant_id = :tenant_id::int
    AND COALESCE(er.ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(a.project_ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    a.project_ref,
    'achievement_project_ref'::varchar(64),
    5
  FROM achievement_utxos a
  WHERE a.tenant_id = :tenant_id::int
    AND COALESCE(a.project_ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(sa.project_ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    sa.project_ref,
    'step_project_ref'::varchar(64),
    6
  FROM step_achievement_utxos sa
  WHERE sa.tenant_id = :tenant_id::int
    AND COALESCE(sa.project_ref, '') ~ '^v://(zhongbei|10000)/'

  UNION ALL
  SELECT
    :tenant_id::int,
    regexp_replace(pn.ref, '^v://(zhongbei|10000)', 'v://cn.zhongbei'),
    pn.ref,
    'project_node_ref'::varchar(64),
    7
  FROM project_nodes pn
  WHERE pn.tenant_id = :tenant_id::int
    AND COALESCE(pn.ref, '') ~ '^v://(zhongbei|10000)/'
),
ranked AS (
  SELECT
    tenant_id,
    alias_ref,
    canonical_ref,
    ref_type,
    ROW_NUMBER() OVER (
      PARTITION BY tenant_id, alias_ref
      ORDER BY priority ASC,
               CASE WHEN canonical_ref LIKE 'v://zhongbei/%' THEN 0 ELSE 1 END ASC,
               canonical_ref ASC
    ) AS rn
  FROM raw
  WHERE alias_ref IS NOT NULL
    AND canonical_ref IS NOT NULL
    AND alias_ref <> canonical_ref
)
INSERT INTO ref_aliases (
  tenant_id, alias_ref, canonical_ref, ref_type, status, created_at, updated_at
)
SELECT
  tenant_id, alias_ref, canonical_ref, ref_type, 'ACTIVE', NOW(), NOW()
FROM ranked
WHERE rn = 1
ON CONFLICT (tenant_id, alias_ref) DO UPDATE
SET canonical_ref = EXCLUDED.canonical_ref,
    ref_type = EXCLUDED.ref_type,
    status = 'ACTIVE',
    updated_at = NOW();

COMMIT;

SELECT ref_type, COUNT(*) AS alias_count
FROM ref_aliases
WHERE tenant_id = :tenant_id::int
GROUP BY ref_type
ORDER BY alias_count DESC, ref_type;
