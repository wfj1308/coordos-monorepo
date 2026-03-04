\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id 10000
\endif

-- Purpose:
-- 1) backfill namespace-level company qualification genesis for cn.zhongbei
-- 2) backfill historical achievement_utxos from real contracts with gathering amount
--
-- Usage:
--   psql "$DATABASE_URL" -v tenant_id=10000 -f scripts/backfill_partner_profile_layers_cn_zhongbei.sql

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

-- Ensure namespace exists.
INSERT INTO namespaces (
    ref, parent_ref, name, inherited_rules, owned_genesis, tenant_id, created_at, updated_at
)
SELECT
    'v://cn', NULL, 'China Root',
    ARRAY['RULE-001','RULE-002','RULE-003','RULE-004','RULE-005']::varchar[],
    ARRAY[]::varchar[],
    :tenant_id::int, NOW(), NOW()
WHERE NOT EXISTS (
    SELECT 1 FROM namespaces n WHERE n.tenant_id = :tenant_id::int AND n.ref = 'v://cn'
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
    SELECT 1 FROM namespaces n WHERE n.tenant_id = :tenant_id::int AND n.ref = 'v://cn.zhongbei'
);

-- Pick a stable credit code candidate from existing real company data.
WITH code_pick AS (
    SELECT COALESCE(
        (
            SELECT c.license_num
            FROM companies c
            WHERE c.tenant_id = :tenant_id::int
              AND c.deleted = FALSE
              AND c.license_num ~ '^[0-9A-Z]{18}$'
              AND c.name LIKE '%中北工程设计咨询有限公司%'
            ORDER BY CASE WHEN c.company_type = 1 THEN 0 ELSE 1 END, c.id
            LIMIT 1
        ),
        ''
    ) AS credit_code
)
INSERT INTO genesis_utxos (
    ref, resource_type, name,
    total_amount, available_amount, unit,
    batch_source, holders, quantity, constraints, consumed_by, remaining,
    status, tenant_id, created_at, updated_at
)
SELECT
    x.ref,
    x.resource_type,
    x.name,
    1, 1, 'LICENSE',
    'INTERNAL',
    '[]'::jsonb,
    1,
    jsonb_build_object(
        'cert_no', x.cert_no,
        'scope', x.scope,
        'verifiable_url', x.verify_url,
        'issued_by', '住房和城乡建设部',
        'grade', x.grade,
        'credit_code', cp.credit_code,
        'valid_until', x.valid_until,
        'rule_binding', x.rule_binding
    ),
    '[]'::jsonb,
    1,
    'ACTIVE',
    :tenant_id::int,
    NOW(),
    NOW()
FROM code_pick cp
CROSS JOIN (
    VALUES
      ('v://cn.zhongbei/genesis/qual/highway_a',   'QUAL_HIGHWAY_INDUSTRY_A',   '公路行业（公路、特大桥梁、特长隧道、交通工程）专业甲级', 'A161003712-1/1', '公路, 特大桥梁, 特长隧道, 交通工程', 'https://jzsc.mohurd.gov.cn/', '甲级', '2030-02-14', '["RULE-002","RULE-003"]'::jsonb),
      ('v://cn.zhongbei/genesis/qual/municipal_a', 'QUAL_MUNICIPAL_INDUSTRY_A', '市政行业（排水工程、城镇燃气工程、道路工程、桥梁工程）专业甲级', 'A161003712-1/1', '排水工程, 城镇燃气工程, 道路工程, 桥梁工程', 'https://jzsc.mohurd.gov.cn/', '甲级', '2030-02-14', '["RULE-002","RULE-003"]'::jsonb),
      ('v://cn.zhongbei/genesis/qual/arch_a',      'QUAL_ARCH_COMPREHENSIVE_A', '建筑行业（建筑工程）甲级',                               'A161003712-1/1', '建筑工程, 建筑装饰工程, 建筑幕墙工程, 轻型钢结构工程, 建筑智能化系统, 照明工程, 消防设施工程', 'https://jzsc.mohurd.gov.cn/', '甲级', '2030-02-14', '["RULE-002","RULE-003"]'::jsonb),
      ('v://cn.zhongbei/genesis/qual/landscape_a', 'QUAL_LANDSCAPE_SPECIAL_A',  '风景园林工程设计专项甲级',                                  'A161003712-1/1', '风景园林工程设计', 'https://jzsc.mohurd.gov.cn/', '甲级', '2030-02-14', '["RULE-002","RULE-003"]'::jsonb),
      ('v://cn.zhongbei/genesis/qual/water_b',     'QUAL_WATER_INDUSTRY_B',     '水利行业乙级',                                            'A161003712-1/1', '水利工程', 'https://jzsc.mohurd.gov.cn/', '乙级', '2030-02-14', '["RULE-002","RULE-003"]'::jsonb)
) AS x(ref, resource_type, name, cert_no, scope, verify_url, grade, valid_until, rule_binding)
ON CONFLICT (ref) DO UPDATE SET
    resource_type = EXCLUDED.resource_type,
    name = EXCLUDED.name,
    constraints = EXCLUDED.constraints,
    status = 'ACTIVE',
    updated_at = NOW();

-- Backfill company-level qualification rows (minimal, tied to root company).
WITH root_company AS (
    SELECT c.id, c.name, c.executor_ref, c.license_num
    FROM companies c
    WHERE c.tenant_id = :tenant_id::int
      AND c.deleted = FALSE
      AND c.company_type = 1
    ORDER BY c.id
    LIMIT 1
)
INSERT INTO qualifications (
    holder_type, holder_id, holder_name, executor_ref,
    qual_type, cert_no, issued_by, issued_at, valid_from, valid_until,
    status, specialty, level, scope, attachment_url, note,
    deleted, tenant_id, created_at, updated_at
)
SELECT
    'COMPANY',
    rc.id,
    rc.name,
    rc.executor_ref,
    q.qual_type,
    q.cert_no,
    '住房和城乡建设部',
    NOW(),
    NOW(),
    NOW() + INTERVAL '4 year',
    'VALID',
    q.specialty,
    q.level,
    q.scope,
    '',
    'cn.zhongbei company qualification baseline',
    FALSE,
    :tenant_id::int,
    NOW(),
    NOW()
FROM root_company rc
CROSS JOIN (
    VALUES
      ('QUAL_HIGHWAY_INDUSTRY_A',   'COMPANY-CN.ZHONGBEI-HIGHWAY-A',   '公路行业',   '甲级', '公路, 特大桥梁, 特长隧道, 交通工程'),
      ('QUAL_MUNICIPAL_INDUSTRY_A', 'COMPANY-CN.ZHONGBEI-MUNICIPAL-A', '市政行业',   '甲级', '排水工程, 城镇燃气工程, 道路工程, 桥梁工程'),
      ('QUAL_ARCH_COMPREHENSIVE_A', 'COMPANY-CN.ZHONGBEI-ARCH-A',      '建筑行业',   '甲级', '建筑工程及相关专项'),
      ('QUAL_LANDSCAPE_SPECIAL_A',  'COMPANY-CN.ZHONGBEI-LANDSCAPE-A', '风景园林',   '甲级', '风景园林工程设计'),
      ('QUAL_WATER_INDUSTRY_B',     'COMPANY-CN.ZHONGBEI-WATER-B',     '水利行业',   '乙级', '水利工程')
) AS q(qual_type, cert_no, specialty, level, scope)
ON CONFLICT (cert_no) DO UPDATE SET
    holder_type = EXCLUDED.holder_type,
    holder_id = EXCLUDED.holder_id,
    holder_name = EXCLUDED.holder_name,
    executor_ref = EXCLUDED.executor_ref,
    qual_type = EXCLUDED.qual_type,
    issued_by = EXCLUDED.issued_by,
    issued_at = EXCLUDED.issued_at,
    valid_from = EXCLUDED.valid_from,
    valid_until = EXCLUDED.valid_until,
    status = EXCLUDED.status,
    specialty = EXCLUDED.specialty,
    level = EXCLUDED.level,
    scope = EXCLUDED.scope,
    note = EXCLUDED.note,
    deleted = FALSE,
    updated_at = NOW();

-- Backfill historical achievement UTXO from real contract financial progress.
WITH contract_base AS (
    SELECT
        c.id,
        c.tenant_id,
        c.project_ref,
        c.ref AS contract_ref,
        COALESCE(NULLIF(c.contract_name, ''), NULLIF(c.num, ''), 'legacy-contract-' || c.id::text) AS project_name,
        COALESCE(NULLIF(c.contract_type, ''), 'LEGACY') AS project_type,
        COALESCE(c.contract_balance, 0)::numeric(18,2) AS contract_amount,
        COALESCE(c.totle_gathering, 0)::numeric(18,2) AS settled_amount,
        COALESCE(e.executor_ref, co.executor_ref, 'v://cn.zhongbei/executor/org/unknown@v1') AS executor_ref,
        COALESCE(c.signing_time, c.contract_date, c.created_at, c.updated_at, NOW()) AS event_time,
        co.name AS owner_name,
        co.address AS region
    FROM contracts c
    LEFT JOIN companies co ON co.id = c.company_id
    LEFT JOIN employees e ON e.id = c.employee_id
    WHERE c.tenant_id = :tenant_id::int
      AND c.deleted = FALSE
      AND COALESCE(c.project_ref, '') <> ''
      AND COALESCE(c.totle_gathering, 0) > 0
),
rows_to_upsert AS (
    SELECT
        cb.*,
        ('v://cn.zhongbei/utxo/achievement/legacy-contract/' || cb.id::text || '@v1')::varchar(500) AS utxo_ref,
        ('v://cn.zhongbei/experience/contract/' || cb.id::text || '@v1')::varchar(500) AS experience_ref,
        ('v://cn.zhongbei/utxo/achievement/legacy-contract/' || cb.id::text || '@v1')::text AS ref,
        ('sha256:' || encode(digest(
            cb.project_ref || '|' ||
            cb.executor_ref || '|' ||
            cb.id::text || '|' ||
            cb.settled_amount::text || '|' ||
            cb.event_time::text
        , 'sha256'), 'hex'))::text AS proof_hash,
        ('sha256:' || encode(digest(
            cb.project_ref || '|' ||
            cb.contract_ref || '|' ||
            cb.executor_ref || '|' ||
            cb.id::text
        , 'sha256'), 'hex'))::text AS inputs_hash
    FROM contract_base cb
)
INSERT INTO achievement_utxos (
    utxo_ref, spu_ref, project_ref, executor_ref, genesis_ref, contract_id, payload,
    proof_hash, status, source, experience_ref, tenant_id, ingested_at, settled_at,
    ref, namespace_ref, project_name, project_type, owner_name, region, scale,
    contract_amount, completed_year, completed_at, qual_ref, attachments, inputs_hash,
    contract_genesis_ref, tender_genesis_ref, step_count, review_ref, settled_amount,
    layer, created_at, updated_at
)
SELECT
    r.utxo_ref,
    'v://cn.zhongbei/spu/legacy/contract_settlement@v1',
    r.project_ref,
    r.executor_ref,
    NULL,
    r.id,
    jsonb_build_object(
        'legacy_contract_id', r.id,
        'source', 'contracts',
        'contract_amount', r.contract_amount,
        'settled_amount', r.settled_amount
    ),
    r.proof_hash,
    'SETTLED',
    'HISTORICAL_IMPORT',
    r.experience_ref,
    r.tenant_id,
    r.event_time,
    r.event_time,
    r.ref,
    'v://cn.zhongbei',
    r.project_name,
    r.project_type,
    r.owner_name,
    r.region,
    '',
    r.contract_amount,
    EXTRACT(YEAR FROM r.event_time)::int,
    r.event_time::date,
    NULL,
    '[]'::jsonb,
    r.inputs_hash,
    NULL,
    NULL,
    0,
    NULL,
    r.settled_amount,
    4,
    NOW(),
    NOW()
FROM rows_to_upsert r
ON CONFLICT (utxo_ref) DO UPDATE SET
    spu_ref = EXCLUDED.spu_ref,
    project_ref = EXCLUDED.project_ref,
    executor_ref = EXCLUDED.executor_ref,
    contract_id = EXCLUDED.contract_id,
    payload = EXCLUDED.payload,
    proof_hash = EXCLUDED.proof_hash,
    status = EXCLUDED.status,
    source = EXCLUDED.source,
    experience_ref = EXCLUDED.experience_ref,
    ingested_at = EXCLUDED.ingested_at,
    settled_at = EXCLUDED.settled_at,
    ref = EXCLUDED.ref,
    namespace_ref = EXCLUDED.namespace_ref,
    project_name = EXCLUDED.project_name,
    project_type = EXCLUDED.project_type,
    owner_name = EXCLUDED.owner_name,
    region = EXCLUDED.region,
    contract_amount = EXCLUDED.contract_amount,
    completed_year = EXCLUDED.completed_year,
    completed_at = EXCLUDED.completed_at,
    attachments = EXCLUDED.attachments,
    inputs_hash = EXCLUDED.inputs_hash,
    settled_amount = EXCLUDED.settled_amount,
    layer = EXCLUDED.layer,
    updated_at = NOW();

SELECT 'genesis_qual_count' AS metric,
       COUNT(*)::bigint AS cnt
FROM genesis_utxos
WHERE tenant_id = :tenant_id::int
  AND status = 'ACTIVE'
  AND ref LIKE 'v://cn.zhongbei/genesis/qual/%';

SELECT 'achievement_utxo_count' AS metric,
       COUNT(*)::bigint AS cnt
FROM achievement_utxos
WHERE tenant_id = :tenant_id::int
  AND namespace_ref = 'v://cn.zhongbei';

COMMIT;
