-- ============================================================
--  CoordOS Container Doctrine v1.0
--  立宪级 Genesis UTXO
--
--  这不是文档，是协议层对象。
--  status: ACTIVE（genesis_utxos 表标准状态）
--  不可变性由触发器强制保证
--  升级须创建 v://coordos/spec/container-doctrine@v2
-- ============================================================

-- ── 宪法命名空间（全网公共，tenant_id=0）────────────────────

INSERT INTO namespaces (
    ref, name, short_code, org_type,
    parent_ref, depth, path,
    inherited_rules, owned_genesis, accessible_genesis,
    manage_fee_rate, route_policy, status, tenant_id
) VALUES (
    'v://coordos',
    'CoordOS Protocol Foundation',
    'coordos',
    'HEAD_OFFICE',
    NULL, 0, 'v://coordos',
    '{}',
    ARRAY['v://coordos/spec/container-doctrine@v1'],
    ARRAY['v://coordos/spec/container-doctrine@v1'],
    0,
    '{"policy": "public_readonly"}'::jsonb,
    'ACTIVE',
    0
) ON CONFLICT (ref) DO NOTHING;


-- ── Container Doctrine v1 Genesis UTXO ──────────────────────

DO $$
DECLARE
    v_content_hash text;
    v_genesis_hash  text;
    v_constraint    jsonb;
BEGIN
    -- 宪法内容哈希（用公理+类型+规则的规范字符串计算）
    v_content_hash := encode(sha256(
        'CoordOS Container Doctrine v1.0|' ||
        'AXIOM0:Resource is Origin|' ||
        'AXIOM1:Container is Capability Atom|' ||
        'AXIOM2:Executor is Container Governor|' ||
        'AXIOM3:Trip Declares Capability Not Executor|' ||
        'AXIOM4:UTXO is State Transition Proof|' ||
        'AXIOM5:Receipt Locks Execution|' ||
        'AXIOM6:Capability State is Derived Not Declared|' ||
        'TYPES:VOLUME,ENERGY,SCHEDULER,IO,TRANSPORT,LOGIC,CERT|' ||
        'RATIFIED:2026-03-01'
    ::bytea), 'hex');

    v_constraint := jsonb_build_object(
        'title',   'CoordOS Container Doctrine v1.0',
        'version', '1.0.0',

        'principles', jsonb_build_array(
            'Resource is Origin',
            'Container is Capability Atom',
            'Executor is Container Governor',
            'Trip Declares Capability, Not Executor',
            'UTXO is State Transition Proof',
            'Receipt Locks Execution',
            'Capability State is Derived, Not Declared'
        ),

        'container_types', jsonb_build_array(
            'VOLUME', 'ENERGY', 'SCHEDULER',
            'IO', 'TRANSPORT', 'LOGIC', 'CERT'
        ),

        'receipt_required_fields', jsonb_build_array(
            'container_ref', 'executor_ref',
            'inputs_hash', 'energy_used', 'proof_hash'
        ),

        'invariants', jsonb_build_array(
            'container.kind IN container_types',
            'receipt.container_ref IS NOT NULL',
            'executor.capabilities DERIVED FROM containers'
        ),

        'versioning', jsonb_build_object(
            'this_version',      '1.0.0',
            'next_version_ref',  'v://coordos/spec/container-doctrine@v2',
            'upgrade_requires',  jsonb_build_array(
                'explicit_compat_declaration',
                'change_log',
                'no_breaking_change_to_existing_utxo'
            )
        ),

        'scope',        'PUBLIC_GLOBAL',
        'content_hash', 'sha256:' || v_content_hash,
        'ratified_at',  '2026-03-01T00:00:00Z',
        'ratified_by',  'coordos-protocol-foundation',
        'immutable',    true
    );

    -- proof_hash = sha256(ref || content_hash || ratified_at)
    v_genesis_hash := encode(sha256(
        ('v://coordos/spec/container-doctrine@v1' ||
         v_content_hash ||
         '2026-03-01T00:00:00Z')::bytea
    ), 'hex');

    INSERT INTO genesis_utxos (
        ref, resource_type, name,
        total_amount, available_amount, unit,
        constraint_json, status, proof_hash,
        tenant_id, created_at
    ) VALUES (
        'v://coordos/spec/container-doctrine@v1',
        'GENESIS_SPEC',
        'CoordOS Container Doctrine v1.0',
        1, 1, 'spec',
        v_constraint,
        'ACTIVE',
        'sha256:' || v_genesis_hash,
        0,
        '2026-03-01T00:00:00Z'::timestamptz
    ) ON CONFLICT (ref) DO NOTHING;

    RAISE NOTICE '== Container Doctrine ratified ==';
    RAISE NOTICE 'ref:          v://coordos/spec/container-doctrine@v1';
    RAISE NOTICE 'content_hash: sha256:%', v_content_hash;
    RAISE NOTICE 'proof_hash:   sha256:%', v_genesis_hash;
    RAISE NOTICE 'status:       IMMUTABLE (enforced by trigger)';
END $$;


-- ── 不可变性触发器 ────────────────────────────────────────────
-- GENESIS_SPEC 类型的 UTXO 一旦写入，任何 UPDATE/DELETE 都被拒绝

CREATE OR REPLACE FUNCTION fn_protect_genesis_spec()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.resource_type = 'GENESIS_SPEC' THEN
        RAISE EXCEPTION
            'IMMUTABLE VIOLATION: [%] is a ratified spec and cannot be modified. '
            'Create a new version at [%] instead.',
            OLD.ref,
            REPLACE(OLD.ref, '@v1', '@v2');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_protect_genesis_spec ON genesis_utxos;
CREATE TRIGGER trg_protect_genesis_spec
    BEFORE UPDATE OR DELETE ON genesis_utxos
    FOR EACH ROW
    EXECUTE FUNCTION fn_protect_genesis_spec();


-- ── Container Resolver 校验函数 ───────────────────────────────
-- 公理一落地：container.kind 必须在七分类内
-- 这个函数在注册容器时调用

CREATE OR REPLACE FUNCTION validate_container_kind(p_kind text)
RETURNS boolean AS $$
DECLARE
    v_valid_kinds text[] := ARRAY[
        'VOLUME', 'ENERGY', 'SCHEDULER',
        'IO', 'TRANSPORT', 'LOGIC', 'CERT'
    ];
BEGIN
    IF p_kind = ANY(v_valid_kinds) THEN
        RETURN TRUE;
    END IF;
    RAISE EXCEPTION
        'DOCTRINE VIOLATION: container.kind [%] not in Container Doctrine v1 types. '
        'Valid types: VOLUME, ENERGY, SCHEDULER, IO, TRANSPORT, LOGIC, CERT. '
        'Doctrine ref: v://coordos/spec/container-doctrine@v1',
        p_kind;
END;
$$ LANGUAGE plpgsql;

-- 注册容器时自动校验 kind
CREATE OR REPLACE FUNCTION fn_validate_container_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM validate_container_kind(NEW.kind);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_container_kind ON containers;
CREATE TRIGGER trg_validate_container_kind
    BEFORE INSERT OR UPDATE ON containers
    FOR EACH ROW
    EXECUTE FUNCTION fn_validate_container_on_insert();


-- ── Receipt 强校验触发器 ──────────────────────────────────────
-- 公理五落地：receipt.container_ref 不能为空

CREATE OR REPLACE FUNCTION fn_validate_receipt()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.container_ref IS NULL OR NEW.container_ref = '' THEN
        RAISE EXCEPTION
            'DOCTRINE VIOLATION: receipt.container_ref is required. '
            'Receipt without container_ref is INVALID per Container Doctrine v1. '
            'Doctrine ref: v://coordos/spec/container-doctrine@v1';
    END IF;

    IF NEW.inputs_hash IS NULL OR NEW.inputs_hash = '' THEN
        RAISE EXCEPTION
            'DOCTRINE VIOLATION: receipt.inputs_hash is required. '
            'Doctrine ref: v://coordos/spec/container-doctrine@v1';
    END IF;

    IF NEW.proof_hash IS NULL OR NEW.proof_hash = '' THEN
        RAISE EXCEPTION
            'DOCTRINE VIOLATION: receipt.proof_hash is required. '
            'Doctrine ref: v://coordos/spec/container-doctrine@v1';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_receipt ON execution_receipts;
CREATE TRIGGER trg_validate_receipt
    BEFORE INSERT ON execution_receipts
    FOR EACH ROW
    EXECUTE FUNCTION fn_validate_receipt();


-- ── 标准词汇表 ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS coordos_std_vocab (
    id            bigserial PRIMARY KEY,
    vocab_type    text NOT NULL,
    value         text NOT NULL,
    label_zh      text,
    label_en      text,
    domain        text DEFAULT 'universal',
    since_version text NOT NULL DEFAULT '1.0.0',
    deprecated    boolean NOT NULL DEFAULT FALSE,
    UNIQUE (vocab_type, value)
);

-- 七类容器（宪法固定）
INSERT INTO coordos_std_vocab
    (vocab_type, value, label_zh, label_en, domain)
VALUES
('container_kind', 'VOLUME',    '体积容器', 'Volume Container',    'universal'),
('container_kind', 'ENERGY',    '能量容器', 'Energy Container',    'universal'),
('container_kind', 'SCHEDULER', '调度容器', 'Scheduler Container', 'universal'),
('container_kind', 'IO',        'IO容器',   'IO Container',        'universal'),
('container_kind', 'TRANSPORT', '运输容器', 'Transport Container', 'universal'),
('container_kind', 'LOGIC',     '逻辑容器', 'Logic Container',     'universal'),
('container_kind', 'CERT',      '资质容器', 'Cert Container',      'universal')
ON CONFLICT DO NOTHING;

-- 通用 cap_tags
INSERT INTO coordos_std_vocab
    (vocab_type, value, label_zh, label_en, domain)
VALUES
('cap_tag', 'loading',           '装载',     'Loading',            'universal'),
('cap_tag', 'constraint',        '约束',     'Constraint',         'universal'),
('cap_tag', 'heating',           '加热',     'Heating',            'universal'),
('cap_tag', 'mixing',            '搅拌',     'Mixing',             'construction'),
('cap_tag', 'pumping',           '泵送',     'Pumping',            'construction'),
('cap_tag', 'state_machine',     '状态机',   'State Machine',      'universal'),
('cap_tag', 'process_control',   '过程控制', 'Process Control',    'universal'),
('cap_tag', 'sensor',            '传感',     'Sensor',             'universal'),
('cap_tag', 'safety_interlock',  '安全联锁', 'Safety Interlock',   'universal'),
('cap_tag', 'road_transport',    '公路运输', 'Road Transport',     'universal'),
('cap_tag', 'structural_calc',   '结构计算', 'Structural Calc',    'engineering'),
('cap_tag', 'bridge_design',     '桥梁设计', 'Bridge Design',      'engineering'),
('cap_tag', 'cad_generation',    'CAD生成',  'CAD Generation',     'engineering'),
('cap_tag', 'structural_design', '结构设计', 'Structural Design',  'engineering'),
('cap_tag', 'review',            '审图',     'Review',             'engineering'),
('cap_tag', 'sign_drawing',      '图纸签发', 'Sign Drawing',       'engineering'),
('cap_tag', 'geotechnical',      '岩土',     'Geotechnical',       'engineering')
ON CONFLICT DO NOTHING;

-- 能耗单位
INSERT INTO coordos_std_vocab
    (vocab_type, value, label_zh, label_en, domain)
VALUES
('energy_unit', 'kwh',            '千瓦时',   'Kilowatt Hour',    'universal'),
('energy_unit', 'token',          'Token',    'Token',            'universal'),
('energy_unit', 'compute_second', '计算秒',   'Compute Second',   'universal'),
('energy_unit', 'L_diesel',       '升柴油',   'Liter Diesel',     'universal'),
('energy_unit', 'review_count',   '审核次数', 'Review Count',     'engineering'),
('energy_unit', 'yuan',           '元',       'Yuan',             'universal')
ON CONFLICT DO NOTHING;


-- ── 验证立宪结果 ─────────────────────────────────────────────

SELECT
    ref,
    name,
    resource_type,
    status,
    LEFT(proof_hash, 24) || '...' AS proof_hash,
    constraint_json->>'ratified_at'     AS ratified_at,
    constraint_json->>'scope'           AS scope,
    jsonb_array_length(
        constraint_json->'container_types') AS type_count,
    jsonb_array_length(
        constraint_json->'principles')   AS axiom_count
FROM genesis_utxos
WHERE resource_type = 'GENESIS_SPEC';

-- 验证不可变性（尝试修改会报错）
-- UPDATE genesis_utxos SET name='test' WHERE resource_type='GENESIS_SPEC';
-- 预期：ERROR: IMMUTABLE VIOLATION

SELECT vocab_type, COUNT(*) AS count
FROM coordos_std_vocab
GROUP BY vocab_type ORDER BY vocab_type;
