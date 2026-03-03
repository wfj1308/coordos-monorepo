-- ============================================================
--  CoordOS — 容器公理 DDL
--
--  建模公理：
--    公理零：资源是起点（Resource is Origin）
--    公理一：容器是能力的原子单元（Container is Capability Atom）
--    公理二：Executor 是容器的治理入口（Executor is Container Governor）
--    公理三：Trip 声明能力需求，不绑定执行体（Trip declares, not assigns）
--    公理四：产出是状态转化的证明（UTXO is State Transition Proof）
--    公理五：Receipt 是执行过程的锁定（Receipt locks the execution）
--
--  三张新表：
--    containers          能力原子单元
--    container_locks     Trip 占用容器的锁
--    execution_receipts  执行过程存证
--
--  两张关联表：
--    executor_containers  Executor 持有的容器集合
--    container_skills     容器提供的技能动作
-- ============================================================


-- ── 容器表（能力原子单元）────────────────────────────────────
CREATE TABLE IF NOT EXISTS containers (
    id              bigserial    PRIMARY KEY,

    -- 身份
    ref             text         NOT NULL UNIQUE,
    -- 格式：v://{ns}/container/{kind}/{name}@{version}
    -- 物理容器：v://zhongbei/container/cert/reg-structure/cyp4310@v1
    -- 逻辑容器：v://zhongbei/container/ai/calc-engine@v1
    -- 设备容器：v://acme/container/mixer/01@v1

    namespace_ref   text         NOT NULL,
    name            text         NOT NULL,

    -- 容器分类
    kind            text         NOT NULL DEFAULT 'PHYSICAL',
    CHECK (kind IN (
        'PHYSICAL',   -- 物理容器（人/设备/车/仓）
        'LOGICAL',    -- 逻辑容器（AI/软件/规则引擎）
        'CERT'        -- 资质容器（注册证书）
    )),

    -- 能力声明
    cap_tags        text[]       NOT NULL DEFAULT '{}',
    -- 例：["mixing","concrete"] / ["structural_design","review"]

    skills          text[]       NOT NULL DEFAULT '{}',
    -- 例：["mix_concrete","control_slump"] / ["sign_structure_drawing"]

    cap_level       numeric(4,2) NOT NULL DEFAULT 3.0,
    -- 0.0~10.0，与 executor_stats.capability_level 对齐

    -- 产能约束
    max_parallel    int          NOT NULL DEFAULT 1,
    -- 同时最多被几个 Trip 占用
    -- 注册工程师证书：max_parallel = max_concurrent_projects
    -- 拌合机：max_parallel = 1
    -- AI 计算引擎：max_parallel = 999

    throughput_unit text,
    throughput_value numeric(12,4),
    -- 例：unit="m3/h" value=30（拌合机每小时30方）

    -- 能耗模型
    energy_unit     text,
    -- kwh / token / compute_second / yuan
    energy_baseline numeric(12,4) DEFAULT 0,
    -- 固定基础成本（每次启动）
    energy_coeffs   jsonb,
    -- 变动成本系数：{"m3": 0.2} / {"spu": 1000} / {"sheet": 30}
    -- 实际能耗 = baseline + Σ(coeff_key × input_value)

    -- 归属（容器属于哪个 Executor）
    -- 注：一个容器可被多个 Executor 持有（共享设备）
    -- 通过 executor_containers 关联表管理

    -- 状态
    status          text         NOT NULL DEFAULT 'ACTIVE',
    CHECK (status IN (
        'ACTIVE',       -- 可用
        'OCCUPIED',     -- 占用中（max_parallel已满）
        'MAINTENANCE',  -- 维护中
        'RETIRED'       -- 退役
    )),

    -- 关联实体（可选，用于追溯）
    linked_qual_ref     text,   -- 关联的 qualification.cert_no（CERT类容器）
    linked_executor_ref text,   -- 主归属执行体
    linked_genesis_ref  text,   -- 关联的 genesis_utxo（设备资产）

    -- 元数据
    spec_json       jsonb,
    -- 存放容器规格的完整描述，不影响约束验证

    proof_hash      text,
    -- 容器初始状态的 proof_hash，防止规格篡改

    tenant_id       int          NOT NULL DEFAULT 10000,
    created_at      timestamptz  NOT NULL DEFAULT NOW(),
    updated_at      timestamptz  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_container_ns
    ON containers (namespace_ref, status);
CREATE INDEX IF NOT EXISTS idx_container_tags
    ON containers USING GIN (cap_tags);
CREATE INDEX IF NOT EXISTS idx_container_skills
    ON containers USING GIN (skills);
CREATE INDEX IF NOT EXISTS idx_container_kind
    ON containers (kind, cap_level DESC);


-- ── Executor 持有的容器集合 ───────────────────────────────────
CREATE TABLE IF NOT EXISTS executor_containers (
    id              bigserial    PRIMARY KEY,
    executor_ref    text         NOT NULL,
    container_ref   text         NOT NULL REFERENCES containers(ref),

    -- 持有方式
    hold_type       text         NOT NULL DEFAULT 'OWN',
    CHECK (hold_type IN (
        'OWN',    -- 自有（拌合站自己的拌合机）
        'LEASE',  -- 租赁（租来的设备）
        'SHARE'   -- 共享（多个 Executor 共用）
    )),

    -- 调度策略（Executor 级别）
    select_strategy text DEFAULT 'cap_match+min_energy',
    lock_scope      text DEFAULT 'operation_id',

    valid_from      timestamptz  DEFAULT NOW(),
    valid_until     timestamptz,

    tenant_id       int          NOT NULL DEFAULT 10000,
    created_at      timestamptz  NOT NULL DEFAULT NOW(),

    UNIQUE (executor_ref, container_ref)
);

CREATE INDEX IF NOT EXISTS idx_exec_containers_executor
    ON executor_containers (executor_ref);
CREATE INDEX IF NOT EXISTS idx_exec_containers_container
    ON executor_containers (container_ref);


-- ── 容器锁（Trip 占用容器）───────────────────────────────────
-- 核心并发控制：同一容器 max_parallel 限制
CREATE TABLE IF NOT EXISTS container_locks (
    id              bigserial    PRIMARY KEY,

    container_ref   text         NOT NULL,
    -- 谁在占用
    locked_by       text         NOT NULL,
    -- Trip ref / operation_id / utxo_ref

    lock_scope      text         NOT NULL DEFAULT 'operation_id',
    -- 锁的粒度：operation_id / trip_id / project_ref

    -- 锁状态
    status          text         NOT NULL DEFAULT 'LOCKED',
    CHECK (status IN ('LOCKED', 'RELEASED', 'EXPIRED')),

    locked_at       timestamptz  NOT NULL DEFAULT NOW(),
    expected_release timestamptz,
    released_at     timestamptz,

    -- 能耗记录（锁释放时填入）
    energy_used     numeric(12,4),
    energy_unit     text,

    tenant_id       int          NOT NULL DEFAULT 10000
);

CREATE INDEX IF NOT EXISTS idx_locks_container
    ON container_locks (container_ref, status);
CREATE INDEX IF NOT EXISTS idx_locks_locked_by
    ON container_locks (locked_by, status);

-- 容器当前占用数视图
CREATE OR REPLACE VIEW container_occupancy AS
SELECT
    c.ref           AS container_ref,
    c.name,
    c.max_parallel,
    COUNT(l.id)     AS current_locks,
    c.max_parallel - COUNT(l.id) AS available_slots,
    (COUNT(l.id) >= c.max_parallel) AS is_full,
    c.status        AS container_status
FROM containers c
LEFT JOIN container_locks l
    ON l.container_ref = c.ref
    AND l.status = 'LOCKED'
GROUP BY c.ref, c.name, c.max_parallel, c.status;


-- ── 执行存证（Receipt）────────────────────────────────────────
-- 公理五：Receipt 是执行过程的锁定
-- 每次 Trip Step 执行完成产出一个 receipt

CREATE TABLE IF NOT EXISTS execution_receipts (
    id              bigserial    PRIMARY KEY,

    receipt_ref     text         NOT NULL UNIQUE,
    -- 格式：v://{ns}/receipt/{domain}/{year}/{seq}
    -- 例：v://zhongbei/receipt/mix/2026/0001

    -- 执行上下文
    trip_ref        text,        -- 所属 Trip
    step_name       text,        -- Trip Step 名称（如"拌合"）
    operation_id    text,        -- 操作ID（跨步骤追踪）

    -- 执行者
    container_ref   text         NOT NULL,  -- 实际执行的容器
    executor_ref    text         NOT NULL,  -- 容器所属的 Executor
    operator_ref    text,                   -- 人工操作员（可选）

    -- 输入输出
    input_refs      text[]       NOT NULL DEFAULT '{}',
    -- 输入资源的 ref 列表

    inputs_hash     text         NOT NULL,
    -- sha256(所有输入资源内容的哈希串联)
    -- 防止输入资源事后被篡改

    output_ref      text,
    -- 产出的 UTXO ref

    -- 能耗记录
    energy_unit     text,
    energy_used     numeric(12,4),
    energy_cost     numeric(12,4),
    -- energy_cost = 按容器 energy_model 计算的金额

    -- 质量指标（领域相关，存 JSON）
    quality_metrics jsonb,
    -- 混凝土：{"slump": 180, "temperature": 24, "air_content": 4.2}
    -- 施工图：{"review_rounds": 1, "issue_count": 3}
    -- 计算：{"calc_method": "JTG", "safety_factor": 1.35}

    -- 存证
    proof_hash      text         NOT NULL,
    -- sha256(container_ref + inputs_hash + output_ref + energy_used + executed_at)

    status          text         NOT NULL DEFAULT 'CONFIRMED',
    CHECK (status IN ('CONFIRMED', 'DISPUTED', 'VOIDED')),

    executed_at     timestamptz  NOT NULL DEFAULT NOW(),
    tenant_id       int          NOT NULL DEFAULT 10000
);

CREATE INDEX IF NOT EXISTS idx_receipt_container
    ON execution_receipts (container_ref, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_receipt_executor
    ON execution_receipts (executor_ref, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_receipt_trip
    ON execution_receipts (trip_ref, step_name);
CREATE INDEX IF NOT EXISTS idx_receipt_output
    ON execution_receipts (output_ref);


-- ── 容器能力匹配函数 ──────────────────────────────────────────
-- Trip Step 调用：给定 cap_tags + skills + min_level，返回可用容器

CREATE OR REPLACE FUNCTION find_available_containers(
    p_namespace_ref  text,
    p_cap_tags       text[],
    p_skills         text[]    DEFAULT NULL,
    p_min_level      numeric   DEFAULT 0.0,
    p_limit          int       DEFAULT 10
)
RETURNS TABLE (
    container_ref   text,
    container_name  text,
    executor_ref    text,
    cap_level       numeric,
    available_slots int,
    energy_unit     text,
    energy_baseline numeric,
    energy_coeffs   jsonb
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.ref,
        c.name,
        ec.executor_ref,
        c.cap_level,
        co.available_slots::int,
        c.energy_unit,
        c.energy_baseline,
        c.energy_coeffs
    FROM containers c
    JOIN executor_containers ec ON ec.container_ref = c.ref
    JOIN container_occupancy co ON co.container_ref = c.ref
    WHERE c.namespace_ref = p_namespace_ref
      AND c.status = 'ACTIVE'
      AND co.available_slots > 0
      AND c.cap_level >= p_min_level
      AND c.cap_tags @> p_cap_tags           -- 包含所有要求的 cap_tags
      AND (p_skills IS NULL OR c.skills @> p_skills)
      AND (ec.valid_until IS NULL OR ec.valid_until > NOW())
    ORDER BY
        c.cap_level DESC,
        co.available_slots DESC,
        c.energy_baseline ASC               -- 优先选能耗低的
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;


-- ── 能耗计算函数 ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION calc_energy_cost(
    p_container_ref  text,
    p_inputs         jsonb    -- {"m3": 30, "spu": 1}
)
RETURNS TABLE (
    energy_used  numeric,
    energy_unit  text,
    energy_cost  numeric
) AS $$
DECLARE
    v_baseline  numeric;
    v_coeffs    jsonb;
    v_unit      text;
    v_cost      numeric := 0;
    v_key       text;
    v_val       numeric;
BEGIN
    SELECT c.energy_baseline, c.energy_coeffs, c.energy_unit
    INTO v_baseline, v_coeffs, v_unit
    FROM containers c WHERE c.ref = p_container_ref;

    v_cost := COALESCE(v_baseline, 0);

    -- 遍历系数，乘以对应输入量
    IF v_coeffs IS NOT NULL AND p_inputs IS NOT NULL THEN
        FOR v_key IN SELECT jsonb_object_keys(v_coeffs) LOOP
            v_val := (p_inputs->>v_key)::numeric;
            IF v_val IS NOT NULL THEN
                v_cost := v_cost + (v_coeffs->>v_key)::numeric * v_val;
            END IF;
        END LOOP;
    END IF;

    RETURN QUERY SELECT v_cost, v_unit, v_cost;
END;
$$ LANGUAGE plpgsql;


-- ── 种子数据：中北工程的容器注册 ─────────────────────────────
-- 把注册工程师的资质转化为 CERT 类容器

INSERT INTO containers (
    ref, namespace_ref, name, kind,
    cap_tags, skills, cap_level, max_parallel,
    energy_unit, energy_baseline,
    linked_qual_ref, linked_executor_ref,
    proof_hash, tenant_id
) VALUES

-- 一级注册结构工程师容器
('v://zhongbei/container/cert/reg-structure/cyp4310@v1',
 'v://zhongbei', '陈勇攀·一级注册结构工程师', 'CERT',
 ARRAY['structural_design','review','sign_drawing'],
 ARRAY['sign_structure_drawing','seal_review_cert','approve_calc'],
 4.0, 3,
 'review_count', 0,
 '6100371-S018', 'v://zhongbei/executor/person/cyp4310',
 'sha256:cert_cyp4310', 10000),

('v://zhongbei/container/cert/reg-structure/dyc4019@v1',
 'v://zhongbei', '戴永常·一级注册结构工程师', 'CERT',
 ARRAY['structural_design','review','sign_drawing'],
 ARRAY['sign_structure_drawing','seal_review_cert','approve_calc'],
 4.0, 3,
 'review_count', 0,
 '6100371-S021', 'v://zhongbei/executor/person/dyc4019',
 'sha256:cert_dyc4019', 10000),

('v://zhongbei/container/cert/reg-structure/lz0012@v1',
 'v://zhongbei', '李准·一级注册结构工程师', 'CERT',
 ARRAY['structural_design','review','sign_drawing'],
 ARRAY['sign_structure_drawing','seal_review_cert','approve_calc'],
 4.0, 3,
 'review_count', 0,
 '6100371-S015', 'v://zhongbei/executor/person/lz0012',
 'sha256:cert_lz0012', 10000),

-- 岩土工程师容器（李准同时持有）
('v://zhongbei/container/cert/geotec/lz0012@v1',
 'v://zhongbei', '李准·注册土木工程师（岩土）', 'CERT',
 ARRAY['geotechnical','survey','site_investigation'],
 ARRAY['sign_geotech_report','approve_foundation'],
 3.5, 3,
 'review_count', 0,
 '6100371-AY009', 'v://zhongbei/executor/person/lz0012',
 'sha256:cert_geotec_lz0012', 10000),

-- AI 计算引擎容器（逻辑容器）
('v://zhongbei/container/ai/bridge-calc@v1',
 'v://zhongbei', '桥梁结构计算引擎', 'LOGICAL',
 ARRAY['structural_calc','bridge_design','pile_design'],
 ARRAY['bearing_capacity','rebar_check','section_verify','load_calc'],
 3.8, 999,
 'token', 0,
 NULL, NULL,
 'sha256:ai_bridge_calc_v1', 10000),

-- AI 出图引擎容器（逻辑容器）
('v://zhongbei/container/ai/drawing-engine@v1',
 'v://zhongbei', 'CAD出图引擎', 'LOGICAL',
 ARRAY['cad_generation','drawing_output'],
 ARRAY['dwg_output','pdf_export','detail_drawing'],
 3.5, 999,
 'compute_second', 0,
 NULL, NULL,
 'sha256:ai_drawing_v1', 10000)

ON CONFLICT (ref) DO UPDATE SET
    cap_level = EXCLUDED.cap_level,
    status    = EXCLUDED.status;


-- ── 种子数据：Executor 持有容器关系 ──────────────────────────

INSERT INTO executor_containers
  (executor_ref, container_ref, hold_type, tenant_id)
VALUES
-- 陈勇攀持有结构工程师容器
('v://zhongbei/executor/person/cyp4310',
 'v://zhongbei/container/cert/reg-structure/cyp4310@v1',
 'OWN', 10000),

-- 戴永常持有结构工程师容器
('v://zhongbei/executor/person/dyc4019',
 'v://zhongbei/container/cert/reg-structure/dyc4019@v1',
 'OWN', 10000),

-- 李准持有两个容器（一人多证）
('v://zhongbei/executor/person/lz0012',
 'v://zhongbei/container/cert/reg-structure/lz0012@v1',
 'OWN', 10000),
('v://zhongbei/executor/person/lz0012',
 'v://zhongbei/container/cert/geotec/lz0012@v1',
 'OWN', 10000),

-- AI 执行体持有计算和出图容器
('v://zhongbei/executor/ai/bridge-design-v1',
 'v://zhongbei/container/ai/bridge-calc@v1',
 'OWN', 10000),
('v://zhongbei/executor/ai/bridge-design-v1',
 'v://zhongbei/container/ai/drawing-engine@v1',
 'OWN', 10000)

ON CONFLICT (executor_ref, container_ref) DO NOTHING;


-- ── 验证：容器能力匹配测试 ────────────────────────────────────
-- 查：谁能签发结构施工图？
SELECT
    container_ref,
    container_name,
    executor_ref,
    cap_level,
    available_slots
FROM find_available_containers(
    'v://zhongbei',
    ARRAY['structural_design'],
    ARRAY['sign_structure_drawing'],
    3.0
);

-- 查：容器占用状态
SELECT
    container_ref,
    name,
    max_parallel,
    current_locks,
    available_slots,
    is_full
FROM container_occupancy
WHERE container_ref LIKE 'v://zhongbei%'
ORDER BY container_ref;
