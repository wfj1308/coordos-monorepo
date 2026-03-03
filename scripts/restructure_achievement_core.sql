-- ============================================================
--  CoordOS 业绩核心重构
--  以业绩为核心输出，重新定义四层结构
--
--  Layer 0  协议层   宪法 Genesis（不变）
--  Layer 1  能力层   资质 Genesis（对齐字段）
--  Layer 2  交易层   招标 Genesis + 合同 Genesis（新增 TENDER）
--  Layer 3  执行层   step_achievement_utxos（新建）
--  Layer 4  业绩层   achievement_utxos（改为触发器自动产出）
--
--  执行顺序：
--    1. genesis_utxos 增加字段和约束
--    2. 新建 step_achievement_utxos
--    3. 升级 fn_bid_awarded（新增 Tender Genesis 父子关系）
--    4. 升级 fn_project_settled（自动产出 Achievement UTXO）
--    5. 新建辅助视图
-- ============================================================

-- ── Step 1: genesis_utxos 语义层级字段 ───────────────────────
-- 增加 layer 字段，明确每条记录属于哪一层
-- 不破坏现有数据，用 DEFAULT 安全加列

ALTER TABLE genesis_utxos
    ADD COLUMN IF NOT EXISTS layer         INT  NOT NULL DEFAULT 2,
    -- 0=协议 1=能力 2=交易 3=执行（step_achievement单独表）4=业绩

    ADD COLUMN IF NOT EXISTS parent_ref    TEXT,
    -- 父级 Genesis UTXO ref（用于招标→合同的裂变关系）

    ADD COLUMN IF NOT EXISTS proof_hash    TEXT,
    -- sha256(ref + resource_type + constraint_json + created_at)

    ADD COLUMN IF NOT EXISTS source        TEXT NOT NULL DEFAULT 'SYSTEM';
    -- REGISTRATION / BID_AWARD / MANUAL / SYSTEM

-- 现有数据补 layer
UPDATE genesis_utxos SET layer = 0 WHERE resource_type = 'GENESIS_SPEC';
UPDATE genesis_utxos SET layer = 1 WHERE resource_type LIKE 'QUAL_%';
UPDATE genesis_utxos SET layer = 2 WHERE resource_type IN ('CONTRACT_FUND', 'TENDER');

-- resource_type 枚举收敛（注释形式，约束留给应用层）
-- Layer 0: GENESIS_SPEC
-- Layer 1: QUAL_HIGHWAY_A / QUAL_MUNICIPAL_A / QUAL_ARCH_A / QUAL_LANDSCAPE_A / QUAL_WATER_B
-- Layer 2: TENDER / CONTRACT_FUND
-- （REG_* 证书在 containers 表，不在 genesis_utxos）

-- ── Step 2: 招标 Genesis 支持（Tender Genesis UTXO）────────────
-- bid_documents 增加 tender_genesis_ref，关联招标源头
ALTER TABLE bid_documents
    ADD COLUMN IF NOT EXISTS tender_genesis_ref TEXT,
    -- v://cn.zhongbei/genesis/tender/{year}/{seq}
    ADD COLUMN IF NOT EXISTS rejected_at        TIMESTAMPTZ;

-- ── Step 3: 新建 step_achievement_utxos（Layer 3 执行层）────────
-- Trip 每个步骤完成后产出一条
-- 这是 Achievement UTXO 的原材料

CREATE TABLE IF NOT EXISTS step_achievement_utxos (
    id              BIGSERIAL PRIMARY KEY,

    -- 协议地址
    ref             TEXT NOT NULL UNIQUE,
    -- 格式：v://{ns}/utxo/step/{project_slug}/{step_seq}
    -- 示例：v://cn.zhongbei/utxo/step/highway-2023-0012/003

    namespace_ref   TEXT NOT NULL,
    project_ref     TEXT NOT NULL,
    -- 关联项目树节点

    -- Trip 上下文
    spu_ref         TEXT,
    trip_ref        TEXT,
    step_seq        INT  NOT NULL DEFAULT 0,
    -- 在项目中的顺序号，用于聚合时保序

    -- 执行体（谁做的）
    executor_ref    TEXT NOT NULL,
    -- v://cn.zhongbei/executor/person/cyp4310@v1

    -- Container Doctrine LAW-3：必须携带 container_ref
    container_ref   TEXT NOT NULL,
    -- v://cn.zhongbei/container/cert/reg-structure/cyp4310@v1

    -- 输入（消耗了什么）
    input_refs      TEXT[] DEFAULT '{}',
    -- 前置 step_achievement 或其他 UTXO 的 ref 列表

    -- 产出描述
    output_type     TEXT NOT NULL DEFAULT 'DESIGN_DOC',
    -- DESIGN_DOC / CALC_REPORT / REVIEW_CERT / SURVEY_REPORT / OTHER
    output_name     TEXT,
    -- "桩基施工图 12 张"
    output_payload  JSONB DEFAULT '{}',
    -- 具体内容描述，结构化

    -- 工程量（用于从合同 Genesis 扣减）
    quota_consumed  NUMERIC(18,2) DEFAULT 0,
    quota_unit      TEXT DEFAULT '项',

    -- 防篡改
    inputs_hash     TEXT NOT NULL,
    -- sha256(executor_ref + container_ref + input_refs + output_type)
    proof_hash      TEXT NOT NULL,
    -- sha256(ref + inputs_hash + project_ref + step_seq)

    -- 签发状态（需审核的步骤由人工确认）
    status          TEXT NOT NULL DEFAULT 'DRAFT',
    -- DRAFT（草稿，AI产出待审）/ SIGNED（已签发）/ VOIDED

    signed_by       TEXT,
    -- 签发人 executor_ref（注册工程师）
    signed_at       TIMESTAMPTZ,

    source          TEXT NOT NULL DEFAULT 'TRIP_DERIVED',
    -- TRIP_DERIVED / MANUAL_ENTRY

    tenant_id       INT  NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_step_ach_project
    ON step_achievement_utxos(project_ref);
CREATE INDEX IF NOT EXISTS idx_step_ach_executor
    ON step_achievement_utxos(executor_ref);
CREATE INDEX IF NOT EXISTS idx_step_ach_container
    ON step_achievement_utxos(container_ref);
CREATE INDEX IF NOT EXISTS idx_step_ach_status
    ON step_achievement_utxos(status);

-- ── Step 4: achievement_utxos 增加字段 ────────────────────────
-- 对齐交易主线，增加合同来源追踪

ALTER TABLE achievement_utxos
    ADD COLUMN IF NOT EXISTS contract_genesis_ref  TEXT,
    -- 产出此业绩的合同 Genesis UTXO ref
    ADD COLUMN IF NOT EXISTS tender_genesis_ref     TEXT,
    -- 产出此业绩的招标 Genesis UTXO ref（可追溯到采购源头）
    ADD COLUMN IF NOT EXISTS step_count             INT DEFAULT 0,
    -- 聚合了多少个 Step Achievement UTXO
    ADD COLUMN IF NOT EXISTS review_ref             TEXT,
    -- 审图/验收 UTXO ref（RULE-002 合规证明）
    ADD COLUMN IF NOT EXISTS settled_amount         NUMERIC(18,2),
    -- 最终结算金额（万元）
    ADD COLUMN IF NOT EXISTS layer                  INT DEFAULT 4;
    -- 固定为 4，业绩层

-- engineer_achievement_receipts 增加字段
ALTER TABLE engineer_achievement_receipts
    ADD COLUMN IF NOT EXISTS step_refs     TEXT[] DEFAULT '{}',
    -- 该工程师参与的 step_achievement refs
    ADD COLUMN IF NOT EXISTS step_count    INT DEFAULT 0;

-- ── Step 5: 升级 fn_bid_awarded ───────────────────────────────
-- 新增：把落标方写入 BidReceipt（落标存证）
-- 新增：Tender Genesis ref 传递给合同 Genesis

CREATE OR REPLACE FUNCTION fn_bid_awarded()
RETURNS TRIGGER AS $$
DECLARE
    v_project_ref       TEXT;
    v_genesis_ref       TEXT;
    v_tender_ref        TEXT;
    v_year              INT;
    v_seq               TEXT;
    v_contract_json     JSONB;
BEGIN
    IF NEW.status != 'AWARDED' OR OLD.status = 'AWARDED' THEN
        RETURN NEW;
    END IF;

    v_year := EXTRACT(YEAR FROM NOW())::INT;
    v_seq  := LPAD(NEW.id::TEXT, 4, '0');

    v_project_ref := NEW.namespace_ref || '/project/bid-' || v_year || '-' || v_seq;
    v_genesis_ref := NEW.namespace_ref || '/genesis/contract/bid-' || v_year || '-' || v_seq;
    v_tender_ref  := COALESCE(NEW.tender_genesis_ref,
                        NEW.namespace_ref || '/genesis/tender/' || v_year || '-' || v_seq);

    -- ① 合同 Genesis UTXO（Layer 2，parent = Tender Genesis）
    v_contract_json := jsonb_build_object(
        'layer',          2,
        'source',         'BID_AWARD',
        'bid_utxo_ref',   NEW.utxo_ref,
        'tender_ref',     v_tender_ref,
        'project_name',   NEW.project_name,
        'owner_name',     NEW.owner_name,
        'project_type',   NEW.project_type,
        'awarded_at',     NOW(),
        'rule_binding',   ARRAY['RULE-003']
    );

    INSERT INTO genesis_utxos (
        ref, resource_type, name,
        total_amount, available_amount, unit,
        layer, parent_ref, source,
        constraint_json, status, tenant_id
    ) VALUES (
        v_genesis_ref,
        'CONTRACT_FUND',
        '合同：' || NEW.project_name,
        COALESCE(NEW.bid_amount, 0),
        COALESCE(NEW.bid_amount, 0),
        '元',
        2, v_tender_ref, 'BID_AWARD',
        v_contract_json,
        'ACTIVE',
        NEW.tenant_id
    ) ON CONFLICT (ref) DO NOTHING;

    -- ② 项目树节点
    INSERT INTO project_nodes (
        ref, name, project_type,
        namespace_ref, contract_ref,
        status, depth, path, tenant_id
    ) VALUES (
        v_project_ref,
        NEW.project_name,
        NEW.project_type,
        NEW.namespace_ref,
        v_genesis_ref,
        'CONTRACTED',
        0, v_project_ref,
        NEW.tenant_id
    ) ON CONFLICT (ref) DO NOTHING;

    -- ③ 中标方工程师：REFERENCED → OCCUPIED
    UPDATE bid_resources SET
        consume_status = 'OCCUPIED',
        updated_at     = NOW()
    WHERE bid_id        = NEW.id
      AND resource_type = 'QUAL_PERSON'
      AND consume_status = 'REFERENCED';

    -- ④ 回写
    NEW.project_ref  := v_project_ref;
    NEW.awarded_at   := NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 落标触发器（新增）
CREATE OR REPLACE FUNCTION fn_bid_rejected()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status != 'REJECTED' OR OLD.status = 'REJECTED' THEN
        RETURN NEW;
    END IF;

    -- 释放工程师占用
    UPDATE bid_resources SET
        consume_status = 'RELEASED',
        updated_at     = NOW()
    WHERE bid_id         = NEW.id
      AND resource_type  = 'QUAL_PERSON'
      AND consume_status IN ('REFERENCED', 'OCCUPIED');

    NEW.rejected_at := NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bid_rejected ON bid_documents;
CREATE TRIGGER trg_bid_rejected
    BEFORE UPDATE ON bid_documents
    FOR EACH ROW
    EXECUTE FUNCTION fn_bid_rejected();

-- ── Step 6: 升级 fn_project_settled ──────────────────────────
-- 核心升级：结算时自动产出 Achievement UTXO + 工程师 Receipt
-- 这是业绩层的唯一正常写入路径（source=TRIP_DERIVED）

CREATE OR REPLACE FUNCTION fn_project_settled()
RETURNS TRIGGER AS $$
DECLARE
    v_ns            TEXT;
    v_year          INT;
    v_seq           TEXT;
    v_ach_ref       TEXT;
    v_contract_ref  TEXT;
    v_tender_ref    TEXT;
    v_project_type  TEXT;
    v_project_name  TEXT;
    v_owner_name    TEXT;
    v_bid_amount    NUMERIC;
    v_step_count    INT;
    v_inputs_hash   TEXT;
    v_proof_hash    TEXT;
    v_step_refs     TEXT[];

    -- 工程师游标
    r_eng           RECORD;
    v_eng_receipt   TEXT;
    v_eng_inputs    TEXT;
    v_eng_proof     TEXT;
    v_eng_steps     TEXT[];
    v_seq_n         INT := 0;
BEGIN
    IF (NEW.status != 'SETTLED' AND NEW.status != 'SIGNED')
       OR OLD.status IN ('SETTLED', 'SIGNED') THEN
        RETURN NEW;
    END IF;

    IF NEW.spu_ref NOT LIKE '%settlement%' AND
       NEW.output_type NOT IN ('SETTLEMENT', 'FINAL_DELIVERY') THEN
        RETURN NEW;
    END IF;

    -- 取项目信息
    SELECT
        pn.namespace_ref, pn.project_type, pn.name,
        gu.total_amount, gu.parent_ref,
        bd.owner_name,
        gu.ref AS contract_genesis_ref
    INTO
        v_ns, v_project_type, v_project_name,
        v_bid_amount, v_tender_ref,
        v_owner_name,
        v_contract_ref
    FROM project_nodes pn
    LEFT JOIN genesis_utxos gu
        ON gu.ref = pn.contract_ref AND gu.resource_type = 'CONTRACT_FUND'
    LEFT JOIN bid_documents bd
        ON bd.project_ref = pn.ref
    WHERE pn.ref = NEW.project_ref
    LIMIT 1;

    IF v_ns IS NULL THEN
        RETURN NEW;
    END IF;

    v_year := EXTRACT(YEAR FROM NOW())::INT;
    v_seq  := LPAD(NEW.id::TEXT, 4, '0');

    -- 统计 Step Achievement UTXO
    SELECT
        COUNT(*),
        ARRAY_AGG(ref ORDER BY step_seq)
    INTO v_step_count, v_step_refs
    FROM step_achievement_utxos
    WHERE project_ref = NEW.project_ref
      AND status = 'SIGNED';

    v_step_refs := COALESCE(v_step_refs, '{}');

    -- ── Layer 4：项目 Achievement UTXO ───────────────────────
    v_ach_ref := v_ns || '/utxo/achievement/'
        || LOWER(COALESCE(v_project_type, 'other'))
        || '/' || v_year || '/' || v_seq;

    -- inputs_hash 锁定完整交易链
    v_inputs_hash := encode(sha256(CONVERT_TO(
        v_contract_ref || '|' ||
        COALESCE(v_tender_ref, '') || '|' ||
        COALESCE(NEW.proof_hash, '') || '|' ||
        array_to_string(v_step_refs, ',') || '|' ||
        COALESCE(v_bid_amount::TEXT, '0'),
        'UTF8'
    )), 'hex');

    v_proof_hash := encode(sha256(CONVERT_TO(
        'sha256:' || v_inputs_hash || '|' ||
        v_ach_ref || '|' ||
        v_ns || '|TRIP_DERIVED',
        'UTF8'
    )), 'hex');

    INSERT INTO achievement_utxos (
        ref, namespace_ref,
        project_name, project_type, owner_name,
        contract_amount, completed_year,
        contract_genesis_ref, tender_genesis_ref,
        step_count, review_ref, settled_amount,
        source, layer,
        inputs_hash, proof_hash,
        status, tenant_id
    ) VALUES (
        v_ach_ref, v_ns,
        COALESCE(v_project_name, '未命名项目'),
        COALESCE(v_project_type, 'OTHER'),
        COALESCE(v_owner_name, ''),
        COALESCE(v_bid_amount / 10000, 0),  -- 元→万元
        v_year,
        v_contract_ref, v_tender_ref,
        v_step_count, NEW.ref,
        COALESCE(v_bid_amount / 10000, 0),
        'TRIP_DERIVED', 4,
        'sha256:' || v_inputs_hash,
        'sha256:' || v_proof_hash,
        'ACTIVE', NEW.tenant_id
    ) ON CONFLICT (ref) DO UPDATE SET
        step_count  = EXCLUDED.step_count,
        proof_hash  = EXCLUDED.proof_hash,
        updated_at  = NOW();

    -- ── Layer 4：工程师 Receipt（每个执行体一条）───────────────
    FOR r_eng IN
        SELECT DISTINCT
            sa.executor_ref,
            sa.container_ref,
            ARRAY_AGG(sa.ref ORDER BY sa.step_seq) AS step_refs,
            COUNT(*) AS step_count,
            -- 角色推导：有签发动作的 → DESIGN_LEAD，其他 → PARTICIPANT
            CASE WHEN bool_or(sa.signed_by = sa.executor_ref)
                 THEN 'DESIGN_LEAD' ELSE 'PARTICIPANT' END AS role
        FROM step_achievement_utxos sa
        WHERE sa.project_ref = NEW.project_ref
          AND sa.status = 'SIGNED'
        GROUP BY sa.executor_ref, sa.container_ref
    LOOP
        v_seq_n := v_seq_n + 1;

        -- 从 executor_ref 提取 engineer_id
        -- v://cn.zhongbei/executor/person/cyp4310@v1 → cyp4310
        DECLARE
            v_eng_id TEXT;
            v_eng_name TEXT;
        BEGIN
            v_eng_id := regexp_replace(
                r_eng.executor_ref,
                '.*/executor/person/(.+)@v1$', '\1'
            );

            SELECT name INTO v_eng_name
            FROM employees
            WHERE executor_ref = r_eng.executor_ref
            LIMIT 1;

            v_eng_receipt := v_ns
                || '/receipt/achievement/' || v_eng_id
                || '/' || v_year || '/' || v_seq
                || LPAD(v_seq_n::TEXT, 2, '0');

            v_eng_inputs := encode(sha256(CONVERT_TO(
                v_ach_ref || '|' ||
                r_eng.executor_ref || '|' ||
                r_eng.container_ref || '|' ||
                r_eng.role,
                'UTF8'
            )), 'hex');

            v_eng_proof := encode(sha256(CONVERT_TO(
                'sha256:' || v_eng_inputs || '|' ||
                v_eng_receipt || '|TRIP_DERIVED',
                'UTF8'
            )), 'hex');

            INSERT INTO engineer_achievement_receipts (
                ref, achievement_ref,
                executor_ref, engineer_name, engineer_id,
                container_ref, role,
                step_refs, step_count,
                inputs_hash, proof_hash,
                source, status, tenant_id
            ) VALUES (
                v_eng_receipt, v_ach_ref,
                r_eng.executor_ref,
                COALESCE(v_eng_name, v_eng_id),
                v_eng_id,
                r_eng.container_ref,
                r_eng.role,
                r_eng.step_refs,
                r_eng.step_count,
                'sha256:' || v_eng_inputs,
                'sha256:' || v_eng_proof,
                'TRIP_DERIVED', 'ACTIVE', NEW.tenant_id
            ) ON CONFLICT (ref) DO UPDATE SET
                step_count = EXCLUDED.step_count,
                proof_hash = EXCLUDED.proof_hash;

            -- 异步更新工程师能力统计
            UPDATE executor_stats SET
                total_spus  = total_spus + r_eng.step_count,
                passed_spus = passed_spus + r_eng.step_count,
                computed_at = NOW()
            WHERE executor_ref = r_eng.executor_ref;
        END;
    END LOOP;

    -- ── 释放资源 ──────────────────────────────────────────────
    -- 工程师证书容器 occupied - 1
    UPDATE containers SET
        occupied = GREATEST(0, occupied - 1),
        updated_at = NOW()
    WHERE linked_executor_ref = ANY(
        SELECT executor_ref FROM step_achievement_utxos
        WHERE project_ref = NEW.project_ref AND status = 'SIGNED'
    );

    -- 合同 Genesis UTXO 关闭
    UPDATE genesis_utxos SET
        status     = 'CLOSED',
        available_amount = 0
    WHERE ref = v_contract_ref;

    -- bid_documents 释放工程师占用
    UPDATE bid_resources br SET
        consume_status = 'RELEASED',
        updated_at     = NOW()
    FROM bid_documents bd
    WHERE bd.project_ref   = NEW.project_ref
      AND br.bid_id        = bd.id
      AND br.resource_type = 'QUAL_PERSON'
      AND br.consume_status = 'OCCUPIED';

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_project_settled ON step_achievement_utxos;
CREATE TRIGGER trg_project_settled
    AFTER INSERT OR UPDATE ON step_achievement_utxos
    FOR EACH ROW
    EXECUTE FUNCTION fn_project_settled();

-- ── Step 7: 辅助视图 ─────────────────────────────────────────

-- 业绩分层视图：四层结构全貌
CREATE OR REPLACE VIEW achievement_layers AS
SELECT
    'layer1_ability'    AS layer_name,
    ref, resource_type  AS type,
    name, status,
    NULL::NUMERIC       AS amount,
    created_at
FROM genesis_utxos WHERE layer = 1
UNION ALL
SELECT
    'layer2_contract',
    ref, resource_type,
    name, status,
    total_amount,
    created_at
FROM genesis_utxos WHERE layer = 2
UNION ALL
SELECT
    'layer3_step',
    ref, output_type,
    output_name, status,
    quota_consumed,
    created_at
FROM step_achievement_utxos
UNION ALL
SELECT
    'layer4_achievement',
    ref, project_type,
    project_name, status,
    contract_amount,
    created_at
FROM achievement_utxos WHERE layer = 4;

-- 项目业绩完整链视图（一个项目的全部层次）
CREATE OR REPLACE VIEW project_achievement_chain AS
SELECT
    a.ref                   AS achievement_ref,
    a.project_name,
    a.project_type,
    a.owner_name,
    a.source,
    a.proof_hash            AS achievement_proof,
    a.contract_genesis_ref,
    a.tender_genesis_ref,
    a.step_count,
    a.completed_year,
    a.contract_amount,
    -- 合同层信息
    gu.total_amount         AS contract_amount_raw,
    gu.status               AS contract_status,
    -- 工程师数量
    COUNT(DISTINCT er.engineer_id) AS engineer_count,
    -- 信用等级
    CASE a.source
        WHEN 'TRIP_DERIVED'       THEN 5
        WHEN 'HISTORICAL_IMPORT'  THEN 2
        ELSE 1
    END                     AS trust_level
FROM achievement_utxos a
LEFT JOIN genesis_utxos gu ON gu.ref = a.contract_genesis_ref
LEFT JOIN engineer_achievement_receipts er ON er.achievement_ref = a.ref
WHERE a.status = 'ACTIVE'
GROUP BY a.ref, a.project_name, a.project_type, a.owner_name,
         a.source, a.proof_hash, a.contract_genesis_ref,
         a.tender_genesis_ref, a.step_count, a.completed_year,
         a.contract_amount, gu.total_amount, gu.status;

-- ── 验证 ──────────────────────────────────────────────────────
SELECT
    layer,
    resource_type,
    COUNT(*) AS count
FROM genesis_utxos
GROUP BY layer, resource_type
ORDER BY layer, resource_type;
