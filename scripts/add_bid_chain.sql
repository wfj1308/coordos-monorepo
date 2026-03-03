-- ============================================================
-- add_bid_chain.sql
-- 投标链闭环数据模型
--
-- 闭环流程：
--  1. ValidateBid → 检查资质、工程师、业绩
--  2. CreateBid   → 锁定资源，生成 proof_hash
--  3. Submit      → 提交投标
--  4. Award        → 中标：项目树诞生，工程师被占用
--  5. 设计链执行  → Achievement UTXO 逐步产出
--  6. Settlement  → 结项：工程师释放，业绩进池
--  7. 下次投标    → 业绩池里多了这次项目
-- ============================================================

-- ══════════════════════════════════════════════════════════════
-- 1. bid_documents - 投标文件 UTXO
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bid_documents (
    id              BIGSERIAL PRIMARY KEY,
    bid_ref         VARCHAR(255) UNIQUE NOT NULL,  -- v://cn.zhongbei/bid/2024/001
    tenant_id       INT NOT NULL,
    namespace_ref   VARCHAR(255) NOT NULL,         -- v://cn.zhongbei
    
    -- 项目信息
    project_name    VARCHAR(500) NOT NULL,
    project_type    VARCHAR(50) NOT NULL,         -- BRIDGE/ROAD/TUNNEL/MUNICIPAL
    owner_name      VARCHAR(255),                 -- 业主
    estimated_amount NUMERIC(15,2),                -- 估算金额
    bid_deadline    TIMESTAMP,
    
    -- 投标信息
    our_bid_amount  NUMERIC(15,2),                -- 我方报价
    bid_package_ref VARCHAR(255),                  -- 标书文件引用
    
    -- 状态机：DRAFT → SUBMITTED → AWARDED → CONTRACTED → FAILED
    status          VARCHAR(20) NOT NULL DEFAULT 'DRAFT',
    
    -- UTXO 关键字段
    proof_hash      VARCHAR(255),                  -- 所有输入资源的 hash
    resource_count  INT DEFAULT 0,                 -- 引用资源数量
    
    -- 中标后关联
    project_ref     VARCHAR(255),                  -- 中标后生成的 project_nodes.ref
    contract_id     BIGINT,                        -- 中标后生成的合同
    
    -- 时间戳
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    submitted_at    TIMESTAMP,
    awarded_at      TIMESTAMP,
    failed_at       TIMESTAMP,
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_bid_doc_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

CREATE INDEX idx_bid_documents_namespace ON bid_documents(namespace_ref);
CREATE INDEX idx_bid_documents_status ON bid_documents(status);
CREATE INDEX idx_bid_documents_tenant_status ON bid_documents(tenant_id, status);

-- ══════════════════════════════════════════════════════════════
-- 2. bid_resources - 投标引用的资源清单
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bid_resources (
    id          BIGSERIAL PRIMARY KEY,
    bid_id      BIGINT NOT NULL REFERENCES bid_documents(id) ON DELETE CASCADE,
    tenant_id   INT NOT NULL,
    
    -- 资源类型
    resource_type VARCHAR(50) NOT NULL,  -- QUAL_COMPANY / QUAL_PERSON / ACHIEVEMENT / FINANCIAL
    resource_ref VARCHAR(255) NOT NULL, -- 资源引用
    
    -- 消耗模式
    consume_mode VARCHAR(20) NOT NULL,  -- REFERENCE（引用型）/ OCCUPY（占用型）
    consume_status VARCHAR(20) DEFAULT 'PENDING', -- PENDING / REFERENCED / OCCUPIED / RELEASED
    
    -- 资源详情（冗余存储，用于快速查询）
    resource_name VARCHAR(255),
    resource_data JSONB,
    
    -- 验证信息
    valid_from   TIMESTAMP,
    valid_until  TIMESTAMP,
    verify_url   VARCHAR(500),
    
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_bid_res_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);

CREATE INDEX idx_bid_resources_bid ON bid_resources(bid_id);
CREATE INDEX idx_bid_resources_type ON bid_resources(resource_type);
CREATE INDEX idx_bid_resources_ref ON bid_resources(resource_ref);
CREATE INDEX idx_bid_resources_consume ON bid_resources(consume_status);

-- ══════════════════════════════════════════════════════════════
-- 3. achievement_pool - 企业业绩池视图
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW achievement_pool AS
SELECT 
    a.id,
    a.utxo_ref,
    a.spu_ref,
    a.project_ref,
    a.executor_ref,
    a.proof_hash,
    a.status,
    a.source,
    a.settled_at,
    a.tenant_id,
    
    -- 项目信息
    p.name AS project_name,
    p.status AS project_status,
    
    -- 合同信息
    c.contract_name,
    c.contract_amount,
    
    -- 项目类型推断（从 spu_ref 或 project_ref 提取）
    CASE 
        WHEN a.spu_ref LIKE '%bridge%' THEN 'BRIDGE'
        WHEN a.spu_ref LIKE '%road%' THEN 'ROAD'
        WHEN a.spu_ref LIKE '%tunnel%' THEN 'TUNNEL'
        ELSE 'OTHER'
    END AS inferred_project_type,
    
    -- 有效期（业绩近3年有效）
    CASE 
        WHEN a.settled_at >= NOW() - INTERVAL '3 years' THEN true
        ELSE false
    END AS within_3_years,
    
    -- 可用性（SETTLED + 审图合格证 = 可引用）
    CASE 
        WHEN a.status = 'SETTLED' 
             AND a.spu_ref LIKE '%review_certificate%' 
        THEN true
        ELSE false
    END AS is_usable_for_bid
    
FROM achievement_utxos a
LEFT JOIN project_nodes p ON p.ref = a.project_ref
LEFT JOIN contracts c ON c.id = (
    SELECT contract_id FROM project_nodes pn 
    WHERE pn.ref = a.project_ref 
    LIMIT 1
);

-- ══════════════════════════════════════════════════════════════
-- 4. fn_bid_awarded - 中标触发器
-- 自动执行：
--   ① 创建项目树根节点
--   ② 创建合同 Genesis UTXO
--   ③ 注册工程师 REFERENCED → OCCUPIED
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_bid_awarded()
RETURNS TRIGGER AS $$
DECLARE
    v_project_ref VARCHAR(255);
    v_project_id BIGINT;
    v_contract_id BIGINT;
    v_genesis_ref VARCHAR(255);
    v_namespace VARCHAR(100);
BEGIN
    -- 只在 status 变为 AWARDED 时触发
    IF NEW.status = 'AWARDED' AND (OLD.status IS NULL OR OLD.status != 'AWARDED') THEN
        
        -- 提取命名空间
        v_namespace := REPLACE(NEW.namespace_ref, 'v://', '');
        
        -- ① 创建项目树根节点
        v_project_ref := 'v://' || v_namespace || '/project/bid-' || NEW.id || '-' || EXTRACT(EPOCH FROM NOW());
        
        INSERT INTO project_nodes (
            ref, tenant_id, depth, path, name,
            owner_ref, contractor_ref, executor_ref,
            status, created_at, updated_at
        ) VALUES (
            v_project_ref, NEW.tenant_id, 0, '/',
            NEW.project_name,
            COALESCE(NEW.owner_name, 'v://' || v_namespace || '/owner/default'),
            NEW.namespace_ref,
            NEW.namespace_ref,
            'INITIATED', NOW(), NOW()
        ) RETURNING id INTO v_project_id;
        
        -- ② 创建合同 Genesis UTXO（作为项目资金锚点）
        IF NEW.our_bid_amount IS NOT NULL AND NEW.our_bid_amount > 0 THEN
            v_genesis_ref := 'v://' || v_namespace || '/genesis/contract/bid-' || NEW.id;
            
            INSERT INTO contracts (
                tenant_id, contract_name, contract_amount,
                party_a_name, party_b_name, party_b_ref,
                status, created_at
            ) VALUES (
                NEW.tenant_id,
                NEW.project_name,
                NEW.our_bid_amount,
                COALESCE(NEW.owner_name, '业主'),
                v_namespace,
                NEW.namespace_ref,
                'PENDING',
                NOW()
            ) RETURNING id INTO v_contract_id;
            
            -- 更新项目节点关联合同
            UPDATE project_nodes SET 
                contract_ref = 'v://' || v_namespace || '/contract/' || v_contract_id
            WHERE id = v_project_id;
        END IF;
        
        -- ③ 把注册工程师从 REFERENCED → OCCUPIED
        UPDATE bid_resources 
        SET consume_status = 'OCCUPIED', updated_at = NOW()
        WHERE bid_id = NEW.id 
          AND resource_type = 'QUAL_PERSON'
          AND consume_mode = 'OCCUPY'
          AND consume_status = 'REFERENCED';
        
        -- 更新投标文档
        NEW.project_ref := v_project_ref;
        NEW.contract_id := v_contract_id;
        NEW.awarded_at := NOW();
        NEW.updated_at := NOW();
        
        -- 记录日志
        INSERT INTO bid_award_log (bid_id, project_ref, contract_id, created_at)
        VALUES (NEW.id, v_project_ref, v_contract_id, NOW())
        ON CONFLICT DO NOTHING;
        
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trg_bid_awarded ON bid_documents;
CREATE TRIGGER trg_bid_awarded
    AFTER UPDATE OF status ON bid_documents
    FOR EACH ROW
    EXECUTE FUNCTION fn_bid_awarded();

-- 中标日志表
CREATE TABLE IF NOT EXISTS bid_award_log (
    id          BIGSERIAL PRIMARY KEY,
    bid_id      BIGINT NOT NULL REFERENCES bid_documents(id),
    project_ref VARCHAR(255),
    contract_id BIGINT,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════════
-- 5. fn_project_settled - 结项触发器
-- 自动执行：
--   ① 注册工程师 OCCUPIED → RELEASED
--   ② 业绩进入企业业绩池（已有 achievement_utxos）
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION fn_project_settled()
RETURNS TRIGGER AS $$
BEGIN
    -- 只在 settlement_cert SPU 的 UTXO 状态变为 SETTLED 时触发
    IF NEW.status = 'SETTLED' 
       AND (OLD.status IS NULL OR OLD.status != 'SETTLED')
       AND NEW.spu_ref LIKE '%settlement_cert%' THEN
        
        -- ① 释放项目占用的所有工程师资源
        -- 找到项目对应的投标
        UPDATE bid_resources 
        SET consume_status = 'RELEASED', updated_at = NOW()
        WHERE consume_status = 'OCCUPIED'
          AND bid_id IN (
            SELECT id FROM bid_documents 
            WHERE project_ref = NEW.project_ref
          );
        
        -- ② 项目树节点状态更新为 SETTLED
        UPDATE project_nodes 
        SET status = 'SETTLED', updated_at = NOW()
        WHERE ref = NEW.project_ref;
        
        -- 记录结项日志
        INSERT INTO project_settle_log (project_ref, utxo_ref, settled_at)
        VALUES (NEW.project_ref, NEW.utxo_ref, NEW.settled_at)
        ON CONFLICT DO NOTHING;
        
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trg_project_settled ON achievement_utxos;
CREATE TRIGGER trg_project_settled
    AFTER UPDATE OF status ON achievement_utxos
    FOR EACH ROW
    EXECUTE FUNCTION fn_project_settled();

-- 结项日志表
CREATE TABLE IF NOT EXISTS project_settle_log (
    id          BIGSERIAL PRIMARY KEY,
    project_ref VARCHAR(255),
    utxo_ref    VARCHAR(255),
    settled_at  TIMESTAMP
);

-- ══════════════════════════════════════════════════════════════
-- 6. 工程师占用状态视图
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW engineer_occupancy AS
SELECT 
    br.resource_ref AS executor_ref,
    q.cert_no,
    q.holder_name,
    br.consume_status,
    bd.project_name,
    bd.bid_ref,
    p.ref AS project_ref,
    p.status AS project_status,
    br.updated_at AS occupied_at
FROM bid_resources br
JOIN bid_documents bd ON bd.id = br.bid_id
LEFT JOIN project_nodes p ON p.ref = bd.project_ref
LEFT JOIN qualifications q ON q.ref = br.resource_ref
WHERE br.resource_type = 'QUAL_PERSON'
  AND br.consume_status IN ('OCCUPIED', 'REFERENCED')
ORDER BY br.updated_at DESC;

-- ══════════════════════════════════════════════════════════════
-- 7. 索引优化
-- ══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_achievement_settled 
ON achievement_utxos(settled_at DESC) 
WHERE status = 'SETTLED';

CREATE INDEX IF NOT EXISTS idx_achievement_spu_settled
ON achievement_utxos(spu_ref, settled_at DESC)
WHERE status = 'SETTLED';

-- ══════════════════════════════════════════════════════════════
-- 8. 增强 bid_documents - 投标作为执行体寻址
-- ══════════════════════════════════════════════════════════════

-- 添加执行体引用和资源快照字段
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS executor_ref VARCHAR(255);
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS credential_snapshot JSONB DEFAULT '{}'::jsonb;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS achievement_snapshot JSONB DEFAULT '[]'::jsonb;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS matching_score INT DEFAULT 0;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS won_at TIMESTAMPTZ;
ALTER TABLE bid_documents ADD COLUMN IF NOT EXISTS lost_at TIMESTAMPTZ;

-- ══════════════════════════════════════════════════════════════
-- 9. 投标匹配度视图
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW bid_match_analysis AS
SELECT 
    bd.id,
    bd.bid_ref,
    bd.project_name,
    bd.project_type,
    bd.status,
    bd.executor_ref,
    bd.matching_score,
    
    -- 资质匹配度
    (SELECT COUNT(*) FROM bid_resources br 
     WHERE br.bid_id = bd.id AND br.resource_type = 'QUAL_COMPANY') AS company_qual_count,
     
    -- 人员匹配度
    (SELECT COUNT(*) FROM bid_resources br 
     WHERE br.bid_id = bd.id AND br.resource_type = 'QUAL_PERSON') AS person_qual_count,
     
    -- 业绩匹配度
    (SELECT COUNT(*) FROM bid_resources br 
     WHERE br.bid_id = bd.id AND br.resource_type = 'ACHIEVEMENT') AS achievement_count,
     
    -- 总资源数
    bd.resource_count,
    
    -- 创建时间
    bd.created_at,
    bd.submitted_at,
    bd.awarded_at
    
FROM bid_documents bd
ORDER BY bd.matching_score DESC NULLS LAST, bd.created_at DESC;

-- ══════════════════════════════════════════════════════════════
-- 10. 执行体寻址市场视图
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW executor_market AS
SELECT 
    e.executor_ref,
    e.employee_name,
    e.company_id,
    e.company_name,
    
    -- 执行体能力
    e.capability_level,
    e.skills,
    
    -- 资质信息
    (SELECT json_agg(json_build_object(
        'qual_type', q.qual_type,
        'cert_no', q.cert_no,
        'valid_until', q.valid_until,
        'status', q.status
    )) FROM qualifications q 
     WHERE q.holder_ref = e.executor_ref 
       AND q.status = 'VALID' 
       AND q.deleted = FALSE
    ) AS qualifications,
    
    -- 业绩统计（近3年）
    (SELECT COUNT(*) FROM achievement_utxos a 
     WHERE a.executor_ref = e.executor_ref 
       AND a.status = 'SETTLED'
       AND a.settled_at >= NOW() - INTERVAL '3 years'
    ) AS recent_achievements,
    
    -- 当前占用状态
    (SELECT COUNT(*) FROM bid_resources br
     WHERE br.resource_ref = e.executor_ref
       AND br.consume_status IN ('OCCUPIED', 'REFERENCED')
    ) AS current_occupancy,
    
    -- 可用性
    CASE WHEN (SELECT COUNT(*) FROM bid_resources br
               WHERE br.resource_ref = e.executor_ref
                 AND br.consume_status = 'OCCUPIED') = 0
    THEN true ELSE false END AS is_available,
    
    e.tenant_id
    
FROM (
    SELECT 
        e.executor_ref,
        emp.name AS employee_name,
        e.company_id,
        c.name AS company_name,
        e.capability_level,
        e.skills,
        e.tenant_id
    FROM executors e
    LEFT JOIN employees emp ON emp.id = e.employee_id
    LEFT JOIN companies c ON c.id = e.company_id
) e;

-- ══════════════════════════════════════════════════════════════
-- 11. 招投标匹配索引
-- ══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_bid_doc_executor ON bid_documents(executor_ref);
CREATE INDEX IF NOT EXISTS idx_bid_doc_matching ON bid_documents(matching_score DESC);
CREATE INDEX IF NOT EXISTS idx_bid_doc_type_status ON bid_documents(project_type, status);

-- ══════════════════════════════════════════════════════════════
-- 8. 注释
-- ══════════════════════════════════════════════════════════════

COMMENT ON TABLE bid_documents IS '投标文件UTXO：执行体寻址的响应，消耗历史业绩';
COMMENT ON VIEW bid_match_analysis IS '投标匹配度分析：展示每个投标的资源匹配情况';
COMMENT ON VIEW executor_market IS '执行体寻址市场：展示所有执行体的可寻址状态';
