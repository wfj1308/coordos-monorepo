-- ============================================================
--  CoordOS — 审图与出版中心 DDL
--  两张核心表：
--    drawing_versions  图纸版本链（出版中心资源批次）
--    review_certs      审图合格证（审图中心资源批次消耗记录）
-- ============================================================

-- ── 图纸版本表 ─────────────────────────────────────────────────
-- 每次出版产生一行，同一图纸编号的多行形成版本链
-- 版本链通过 supersedes_id 串联：新版本指向被取代的旧版本

CREATE TABLE IF NOT EXISTS drawing_versions (
    id               bigserial    PRIMARY KEY,

    -- 图纸身份
    drawing_no       text         NOT NULL,    -- 全局唯一图纸编号，如 ZB-2026-G30-S-001
    version          text         NOT NULL,    -- 版本号：v1/v2/v3
    title            text         NOT NULL,    -- 图纸名称
    major            text         NOT NULL,    -- 专业：结构/建筑/岩土/道路/桥梁
    UNIQUE (drawing_no, version),              -- 同一图纸同一版本不能重复出版

    -- 关联
    project_ref      text         NOT NULL,    -- 关联项目 v://...
    spu_ref          text,                     -- 产出此图纸的 SPU
    executor_ref     text         NOT NULL,    -- 主设工程师 executor_ref

    -- 文件
    file_hash        text,                     -- 文件 SHA-256，防篡改
    file_url         text,                     -- 文件存储地址

    -- 状态
    status           text         NOT NULL DEFAULT 'DRAFT',
    CHECK (status IN ('DRAFT','PUBLISHED','SUPERSEDED','REVOKED')),

    -- 审图信息（IssueReviewCert 后写入）
    review_cert_ref  text,                     -- 审图合格证编号
    reviewer_ref     text,                     -- 主审工程师 executor_ref
    chief_eng_ref    text,                     -- 总工程师 executor_ref
    reviewed_at      timestamptz,

    -- 出版信息（PublishDrawing 后写入）
    published_at     timestamptz,
    published_by     text,                     -- 出版操作人 executor_ref

    -- 版本链
    supersedes_id    bigint REFERENCES drawing_versions(id),  -- 取代的旧版本

    -- 存证
    utxo_ref         text,                     -- 产出的 achievement UTXO ref
    proof_hash       text,                     -- 本版本存证哈希

    tenant_id        int          NOT NULL,
    created_at       timestamptz  NOT NULL DEFAULT NOW(),
    updated_at       timestamptz  NOT NULL DEFAULT NOW()
);

-- 按图纸编号查当前有效版本（最高频查询）
CREATE INDEX IF NOT EXISTS idx_dv_drawing_no_status
    ON drawing_versions (drawing_no, status);

-- 按项目查所有图纸
CREATE INDEX IF NOT EXISTS idx_dv_project
    ON drawing_versions (project_ref, status);

-- 版本链遍历
CREATE INDEX IF NOT EXISTS idx_dv_supersedes
    ON drawing_versions (supersedes_id);


-- ── 审图合格证表 ───────────────────────────────────────────────
-- 每次签发审图合格证产生一行
-- 是 RULE-002 的核心证据，没有这条记录就不能结算

CREATE TABLE IF NOT EXISTS review_certs (
    id                  bigserial    PRIMARY KEY,

    -- 证书身份
    cert_no             text         NOT NULL UNIQUE,  -- 如 SH-2026-0001
    drawing_version_id  bigint       NOT NULL REFERENCES drawing_versions(id),
    drawing_no          text         NOT NULL,
    project_ref         text         NOT NULL,

    -- 审图人员（RULE-002：必须是总院注册结构师）
    reviewer_ref        text         NOT NULL,   -- 主审工程师 executor_ref
    chief_eng_ref       text,                    -- 总工程师 executor_ref

    -- 审查数据
    issue_count         int          NOT NULL DEFAULT 0,   -- 审查意见总数
    major_count         int          NOT NULL DEFAULT 0,   -- 重大意见数（必须=0才能通过）
    resolved_count      int          NOT NULL DEFAULT 0,   -- 已处理意见数
    resolution_rate     numeric(5,2) NOT NULL DEFAULT 0,   -- 处理率（%）

    -- 状态
    status              text         NOT NULL DEFAULT 'PENDING',
    CHECK (status IN ('PENDING','IN_PROCESS','PASSED','FAILED')),

    valid_until         date,                    -- 合格证有效期，默认2年

    -- 存证
    utxo_ref            text,                    -- 产出的 achievement UTXO ref
    proof_hash          text         NOT NULL,

    tenant_id           int          NOT NULL,
    issued_at           timestamptz  NOT NULL DEFAULT NOW()
);

-- 按图纸版本查审图合格证（Verify 时用）
CREATE INDEX IF NOT EXISTS idx_rc_drawing_version
    ON review_certs (drawing_version_id);

-- 按项目查所有审图记录（RULE-002 合规证明）
CREATE INDEX IF NOT EXISTS idx_rc_project
    ON review_certs (project_ref, status);

-- ── 资源批次初始化 ─────────────────────────────────────────────
-- 审图中心和出版中心的 Genesis UTXO
-- 插入 genesis_utxos 表，作为两个中心的资源来源

-- 注意：constraint_json 里记录了消耗约束
-- 每次 IssueReviewCert 和 PublishDrawing 都是对这两个 Genesis 的消耗

INSERT INTO genesis_utxos
    (ref, tenant_id, project_ref, total_quota, quota_unit,
     consumed_quota, status, proof_hash, constraint_json)
VALUES
    -- 审图章（总院独有，每次签发消耗1次，没有上限）
    ('v://zhongbei/genesis/right/review_stamp',
     '10000', 'v://zhongbei/project/root',
     999999, 'TIMES',
     0, 'ACTIVE', '',
     '{
       "resource_type": "RIGHT_REVIEW_STAMP",
       "head_office_only": true,
       "require_cert": "REG_STRUCT",
       "require_resolution_rate": 100,
       "require_major_count_zero": true,
       "require_chief_engineer_sign": true,
       "description": "总院审图章——RULE-002核心资源，每次签发审图合格证消耗1次"
     }'),

    -- 出版权（总院出版中心，每次出版消耗1次，没有上限）
    ('v://zhongbei/genesis/right/publish',
     '10000', 'v://zhongbei/project/root',
     999999, 'TIMES',
     0, 'ACTIVE', '',
     '{
       "resource_type": "RIGHT_PUBLISH",
       "head_office_only": true,
       "require_review_cert": true,
       "require_unique_drawing_no": true,
       "require_supersede_prev_version": true,
       "description": "出版权——必须持有有效审图合格证方可消耗"
     }')

ON CONFLICT (ref) DO NOTHING;

-- ── 说明 ──────────────────────────────────────────────────────
-- 使用流程：
--
-- 1. 主设工程师提交图纸草稿
--    INSERT drawing_versions (status='DRAFT', ...)
--
-- 2. 审图中心签发审图合格证（消耗 RIGHT_REVIEW_STAMP）
--    POST /api/v1/publishing/review-cert
--    → 验证主审工程师 REG_STRUCT 证书
--    → 验证意见处理率 = 100%，重大意见 = 0
--    → INSERT review_certs (status='PASSED')
--    → UPDATE genesis_utxos SET consumed_quota = consumed_quota + 1
--
-- 3. 出版中心出版图纸（消耗 RIGHT_PUBLISH）
--    POST /api/v1/publishing/publish
--    → 验证关联审图合格证 status='PASSED' 且未过期
--    → 旧版本 UPDATE status='SUPERSEDED'
--    → INSERT drawing_versions (status='PUBLISHED', proof_hash=...)
--    → UPDATE genesis_utxos SET consumed_quota = consumed_quota + 1
--
-- 4. 施工方查询当前有效版本
--    GET /api/v1/publishing/drawings/{drawing_no}/current
--    → 只返回 status='PUBLISHED' 的版本
--    → 附带 proof_hash，施工方可独立验证
