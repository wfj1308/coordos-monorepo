-- ============================================================
--  CoordOS — credentials 表
--  资质/证书/授权权限的主数据
--  对应 packages/resolver/types.go 的 Credential 结构
-- ============================================================

CREATE TABLE IF NOT EXISTS credentials (
    id           bigserial    PRIMARY KEY,

    -- 持有方（人或企业）
    holder_ref   text         NOT NULL,   -- executor_ref 或 company_ref
    holder_type  text         NOT NULL,   -- PERSON / COMPANY
    CHECK (holder_type IN ('PERSON', 'COMPANY')),

    -- 证书信息
    cert_type    text         NOT NULL,   -- REG_STRUCT / REG_ARCH / COMP_A / RIGHT_INVOICE ...
    cert_number  text,                    -- 证书编号，企业资质可为空
    issued_at    date,                    -- 颁发日期
    expires_at   date,                    -- 到期日，NULL=长期有效

    -- 授权范围：允许执行的 SPU ref 列表，逗号分隔，空=不限
    scope        text         NOT NULL DEFAULT '',

    -- 状态
    status       text         NOT NULL DEFAULT 'ACTIVE',
    CHECK (status IN ('ACTIVE', 'EXPIRED', 'REVOKED', 'SUSPENDED')),

    tenant_id    int          NOT NULL,
    created_at   timestamptz  NOT NULL DEFAULT NOW(),
    updated_at   timestamptz  NOT NULL DEFAULT NOW()
);

-- 按持有人查有效证书（Verify 主查询）
CREATE INDEX IF NOT EXISTS idx_credentials_holder
    ON credentials (holder_ref, status, expires_at);

-- 按证书类型查持证人（Resolve 主查询）
CREATE INDEX IF NOT EXISTS idx_credentials_type
    ON credentials (tenant_id, cert_type, status, expires_at);

-- ── 初始种子数据：中北工程设计咨询有限公司 ──────────────────
-- 企业资质（总院）
INSERT INTO credentials
    (holder_ref, holder_type, cert_type, cert_number, issued_at, expires_at, scope, status, tenant_id)
VALUES
    -- 综合甲级资质
    ('v://zhongbei/company/headquarters/zbgc', 'COMPANY',
     'COMP_COMPREHENSIVE_A', 'A134002344', '2020-01-01', NULL, '', 'ACTIVE', 10000),

    -- 开票权（总院法人实体）
    ('v://zhongbei/company/headquarters/zbgc', 'COMPANY',
     'RIGHT_INVOICE', NULL, '2020-01-01', NULL, '', 'ACTIVE', 10000),

    -- 总院审图盖章权
    ('v://zhongbei/company/headquarters/zbgc', 'COMPANY',
     'RIGHT_REVIEW_STAMP', NULL, '2020-01-01', NULL, '', 'ACTIVE', 10000),

    -- 总院身份标记
    ('v://zhongbei/executor/headquarters/zbgc', 'COMPANY',
     'RIGHT_HEAD_OFFICE', NULL, '2020-01-01', NULL, '', 'ACTIVE', 10000)

ON CONFLICT DO NOTHING;

-- ── 注释：个人证书录入方式 ──────────────────────────────────
-- 员工注册证书通过 API 录入：
-- POST /api/v1/credentials
-- {
--   "holder_ref":   "v://zhongbei/executor/person/zhangsan",
--   "holder_type":  "PERSON",
--   "cert_type":    "REG_STRUCT",
--   "cert_number":  "19320710041",
--   "issued_at":    "2019-06-01",
--   "expires_at":   "2027-06-01",
--   "tenant_id":    10000
-- }
