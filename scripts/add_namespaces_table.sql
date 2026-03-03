-- ============================================================
--  CoordOS — namespaces 表
--  协议网络路由表
--
--  v:// 命名空间是天然的多租户边界。
--  这张表让命名空间显式化：
--    - 每个院/组织/团队是一个命名空间
--    - 命名空间之间有父子关系（总院→分院→项目组）
--    - 每个命名空间继承哪些 RULE
--    - 每个命名空间能访问哪些 Genesis UTXO（稀缺资源）
--
--  Trip 流转时，引擎查这张表决定：
--    ① 这个执行体有没有权限消耗目标资源
--    ② 这次操作受哪些 RULE 约束
--    ③ 跨命名空间的 Trip 需要哪些额外验证
-- ============================================================

CREATE TABLE IF NOT EXISTS namespaces (
    -- 身份
    ref              text         PRIMARY KEY,
    -- 格式：v://{org}  如 v://zhongbei, v://chengdu-branch

    name             text         NOT NULL,   -- 显示名称：中北工程总院
    short_code       text         NOT NULL,   -- 短码：zhongbei（唯一）
    UNIQUE (short_code),

    org_type         text         NOT NULL DEFAULT 'BRANCH',
    -- HEAD_OFFICE    总院（拥有审图章、开票权等稀缺资源）
    -- BRANCH         分院（执行主体，受总院规则约束）
    -- PARTNER        合作院（外部协作，受托执行特定 SPU）
    -- PROJECT_GROUP  项目组（命名空间内的最小执行单元）
    CHECK (org_type IN ('HEAD_OFFICE','BRANCH','PARTNER','PROJECT_GROUP')),

    -- 层级关系
    parent_ref       text REFERENCES namespaces(ref),
    -- 总院：NULL
    -- 分院：v://zhongbei
    -- 项目组：v://zhongbei 或 v://chengdu-branch

    depth            int          NOT NULL DEFAULT 0,
    -- 0=总院, 1=分院/合作院, 2=项目组

    path             text         NOT NULL DEFAULT '',
    -- 完整路径：v://zhongbei/v://chengdu-branch
    -- 用于快速查找所有子命名空间

    -- 继承的规则集（来自总院，分院不可覆盖）
    inherited_rules  text[]       NOT NULL DEFAULT '{}',
    -- ['RULE-001','RULE-002','RULE-003','RULE-004','RULE-005']
    -- 分院所有 Trip 都在这些规则约束下流转

    -- 可访问的稀缺资源（Genesis UTXO ref 列表）
    accessible_genesis text[]     NOT NULL DEFAULT '{}',
    -- 分院不拥有审图章，但可以发起 resource-call 请求总院的审图章
    -- 总院命名空间：['v://zhongbei/genesis/right/review_stamp',
    --               'v://zhongbei/genesis/right/invoice', ...]
    -- 分院命名空间：[]（通过 resource-call 向父级请求）

    -- 命名空间自有的 Genesis UTXO（本院直接拥有的资源）
    owned_genesis    text[]       NOT NULL DEFAULT '{}',
    -- 分院可能拥有自己的人力资源 Genesis、项目额度 Genesis 等

    -- 管理费协议（分院向总院上交）
    manage_fee_rate  numeric(5,4) NOT NULL DEFAULT 0,
    -- 0.08 = 8%，分院每笔收款的8%上交总院

    -- 状态
    status           text         NOT NULL DEFAULT 'ACTIVE',
    CHECK (status IN ('ACTIVE','SUSPENDED','DISSOLVED')),

    -- Trip 路由策略
    -- 当分院发起需要总院资源的 Trip，如何路由到总院验证
    route_policy     jsonb        NOT NULL DEFAULT '{}',
    -- {
    --   "review_stamp": "require_parent_approval",
    --   "invoice":      "require_parent_approval",
    --   "settlement":   "notify_parent"
    -- }

    -- 元数据
    tenant_id        int          NOT NULL DEFAULT 10000,
    -- 总院的 tenant_id，所有子命名空间共享同一个 tenant_id
    -- 这是和传统多租户的关键区别：
    -- 传统多租户：不同租户完全隔离
    -- Trip多租户：同一协议网络内，不同命名空间，共享规则引擎

    contact_name     text,
    contact_phone    text,
    address          text,
    established_at   date,

    created_at       timestamptz  NOT NULL DEFAULT NOW(),
    updated_at       timestamptz  NOT NULL DEFAULT NOW()
);

-- 按父级查子命名空间（扩张时批量管理分院）
CREATE INDEX IF NOT EXISTS idx_ns_parent
    ON namespaces (parent_ref);

-- 按路径前缀查（找某个总院下的所有命名空间）
CREATE INDEX IF NOT EXISTS idx_ns_path
    ON namespaces (path text_pattern_ops);

-- 按状态查活跃命名空间
CREATE INDEX IF NOT EXISTS idx_ns_status
    ON namespaces (status, org_type);


-- ── 命名空间间 Trip 授权表 ──────────────────────────────────
-- 记录哪些命名空间被授权在哪些命名空间里执行什么 SPU
-- 这是跨组织委托的协议层记录

CREATE TABLE IF NOT EXISTS namespace_delegations (
    id               bigserial    PRIMARY KEY,

    -- 委托方（发包方）
    delegator_ref    text         NOT NULL REFERENCES namespaces(ref),
    -- v://zhongbei

    -- 受托方（执行方）
    delegate_ref     text         NOT NULL REFERENCES namespaces(ref),
    -- v://chengdu-branch

    -- 授权范围
    allowed_spus     text[]       NOT NULL DEFAULT '{}',
    -- ['v://zhongbei/spu/bridge/pile_foundation_drawing@v1', ...]
    -- 空数组=不限制 SPU 类型

    allowed_projects text[]       NOT NULL DEFAULT '{}',
    -- 限定到具体项目，空=该委托方名下所有项目

    -- 资源约束
    max_contract_amount numeric,
    -- 单次委托合同额上限，NULL=不限

    -- 必须回流的操作（受托方做了这些操作，必须通知委托方）
    require_notify   text[]       NOT NULL DEFAULT '{}',
    -- ['ISSUE_REVIEW_CERT', 'ISSUE_INVOICE', 'SETTLE']

    -- 委托有效期
    valid_from       date         NOT NULL DEFAULT CURRENT_DATE,
    valid_until      date,        -- NULL=长期有效

    status           text         NOT NULL DEFAULT 'ACTIVE',
    CHECK (status IN ('ACTIVE','EXPIRED','REVOKED')),

    -- 存证
    proof_hash       text,
    contract_ref     text,        -- 关联的框架协议合同

    tenant_id        int          NOT NULL DEFAULT 10000,
    created_at       timestamptz  NOT NULL DEFAULT NOW(),
    updated_at       timestamptz  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nd_delegator
    ON namespace_delegations (delegator_ref, status);
CREATE INDEX IF NOT EXISTS idx_nd_delegate
    ON namespace_delegations (delegate_ref, status);


-- ── 种子数据：中北工程总院命名空间 ────────────────────────────

INSERT INTO namespaces
    (ref, name, short_code, org_type, parent_ref, depth, path,
     inherited_rules, accessible_genesis, owned_genesis,
     manage_fee_rate, route_policy, tenant_id)
VALUES
    -- 总院
    ('v://zhongbei',
     '中北工程设计咨询有限公司', 'zhongbei',
     'HEAD_OFFICE', NULL, 0, 'v://zhongbei',
     ARRAY['RULE-001','RULE-002','RULE-003','RULE-004','RULE-005'],
     ARRAY[
       'v://zhongbei/genesis/right/review_stamp',
       'v://zhongbei/genesis/right/invoice',
       'v://zhongbei/genesis/right/publish'
     ],
     ARRAY[
       'v://zhongbei/genesis/right/review_stamp',
       'v://zhongbei/genesis/right/invoice',
       'v://zhongbei/genesis/right/publish'
     ],
     0,
     '{"review_stamp":"self","invoice":"self","settlement":"self"}',
     10000)

ON CONFLICT (ref) DO NOTHING;

-- ── 新增分院示例（实际使用时通过 API 动态创建）──────────────
-- 创建成都分院：
-- POST /api/v1/namespaces
-- {
--   "name": "中北工程成都分院",
--   "short_code": "chengdu-branch",
--   "org_type": "BRANCH",
--   "parent_ref": "v://zhongbei",
--   "manage_fee_rate": 0.08
-- }
--
-- 系统自动：
--   ref = v://chengdu-branch
--   inherited_rules = 继承总院的5条RULE
--   accessible_genesis = []（通过resource-call向总院请求）
--   route_policy = {"review_stamp":"require_parent_approval",...}
--   path = "v://zhongbei/v://chengdu-branch"
