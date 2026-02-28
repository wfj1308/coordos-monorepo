# CoordOS Monorepo

中国工程设计行业委托链协议引擎 + 设计院管理系统

---

## 目录结构

```
coordos/
├── packages/                      # 共享协议包（Go）
│   ├── vuri/                      # v:// 路由协议
│   ├── project-core/              # 四元公理协议引擎
│   └── spu-runtime/               # SPU 执行引擎（TypeScript）
│
├── services/
│   ├── design-institute/          # 设计院管理系统（业务层）
│   │   ├── project/               # 项目树服务（骨架，其他服务依赖）
│   │   ├── contract/              # 合同服务
│   │   ├── gathering/             # 收款服务
│   │   ├── settlement/            # 结算服务
│   │   ├── invoice/               # 发票服务
│   │   ├── costticket/            # 费用单服务
│   │   ├── payment/               # 付款服务（RULE-003 拦截）
│   │   ├── company/               # 分公司服务
│   │   ├── employee/              # 员工服务
│   │   ├── achievement/           # 业绩库（SPU 产出 UTXO 落地）
│   │   ├── approve/               # 审批流服务
│   │   └── report/                # 报表服务（聚合查询）
│   │
│   ├── vault-service/             # 主权节点服务（协议层）
│   │   ├── api/                   # HTTP 端点
│   │   │   ├── server.go          # 8 个协议端点
│   │   │   └── utxo_ingest.go     # POST /api/utxo/ingest（SPU对接唯一入口）
│   │   ├── app/                   # 用例层
│   │   └── infra/
│   │       ├── store/             # Store 接口定义
│   │       └── dualwrite/         # MySQL↔PG 双写中间件
│   │
│   └── sovereign-skills/          # SPU 技能库（TypeScript）
│       └── bridge/                # 桥梁专业技能实现
│
├── specs/spu/bridge/              # 桥梁 SPU 规格库（JSON）
│   ├── catalog.v1.json            # 完整闭合链 + DisputePRC 举证地图
│   └── *.v1.json                  # 10 个 SPU 规格文件
│
├── protocols/zhongbei/            # 中北租户协议配置
│
├── scripts/                       # 迁移脚本
│   ├── migrate_pg_schema.sql      # PostgreSQL 建表
│   └── migrate.py                 # MySQL→PG 存量迁移
│
├── docs/api/                      # API 文档（待补充）
├── ui/design-institute/           # 前端（待开发）
└── go.work                        # Go workspace
```

---

## 层次关系

```
协议层（不变）                     业务层（按院配置）
─────────────────────────          ──────────────────────────────
packages/vuri                      services/design-institute/
packages/project-core                project   contract   invoice
  RULE-001~005                        gathering  settlement  payment
  ProjectNode / GenesisUTXO          company   employee  achievement
  FissionEngine / StateMachine        costticket  approve   report
        ↑                                   ↑
        └──────── services/vault-service ───┘
                  （协议引擎的 HTTP 入口）
```

---

## 四元公理

| 公理 | 内容 |
|------|------|
| 1 | 世界只有**执行体**和**资源**（非执行体即资源） |
| 2 | **SPU** 三位一体：标准产品单元 + 标注参数单元 + 标准过程单元 |
| 3 | 执行体消耗资源，按 SPU 规格产出 **UTXO** |
| 4 | UTXO 作为资源进入下一个 SPU（UTXO 天然是执行体） |

## 五条物理定律

| 规则 | 内容 |
|------|------|
| RULE-001 | 执行体能力级 ≥ SPU 要求 |
| RULE-002 | 总院必须保留审图权/交付权/开票权/质量责任 |
| RULE-003 | 对外付款必须引用有效合同，金额不得超额 |
| RULE-004 | 子 GenesisUTXO 额度之和 ≤ 父 GenesisUTXO 额度 |
| RULE-005 | 叶子节点有实际产出 UTXO 才能向上触发结算 |

---

## 两套系统的边界

```
桥梁 SPU 系统                      设计院管理系统
（出图 / 计算 / 验收）              （钱 / 人 / 合同 / 业绩 / 结算）

SPU 执行完成
  → POST /api/utxo/ingest   →   achievement_utxos 入库
                                      ↓ 自动匹配合同
                                 GenesisUTXO 消耗检查（RULE-005）
                                      ↓
                                 结算触发 → 钱包分账

接口只有这一条，两套系统独立运行
```

---

## 桥梁 SPU 闭合链

**桥墩 10 个 SPU，设计→施工→竣工→结算全覆盖**

```
seq  SPU                      类型     阶段
 1   pile_foundation_drawing  DRAWING  设计
 2   pile_cap_drawing         DRAWING  设计
 3   pier_rebar_drawing       DRAWING  设计
 4   superstructure_drawing   DRAWING  设计
 5   concealed_acceptance     RECORD   施工   ← 三方签字
 6   prestress_record         RECORD   施工   ← IoT 接入
 7   concrete_strength_report REPORT   施工
 8   coordinate_proof         PROVE    竣工
 9   review_certificate       CERT     竣工   ← RULE-002 触发点
10   settlement_cert          CERT     结算   ← 钱包分账触发
```

---

## 数据迁移

**来源：MySQL 5.7 iCRM（216 表）**

核心数据量：合同 ~19500 条 / 分公司 ~631 家 / 收款 ~18291 条 / 发票 ~19651 条

```bash
# 按序执行，前一步稳定再跑下一步
python3 scripts/migrate.py --phase company    # 631 家分公司
python3 scripts/migrate.py --phase employee   # 435 名员工
python3 scripts/migrate.py --phase contract   # 19500 条合同（含委托链重建）
python3 scripts/migrate.py --phase finance    # 收款 / 结算 / 发票
python3 scripts/migrate.py --phase drawing    # 9263 条图纸
python3 scripts/migrate.py --phase verify     # 校验
```

---

## Go 模块

```
go.work
  packages/vuri              → module coordos/vuri
  packages/project-core      → module coordos/project-core
  services/vault-service     → module coordos/vault-service
  services/design-institute  → module coordos/design-institute
```
