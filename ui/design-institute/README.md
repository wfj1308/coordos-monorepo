# Design Institute UI Console (React + Tailwind)

Frontend console for validating CoordOS service behaviors.

Stack:
- React 18
- TailwindCSS
- Vite

## Install

```bash
cd ui/design-institute
npm install
```

## Run (dev)

```bash
npm run dev
```

Open: `http://127.0.0.1:5173`

Standalone partner profile page:
- `http://127.0.0.1:5173/partner-profile.html`

Standalone join page:
- `http://127.0.0.1:5173/join/` (recommended)
- `http://127.0.0.1:5173/join.html` (legacy compatibility)

## Build

```bash
npm run build
npm run preview
```

## Zhongbei Seed Quick Start

Run in repo `scripts/` directory:

```bash
psql "$DATABASE_URL" -f add_zhongbei_seed_prerequisites.sql
psql "$DATABASE_URL" -f seed_zhongbei_genesis.sql
psql "$DATABASE_URL" -f seed_zhongbei_engineers.sql
psql "$DATABASE_URL" -f seed_zhongbei_person_genesis.sql
psql "$DATABASE_URL" -f auto_settle_zhongbei_achievements.sql
```

Detailed runbook:

- `docs/deploy/DEPLOY_ZHONGBEI.md`

## Usage

1. Start backend services:
   - `go run ./services/design-institute`
   - `go run ./services/vault-service`
2. Optional for OCR extraction quality:
   - Set `CLAUDE_API_KEY` before starting `design-institute` service.
   - Without this key, certificate extraction falls back to regex + manual review mode.
3. Open UI in browser.
4. Run the built-in scenario:
   - `中北桥梁项目核心主流程闭环`
   - Covers: project -> contract -> employee -> qualification -> achievement -> invoice -> settlement -> project resources.
5. Or use quick templates / custom request console for ad-hoc API checks.

## Non-Engineer Executor Import

Endpoint:

- `POST /api/v1/register/org/{ns}/executors`
- `POST /api/v1/register/org/{ns}/engineers`
- `POST /api/v1/register/org`

Supported roles (built-in mapping):

- `税务申报员` `会计` `出纳` `合同管理员` `项目经理` `质量负责人`
- `进度管理员` `CAD制图员` `资料员` `外审协调员` `市场经理` `投标专员`

CSV/XLSX recommended columns:

- `姓名, 身份证号, 角色, 专业方向, 岗位, 并发上限, 内部证号, 有效期`

Template file:

- `scripts/seed_zhongbei_executors_template.csv`

Import example:

```bash
curl -X POST "http://127.0.0.1:8090/api/v1/register/org/zhongbei/executors" \
  -F "file=@scripts/seed_zhongbei_executors_template.csv" \
  -F "default_valid_until=2029-12-31" \
  -F "default_max_concurrent_tasks=5"
```

Engineer import example:

```bash
curl -X POST "http://127.0.0.1:8090/api/v1/register/org/zhongbei/engineers" \
  -F "file=@/path/to/engineers.csv" \
  -F "default_valid_until=2029-12-31"
```

## Capability Engine APIs

- `POST /api/v1/capability/violations`
- `GET /api/v1/capability/violations?ref=...`
- `GET /api/v1/capability/violations/{ref}`
- `GET /api/v1/capability/stats?ref=...`
- `GET /api/v1/capability/stats/{ref}`
- `POST /api/v1/capability/compute`
- `GET /api/v1/capability/org?ns=v://zhongbei&deep=true`
- `GET /api/v1/capability/org/{ns}?deep=true`

Note:
- Prefer the query-style endpoints for refs containing `/` (for example `v://...`) to avoid path escaping issues.

## Achievement Library (2-layer)

Run DDL first:

```bash
psql "$DATABASE_URL" -f scripts/add_achievement_lib.sql
```

Core objects:

- `achievement_utxos`: project layer, one completed project per row.
- `engineer_achievement_receipts`: engineer layer, one engineer x one project per row.
- `achievement_pool`: view for enterprise query, includes `within_3years` and `within_5years`.
- `engineer_achievement_pool`: view for engineer personal query.

Endpoints:

- `POST /api/v1/achievement/{ns}/batch/csv`
- `POST /api/v1/achievement/{ns}/batch/json`
- `GET /api/v1/achievement/{ns}`
- `GET /api/v1/achievement/{ns}/engineer/{id}`
- `GET /api/v1/achievement/verify?ref=...`
- `GET /api/v1/achievement/template/csv`

Proof hash rules:

- Project layer: `sha256(ref + inputs_hash + source + namespace_ref)`
- Engineer layer:
  - `inputs_hash = sha256(achievement_ref + executor_ref + container_ref + role)`
  - `proof_hash = sha256(receipt_ref + inputs_hash + source)`

CSV format (multi-row per project):

```csv
项目名称,业主单位,项目类型,合同金额(万元),完工年份,地区,规模,工程师ID,证书类型,角色,承担工作
陕西榆林绥德至延川高速,陕西省交通运输厅,HIGHWAY,45000,2023,陕西榆林,双向四车道高速,cyp4310,REG_STRUCTURE,DESIGN_LEAD,负责桥梁结构设计
,,,,,,,lz0012,REG_CIVIL_GEOTEC,PARTICIPANT,负责路基岩土勘察
榆林市道路改造,榆林市城投,MUNICIPAL,3200,2022,陕西榆林,城市主干道改造,dyc4019,REG_STRUCTURE,LEAD_ENGINEER,主持全程设计
```

Deployment order:

```bash
psql -d coordos -f scripts/add_achievement_lib.sql
# restart design-institute service
curl -OJ http://127.0.0.1:8090/api/v1/achievement/template/csv
curl -X POST "http://127.0.0.1:8090/api/v1/achievement/cn.zhongbei/batch/csv" \
  -F "file=@achievement_import_template.csv"
curl "http://127.0.0.1:8090/api/v1/achievement/cn.zhongbei?within_3years=true"
```

## Files

- `src/App.jsx`: page and request console.
- `src/PartnerProfilePage.jsx`: standalone public partner profile page.
- `src/index.css`: Tailwind entry and shared component styles.
- `tailwind.config.js`: theme and scanning config.
