# Cutover Runbook: iCRM -> CoordOS (cn.zhongbei)

This runbook migrates production-like data from iCRM MySQL dump into CoordOS PostgreSQL,
then binds the data into the `v://cn.zhongbei` namespace model.

## 0. Scope and principles

- Source of truth for historical business data: `icrm_20260122020001.7z`
- Target runtime data store: PostgreSQL used by `services/design-institute`
- Protocol layer addressing: `v://cn.zhongbei` (hierarchical namespace)
- No manual row-by-row edits. Use migration scripts and API workflows only.

## 1. Preconditions

- Repo root: `C:\Users\xm_91\Desktop\coordos`
- PostgreSQL is reachable with `DATABASE_URL`
- Python environment can run `scripts/migrate.py`
- Docker is available (for temporary MySQL 5.7 restore)
- A maintenance window is scheduled (write freeze on old system if needed)

Recommended env:

```powershell
$env:DATABASE_URL="postgres://coordos:coordos@127.0.0.1:5432/coordos?sslmode=disable"
$env:TENANT_ID="10000"
```

## 2. Checkpoint strategy (must do)

Define hard checkpoints with rollback assets:

- `CP0`: Before any DB operation
- `CP1`: After MySQL dump restore
- `CP2`: After PostgreSQL schema init
- `CP3`: After phased migration (`company/employee/contract/finance/drawing/supplement`)
- `CP4`: After namespace + registration into `cn.zhongbei`
- `CP5`: After optional cleanup of test/demo data

At each checkpoint:

1. Save command log and SQL output.
2. Save row-count snapshot (use `scripts/verify_cutover_cn_zhongbei.sql`).
3. Save DB backup artifact.

## 3. CP0 backup (rollback anchor)

Backup PostgreSQL before touching data:

```powershell
mkdir backups -ErrorAction SilentlyContinue | Out-Null
pg_dump "$env:DATABASE_URL" -Fc -f backups\cp0_before_cutover.dump
```

If an existing MySQL is already used in environment, back it up too.

## 4. Restore source dump to temporary MySQL (CP1)

Start temporary MySQL 5.7:

```powershell
docker rm -f icrm57 2>$null
docker run -d --name icrm57 `
  -e MYSQL_ROOT_PASSWORD=123456 `
  -e MYSQL_DATABASE=icrm `
  -p 3307:3306 `
  mysql:5.7 --character-set-server=utf8mb4 --collation-server=utf8mb4_general_ci
```

Import `.7z` SQL stream:

```powershell
tar -xOf C:\Users\xm_91\Desktop\icrm_20260122020001.7z icrm_20260122020001.sql `
| docker exec -i icrm57 mysql -uroot -p123456 icrm
```

Quick source check:

```powershell
docker exec -i icrm57 mysql -uroot -p123456 -e "USE icrm; SELECT COUNT(*) AS c FROM contract; SELECT COUNT(*) AS c FROM company; SELECT COUNT(*) AS c FROM invoice;"
```

Rollback from CP1:

- Stop cutover.
- `docker rm -f icrm57`
- Return to CP0 PostgreSQL backup.

## 5. Initialize PostgreSQL schema (CP2)

```powershell
psql "$env:DATABASE_URL" -f scripts/migrate_pg_schema.sql
psql "$env:DATABASE_URL" -f scripts/migrations/20260304_traceability_phase2.sql
psql "$env:DATABASE_URL" -f scripts/add_achievement_lib.sql
psql "$env:DATABASE_URL" -f scripts/add_capability_engine.sql
psql "$env:DATABASE_URL" -f scripts/add_resource_refs_v1.sql
psql "$env:DATABASE_URL" -v tenant_id=10000 -f scripts/backfill_contract_project_binding.sql
```

Take backup:

```powershell
pg_dump "$env:DATABASE_URL" -Fc -f backups\cp2_schema_ready.dump
```

Rollback from CP2:

- Restore `backups\cp0_before_cutover.dump`.

## 6. Run phased migration (CP3)

Configure migration script source/target:

```powershell
$env:MYSQL_HOST="127.0.0.1"
$env:MYSQL_PORT="3307"
$env:MYSQL_USER="root"
$env:MYSQL_PASSWORD="123456"
$env:MYSQL_DATABASE="icrm"

$env:PG_HOST="127.0.0.1"
$env:PG_PORT="5432"
$env:PG_USER="coordos"
$env:PG_PASSWORD="coordos"
$env:PG_DATABASE="coordos"
$env:TENANT_ID="10000"
```

Execute:

```powershell
python scripts/migrate.py --phase company
python scripts/migrate.py --phase employee
python scripts/migrate.py --phase contract
python scripts/migrate.py --phase finance
python scripts/migrate.py --phase drawing
python scripts/migrate.py --phase approve_history
python scripts/migrate.py --phase cost_payment
python scripts/migrate.py --phase artifacts
python scripts/migrate.py --phase supplement
python scripts/migrate.py --phase verify
```

Take backup:

```powershell
pg_dump "$env:DATABASE_URL" -Fc -f backups\cp3_migrated.dump
```

Rollback from CP3:

- Restore `backups\cp2_schema_ready.dump`.

## 7. Bind to protocol namespace model (CP4)

Important: use `cn.zhongbei` hierarchy and avoid old one-off hardcoded `v://zhongbei` seeds for new registrations.

1. Register organization in UI/API with:
   - `short_code = "cn.zhongbei"`
2. Validate generated namespace fields:
   - `namespace_ref = v://cn.zhongbei`
   - `parent_ref = v://cn`
   - `depth = 2`
3. Import engineers/executors (step 2/3 in register flow).
4. Validate partner profile endpoint:
   - `GET /public/v1/partner-profile/cn.zhongbei`

Take backup:

```powershell
pg_dump "$env:DATABASE_URL" -Fc -f backups\cp4_namespace_bound.dump
```

Rollback from CP4:

- Restore `backups\cp3_migrated.dump`.

## 8. Optional cleanup test/demo data (CP5)

Run dry-run first (default tail is `ROLLBACK`):

```powershell
psql "$env:DATABASE_URL" -f scripts/cleanup_keep_real_remove_test_demo.sql
```

If result set is confirmed correct:

1. Edit file tail from `ROLLBACK` to `COMMIT`.
2. Re-run script.

Take backup:

```powershell
pg_dump "$env:DATABASE_URL" -Fc -f backups\cp5_cleaned.dump
```

Rollback from CP5:

- Restore `backups\cp4_namespace_bound.dump`.

## 9. Acceptance gates (must pass before release)

Run:

```powershell
psql "$env:DATABASE_URL" -f scripts/verify_cutover_cn_zhongbei.sql
```

Must pass:

- Core tables have non-zero migrated data.
- `migration_log` failed count is zero or fully explained and accepted.
- Namespace `v://cn.zhongbei` exists and is active.
- `project_ref/ref/executor_ref` key fields are populated for migrated rows.
- Partner profile and resolver APIs return expected payloads.

## 10. Service startup and smoke test

```powershell
go run ./services/design-institute
```

Smoke:

```powershell
curl -s http://127.0.0.1:8081/public/v1/partner-profile/cn.zhongbei
curl -s -X POST http://127.0.0.1:8081/api/v1/resolve/resolve `
  -H "Content-Type: application/json" `
  -d "{\"spu_ref\":\"v://cn.zhongbei/spu/bid/preparation@v1\",\"required_quals\":[\"REG_STRUCTURE\"],\"tenant_id\":10000}"
```

## 11. Final cutover sign-off checklist

- [ ] Backups for CP0/CP2/CP3/CP4 (and CP5 if used) are present.
- [ ] Migration phase logs archived.
- [ ] `verify_cutover_cn_zhongbei.sql` output archived.
- [ ] Partner profile page data and API are consistent.
- [ ] Rollback command owner and recovery SLA confirmed.
