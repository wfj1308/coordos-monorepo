# Scripts Guide

This directory stores operational scripts for schema migration, data backfill, cleanup, and verification.

## Directory Layout
- `scripts/migrate_pg_schema.sql`: PostgreSQL baseline schema.
- `scripts/migrations/*.sql`: incremental schema migrations (date-prefixed).
- `scripts/add_*.sql`: add one domain/table/functionality.
- `scripts/backfill_*.sql`: fill missing refs/derived data.
- `scripts/cleanup_*.sql`: cleanup by rule, dry-run by default when possible.
- `scripts/verify_*.sql`: verification/report scripts (read-only).
- `scripts/query_*.sql`: ad-hoc query helpers.
- `scripts/internal/`: utility scripts (lint/check/ops helper).
- `scripts/export_jdy_regulations.py`: export JianDaoYun regulations to CSV.
- `scripts/migrate.py`: legacy-to-CoordOS data migration tool.

## Naming Rules
- Use lowercase snake_case only.
- `scripts/migrations/` SQL should use: `YYYYMMDD_<action>_<domain>.sql`.
- Root-level SQL should use verb prefix:
  - `add_`, `create_`, `backfill_`, `cleanup_`, `delete_`, `fix_`, `verify_`, `query_`, `apply_`, `bootstrap_`, `migrate_`.
- Compatibility variant suffix recommendation:
  - Prefer `_compat.sql` for new files.
  - Existing `_rebuild.sql` is allowed for backward compatibility.

## Execution Safety Rules
- Prefer transaction guard for mutating SQL:
  - `BEGIN; ...`
  - `\if :{?apply} ... \endif`
  - default `ROLLBACK`, and `-v apply=true` to `COMMIT`.
- Keep scripts idempotent where possible (`IF NOT EXISTS`, `ON CONFLICT`, defensive updates).
- Do not place SQL under service source folders.
- Do not commit local test/demo/smoke scripts to this tree.

## Naming Checker
Run filename style check:

```bash
python scripts/internal/check_scripts_naming.py
```

It validates naming format and reports compatibility/legacy warnings.

## Build Check
```bash
go build ./services/design-institute
go build ./services/vault-service
```

## Regulation Export (JianDaoYun -> CSV)
```bash
python scripts/export_jdy_regulations.py \
  --dash-url "https://<tenant>.jiandaoyun.com/dash/<dash_id>" \
  --output "scripts/regulations_jdy_export.csv"
```

Then run regulation migration with the exported CSV:
```bash
REGULATION_SOURCE_CSV=scripts/regulations_jdy_export.csv python scripts/migrate.py --phase regulation
```

Run data quality gate for three libraries:
```bash
python scripts/migrate.py --phase quality_gate
```

Block pipeline on red quality gate (recommended in CI/import jobs):
```bash
python scripts/migrate.py --phase regulation --quality-gate-strict
python scripts/migrate.py --phase verify --quality-gate-strict
```

## Historical Achievement Backfill
Build historical `achievement_utxos` from migrated contracts (idempotent):

```bash
python scripts/migrate.py --phase achievement
```
