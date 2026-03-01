# Scripts Quick Guide

This directory only keeps production migration/backfill/report utilities.

## Kept scripts
- `migrate.py`: legacy MySQL -> PostgreSQL data migration helper.
- `migrate.sh`: code migration helper from old repo.
- `migrate_pg_schema.sql`: PostgreSQL schema baseline.
- `add_*.sql`, `backfill_*.sql`, `query_*.sql`: PG maintenance and data-fix SQL.
- `gen_qual_report.js`, `gen_qual_report_from_api.*`: qualification report export.

## Removed on purpose
- all `*_test.go` files
- all test/e2e/smoke scripts
- all flow-run validation scripts
- duplicated internal migration helper (`scripts/internal/migrate_code.sh`)

## Build check
```bash
go build ./services/design-institute
go build ./services/vault-service
```
