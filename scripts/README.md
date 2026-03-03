# Scripts Quick Guide

This directory stores production-use scripts only.

## Layout
- `scripts/migrate_pg_schema.sql`: PostgreSQL baseline schema.
- `scripts/add_*.sql`, `scripts/backfill_*.sql`, `scripts/query_*.sql`: migration/backfill/query helpers.
- `scripts/migrations/`: incremental schema scripts organized by domain.
- `scripts/gen_qual_report.js`: qualification report export.

## Conventions
- Do not place SQL in service source folders; put all SQL under `scripts/`.
- Do not keep local smoke/test/demo scripts in this repo tree.

## Build check
```bash
go build ./services/design-institute
go build ./services/vault-service
```
