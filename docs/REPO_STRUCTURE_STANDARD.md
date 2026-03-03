# Repository Structure Standard

This repository follows a service-first and migration-centralized layout.

## 1) Business Code
- `services/<service-name>/...`: service runtime code only (Go/TS), no SQL files.
- `packages/...`: reusable libraries.
- `ui/...`: frontend code.

## 2) Database Scripts
- All SQL must live under `scripts/`.
- Suggested sublayout:
  - `scripts/migrations/<domain>/...`
  - `scripts/backfill/...`
  - `scripts/query/...`
- Do not place SQL under `services/...` source folders.

## 3) Specs and Docs
- `specs/...`: protocol/resource specs (JSON schema/catalog).
- `docs/core/...`: architecture and domain docs.
- `docs/deploy/...`: deploy/runbook docs.

## 4) Cleanup Policy
- Temporary artifacts must not be committed: binaries/logs/tmp files.
- Local tests are excluded by current repo policy (`*_test.go` ignored/removed).
- Keep only files that are referenced by runtime, migration, or official docs.

## 5) Commit Gate (Practical)
Before commit, run:

```bash
git status --short
```

- Remove leftover `??` files that are not part of target feature.
- Ensure no binaries/logs/temp files are staged.

