# Resource Addressing v0.1

This document defines a minimal, stable naming scheme for scarce management-fee resources.

## Scope

Core resources:

1. company credentials
2. person credentials
3. authority rights (review stamp/sign/invoice)
4. experience assets
5. contract quota and settlement capacity

All resources must be addressable via `v://` refs, even when stored in PostgreSQL.

## Naming Convention

1. Company credential
`v://{tenant}/credential/company/{holder}/{cert_type}/{cert_no}@v1`

2. Person credential
`v://{tenant}/credential/person/{holder}/{cert_type}/{cert_no}@v1`

3. Rights
`v://{tenant}/right/{right_type}/{holder}@v1`

4. Experience
`v://{tenant}/experience/project/{project_ref}/{utxo_ref}@v1`

5. Finance
`v://{tenant}/finance/contract/{id}@v1`
`v://{tenant}/finance/settlement/{id}@v1`

## Current Landing in This Repo

1. API resource list:
- `GET /api/v1/projects/{ref}/resources`
- response now includes `resource_refs` grouped by kind.

2. Migration script:
- `scripts/add_resource_refs_v1.sql`
- adds/backfills:
  - `contracts.ref`
  - `achievement_utxos.experience_ref`
  - `balances.settlement_ref`
  - `credentials.ref`
  - `qualifications.ref`

3. Rights table:
- `scripts/add_rights_table.sql`
- resource:
  - `v://{tenant}/right/{right_type}/{holder_ref}@v1`
- API:
  - `POST /api/v1/rights`
  - `GET /api/v1/rights`

4. Baseline schema:
- `scripts/migrate_pg_schema.sql` includes the same columns/indexes for fresh environments.

## Verification

1. Run migration:
```sql
\i scripts/add_resource_refs_v1.sql
```

2. Query a project resources pack:
```bash
curl "http://127.0.0.1:8090/api/v1/projects/{project_ref}/resources"
```

3. Expect response key:
- `resource_refs.contracts`
- `resource_refs.credentials`
- `resource_refs.experiences`
- `resource_refs.settlements`
- `resource_refs.projects`
