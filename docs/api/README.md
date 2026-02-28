# API Documentation (OpenAPI 3.0)

This directory provides implementation-aligned OpenAPI specs for the current services.

## Files

- `openapi.design-institute.yaml`
  - API contract baseline for `services/design-institute`.
  - Covers all routed endpoints currently registered in the handler.
- `openapi.vault-service.yaml`
  - API contract baseline for `services/vault-service`.
  - Includes authentication scheme and project/genesis/event flows.

## Design principles used

1. Endpoint-complete first
   - Every registered route is represented under `paths`.
2. Schema evolution friendly
   - Dynamic payload-heavy endpoints use permissive object envelopes.
3. Contract-test ready
   - Operation IDs, status codes, and key request parameters are explicit.

## Suggested usage

1. Generate interactive docs:
   - `swagger-ui` / `redoc` pointing to these YAML files.
2. Generate typed clients:
   - `openapi-generator` / `oapi-codegen`.
3. Add CI validation:
   - Validate YAML syntax and run route-vs-spec drift checks in pipeline.
