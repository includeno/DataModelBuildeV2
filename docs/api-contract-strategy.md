# API Contract Strategy

## Versioning

- Legacy endpoints remain available under unversioned paths such as `/projects/*` and `/sessions/*`.
- Contract-stable endpoints use `/v2/*`.
- The `v2` contract uses a unified response envelope:
  - `data`
  - `error`
  - `meta`
  - `request_id`

## Naming

- Resources use plural nouns: `/projects`, `/organizations`, `/jobs`.
- Nested resources stay scoped: `/projects/{project_id}/members`.
- Query parameters use camelCase when a public name already exists, such as `pageSize` and `sinceVersion`.

## Compatibility

- `/sessions/*` remains available as the legacy compatibility surface.
- When `legacy_session_project_bridge_enabled=true`, project-backed ids like `prj_*` can be read through `/sessions/*`.
- Legacy endpoints emit:
  - `Deprecation: true`
  - `Sunset`
  - `Link: </docs/api-contract-strategy.md>; rel="deprecation"`

## Request Tracking

- Every HTTP response includes `X-Request-ID`.
- Clients may provide `X-Request-ID`; otherwise the backend generates one.

## Retry Safety

- `Idempotency-Key` is supported on:
  - `POST /projects/{project_id}/state/commit`
  - `POST /projects/{project_id}/execute`
  - `POST /projects/{project_id}/jobs/execute`
  - `POST /projects/{project_id}/export`

## Rate Limits

- Write-heavy endpoints are rate limited per user and project:
  - commit
  - upload
  - execute
  - export

## OpenAPI

- Runtime schema is exposed at `/openapi.json`.
- Generated artifact is stored at `backend/openapi.generated.json`.
