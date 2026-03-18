# API Changelog

## 2026-03-19

- Added `v2` contract endpoints with response envelope support.
- Added `X-Request-ID` propagation on HTTP responses.
- Added deprecated headers for legacy `/sessions/*` and root compatibility endpoints.
- Added `Idempotency-Key` support for project commit, execute, async execute, and export.
- Added per-project rate limiting for upload, commit, and execute flows.
- Added OpenAPI schema generation output at `backend/openapi.generated.json`.
