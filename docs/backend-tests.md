**Backend Tests**
Test run (2026-03-09): `pytest -q` in `backend/`
Result: `248 passed in 6.13s`

**What’s Covered (by suite)**
- `test_api_basics.py`: basic API request/response behavior and baseline endpoints.
- `test_api_config.py`: backend configuration endpoints and validation flows.
- `test_api_execution.py`: command execution pipeline behavior (filter/join/group/transform/sort/view, variable handling, multi-step execution).
- `test_api_sql.py`: SQL parsing/execution API routes and error handling.
- `test_api_sql_generation.py`: SQL generation outputs for supported commands.
- `test_api_parquet_replace.py`: parquet imports with same dataset name replace prior tables.
- `test_api_identifiers.py`: dataset names with hyphens are supported; reserved keyword names are rejected.
- `test_scenario_general.py`: end-to-end scenario flows across operations and datasets.
- `test_scenario_sessions.py`: session lifecycle and storage behavior.
- `test_unit_sql.py`: unit-level SQL parsing/translation logic.

**Known Gaps / Not Covered Yet**
- Large-file and performance tests (parquet with millions of rows, memory pressure, paging limits).
- Concurrency and race conditions (multiple sessions executing in parallel, overlapping dataset imports).
- Storage edge cases (permission errors, missing folders, corrupted session state, disk full).
- Log-path configuration and rotation behavior (logs dir permissions, retention).
- Dataset preview API behavior under partial/invalid metadata.
- Join builder edge cases beyond simple equality in sub-table previews.
- Resilience around backend offline/online transitions and retries.
- Security surfaces (SQL injection, path traversal for uploads, malformed payloads).

**Notes**
- Tests are currently fast and deterministic; no external services required.
