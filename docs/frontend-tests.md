**Frontend Tests**
Test run (2026-03-09): `npm test -- --run`
Result: `129 passed (129)`
Notes: Vitest reports React `act(...)` warnings in UI tests (non-fatal).

**What’s Covered (by suite)**
- `tests/sqlParser.test.ts`: SQL Builder parsing for nested AND/OR groups, IN/NOT IN, IS NULL/NOT NULL, LIKE variants, LIMIT/ORDER BY, and complex parentheses.
- `tests/sqlParser.error.test.ts`: SQL Builder invalid SQL inputs (non-select SQL, missing FROM, unbalanced parentheses, dangling operators).
- `tests/sqlParser.additional.test.ts`: Additional 40 SQL parsing cases (valid/invalid/warning paths, ordering/limit, nested conditions).
- `tests/sqlParser.boundary.test.ts`: Boundary SQL cases (uppercase keywords, trailing semicolons, multiline SQL, escaped quotes, empty IN list, limit=0).
- `tests/mockEngine.test.ts`: client-side mock engine behavior for source load, filter operators, joins, group/aggregate, transforms, sort, define/save variables, and nested filter groups.
- `tests/ui/sessionStorageSwitch.test.tsx`: UI interaction to switch session storage and refresh list.
- `tests/ui/sqlBuilderValidation.test.tsx`: SQL Builder Apply disabled for missing dataset/fields and enabled when valid.
- `tests/ui/sqlStudioAutocomplete.test.tsx`: SQL Studio keyword/table/field autocomplete and suggestion apply.
- `tests/ui/sqlStudioAutocompleteAliases.test.tsx`: SQL Studio alias-aware autocomplete for `FROM`/`JOIN` aliases.
- `tests/ui/sqlStudioResults.test.tsx`: SQL Studio success/error rendering, history update, and pagination requests.
- `tests/ui/backendStatusOffline.test.tsx`: Backend status badge switches to Offline when ping fails.
- `tests/ui/backendStatusRecovery.test.tsx`: Backend status recovery from Offline to Online and Mock status behavior.
- `tests/e2e/complete-flow.spec.ts` (Playwright, run separately): full workflow for dataset import, data source config, command building, complex view, SQL Builder flow.

**Known Gaps / Not Covered Yet**
- Dataset import of parquet with same name reuse and replace semantics (UI coverage).
- ID-visibility toggles (global settings) and UI display of IDs across views.
- Drag reordering for operations/steps and pin/unpin outline interactions.
- File upload restrictions for out-of-project paths.
- SQL Builder failure states (invalid tables/fields) in end-to-end flow.
- Accessibility smoke tests (keyboard navigation, focus trapping in modals).

**How to Run E2E**
- `npm run test:e2e`
