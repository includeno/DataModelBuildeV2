# Bug Report - Workflow UI Audit

## Summary
An automated audit of the DataFlow Engine Workflow UI was conducted using a Playwright test suite covering 50+ scenarios. The tests targeted both Mock Server and Real Backend modes. Several issues were identified ranging from usability improvements to functional limitations.

## Critical Findings

### 1. Cannot Execute "Setup" Nodes
**Severity:** High
**Description:** Users cannot preview the raw data imported in a Setup node. The "Run" / "Execute" button is explicitly disabled when a Setup node is selected.
**Impact:** Users cannot verify their data import configuration before building the pipeline.
**Reproduction:**
1. Create a new session.
2. Add a Setup Node and configure a data source.
3. Select the Setup Node.
4. Observe the "Run" button in the top bar is disabled.

### 2. Root Node Hidden / Empty State Confusion
**Severity:** Medium
**Description:** The "Root" container node is hidden in the Sidebar. The initial state shows an empty list with a "+" button.
**Impact:** Users might be confused about the hierarchy or where to start.
**Reproduction:**
1. Create a new session.
2. Observe the Sidebar. It says "No operations yet" but does not show the "Root" parent.

### 3. Data Source Selection in Downstream Nodes
**Severity:** Medium
**Description:** Automated tests struggled to select data sources in downstream Process nodes (Filter, Join). This suggests potential issues with how available sources are propagated or rendered in the dropdown (e.g., `Alias to TableName` format might be confusing or options might not be populating correctly in all states).
**Reproduction:**
1. Configure a Setup node with a source.
2. Add a child Process node.
3. Try to select the source in the "Select Dataset" dropdown.

### 4. Mock Server Session Delete is No-op
**Severity:** Medium
**Description:** Deleting a session in Mock Server mode does not actually remove the session from the list. The API returns success but the state is not updated.
**Reproduction:**
1. Create a session in Mock mode.
2. Open the session dropdown.
3. Click the trash icon to delete.
4. Confirm the dialog.
5. Observe the session remains in the list.

## UI/UX Inconsistencies

### 5. Session Renaming Update Lag
**Severity:** Low
**Description:** After renaming a session in the Settings modal, the session name in the Top Bar dropdown button does not update immediately or requires a refresh/interaction to reflect the change.

### 6. Button Text Mismatches
**Severity:** Low
**Description:**
- "Save Settings" button in Session Settings was expected to be "Save Configuration".
- "Add Setup Node" vs "Add Operation" vs "Add Child" tooltips/titles are inconsistent across different contexts.

### 7. Validation Unreachability
**Severity:** Low
**Description:** The "This table is already selected" validation error in Setup nodes is difficult to trigger because the UI filters out already-selected tables from the dropdown. While good for UX, it makes the validation dead code or only relevant for API/Import edge cases.

### 8. Missing Field Labels
**Severity:** Low
**Description:** Several configuration inputs (e.g., Join Target Type) lack explicit text labels, relying on the dropdown value to convey meaning.

## Test Suite
A comprehensive test suite `tests/e2e_workflow_test.py` has been created covering:
- Session Management (Create, Rename, Delete)
- Node Management (Add, Delete, Tree Navigation)
- Command Configuration (Filter, Join, Sort, Group, Transform, Save, View)
- Execution & Export
- Backend Integration

Tests can be run via:
```bash
pytest tests/e2e_workflow_test.py
```
