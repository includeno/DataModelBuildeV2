# DataFlow Engine - User Operations & Testing Guide

This document outlines 10 core user workflows for the DataFlow Engine. For each flow, verification steps are provided for both the **Mock Server** (Frontend-only demo) and the **Python Backend** (Real execution) environments.

## Prerequisites

*   **Mock Mode**: Run `npm run dev`. Ensure "Mock Server" is selected in Global Settings (top right).
*   **Backend Mode**: Run `npm run backend` and `npm run dev`. Switch Global Settings to `http://localhost:8000`.

---

## Flow 1: Import Dataset & Initialize Session

**User Goal**: Upload a raw CSV file and start a new analysis session.

**Steps**:
1.  Click the "Session" dropdown in the top bar.
2.  Select "Create New Session".
3.  In the Sidebar (Datasets section), click the `+` icon.
4.  Drag & drop a CSV file (e.g., `employees.csv`) or click to browse.
5.  Click "Import Dataset".

**Testing / Verification**:
*   **Mock Mode**: 
    *   The upload simulates a delay.
    *   A generic "mock_table_{timestamp}" appears in the sidebar.
    *   Clicking the table name opens the SQL view with sample data (A, B, C columns).
*   **Python Backend**:
    *   Use `scripts/generate_datasets.py` to create `test_data/hr_employees.csv`.
    *   Upload this file.
    *   Verify the table name matches the filename in the sidebar.
    *   **Automated Test**: Run `scripts/upload_data_web.py` to verify the full E2E upload flow via Playwright.

---

## Flow 2: Setup Data Source

**User Goal**: Define which dataset flows into the pipeline root.

**Steps**:
1.  Select the "Root" node in the Operation Tree.
2.  In the Configuration Panel, locate "Configured Sources".
3.  Click "Dataset" dropdown and select the uploaded table (e.g., `employees.csv`).
4.  Enter an Alias (e.g., `Emp`).

**Testing / Verification**:
*   **Mock Mode**: Select `mock_employees`. The "Alias Name" should auto-fill.
*   **Python Backend**: Select `hr_employees`. The dropdown should list tables stored in DuckDB (`backend/sessions/{id}/database.db`).

---

## Flow 3: Filtering Data

**User Goal**: Remove rows that do not meet specific criteria.

**Steps**:
1.  Hover over the "Root" node, click `+` to add a child node.
2.  Select the new node. Change the Command Type dropdown to **Filter**.
3.  In "Rule Builder":
    *   Field: `salary` (or `amount` in mock).
    *   Operator: `>` (Greater Than).
    *   Value: `50000` (or `100`).
4.  Click "Run" in the top bar.

**Testing / Verification**:
*   **Mock Mode**: Use `amount > 100`. Result row count should be less than total `mock_sales` count (200).
*   **Python Backend**: Result rows should strictly adhere to `salary > 50000`. Verify using SQL Studio: `SELECT count(*) FROM hr_employees WHERE salary > 50000`.

---

## Flow 4: Join Two Tables

**User Goal**: Combine columns from two datasets based on a common key.

**Steps**:
1.  Ensure two datasets are imported (e.g., `Orders` and `Employees`).
2.  Create a new Operation Node. Set Command Type to **Join**.
3.  **Target**: Select `Employees` (or `mock_employees`) from the dropdown.
4.  **Join Type**: Select `Left`.
5.  **ON Condition**: `Orders.emp_id = Employees.id`.
6.  Click "Run".

**Testing / Verification**:
*   **Mock Mode**: Join `mock_sales` with `mock_employees`. Use `on: uid = id`. Result columns should include both `amount` and `salary`.
*   **Python Backend**: Join `ecommerce_orders` and `hr_employees`. Check for columns with `_joined` suffix if names collide. Run `backend/test_scenario_suite.py::test_join_employees_sales_cross_dept` for logic verification.

---

## Flow 5: Group By & Aggregation

**User Goal**: Summarize data (e.g., Average salary per department).

**Steps**:
1.  Create a new node. Set Command Type to **Group**.
2.  **Group By**: Select a categorical column (e.g., `dept` or `status`).
3.  **Metrics**: 
    *   Func: `mean` (Average).
    *   Field: `salary` (or `amount`).
    *   Alias: `avg_salary`.
4.  Click "Run".

**Testing / Verification**:
*   **Mock Mode**: Group by `status`, Mean `amount`. Result should have unique `status` rows only.
*   **Python Backend**: Verify arithmetic accuracy. Result row count should equal `SELECT count(DISTINCT dept) FROM hr_employees`.

---

## Flow 6: Column Transformation (Python/Formula)

**User Goal**: Create a new column based on calculation.

**Steps**:
1.  Create a new node. Set Command Type to **Mapping** (Transform).
2.  Click "Add Mapping".
3.  **Mode**: Select "Simple" or "Python".
4.  **Expression**: `row['salary'] * 1.1` (Python) or `salary * 1.1` (Simple).
5.  **Output Field**: `new_salary`.
6.  Click "Run".

**Testing / Verification**:
*   **Mock Mode**: Use `amount * 2`. Verify `new_column` appears in Preview and is double the original. *Note: Mock uses JS eval, Backend uses Python exec.*
*   **Python Backend**: Ensure Python syntax is used. Check `backend/engine.py` logs for compilation errors. Verify `[BUG-PY-002]` doesn't occur (performance hang) on large datasets.

---

## Flow 7: Defining & Using Variables

**User Goal**: Store a value (like a threshold) and reuse it in filters.

**Steps**:
1.  **Define**: In "Import Datasets" (Setup node), click "Add Variable".
    *   Name: `min_limit`.
    *   Value: `5000`.
2.  **Use**: In a Filter node downstream.
    *   Value input: `{min_limit}`.
    *   Or use Operator: `in_variable` and select `min_limit` from suggestions.

**Testing / Verification**:
*   **Mock Mode**: Define `var_a = 10`. Filter `amount > {var_a}`.
*   **Python Backend**: Verify variable substitution works in `_get_condition_mask` in `backend/engine.py`.

---

## Flow 8: SQL Studio Querying

**User Goal**: Run direct SQL for quick checks without building a pipeline.

**Steps**:
1.  Click "SQL Studio" in the top view switcher.
2.  In the editor, type: `SELECT * FROM employees WHERE salary > 100000`.
3.  Click "Run Query".
4.  View results in the grid.

**Testing / Verification**:
*   **Mock Mode**: Supports basic regex parsing. Query `SELECT * FROM mock_employees` works. Complex SQL is limited.
*   **Python Backend**: Full DuckDB SQL syntax support. Try `SELECT dept, avg(salary) FROM hr_employees GROUP BY dept`. Verify execution time log in the toolbar.

---

## Flow 9: Disable/Enable Operation Branches

**User Goal**: Temporarily exclude logic steps to debug the pipeline.

**Steps**:
1.  Hover over a middle node in the tree.
2.  Click the **Power** icon (Toggle Enabled).
3.  The node greys out.
4.  Select the leaf (final) node and click "Run".

**Testing / Verification**:
*   **Mock/Backend**: The disabled node's logic should be skipped. The data should flow from the disabled node's parent directly to its child (pass-through) effectively ignoring the transformation.

---

## Flow 10: Complex Multi-Table Preview

**User Goal**: View the main result alongside related records from another table (e.g., View Order, see related Customer details below it).

**Steps**:
1.  Select a node and set Command Type to **Complex View**.
2.  **Sub-Tables**: Click "Add Sub-Table".
3.  **Table**: Select `Employees`.
4.  **Join Condition**: `main.emp_id = sub.id`.
5.  Click "Run".
6.  In the Preview panel, click a row's chevron (`>`) to expand.

**Testing / Verification**:
*   **Mock Mode**: Use `main.uid = sub.id`. Expanding a row shows the specific user record corresponding to the `uid`.
*   **Python Backend**: The backend executes a `WHERE EXISTS` sub-query. Verify latency does not spike linearly with displayed rows.

---

## Automated Testing Suite

To ensure these flows remain stable, run the provided test suites:

1.  **Backend Unit Tests**:
    ```bash
    cd backend
    pytest test_main.py  # Basic API
    pytest test_scenario_suite.py # Logic permutations (Filters, Joins, Aggs)
    ```

2.  **E2E Data Upload Test**:
    ```bash
    python scripts/upload_data_web.py
    ```
