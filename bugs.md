
# System Bugs & Issues Log

## 2026-02-02

### Python Interaction & Backend Execution (High Priority)

#### [BUG-PY-001] Security: Unrestricted Remote Code Execution (RCE)
*   **Location**: `backend/engine.py` -> `_apply_transform`
*   **Description**: The system uses `exec(m.expression, exec_globals, local_scope)` to execute user-provided Python code. There is no sandboxing or input sanitization.
*   **Impact**: A malicious user can execute arbitrary system commands (e.g., `import os; os.system('rm -rf /')`), access the file system, or exfiltrate environment variables.
*   **Reproduction**: In a "Transform" node, switch to Python mode and enter:
    ```python
    import os
    def transform(row):
        return str(os.listdir('.'))
    ```

#### [BUG-PY-002] Performance: Non-Vectorized Row-wise Execution
*   **Location**: `backend/engine.py` -> `_apply_transform`
*   **Description**: The engine uses `df.apply(apply_row, axis=1)` to apply the Python transformation. This iterates through the DataFrame row by row in Python, bypassing Pandas/NumPy vectorization optimizations.
*   **Impact**: Performance will degrade significantly (O(n)) with large datasets (>100k rows), potentially causing timeouts or UI freezes during execution.
*   **Suggested Fix**: Encourage or support vectorized functions that accept the entire DataFrame/Series instead of a single row.

#### [BUG-PY-003] Stability: Lack of Resource Limits (Time/Memory)
*   **Location**: `backend/engine.py`
*   **Description**: User scripts run in the main thread without timeouts or memory limits.
*   **Impact**: A script with an infinite loop (`while True: pass`) or massive memory allocation will hang the entire backend server (`uvicorn` worker), causing a denial of service for all users.
*   **Reproduction**:
    ```python
    def transform(row):
        while True: pass
    ```

#### [BUG-PY-004] Data Integrity: NumPy Type Serialization Failure
*   **Location**: `backend/main.py` -> `clean_df_for_json`
*   **Description**: While `clean_df_for_json` handles `NaN` and `Inf`, it relies on `to_dict(orient='records')`. If the user's Python script returns specific NumPy types (e.g., `np.int64`, `np.float32`) that are not standard Python types, FastAPI/JSON serialization might fail or behave unexpectedly depending on the JSON encoder used.
*   **Impact**: The frontend execution preview will fail with a 500 Internal Server Error upon returning results.

#### [BUG-PY-005] Scope Pollution & Concurrency
*   **Location**: `backend/engine.py` -> `_apply_transform`
*   **Description**: `exec_globals` is defined once per execution but checks need to ensure `exec` doesn't mutate global state if not carefully managed.

### Frontend/Logic Interaction

#### [BUG-UI-001] Variable Type Mismatch in Custom Variables
*   **Location**: `components/CommandEditor.tsx` & `backend/engine.py`
*   **Description**: The frontend allows defining variables as "Text List". If a user defines a list variable `my_list = ["1", "2"]` but then uses it in a backend Python script expecting a scalar, or vice versa, the error handling in `_apply_transform` effectively silences the error, leading to silent data corruption.

#### [BUG-UI-002] Dataset Mutual Exclusivity Edge Case
*   **Location**: `components/CommandEditor.tsx`
*   **Description**: If a table is deleted from the system, the command config remains pointing to it. The validation logic might fail to flag this properly if it relies solely on filtering existing datasets, leaving the dropdown blank or in an invalid state.
