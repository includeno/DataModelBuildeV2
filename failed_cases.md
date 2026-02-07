# 测试失败用例报告 (Failed Test Cases Report)

## 1. 后端单元测试 (`backend/test_api_routes.py`, `backend/test_main.py`)

### 失败原因 (Failure Reason)
测试在 `setup` 阶段 (`clean_env` fixture) 失败，错误信息为：
`AttributeError: 'SessionStorage' object has no attribute 'clear'`

这是因为测试夹具试图调用 `storage.clear()` 来重置环境，但是 `backend/storage.py` 中的 `SessionStorage` 类没有实现这个方法。

### 改进方法 (Improvement Method)
在 `backend/storage.py` 的 `SessionStorage` 类中实现 `clear` 方法。该方法应该删除会话目录并重新创建它，以确保状态干净。

### 改进代码 (Improvement Code)
在 `backend/storage.py` 中：

```python
    def clear(self):
        """Clears all sessions data."""
        if os.path.exists(SESSIONS_DIR):
            shutil.rmtree(SESSIONS_DIR)
        if not os.path.exists(SESSIONS_DIR):
            os.makedirs(SESSIONS_DIR)
```

## 2. 后端场景测试 (`backend/test_scenario_suite.py`)

### 失败原因 (Failure Reason)
测试失败并出现断言错误，例如 `assert 0 > 0` 或 `assert 'col' in []`。这表明执行结果为空（没有返回行或列）。

根本原因是测试套件期望一个已经填充了特定数据集（`ecommerce_orders`, `hr_employees`, `student_scores` 等）的会话，但是没有设置例程来创建此会话或加载数据。回退会话 `test_session` 可能是空的或不存在的。

### 改进方法 (Improvement Method)
在 `backend/test_scenario_suite.py` 中添加一个 `autouse=True` 的 pytest fixture（或显式使用），用于：
1.  初始化测试会话。
2.  使用 `storage.add_dataset` 填充必要的模拟数据集。
3.  确保测试使用的 `SESSION_ID` 指向此已填充的会话。

### 改进代码 (Improvement Code)
在 `backend/test_scenario_suite.py` 中：

```python
import pandas as pd
import numpy as np
from storage import storage

# ... existing imports ...

@pytest.fixture(scope="module", autouse=True)
def setup_test_data():
    global SESSION_ID
    SESSION_ID = "test_suite_session"
    storage.create_session(SESSION_ID)

    # Create ecommerce_orders
    orders_data = {
        "order_id": range(1, 101),
        "customer_id": [f"CUST_{i%10}" for i in range(1, 101)],
        "amount": [float(i * 10) for i in range(1, 101)],
        "status": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"] * 25
    }
    storage.add_dataset(SESSION_ID, "ecommerce_orders", pd.DataFrame(orders_data))

    # Create hr_employees
    employees_data = {
        "emp_id": [f"CUST_{i}" for i in range(10)],
        "name": [f"Employee {i}" for i in range(10)],
        "department": ["Sales", "Engineering"] * 5,
        "salary": [50000 + i*1000 for i in range(10)],
        "join_date": ["2023-01-01"] * 10,
        "is_active": [True, False] * 5
    }
    storage.add_dataset(SESSION_ID, "hr_employees", pd.DataFrame(employees_data))

    # Create student_scores
    scores_data = {
        "student_id": [f"CUST_{i}" for i in range(10)],
        "subject": ["Math", "Science"] * 5,
        "score": [80 + i for i in range(10)]
    }
    storage.add_dataset(SESSION_ID, "student_scores", pd.DataFrame(scores_data))

    # Create iot_logs
    logs_data = {
        "log_id": range(200),
        "location": ["Factory_A", "Factory_B"] * 100,
        "sensor_id": ["S1", "S2"] * 100,
        "temperature": [25.5] * 200,
        "humidity": [60] * 200
    }
    storage.add_dataset(SESSION_ID, "iot_logs", pd.DataFrame(logs_data))

    # Create financial_ledger
    ledger_data = {
        "tx_id": range(1, 101),
        "amount": [100.0] * 100,
        "tx_type": ["DEBIT", "CREDIT"] * 50
    }
    storage.add_dataset(SESSION_ID, "financial_ledger", pd.DataFrame(ledger_data))

    yield

    # Cleanup
    storage.delete_session(SESSION_ID)
```

## 3. E2E UI 测试 (`scripts/test_e2e_ui.py`)

### 失败原因 1: `test_switch_to_mock_and_query`
**错误**: `playwright._impl._errors.Error: Locator.click: Error: strict mode violation: get_by_text("Mock Server") resolved to 3 elements`。
定位器匹配了多个包含 "Mock Server" 的元素（例如在下拉菜单中、状态栏中或加粗文本中）。

### 改进方法 1 (Improvement Method 1)
使用更具体的定位策略。如果文本完全匹配，使用 `exact=True`，或者针对特定的交互元素（如菜单项）。

### 改进代码 1 (Improvement Code 1)
在 `scripts/test_e2e_ui.py` 中，替换：
```python
page.get_by_text("Mock Server").click()
```
为：
```python
# Use exact match if applicable, or limit to the dropdown menu context if possible
page.get_by_text("Mock Server", exact=True).click()
```

### 失败原因 2: `test_e2e_backend_workflow`
**错误**: `playwright._impl._errors.Error: Locator.click: Error: strict mode violation: get_by_role("button", name="Import Dataset") resolved to 2 elements`。
页面上有两个名为 "Import Dataset" 的按钮（可能是一个在主 UI 中打开模态框，另一个在模态框中确认）。

### 改进方法 2 (Improvement Method 2)
区分 "打开模态框" 按钮和 "确认上传" 按钮。测试在此时可能想要点击模态框内的 "确认" 按钮（因为文件选择已完成）。

### 改进代码 2 (Improvement Code 2)
在 `scripts/test_e2e_ui.py` 中，替换：
```python
page.get_by_role("button", name="Import Dataset").click()
```
为：
```python
# Target the button that is likely the primary action in the modal/dialog
page.locator("div[role='dialog'] button").filter(has_text="Import Dataset").click()
# OR simply take the last one if it's the confirm button
# page.get_by_role("button", name="Import Dataset").last.click()
```
