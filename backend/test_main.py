import pytest
from fastapi.testclient import TestClient
import pandas as pd
import uuid
import json
from backend.main import app
from backend.storage import storage

client = TestClient(app)
DEFAULT_SESSION_ID = "default"

def add_dataset_for_test(name: str, df: pd.DataFrame, session_id: str = DEFAULT_SESSION_ID) -> None:
    dataset_id = str(uuid.uuid4())
    session_dir = storage.get_session_dir(session_id)
    session_dir.mkdir(parents=True, exist_ok=True)
    file_path = session_dir / f"{dataset_id}__{name}"
    df.to_csv(file_path, index=False)
    storage.add_dataset(session_id, dataset_id, name, df, file_path)

@pytest.fixture(autouse=True)
def clean_storage():
    """Fixture to clear storage before each test to ensure isolation."""
    storage.clear()
    yield
    storage.clear()

# --- API TESTS ---

def test_upload_csv_success():
    csv_content = "id,name,department,salary\n1,Alice,Eng,60000\n2,Bob,HR,40000"
    files = {"file": ("employees.csv", csv_content, "text/csv")}
    
    response = client.post("/upload", files=files)
    
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "employees.csv"
    assert data["totalCount"] == 2
    assert "id" in data["fields"]
    assert "salary" in data["fields"]
    assert len(data["rows"]) == 2

def test_upload_invalid_csv():
    files = {"file": ("test.txt", "not a csv content", "text/plain")}
    response = client.post("/upload", files=files)
    # The current implementation catches parse errors and returns JSON with error key
    assert response.status_code == 200 
    assert "error" in response.json()

@pytest.mark.parametrize(
    "value",
    [
        "inf",
        "-inf",
        "NaN",
        "nan",
        "Infinity",
        "-Infinity",
        "1e309",
        "-1e309",
        "Inf",
        "-Inf",
        "INF",
        "-INF",
        "infinity",
        "-infinity",
        "NAN",
        "nan(ind)",
        "NaN",
        "Infinity",
        "-Infinity",
        "1e10000",
    ],
)
def test_upload_non_finite_values_serialized_as_null(value):
    csv_content = f"id,value\n1,{value}"
    files = {"file": ("metrics.csv", csv_content, "text/csv")}

    response = client.post("/upload", files=files)

    assert response.status_code == 200
    data = response.json()
    rows = json.loads(data["rows"])
    assert rows[0]["value"] is None

def test_sessions_endpoint_returns_sessions():
    df = pd.DataFrame({"id": [1], "value": [2.5]})
    add_dataset_for_test("sample.csv", df)
    response = client.get("/sessions")
    assert response.status_code == 200
    data = response.json()
    assert "sessions" in data

def test_execute_returns_stringified_rows():
    df = pd.DataFrame({"id": [1, 2], "value": [1.0, 2.0]})
    add_dataset_for_test("exec.csv", df)
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [],
        "children": []
    }
    response = client.post("/execute", json={"tree": tree, "targetNodeId": "root"})
    assert response.status_code == 200
    data = response.json()
    rows = json.loads(data["rows"])
    assert len(rows) == 2

# --- ENGINE & LOGIC TESTS ---

def test_execute_flow_simple_filter():
    # 1. Setup Data
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "val": [10, 20, 30, 40]
    })
    add_dataset_for_test("data.csv", df)

    # 2. Define Operation Tree
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "cmd1",
                "type": "filter",
                "order": 1,
                "config": {
                    "field": "val",
                    "operator": ">",
                    "value": 25,
                    "dataType": "number"
                }
            }
        ],
        "children": []
    }

    # 3. Execute
    response = client.post("/execute", json={"tree": tree, "targetNodeId": "root"})
    
    assert response.status_code == 200
    data = response.json()
    assert data["totalCount"] == 2
    rows = data["rows"]
    # Should only have 30 and 40
    assert all(r["val"] > 25 for r in rows)

def test_execute_nested_operations():
    # Test that child nodes inherit parent operations
    df = pd.DataFrame({
        "group": ["A", "A", "B", "B"],
        "score": [10, 90, 20, 80]
    })
    add_dataset_for_test("scores.csv", df)

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            # Parent: Filter Group A
            {
                "id": "c1",
                "type": "filter",
                "order": 1,
                "config": {"field": "group", "operator": "=", "value": "A"}
            }
        ],
        "children": [
            {
                "id": "child1",
                "type": "operation",
                "name": "Child",
                "enabled": True,
                "commands": [
                    # Child: Filter Score > 50
                    {
                        "id": "c2",
                        "type": "filter",
                        "order": 1,
                        "config": {"field": "score", "operator": ">", "value": 50, "dataType": "number"}
                    }
                ]
            }
        ]
    }

    response = client.post("/execute", json={"tree": tree, "targetNodeId": "child1"})
    
    assert response.status_code == 200
    data = response.json()
    # Expect: Group A (10, 90) -> Score > 50 (90) -> Result: 1 row
    assert data["totalCount"] == 1
    assert data["rows"][0]["score"] == 90
    assert data["rows"][0]["group"] == "A"

def test_execute_join_operation():
    # 1. Setup two datasets
    df_users = pd.DataFrame({"user_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    df_orders = pd.DataFrame({"order_id": [101, 102], "uid": [1, 2], "amount": [500, 300]})
    
    # Add users first (default source)
    add_dataset_for_test("users.csv", df_users)
    add_dataset_for_test("orders.csv", df_orders)

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "join1",
                "type": "join",
                "order": 1,
                "config": {
                    "joinTable": "orders.csv",
                    "joinType": "INNER",
                    "on": "user_id = uid"
                }
            }
        ]
    }

    response = client.post("/execute", json={"tree": tree, "targetNodeId": "root"})
    
    assert response.status_code == 200
    data = response.json()
    # Inner join should match user 1 and 2
    assert data["totalCount"] == 2
    # Verify merged columns exist
    first_row = data["rows"][0]
    assert "amount" in first_row
    assert "name" in first_row

def test_execute_sort_operation():
    df = pd.DataFrame({"val": [3, 1, 2]})
    add_dataset_for_test("sort_test.csv", df)

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "s1",
                "type": "sort",
                "order": 1,
                "config": {"field": "val", "ascending": True}
            }
        ]
    }

    response = client.post("/execute", json={"tree": tree, "targetNodeId": "root"})
    rows = response.json()["rows"]
    assert rows[0]["val"] == 1
    assert rows[1]["val"] == 2
    assert rows[2]["val"] == 3

def test_execute_aggregation_operation():
    df = pd.DataFrame({
        "dept": ["IT", "IT", "HR", "HR"],
        "salary": [100, 200, 150, 150]
    })
    add_dataset_for_test("agg_test.csv", df)

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "a1",
                "type": "aggregate",
                "order": 1,
                "config": {
                    "groupBy": ["dept"],
                    "aggFunc": "mean",
                    "field": "salary"
                }
            }
        ]
    }

    response = client.post("/execute", json={"tree": tree, "targetNodeId": "root"})
    data = response.json()
    # Should reduce to 2 rows (IT, HR)
    assert data["totalCount"] == 2
    
    rows = data["rows"]
    it_row = next(r for r in rows if r["dept"] == "IT")
    hr_row = next(r for r in rows if r["dept"] == "HR")
    
    assert it_row["salary"] == 150.0  # (100+200)/2
    assert hr_row["salary"] == 150.0  # (150+150)/2

def test_execute_target_not_found():
    df = pd.DataFrame({"a": [1]})
    add_dataset_for_test("a.csv", df)
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [],
        "children": []
    }
    
    response = client.post("/execute", json={"tree": tree, "targetNodeId": "non-existent"})
    assert response.status_code == 500  # Engine raises ValueError, caught as 500 in main
