import pytest
from fastapi.testclient import TestClient
import pandas as pd
import io
from main import app
from storage import storage

client = TestClient(app)

@pytest.fixture()
def session_id():
    res = client.post("/sessions")
    assert res.status_code == 200
    return res.json()["sessionId"]

def build_tree(commands, main_table):
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "src", "type": "source", "order": 0, "config": {"mainTable": main_table}}
        ] + commands,
        "children": []
    }

@pytest.fixture(autouse=True)
def clean_storage():
    """Fixture to clear storage before each test to ensure isolation."""
    storage.clear()
    yield
    storage.clear()

# --- API TESTS ---

def test_upload_csv_success(session_id):
    csv_content = "id,name,department,salary\n1,Alice,Eng,60000\n2,Bob,HR,40000"
    files = {"file": ("employees.csv", csv_content, "text/csv")}
    
    response = client.post("/upload", files=files, data={"sessionId": session_id})
    
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "employees"
    assert data["totalCount"] == 2
    assert "id" in data["fields"]
    assert "salary" in data["fields"]
    assert len(data["rows"]) == 2

def test_upload_invalid_csv(session_id):
    files = {"file": ("test.txt", "not a csv content", "text/plain")}
    response = client.post("/upload", files=files, data={"sessionId": session_id})
    # The current implementation catches parse errors and returns JSON with error key
    assert response.status_code == 200 
    assert "error" in response.json()

# --- ENGINE & LOGIC TESTS ---

def test_execute_flow_simple_filter(session_id):
    # 1. Setup Data
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "val": [10, 20, 30, 40]
    })
    storage.add_dataset(session_id, "data.csv", df)

    # 2. Define Operation Tree
    tree = build_tree(
        [
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
        "data"
    )

    # 3. Execute
    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "root"})
    
    assert response.status_code == 200
    data = response.json()
    assert data["totalCount"] == 2
    rows = data["rows"]
    # Should only have 30 and 40
    assert all(r["val"] > 25 for r in rows)

def test_execute_nested_operations(session_id):
    # Test that child nodes inherit parent operations
    df = pd.DataFrame({
        "group": ["A", "A", "B", "B"],
        "score": [10, 90, 20, 80]
    })
    storage.add_dataset(session_id, "scores.csv", df)

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "scores"}},
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

    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "child1"})
    
    assert response.status_code == 200
    data = response.json()
    # Expect: Group A (10, 90) -> Score > 50 (90) -> Result: 1 row
    assert data["totalCount"] == 1
    assert data["rows"][0]["score"] == 90
    assert data["rows"][0]["group"] == "A"

def test_execute_join_operation(session_id):
    # 1. Setup two datasets
    df_users = pd.DataFrame({"user_id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
    df_orders = pd.DataFrame({"order_id": [101, 102], "uid": [1, 2], "amount": [500, 300]})
    
    # Add users first (default source)
    storage.add_dataset(session_id, "users.csv", df_users)
    storage.add_dataset(session_id, "orders.csv", df_orders)

    tree = build_tree(
        [
            {
                "id": "join1",
                "type": "join",
                "order": 1,
                "config": {
                    "joinTable": "orders",
                    "joinType": "INNER",
                    "on": "user_id = uid"
                }
            }
        ],
        "users"
    )

    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "root"})
    
    assert response.status_code == 200
    data = response.json()
    # Inner join should match user 1 and 2
    assert data["totalCount"] == 2
    # Verify merged columns exist
    first_row = data["rows"][0]
    assert "amount" in first_row
    assert "name" in first_row

def test_execute_sort_operation(session_id):
    df = pd.DataFrame({"val": [3, 1, 2]})
    storage.add_dataset(session_id, "sort_test.csv", df)

    tree = build_tree(
        [
            {
                "id": "s1",
                "type": "sort",
                "order": 1,
                "config": {"field": "val", "ascending": True}
            }
        ],
        "sort_test"
    )

    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "root"})
    rows = response.json()["rows"]
    assert rows[0]["val"] == 1
    assert rows[1]["val"] == 2
    assert rows[2]["val"] == 3

def test_execute_aggregation_operation(session_id):
    df = pd.DataFrame({
        "dept": ["IT", "IT", "HR", "HR"],
        "salary": [100, 200, 150, 150]
    })
    storage.add_dataset(session_id, "agg_test.csv", df)

    tree = build_tree(
        [
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
        ],
        "agg_test"
    )

    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "root"})
    data = response.json()
    # Should reduce to 2 rows (IT, HR)
    assert data["totalCount"] == 2
    
    rows = data["rows"]
    it_row = next(r for r in rows if r["dept"] == "IT")
    hr_row = next(r for r in rows if r["dept"] == "HR")
    
    assert it_row["mean_salary"] == 150.0  # (100+200)/2
    assert hr_row["mean_salary"] == 150.0  # (150+150)/2

def test_execute_target_not_found(session_id):
    df = pd.DataFrame({"a": [1]})
    storage.add_dataset(session_id, "a.csv", df)
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [],
        "children": []
    }
    
    response = client.post("/execute", json={"sessionId": session_id, "tree": tree, "targetNodeId": "non-existent"})
    assert response.status_code == 500  # Engine raises ValueError, caught as 500 in main
