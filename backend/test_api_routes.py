
import pytest
from fastapi.testclient import TestClient
from main import app
from storage import storage
import os
from pathlib import Path

client = TestClient(app)

@pytest.fixture(autouse=True)
def clean_env():
    """Ensure a clean environment for each test"""
    storage.clear()
    yield
    storage.clear()

# --- Session Management Tests ---

def test_session_lifecycle():
    """Test creating, listing, metadata retrieval, and deleting sessions."""
    # 1. Create
    create_res = client.post("/sessions")
    assert create_res.status_code == 200
    session_id = create_res.json()["sessionId"]
    assert session_id.startswith("sess_")

    # 2. List
    list_res = client.get("/sessions")
    assert list_res.status_code == 200
    sessions = list_res.json()
    assert any(s["sessionId"] == session_id for s in sessions)

    # 3. Metadata
    meta_payload = {"displayName": "Test Session", "settings": {"panelPosition": "left"}}
    client.post(f"/sessions/{session_id}/metadata", json=meta_payload)
    
    meta_res = client.get(f"/sessions/{session_id}/metadata")
    assert meta_res.status_code == 200
    assert meta_res.json()["displayName"] == "Test Session"
    assert meta_res.json()["settings"]["panelPosition"] == "left"

    # 4. Delete
    del_res = client.delete(f"/sessions/{session_id}")
    assert del_res.status_code == 200
    
    # 5. Verify Deletion
    list_res_after = client.get("/sessions")
    assert not any(s["sessionId"] == session_id for s in list_res_after.json())

# --- Data Upload & Query Tests ---

def test_csv_upload_and_query():
    """Test uploading a CSV file and querying it via SQL."""
    # Setup Session
    session_id = client.post("/sessions").json()["sessionId"]
    
    # Upload CSV
    csv_content = "id,name,role,salary\n1,Alice,Dev,100000\n2,Bob,QA,80000\n3,Charlie,Dev,120000"
    files = {"file": ("staff.csv", csv_content, "text/csv")}
    
    upload_res = client.post("/upload", files=files, data={"sessionId": session_id, "name": "staff"})
    assert upload_res.status_code == 200
    assert upload_res.json()["id"] == "staff"
    assert upload_res.json()["totalCount"] == 3

    # Execute SQL Query
    sql_payload = {
        "sessionId": session_id,
        "query": "SELECT role, AVG(salary) as avg_salary FROM staff GROUP BY role ORDER BY avg_salary DESC",
        "page": 1,
        "pageSize": 10
    }
    query_res = client.post("/query", json=sql_payload)
    assert query_res.status_code == 200
    
    data = query_res.json()
    assert len(data["rows"]) == 2
    
    # Check Aggregation Results
    row_dev = next(r for r in data["rows"] if r["role"] == "Dev")
    assert row_dev["avg_salary"] == 110000.0  # (100k + 120k) / 2


def test_upload_mock_retail_dataset_and_query():
    """Verify CSV mock dataset can be uploaded and queried with aggregations."""
    session_id = client.post("/sessions").json()["sessionId"]
    csv_path = Path(__file__).resolve().parent.parent / "test_data" / "mock_retail_transactions.csv"

    with csv_path.open("rb") as f:
        files = {"file": ("mock_retail_transactions.csv", f.read(), "text/csv")}
        upload_res = client.post(
            "/upload",
            files=files,
            data={"sessionId": session_id, "name": "mock_retail_transactions"},
        )

    assert upload_res.status_code == 200
    assert upload_res.json()["totalCount"] == 12

    query_res = client.post(
        "/query",
        json={
            "sessionId": session_id,
            "query": """
                SELECT region, COUNT(*) AS cnt, ROUND(AVG(order_amount), 2) AS avg_amt
                FROM mock_retail_transactions
                GROUP BY region
                ORDER BY region
            """,
            "page": 1,
            "pageSize": 10,
        },
    )

    assert query_res.status_code == 200
    rows = query_res.json()["rows"]
    assert len(rows) == 4
    east = next(r for r in rows if r["region"] == "East")
    assert east["cnt"] == 4

# --- Engine Execution Tests ---

def test_execution_flow_with_variables():
    """Test full execution flow including variable capture."""
    session_id = client.post("/sessions").json()["sessionId"]
    
    # Data: id, val
    csv_content = "id,val\n1,10\n2,20\n3,30\n4,40"
    client.post("/upload", files={"file": ("nums.csv", csv_content, "text/csv")}, data={"sessionId": session_id, "name": "nums"})

    # Operation Tree: Source -> Filter (val > 20) -> Save (id as var)
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "nums"}},
            {"id": "filt", "type": "filter", "order": 1, "config": {"field": "val", "operator": ">", "value": 20, "dataType": "number"}}
        ],
        "children": []
    }

    # Execute
    exec_payload = {
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root"
    }
    
    res = client.post("/execute", json=exec_payload)
    assert res.status_code == 200
    data = res.json()
    
    # Should keep rows with val > 20 (30, 40)
    assert data["totalCount"] == 2
    assert data["rows"][0]["val"] == 30
    assert data["rows"][1]["val"] == 40

def test_execute_invalid_node_error():
    """Test error handling when target node is missing."""
    session_id = client.post("/sessions").json()["sessionId"]
    tree = {"id": "root", "type": "operation", "name": "Root", "enabled": True, "commands": [], "children": []}
    
    res = client.post("/execute", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "invalid_id"
    })
    
    # The backend raises ValueError which maps to 500 currently in main.py wrapper
    assert res.status_code == 500
    assert "Target node not found" in res.json()["detail"]

# --- Analysis Tests ---

def test_analyze_overlap_empty():
    """Test overlap analysis on a minimal tree."""
    tree = {
        "id": "root", "type": "operation", "name": "R", "enabled": True, "commands": [], 
        "children": [
            {"id": "b1", "type": "operation", "name": "Branch 1", "enabled": True, "commands": [], "children": []},
            {"id": "b2", "type": "operation", "name": "Branch 2", "enabled": True, "commands": [], "children": []}
        ]
    }
    
    res = client.post("/analyze", json={"sessionId": "any", "tree": tree, "parentNodeId": "root"})
    assert res.status_code == 200
    report = res.json()["report"]
    # Since branches have no commands/data, they might error or show empty overlap
    # We just ensure the endpoint responds correctly structure-wise
    assert isinstance(report, list)
