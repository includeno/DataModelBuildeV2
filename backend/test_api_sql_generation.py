
import pytest
from fastapi.testclient import TestClient
from main import app
from storage import storage

client = TestClient(app)

@pytest.fixture(autouse=True)
def clean_env():
    storage.clear()
    yield
    storage.clear()

# --- Helper to create a basic session and upload data ---
def setup_session_with_data():
    # 1. Create Session
    session_res = client.post("/sessions")
    assert session_res.status_code == 200
    session_id = session_res.json()["sessionId"]

    # 2. Upload Data (Mock Users)
    csv_content = "id,name,age,role\n1,Alice,30,admin\n2,Bob,25,user\n3,Charlie,35,user"
    client.post("/upload", 
        files={"file": ("users.csv", csv_content, "text/csv")}, 
        data={"sessionId": session_id, "name": "users"}
    )
    
    return session_id

# --- Tests ---

def test_generate_sql_source():
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "cmd_src", 
                "type": "source", 
                "order": 0, 
                "config": {"mainTable": "users"}
            }
        ],
        "children": []
    }

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "cmd_src"
    })
    
    assert res.status_code == 200
    assert res.json()["sql"] == "SELECT * FROM users"

def test_generate_sql_filter_chain():
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "filter", "config": {"field": "age", "operator": ">", "value": 20, "valueType": "raw"}}
        ],
        "children": []
    }

    # Generate for the filter command
    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert "SELECT * FROM" in sql
    assert "WHERE age > 20" in sql
    # It should infer input table from source command
    assert "FROM users" in sql

def test_generate_sql_with_variable_substitution():
    session_id = setup_session_with_data()
    
    # We need to define a variable first. 
    # In the engine, variables are defined by 'define_variable' commands or 'save' commands in previous nodes.
    # Let's use a 'define_variable' command if supported, or a 'save' command.
    # The engine.py _apply_node_commands handles 'define_variable'.
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "define_variable", "config": {"variableName": "min_age", "variableValue": "25", "variableType": "text"}},
            {"id": "c2", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c3", "type": "filter", "config": {"field": "age", "operator": ">", "value": "{min_age}", "valueType": "variable"}}
        ],
        "children": []
    }

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c3"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    # The variable {min_age} should be replaced by '25' (or 25 if casted, but text var usually string)
    # Since it's text type in define_variable, it might be quoted '25'. 
    # But let's check if the logic handles it.
    assert "age > '25'" in sql or "age > 25" in sql

def test_generate_sql_unsupported_python():
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {
                "id": "c2", 
                "type": "transform", 
                "config": {
                    "mappings": [
                        {"expression": "row['age'] + 1", "outputField": "age_plus", "mode": "python"}
                    ]
                }
            }
        ],
        "children": []
    }

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    assert "-- SQL generation not supported for Python transformations" in res.json()["sql"]

def test_generate_sql_unsupported_node_join():
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {
                "id": "c2", 
                "type": "join", 
                "config": {
                    "joinTargetType": "node",
                    "joinTargetNodeId": "some_other_node"
                }
            }
        ],
        "children": []
    }

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    assert "-- SQL generation not supported for dynamic Node joins" in res.json()["sql"]

def test_generate_sql_missing_target_id():
    session_id = setup_session_with_data()
    tree = {"id": "root", "type": "operation", "name": "R", "enabled": True, "commands": [], "children": []}
    
    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root"
        # Missing targetCommandId
    })
    
    assert res.status_code == 400 # Bad Request
    assert "targetCommandId is required" in res.json()["detail"]

def test_generate_sql_command_not_found():
    session_id = setup_session_with_data()
    tree = {
        "id": "root", "type": "operation", "name": "R", "enabled": True, 
        "commands": [{"id": "c1", "type": "source", "config": {"mainTable": "users"}}], 
        "children": []
    }
    
    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "non_existent_cmd"
    })
    
    assert res.status_code == 500
    assert "Target command not found" in res.json()["detail"]

