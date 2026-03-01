
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

def setup_session_with_data():
    session_res = client.post("/sessions")
    assert session_res.status_code == 200
    session_id = session_res.json()["sessionId"]

    csv_content = "id,name,age,role\n1,Alice,30,admin\n2,Bob,25,user\n3,Charlie,35,user"
    client.post("/upload", 
        files={"file": ("users.csv", csv_content, "text/csv")}, 
        data={"sessionId": session_id, "name": "users"}
    )
    
    return session_id

def add_setup_node(tree: dict, tables: list[str]):
    setup_cmds = []
    for idx, table in enumerate(tables):
        setup_cmds.append({
            "id": f"setup_src_{idx}",
            "type": "source",
            "config": {
                "mainTable": table,
                "alias": table,
                "linkId": f"link_{table}"
            }
        })
    setup_node = {
        "id": "setup",
        "type": "operation",
        "operationType": "setup",
        "name": "Data Setup",
        "enabled": True,
        "commands": setup_cmds,
        "children": []
    }
    children = tree.get("children") or []
    tree["children"] = children + [setup_node]
    return tree

def test_generate_sql_two_not_equals_and():
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
                "type": "filter", 
                "config": {
                    "filterRoot": {
                        "logicalOperator": "AND",
                        "conditions": [
                            {"field": "role", "operator": "!=", "value": "admin", "valueType": "raw"},
                            {"field": "age", "operator": "!=", "value": 30, "valueType": "raw"}
                        ]
                    }
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    print(sql)
    
    assert "WITH" not in sql
    assert "step_" not in sql
    assert "FROM users" in sql
    
    # Verify logic
    assert "role != 'admin'" in sql
    assert "age != 30" in sql
    assert " AND " in sql

def test_generate_sql_chain_linking():
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "filter", "config": {"field": "age", "operator": ">", "value": 20, "valueType": "raw"}},
            {"id": "c3", "type": "sort", "config": {"field": "name", "ascending": True}}
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c3"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    print(sql)
    
    assert "FROM (SELECT * FROM users WHERE age > 20)" in sql
    assert "ORDER BY name ASC" in sql
    assert "step_" not in sql

def test_generate_sql_multiple_sources_chain():
    # Test that if a new source command appears, it resets the input for that step
    session_id = setup_session_with_data()
    
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "filter", "config": {"field": "age", "operator": ">", "value": 20, "valueType": "raw"}},
            # New source command, should ignore step_1
            {"id": "c3", "type": "source", "config": {"mainTable": "users"}}, 
            {"id": "c4", "type": "filter", "config": {"field": "role", "operator": "=", "value": "user", "valueType": "raw"}}
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c4"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    print(sql)
    
    assert "role = 'user'" in sql
    assert "age > 20" not in sql
    assert "step_" not in sql

def test_generate_sql_or_condition():
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
                "type": "filter", 
                "config": {
                    "filterRoot": {
                        "logicalOperator": "OR",
                        "conditions": [
                            {"field": "role", "operator": "=", "value": "admin", "valueType": "raw"},
                            {"field": "age", "operator": ">", "value": 30, "valueType": "raw"}
                        ]
                    }
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert " OR " in sql
    assert "role = 'admin'" in sql
    assert "age > 30" in sql

def test_generate_sql_in_list():
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
                "type": "filter", 
                "config": {
                    "field": "role", 
                    "operator": "in_list", 
                    "value": ["admin", "user"], 
                    "valueType": "raw"
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert "role IN ('admin', 'user')" in sql

def test_generate_sql_contains():
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
                "type": "filter", 
                "config": {
                    "field": "name", 
                    "operator": "contains", 
                    "value": "ali", 
                    "valueType": "raw"
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert "name LIKE '%ali%'" in sql

def test_generate_sql_group_by():
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
                "type": "group", 
                "config": {
                    "groupByFields": ["role"],
                    "aggregations": [
                        {"func": "count", "field": "*", "alias": "count"}
                    ]
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert "GROUP BY role" in sql
    assert "COUNT(*) AS count" in sql

def test_generate_sql_join():
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
                    "joinTable": "other_table",
                    "joinType": "left",
                    "on": "users.id = other_table.user_id"
                }
            }
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users", "other_table"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    sql = res.json()["sql"]
    assert "LEFT JOIN other_table" in sql
    assert "ON t1.id = t2.user_id" in sql
