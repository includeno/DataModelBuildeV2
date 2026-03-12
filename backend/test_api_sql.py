
import json
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

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "cmd_src"
    })
    
    assert res.status_code == 200
    assert res.json()["sql"] == "SELECT * FROM users"

def test_generate_sql_with_command_meta():
    session_id = setup_session_with_data()

    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "sort", "config": {"field": "age", "ascending": False}}
        ],
        "children": []
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2",
        "includeCommandMeta": True
    })

    assert res.status_code == 200
    payload = res.json()["dmb"]
    sql = res.json()["sql"]
    assert payload["type"] == "sort"
    assert payload["config"]["field"] == "age"
    assert "ORDER BY age DESC" in sql

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

    tree = add_setup_node(tree, ["users"])

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

    tree = add_setup_node(tree, ["users"])

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

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    assert res.json()["sql"].startswith("-- SQL generation not supported for Python transformations")

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

    tree = add_setup_node(tree, ["users"])

    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "c2"
    })
    
    assert res.status_code == 200
    assert res.json()["sql"].startswith("-- SQL generation not supported for dynamic Node joins")

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

    tree = add_setup_node(tree, ["users"])
    
    res = client.post("/generate_sql", json={
        "sessionId": session_id,
        "tree": tree,
        "targetNodeId": "root",
        "targetCommandId": "non_existent_cmd"
    })
    
    assert res.status_code == 500
    assert "Target command not found" in res.json()["detail"]

def test_generate_sql_complex_filter_group():
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
                            {
                                "type": "group",
                                "logicalOperator": "AND",
                                "conditions": [
                                    {"field": "age", "operator": ">", "value": 20, "valueType": "raw"},
                                    {"field": "age", "operator": "<", "value": 40, "valueType": "raw"}
                                ]
                            }
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
    assert "role = 'admin'" in sql
    assert "OR" in sql
    assert "(age > 20 AND age < 40)" in sql

def test_generate_sql_group_by_aggregation():
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
                        {"func": "count", "field": "*", "alias": "user_count"},
                        {"func": "mean", "field": "age", "alias": "avg_age"}
                    ],
                    "havingConditions": [
                        {"metricAlias": "user_count", "operator": ">", "value": 0}
                    ],
                    "outputTableName": "grouped_users"
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
    assert "SELECT role" in sql
    assert "COUNT(*) AS user_count" in sql
    assert "MEAN(age) AS avg_age" in sql
    assert "GROUP BY role" in sql
    assert "HAVING user_count > 0" in sql

def test_generate_sql_input_table_inference():
    # Test that if previous command sets outputTableName, next command uses it
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
                    "outputTableName": "role_stats"
                }
            },
            {
                "id": "c3",
                "type": "filter",
                "config": {"field": "role", "operator": "=", "value": "admin"}
            }
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
    assert "GROUP BY role" in sql
    assert "WHERE role = 'admin'" in sql
    assert "role_stats" not in sql

def test_generate_sql_sort_command():
    session_id = setup_session_with_data()
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "sort", "config": {"field": "age", "ascending": False}}
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
    assert "ORDER BY age DESC" in sql

def test_generate_sql_view_limit_zero():
    session_id = setup_session_with_data()
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "view", "config": {"viewFields": [{"field": "id"}], "viewLimit": 0}}
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
    assert "SELECT id FROM users LIMIT 0" in sql

def test_generate_sql_save_distinct():
    session_id = setup_session_with_data()
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c2", "type": "save", "config": {"field": "role", "distinct": True}}
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
    assert "SELECT DISTINCT role FROM" in sql

def test_generate_sql_transform_chain():
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
                        {"expression": "age * 2", "outputField": "double_age", "mode": "sql"}
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
    assert "age * 2 AS double_age" in sql

def test_generate_sql_list_variable_substitution():
    session_id = setup_session_with_data()
    
    # Define a list variable
    tree = {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "c1", "type": "define_variable", "config": {"variableName": "roles", "variableValue": ["admin", "user"], "variableType": "list"}},
            {"id": "c2", "type": "source", "config": {"mainTable": "users"}},
            {"id": "c3", "type": "filter", "config": {"field": "role", "operator": "in_variable", "value": "{roles}", "valueType": "variable"}}
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
    # Should be IN ('admin', 'user')
    assert "role IN ('admin', 'user')" in sql

def test_generate_sql_filter_data_source_resets_previous_group_chain():
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
                    "aggregations": [{"func": "sum", "field": "age", "alias": "total_age"}]
                }
            },
            {
                "id": "c3",
                "type": "filter",
                "config": {
                    "dataSource": "users",
                    "filterRoot": {
                        "id": "root_1",
                        "type": "group",
                        "logicalOperator": "AND",
                        "conditions": [
                            {"id": "cond_1", "type": "condition", "field": "role", "operator": "=", "value": "user", "valueType": "raw"}
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
        "targetCommandId": "c3",
        "includeCommandMeta": True
    })

    assert res.status_code == 200
    sql = res.json()["sql"]
    payload = res.json()["dmb"]
    assert payload["type"] == "filter"
    assert "WHERE role = 'user'" in sql
    assert "FROM users" in sql
    assert "GROUP BY" not in sql
    assert "total_age" not in sql


@pytest.mark.parametrize(
    "target_cmd, expected_sql_parts, forbidden_sql_parts",
    [
        (
            {
                "id": "c3",
                "type": "filter",
                "config": {
                    "dataSource": "users",
                    "field": "role",
                    "operator": "=",
                    "value": "user",
                    "valueType": "raw",
                },
            },
            ["FROM users", "WHERE role = 'user'"],
            ["polluted_total_age", "GROUP BY role"],
        ),
        (
            {
                "id": "c3",
                "type": "sort",
                "config": {"dataSource": "users", "field": "age", "ascending": False},
            },
            ["FROM users", "ORDER BY age DESC"],
            ["polluted_total_age", "GROUP BY role"],
        ),
        (
            {
                "id": "c3",
                "type": "group",
                "config": {
                    "dataSource": "users",
                    "groupByFields": ["name"],
                    "aggregations": [{"func": "count", "field": "*", "alias": "fresh_cnt"}],
                },
            },
            ["FROM users", "GROUP BY name", "COUNT(*) AS fresh_cnt"],
            ["polluted_total_age", "SUM(age)"],
        ),
        (
            {
                "id": "c3",
                "type": "transform",
                "config": {
                    "dataSource": "users",
                    "mappings": [{"expression": "age + 1", "outputField": "age_plus", "mode": "sql"}],
                },
            },
            ["FROM users", "age + 1 AS age_plus"],
            ["polluted_total_age", "GROUP BY role"],
        ),
        (
            {
                "id": "c3",
                "type": "save",
                "config": {"dataSource": "users", "field": "role", "distinct": True},
            },
            ["SELECT DISTINCT role FROM users"],
            ["polluted_total_age", "GROUP BY role"],
        ),
        (
            {
                "id": "c3",
                "type": "view",
                "config": {
                    "dataSource": "users",
                    "viewFields": [{"field": "name"}],
                    "viewSorts": [{"field": "name", "ascending": True}],
                    "viewLimit": 2,
                },
            },
            ["SELECT name FROM users", "ORDER BY name ASC", "LIMIT 2"],
            ["polluted_total_age", "GROUP BY role"],
        ),
    ],
)
def test_generate_sql_single_table_commands_with_data_source_reset_chain(
    target_cmd,
    expected_sql_parts,
    forbidden_sql_parts,
):
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
                    "aggregations": [{"func": "sum", "field": "age", "alias": "polluted_total_age"}],
                },
            },
            target_cmd,
        ],
        "children": [],
    }

    tree = add_setup_node(tree, ["users"])

    res = client.post(
        "/generate_sql",
        json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": "c3",
            "includeCommandMeta": True,
        },
    )

    assert res.status_code == 200
    sql = res.json()["sql"]
    payload = res.json()["dmb"]
    assert payload["type"] == target_cmd["type"]

    for part in expected_sql_parts:
        assert part in sql
    for part in forbidden_sql_parts:
        assert part not in sql
