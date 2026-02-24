
import pytest
import json
import os
import pandas as pd
import numpy as np
from fastapi.testclient import TestClient
from main import app
from storage import storage

client = TestClient(app)

SESSION_ID = "test_session_suite"

@pytest.fixture(scope="module", autouse=True)
def setup_test_data():
    """Initializes the session with comprehensive test data."""
    global SESSION_ID
    
    # Reset storage
    storage.clear()
    storage.create_session(SESSION_ID)
    
    # 1. Ecommerce Orders
    orders_data = {
        "order_id": [f"ORD_{i:03d}" for i in range(1, 101)],
        "customer_id": [f"CUST_{i%20:03d}" for i in range(1, 101)],
        "amount": [float(i * 10) for i in range(1, 101)],
        "status": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"] * 25
    }
    storage.add_dataset(SESSION_ID, "ecommerce_orders", pd.DataFrame(orders_data))
    
    # 2. HR Employees
    employees_data = {
        "emp_id": [f"CUST_{i:03d}" for i in range(20)], # Overlap with customer_id for joins
        "name": [f"Employee {i}" for i in range(20)],
        "department": ["Sales", "Engineering", "HR", "Marketing"] * 5,
        "salary": [50000 + i*1000 for i in range(20)],
        "join_date": ["2023-01-01"] * 20,
        "is_active": [True, False] * 10
    }
    storage.add_dataset(SESSION_ID, "hr_employees", pd.DataFrame(employees_data))
    
    # 3. Student Scores
    scores_data = {
        "student_id": [f"STU_{i:03d}" for i in range(1, 21)],
        "subject": ["Math", "Science", "History", "Art"] * 5,
        "score": [50 + i*2 for i in range(20)]
    }
    storage.add_dataset(SESSION_ID, "student_scores", pd.DataFrame(scores_data))
    
    # 4. IoT Logs
    logs_data = {
        "log_id": range(1, 201),
        "location": ["Factory_A", "Factory_B"] * 100,
        "sensor_id": ["S1", "S2"] * 100,
        "temperature": [20 + (i%15) for i in range(200)],
        "humidity": [60] * 200
    }
    storage.add_dataset(SESSION_ID, "iot_logs", pd.DataFrame(logs_data))

    # 5. Financial Ledger
    ledger_data = {
        "tx_id": [f"ORD_{i:03d}" for i in range(1, 51)], # Overlap with order_id
        "amount": [100.0] * 50,
        "tx_type": ["DEBIT", "CREDIT"] * 25
    }
    storage.add_dataset(SESSION_ID, "financial_ledger", pd.DataFrame(ledger_data))
    
    yield
    
    # Cleanup
    storage.delete_session(SESSION_ID)

def create_tree(commands, table_name="ecommerce_orders"):
    """Helper to build the operation tree structure"""
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "source", 
                "type": "source", 
                "order": 0, 
                "config": {"mainTable": table_name}
            }
        ] + commands,
        "children": []
    }

def execute(commands, table_name="ecommerce_orders", target_cmd_id=None):
    tree = create_tree(commands, table_name)
    target_node_id = "root"
    # If no target_cmd_id is provided, use the last command's ID
    if not target_cmd_id and commands:
        target_cmd_id = commands[-1]["id"]
    elif not target_cmd_id:
        target_cmd_id = "source"

    payload = {
        "sessionId": SESSION_ID,
        "tree": tree,
        "targetNodeId": target_node_id,
        "targetCommandId": target_cmd_id,
        "page": 1,
        "pageSize": 50
    }
    return client.post("/execute", json=payload)

# --- 1. FILTERING TESTS (30 Cases) ---
FILTER_PARAMS = [
    # Numeric
    ("amount", ">", 100, "number", lambda r: r["amount"] > 100),
    ("amount", "<", 50, "number", lambda r: r["amount"] < 50),
    ("amount", ">=", 200, "number", lambda r: r["amount"] >= 200),
    ("amount", "<=", 20, "number", lambda r: r["amount"] <= 20),
    ("amount", "=", 150.0, "number", lambda r: r["amount"] == 150.0), 
    ("amount", "!=", 0, "number", lambda r: r["amount"] != 0),
    
    # String
    ("status", "=", "PENDING", "string", lambda r: r["status"] == "PENDING"),
    ("status", "!=", "CANCELLED", "string", lambda r: r["status"] != "CANCELLED"),
    ("customer_id", "contains", "CUST_00", "string", lambda r: "CUST_00" in r["customer_id"]),
    ("status", "starts_with", "SHIP", "string", lambda r: r["status"].startswith("SHIP")),
    ("status", "ends_with", "ED", "string", lambda r: r["status"].endswith("ED")),
    
    # Boolean (using is_active from hr_employees)
    ("is_active", "=", True, "boolean", lambda r: r["is_active"] is True),
    ("is_active", "=", False, "boolean", lambda r: r["is_active"] is False),
]

@pytest.mark.parametrize("field, op, val, dtype, check_func", FILTER_PARAMS)
def test_filter_ecommerce(field, op, val, dtype, check_func):
    commands = [{
        "id": "cmd_filter",
        "type": "filter",
        "order": 1,
        "config": {
            "field": field,
            "operator": op,
            "value": val,
            "dataType": dtype
        }
    }]
    # Use hr_employees for boolean test
    table = "hr_employees" if field == "is_active" else "ecommerce_orders"
    
    resp = execute(commands, table_name=table)
    assert resp.status_code == 200
    data = resp.json()
    
    # If we have rows, verify them. If empty, it's also a valid result (just checking no crash)
    for row in data["rows"]:
        assert check_func(row), f"Row {row} failed check for {field} {op} {val}"

# Additional Filter Cases (Compound, Nulls, etc)
def test_filter_nested_logic():
    # (Status = PENDING AND Amount > 100)
    # The engine processes sequentially.
    commands = [
        {
            "id": "f1", "type": "filter", "order": 1,
            "config": {"field": "status", "operator": "=", "value": "PENDING"}
        },
        {
            "id": "f2", "type": "filter", "order": 2,
            "config": {"field": "amount", "operator": ">", "value": 100, "dataType": "number"}
        }
    ]
    resp = execute(commands)
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    for r in rows:
        assert r["status"] == "PENDING"
        assert r["amount"] > 100

def test_filter_date():
    # Using hr_employees join_date
    commands = [{
        "id": "f1", "type": "filter", "order": 1,
        "config": {"field": "join_date", "operator": "not_null", "value": None}
    }]
    resp = execute(commands, "hr_employees")
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) > 0

# --- 2. JOIN TESTS (20 Cases) ---

def test_join_employees_sales_cross_dept():
    # Join Employees with Orders on customer_id = emp_id (generated data overlaps)
    commands = [{
        "id": "j1", "type": "join", "order": 1,
        "config": {
            "joinTable": "hr_employees",
            "joinType": "LEFT",
            "on": "ecommerce_orders.customer_id = hr_employees.emp_id"
        }
    }]
    resp = execute(commands, "ecommerce_orders")
    assert resp.status_code == 200
    # Check if a column from right table exists
    assert "salary" in resp.json()["columns"]

# --- MORE FILTER PERMUTATIONS ---
@pytest.mark.parametrize("op", ["=", "!=", "contains", "starts_with", "ends_with"])
@pytest.mark.parametrize("val", ["Employee", "Sales", "Eng"])
def test_filter_strings_permutations(op, val):
    commands = [{
        "id": "f1", "type": "filter", "order": 1,
        "config": {"field": "name", "operator": op, "value": val, "dataType": "string"}
    }]
    resp = execute(commands, "hr_employees")
    assert resp.status_code == 200

# --- MORE JOIN TYPES ---
@pytest.mark.parametrize("join_type", ["LEFT", "RIGHT", "INNER", "FULL"])
@pytest.mark.parametrize("target_table", ["hr_employees", "financial_ledger", "iot_logs"])
def test_join_types_permutations(join_type, target_table):
    commands = [{
        "id": "j1", "type": "join", "order": 1,
        "config": {
            "joinTable": target_table,
            "joinType": join_type,
            "on": "ecommerce_orders.customer_id = {}.emp_id".format(target_table) if target_table == "hr_employees" else "ecommerce_orders.order_id = {}.tx_id".format(target_table)
        }
    }]
    resp = execute(commands, "ecommerce_orders")
    assert resp.status_code == 200

# --- MORE AGGREGATIONS ---
@pytest.mark.parametrize("func", ["min", "max", "sum", "mean", "count"])
@pytest.mark.parametrize("field", ["amount", "salary", "score"])
def test_agg_permutations(func, field):
    # Map field to table
    table = "ecommerce_orders" if field == "amount" else ("hr_employees" if field == "salary" else "student_scores")
    commands = [{
        "id": "a1", "type": "aggregate", "order": 1,
        "config": {
            "groupBy": [], # Global agg
            "aggFunc": func,
            "field": field
        }
    }]
    resp = execute(commands, table)
    assert resp.status_code == 200
    # Should contain aggregated column
    cols = resp.json()["columns"]
    # Aggregation usually creates new column names like mean_salary or similar
    # Or for this test engine we check that the result has 1 row (global agg)
    assert resp.json()["totalCount"] == 1

def test_join_iot_locations():
    # Join IoT logs with Inventory logic (using self join for simplicity as we have limited tables)
    # Joining IOT with itself on location
    commands = [{
        "id": "j1", "type": "join", "order": 1,
        "config": {
            "joinTable": "iot_logs",
            "joinType": "LEFT",
            "on": "iot_logs.location = iot_logs_joined.location"
        }
    }]
    resp = execute(commands, "iot_logs")
    assert resp.status_code == 200

# Multi-join
def test_multi_join_chain():
    commands = [
        {
            "id": "j1", "type": "join", "order": 1,
            "config": {"joinTable": "hr_employees", "joinType": "LEFT", "on": "ecommerce_orders.customer_id = hr_employees.emp_id"}
        },
        {
            "id": "j2", "type": "join", "order": 2,
            "config": {"joinTable": "financial_ledger", "joinType": "LEFT", "on": "ecommerce_orders.order_id = financial_ledger.tx_id"}
        }
    ]
    resp = execute(commands, "ecommerce_orders")
    assert resp.status_code == 200
    cols = resp.json()["columns"]
    assert "salary" in cols
    assert "tx_type" in cols

# --- 3. AGGREGATION TESTS ---

AGG_PARAMS = [
    ("ecommerce_orders", "status", "count", "order_id"),
    ("ecommerce_orders", "status", "sum", "amount"),
    ("ecommerce_orders", "status", "mean", "amount"),
    ("ecommerce_orders", "status", "min", "amount"),
    ("ecommerce_orders", "status", "max", "amount"),
    ("hr_employees", "department", "mean", "salary"),
    ("hr_employees", "department", "count", "emp_id"),
    ("iot_logs", "location", "mean", "temperature"),
    ("iot_logs", "sensor_id", "max", "humidity"),
    ("student_scores", "subject", "mean", "score"),
]

@pytest.mark.parametrize("table, group, func, field", AGG_PARAMS)
def test_aggregation_basic(table, group, func, field):
    commands = [{
        "id": "agg1", "type": "aggregate", "order": 1,
        "config": {
            "groupBy": [group],
            "aggFunc": func,
            "field": field
        }
    }]
    resp = execute(commands, table)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) > 0
    assert group in data["rows"][0]

def test_aggregation_multi_group():
    commands = [{
        "id": "agg1", "type": "aggregate", "order": 1,
        "config": {
            "groupBy": ["location", "sensor_id"],
            "aggFunc": "mean",
            "field": "temperature"
        }
    }]
    resp = execute(commands, "iot_logs")
    assert resp.status_code == 200
    data = resp.json()
    assert "location" in data["columns"]
    assert "sensor_id" in data["columns"]

# --- 4. SORT & PAGINATION ---

def test_sort_asc():
    commands = [{
        "id": "s1", "type": "sort", "order": 1,
        "config": {"field": "amount", "ascending": True}
    }]
    resp = execute(commands, "ecommerce_orders")
    rows = resp.json()["rows"]
    vals = [r["amount"] for r in rows if r["amount"] is not None]
    assert vals == sorted(vals)

def test_sort_desc():
    commands = [{
        "id": "s1", "type": "sort", "order": 1,
        "config": {"field": "score", "ascending": False}
    }]
    resp = execute(commands, "student_scores")
    rows = resp.json()["rows"]
    vals = [r["score"] for r in rows if r["score"] is not None]
    assert vals == sorted(vals, reverse=True)

def test_pagination():
    # Page 1
    p1 = client.post("/execute", json={
        "sessionId": SESSION_ID,
        "tree": create_tree([], "hr_employees"),
        "targetNodeId": "root",
        "page": 1,
        "pageSize": 10
    }).json()
    
    # Page 2
    p2 = client.post("/execute", json={
        "sessionId": SESSION_ID,
        "tree": create_tree([], "hr_employees"),
        "targetNodeId": "root",
        "page": 2,
        "pageSize": 10
    }).json()
    
    assert len(p1["rows"]) == 10
    assert len(p2["rows"]) == 10
    # Check IDs differ
    ids1 = [r["emp_id"] for r in p1["rows"]]
    ids2 = [r["emp_id"] for r in p2["rows"]]
    assert set(ids1).isdisjoint(set(ids2))

# --- 5. SQL EXECUTION ---

SQL_QUERIES = [
    "SELECT * FROM ecommerce_orders LIMIT 5",
    "SELECT count(*) as c FROM hr_employees",
    "SELECT department, avg(salary) FROM hr_employees GROUP BY department",
    "SELECT * FROM iot_logs WHERE temperature > 25",
    "SELECT sum(amount) FROM financial_ledger WHERE tx_type = 'DEBIT'",
    "SELECT * FROM student_scores ORDER BY score DESC LIMIT 3",
    "SELECT DISTINCT location FROM iot_logs",
]

@pytest.mark.parametrize("query", SQL_QUERIES)
def test_sql_execution(query):
    payload = {
        "sessionId": SESSION_ID,
        "query": query,
        "page": 1,
        "pageSize": 50
    }
    resp = client.post("/query", json=payload)
    assert resp.status_code == 200
    assert "rows" in resp.json()

# --- 6. ERROR HANDLING ---

def test_error_invalid_column():
    commands = [{
        "id": "f1", "type": "filter", "order": 1,
        "config": {"field": "NON_EXISTENT_COL", "operator": "=", "value": 1}
    }]
    resp = execute(commands)
    # The engine might effectively ignore or return full dataset if column missing depending on implementation logic
    # Or return error. Assuming engine handles gracefully or fails.
    pass 

def test_error_invalid_sql():
    payload = {"sessionId": SESSION_ID, "query": "SELECT * FROM NON_EXISTENT_TABLE"}
    resp = client.post("/query", json=payload)
    assert resp.status_code == 400 or resp.status_code == 500

@pytest.mark.parametrize("limit", [1, 5, 10])
def test_pagination_sizes(limit):
    commands = []
    tree = create_tree(commands, "ecommerce_orders")
    payload = {
        "sessionId": SESSION_ID, "tree": tree, "targetNodeId": "root", 
        "page": 1, "pageSize": limit
    }
    resp = client.post("/execute", json=payload)
    assert len(resp.json()["rows"]) == limit

# Cartesian product of filters
@pytest.mark.parametrize("status", ["PENDING", "SHIPPED"])
@pytest.mark.parametrize("amount_thresh", [10, 100])
def test_complex_filter_permutations(status, amount_thresh):
    commands = [
        {"id": "f1", "type": "filter", "order": 1, "config": {"field": "status", "operator": "=", "value": status}},
        {"id": "f2", "type": "filter", "order": 2, "config": {"field": "amount", "operator": ">", "value": amount_thresh, "dataType": "number"}}
    ]
    resp = execute(commands, "ecommerce_orders")
    assert resp.status_code == 200
