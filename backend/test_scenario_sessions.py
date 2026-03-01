"""
Comprehensive test suite based on real session scenarios.
Tests cover: variable types, in_list/not_in_list operators, nested filters,
define_variable command, and full workflow scenarios observed in existing sessions.
"""

import pytest
import json
from pathlib import Path
import pandas as pd
from fastapi.testclient import TestClient
from main import app
from storage import storage

client = TestClient(app)


# === FIXTURES ===

@pytest.fixture(autouse=True)
def clean_storage():
    """Ensure clean state before each test."""
    storage.clear()
    yield
    storage.clear()


@pytest.fixture
def session_with_ecommerce():
    """Create a session with ecommerce_orders dataset matching real session data."""
    res = client.post("/sessions")
    session_id = res.json()["sessionId"]

    # Create dataset matching sess_aad86dcb structure
    df = pd.DataFrame({
        "order_id": [f"ORD_000{i}" for i in range(1, 11)],
        "customer_id": [f"CUST_0{i:02d}" for i in [17, 5, 15, 7, 16, 13, 19, 10, 3, 19]],
        "amount": [317.71, 49.06, 274.25, 217.41, 450.93, 21.67, 297.56, 52.71, 63.85, 25.58],
        "status": ["DELIVERED", "DELIVERED", "PENDING", "CANCELLED", "SHIPPED",
                   "CANCELLED", "DELIVERED", "DELIVERED", "SHIPPED", "PENDING"],
        "order_date": [f"2025-{m:02d}-{d:02d}" for m, d in
                       [(5, 12), (11, 14), (2, 15), (9, 17), (12, 19),
                        (10, 1), (4, 12), (2, 3), (2, 24), (3, 5)]]
    })
    storage.add_dataset(session_id, "ecommerce_orders.csv", df)
    return session_id


def build_tree_with_source(commands, table_name, link_id="link_test"):
    """Build operation tree with source command and linkId."""
    return {
        "id": "root",
        "type": "operation",
        "operationType": "root",
        "name": "Project",
        "enabled": True,
        "commands": [],
        "children": [
            {
                "id": "setup_1",
                "type": "operation",
                "operationType": "setup",
                "name": "Data Setup",
                "enabled": True,
                "commands": [
                    {
                        "id": "cmd_src",
                        "type": "source",
                        "order": 1,
                        "config": {
                            "mainTable": table_name,
                            "alias": table_name,
                            "linkId": link_id
                        }
                    }
                ] + commands,
                "children": []
            }
        ]
    }


# === TEST: DEFINE_VARIABLE COMMAND ===

class TestDefineVariable:
    """Tests for define_variable command with different variable types."""

    def test_define_list_variable(self, session_with_ecommerce):
        """Test defining a list variable and using it in filter."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "operationType": "root",
            "name": "Root",
            "enabled": True,
            "commands": [],
            "children": [
                {
                    "id": "setup",
                    "type": "operation",
                    "operationType": "setup",
                    "name": "Setup",
                    "enabled": True,
                    "commands": [
                        {
                            "id": "src",
                            "type": "source",
                            "order": 1,
                            "config": {"mainTable": "ecommerce_orders", "linkId": "link_1"}
                        },
                        {
                            "id": "var_list",
                            "type": "define_variable",
                            "order": 2,
                            "config": {
                                "variableName": "target_orders",
                                "variableType": "list",
                                "variableValue": ["ORD_0003", "ORD_0004"]
                            }
                        }
                    ],
                    "children": [
                        {
                            "id": "filter_node",
                            "type": "operation",
                            "operationType": "process",
                            "name": "Filter by List",
                            "enabled": True,
                            "commands": [
                                {
                                    "id": "filter_cmd",
                                    "type": "filter",
                                    "order": 1,
                                    "config": {
                                        "filterRoot": {
                                            "id": "root_filter",
                                            "type": "group",
                                            "logicalOperator": "AND",
                                            "conditions": [
                                                {
                                                    "id": "cond_1",
                                                    "type": "condition",
                                                    "field": "order_id",
                                                    "operator": "in_list",
                                                    "value": "target_orders",
                                                    "valueType": "variable"
                                                }
                                            ]
                                        },
                                        "dataSource": "link_1"
                                    }
                                }
                            ],
                            "children": []
                        }
                    ]
                }
            ]
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "filter_node"
        })

        assert res.status_code == 200
        data = res.json()
        assert data["totalCount"] == 2
        order_ids = [r["order_id"] for r in data["rows"]]
        assert set(order_ids) == {"ORD_0003", "ORD_0004"}

    def test_define_text_variable_with_contains(self, session_with_ecommerce):
        """Test defining a text variable (comma-separated) for contains operator."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "operationType": "root",
            "name": "Root",
            "enabled": True,
            "commands": [],
            "children": [
                {
                    "id": "setup",
                    "type": "operation",
                    "operationType": "setup",
                    "name": "Setup",
                    "enabled": True,
                    "commands": [
                        {
                            "id": "src",
                            "type": "source",
                            "order": 1,
                            "config": {"mainTable": "ecommerce_orders", "linkId": "link_1"}
                        },
                        {
                            "id": "var_text",
                            "type": "define_variable",
                            "order": 2,
                            "config": {
                                "variableName": "search_pattern",
                                "variableType": "text",
                                "variableValue": "ORD_0003, ORD_0004"
                            }
                        }
                    ],
                    "children": [
                        {
                            "id": "filter_node",
                            "type": "operation",
                            "operationType": "process",
                            "name": "Filter by Text Pattern",
                            "enabled": True,
                            "commands": [
                                {
                                    "id": "filter_cmd",
                                    "type": "filter",
                                    "order": 1,
                                    "config": {
                                        "filterRoot": {
                                            "id": "root_filter",
                                            "type": "group",
                                            "logicalOperator": "AND",
                                            "conditions": [
                                                {
                                                    "id": "cond_1",
                                                    "type": "condition",
                                                    "field": "order_id",
                                                    "operator": "in_list",
                                                    "value": "search_pattern",
                                                    "valueType": "variable"
                                                }
                                            ]
                                        },
                                        "dataSource": "link_1"
                                    }
                                }
                            ],
                            "children": []
                        }
                    ]
                }
            ]
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "filter_node"
        })

        assert res.status_code == 200
        data = res.json()
        # Text variable "ORD_0003, ORD_0004" split by comma should match 2 orders
        assert data["totalCount"] == 2


# === TEST: IN_LIST / NOT_IN_LIST OPERATORS ===

class TestInListOperators:
    """Tests for in_list and not_in_list filter operators."""

    def test_in_list_with_literal_values(self, session_with_ecommerce):
        """Test in_list operator with directly specified list values."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {
                    "id": "src",
                    "type": "source",
                    "order": 0,
                    "config": {"mainTable": "ecommerce_orders"}
                },
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "in_list",
                                    "value": "PENDING, SHIPPED"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        # PENDING: ORD_0003, ORD_0010; SHIPPED: ORD_0005, ORD_0009 = 4 rows
        assert data["totalCount"] == 4
        for row in data["rows"]:
            assert row["status"] in ["PENDING", "SHIPPED"]

    def test_not_in_list_operator(self, session_with_ecommerce):
        """Test not_in_list operator excludes specified values."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {
                    "id": "src",
                    "type": "source",
                    "order": 0,
                    "config": {"mainTable": "ecommerce_orders"}
                },
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "not_in_list",
                                    "value": "CANCELLED, PENDING"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        # Exclude CANCELLED (ORD_0004, ORD_0006) and PENDING (ORD_0003, ORD_0010)
        # Remaining: DELIVERED (4) + SHIPPED (2) = 6
        assert data["totalCount"] == 6
        for row in data["rows"]:
            assert row["status"] not in ["CANCELLED", "PENDING"]

    def test_in_list_with_order_ids(self, session_with_ecommerce):
        """Test in_list operator with string field values (order IDs)."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {
                    "id": "src",
                    "type": "source",
                    "order": 0,
                    "config": {"mainTable": "ecommerce_orders"}
                },
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "order_id",
                                    "operator": "in_list",
                                    "value": "ORD_0001, ORD_0002, ORD_0003"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        assert data["totalCount"] == 3
        order_ids = [r["order_id"] for r in data["rows"]]
        assert set(order_ids) == {"ORD_0001", "ORD_0002", "ORD_0003"}


# === TEST: NESTED FILTER GROUPS (AND/OR) ===

class TestNestedFilterGroups:
    """Tests for complex nested filter conditions with AND/OR logic."""

    def test_nested_and_or_filter(self, session_with_ecommerce):
        """Test nested filter: (status = DELIVERED) AND (amount > 100 OR amount < 30)"""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {
                    "id": "src",
                    "type": "source",
                    "order": 0,
                    "config": {"mainTable": "ecommerce_orders"}
                },
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "root_group",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "=",
                                    "value": "DELIVERED"
                                },
                                {
                                    "id": "nested_or",
                                    "type": "group",
                                    "logicalOperator": "OR",
                                    "conditions": [
                                        {
                                            "id": "c2",
                                            "type": "condition",
                                            "field": "amount",
                                            "operator": ">",
                                            "value": 100,
                                            "dataType": "number"
                                        },
                                        {
                                            "id": "c3",
                                            "type": "condition",
                                            "field": "amount",
                                            "operator": "<",
                                            "value": 30,
                                            "dataType": "number"
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()

        # DELIVERED orders: ORD_0001 (317.71), ORD_0002 (49.06), ORD_0007 (297.56), ORD_0008 (52.71)
        # Filter: amount > 100 OR amount < 30
        # Matching: ORD_0001 (317.71 > 100), ORD_0007 (297.56 > 100) = 2 rows
        for row in data["rows"]:
            assert row["status"] == "DELIVERED"
            assert row["amount"] > 100 or row["amount"] < 30

    def test_deeply_nested_filters(self, session_with_ecommerce):
        """Test deeply nested filter: ((A AND B) OR (C AND D))"""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {
                    "id": "src",
                    "type": "source",
                    "order": 0,
                    "config": {"mainTable": "ecommerce_orders"}
                },
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "root_or",
                            "type": "group",
                            "logicalOperator": "OR",
                            "conditions": [
                                {
                                    "id": "group_a",
                                    "type": "group",
                                    "logicalOperator": "AND",
                                    "conditions": [
                                        {"id": "a1", "type": "condition", "field": "status", "operator": "=", "value": "DELIVERED"},
                                        {"id": "a2", "type": "condition", "field": "amount", "operator": ">", "value": 200, "dataType": "number"}
                                    ]
                                },
                                {
                                    "id": "group_b",
                                    "type": "group",
                                    "logicalOperator": "AND",
                                    "conditions": [
                                        {"id": "b1", "type": "condition", "field": "status", "operator": "=", "value": "SHIPPED"},
                                        {"id": "b2", "type": "condition", "field": "amount", "operator": "<", "value": 100, "dataType": "number"}
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()

        # Group A: DELIVERED AND amount > 200 -> ORD_0001 (317.71), ORD_0007 (297.56)
        # Group B: SHIPPED AND amount < 100 -> ORD_0009 (63.85)
        # Total: 3 rows
        for row in data["rows"]:
            is_group_a = row["status"] == "DELIVERED" and row["amount"] > 200
            is_group_b = row["status"] == "SHIPPED" and row["amount"] < 100
            assert is_group_a or is_group_b


# === TEST: SEQUENTIAL NODE EXECUTION (PARENT -> CHILD) ===

class TestSequentialNodeExecution:
    """Tests for hierarchical node execution matching real session structure."""

    def test_parent_filter_then_child_filter(self, session_with_ecommerce):
        """Test that child node inherits filtered data from parent."""
        session_id = session_with_ecommerce

        # Matches sess_aad86dcb structure: "not ORD_0002" -> "not ORD_0001"
        tree = {
            "id": "root",
            "type": "operation",
            "operationType": "root",
            "name": "Project",
            "enabled": True,
            "commands": [],
            "children": [
                {
                    "id": "setup_1",
                    "type": "operation",
                    "operationType": "setup",
                    "name": "Data Setup",
                    "enabled": True,
                    "commands": [
                        {
                            "id": "cmd_src",
                            "type": "source",
                            "order": 1,
                            "config": {"mainTable": "ecommerce_orders", "linkId": "link_1"}
                        }
                    ],
                    "children": [
                        {
                            "id": "op_filter_1",
                            "type": "operation",
                            "operationType": "process",
                            "name": "not ORD_0002",
                            "enabled": True,
                            "commands": [
                                {
                                    "id": "cmd_filter_1",
                                    "type": "filter",
                                    "order": 1,
                                    "config": {
                                        "filterRoot": {
                                            "id": "root_1",
                                            "type": "group",
                                            "logicalOperator": "AND",
                                            "conditions": [
                                                {
                                                    "id": "cond_1",
                                                    "type": "condition",
                                                    "field": "order_id",
                                                    "operator": "!=",
                                                    "value": "ORD_0002"
                                                }
                                            ]
                                        },
                                        "dataSource": "link_1"
                                    }
                                }
                            ],
                            "children": [
                                {
                                    "id": "op_filter_2",
                                    "type": "operation",
                                    "operationType": "process",
                                    "name": "not ORD_0001",
                                    "enabled": True,
                                    "commands": [
                                        {
                                            "id": "cmd_filter_2",
                                            "type": "filter",
                                            "order": 1,
                                            "config": {
                                                "filterRoot": {
                                                    "id": "root_2",
                                                    "type": "group",
                                                    "logicalOperator": "AND",
                                                    "conditions": [
                                                        {
                                                            "id": "cond_2",
                                                            "type": "condition",
                                                            "field": "order_id",
                                                            "operator": "!=",
                                                            "value": "ORD_0001"
                                                        }
                                                    ]
                                                },
                                                "dataSource": "link_1"
                                            }
                                        }
                                    ],
                                    "children": []
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        # Execute at first filter node
        res1 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "op_filter_1"
        })
        assert res1.status_code == 200
        data1 = res1.json()
        # Should have 9 rows (10 - ORD_0002)
        assert data1["totalCount"] == 9
        assert all(r["order_id"] != "ORD_0002" for r in data1["rows"])

        # Execute at second filter node (child)
        res2 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "op_filter_2"
        })
        assert res2.status_code == 200
        data2 = res2.json()
        # Should have 8 rows (10 - ORD_0002 - ORD_0001)
        assert data2["totalCount"] == 8
        order_ids = [r["order_id"] for r in data2["rows"]]
        assert "ORD_0001" not in order_ids
        assert "ORD_0002" not in order_ids


# === TEST: STRING OPERATORS ===

class TestStringOperators:
    """Tests for string-specific filter operators."""

    def test_contains_operator(self, session_with_ecommerce):
        """Test contains operator with partial string match."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "order_id",
                                    "operator": "contains",
                                    "value": "0001, 0002"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        # Should match ORD_0001, ORD_0002, and ORD_00010 (contains "0001")
        assert data["totalCount"] >= 2
        for row in data["rows"]:
            assert "0001" in row["order_id"] or "0002" in row["order_id"]

    def test_not_contains_operator(self, session_with_ecommerce):
        """Test not_contains operator excludes partial string matches."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "not_contains",
                                    "value": "DELIVER"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        # Should exclude DELIVERED status
        for row in data["rows"]:
            assert "DELIVER" not in row["status"]

    def test_starts_with_operator(self, session_with_ecommerce):
        """Test starts_with operator."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "starts_with",
                                    "value": "DE"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        for row in data["rows"]:
            assert row["status"].startswith("DE")

    def test_ends_with_operator(self, session_with_ecommerce):
        """Test ends_with operator."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "status",
                                    "operator": "ends_with",
                                    "value": "ED"
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()
        # DELIVERED, SHIPPED, CANCELLED all end with ED
        for row in data["rows"]:
            assert row["status"].endswith("ED")


# === TEST: EMPTY/NULL HANDLING ===

class TestEmptyNullHandling:
    """Tests for is_empty and is_not_empty operators."""

    def test_is_empty_operator(self):
        """Test is_empty operator filters null/empty values."""
        res = client.post("/sessions")
        session_id = res.json()["sessionId"]

        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Alice", "", None, "Bob"],
            "value": [10, 20, 30, 40]
        })
        storage.add_dataset(session_id, "test.csv", df)

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "test"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "name",
                                    "operator": "is_empty",
                                    "value": None
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        result = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert result.status_code == 200
        data = result.json()
        # Should match rows with empty string or null name (id=2, id=3)
        assert data["totalCount"] == 2

    def test_is_not_empty_operator(self):
        """Test is_not_empty operator filters out null/empty values."""
        res = client.post("/sessions")
        session_id = res.json()["sessionId"]

        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Alice", "", None, "Bob"],
            "value": [10, 20, 30, 40]
        })
        storage.add_dataset(session_id, "test.csv", df)

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "test"}},
                {
                    "id": "filter",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {
                                    "id": "c1",
                                    "type": "condition",
                                    "field": "name",
                                    "operator": "is_not_empty",
                                    "value": None
                                }
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        result = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert result.status_code == 200
        data = result.json()
        # Should match rows with non-empty name (id=1, id=4)
        assert data["totalCount"] == 2
        for row in data["rows"]:
            assert row["name"] and row["name"].strip()


# === TEST: TARGET COMMAND ID (PARTIAL EXECUTION) ===

class TestTargetCommandExecution:
    """Tests for executing up to a specific command ID."""

    def test_execute_up_to_specific_command(self, session_with_ecommerce):
        """Test executing only up to a specified command, not the full node."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "filter_1",
                    "type": "filter",
                    "order": 1,
                    "config": {
                        "filterRoot": {
                            "id": "g1",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {"id": "c1", "type": "condition", "field": "status", "operator": "=", "value": "DELIVERED"}
                            ]
                        }
                    }
                },
                {
                    "id": "filter_2",
                    "type": "filter",
                    "order": 2,
                    "config": {
                        "filterRoot": {
                            "id": "g2",
                            "type": "group",
                            "logicalOperator": "AND",
                            "conditions": [
                                {"id": "c2", "type": "condition", "field": "amount", "operator": ">", "value": 100, "dataType": "number"}
                            ]
                        }
                    }
                }
            ],
            "children": []
        }

        # Execute up to filter_1 only
        res1 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": "filter_1"
        })

        assert res1.status_code == 200
        data1 = res1.json()
        # Should have all DELIVERED orders (4 rows)
        assert data1["totalCount"] == 4

        # Execute up to filter_2 (full node)
        res2 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": "filter_2"
        })

        assert res2.status_code == 200
        data2 = res2.json()
        # Should have DELIVERED orders with amount > 100 (2 rows: 317.71, 297.56)
        assert data2["totalCount"] == 2


# === TEST: AGGREGATION WITH HAVING ===

class TestAggregationWithHaving:
    """Tests for GROUP BY with HAVING conditions."""

    def test_group_with_having_filter(self, session_with_ecommerce):
        """Test aggregation with HAVING clause filtering grouped results."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "agg",
                    "type": "group",
                    "order": 1,
                    "config": {
                        "groupByFields": ["status"],
                        "aggregations": [
                            {"field": "amount", "func": "sum", "alias": "total_amount"},
                            {"field": "*", "func": "count", "alias": "order_count"}
                        ],
                        "havingConditions": [
                            {"metricAlias": "order_count", "operator": ">=", "value": 2}
                        ]
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()

        # Statuses with >= 2 orders: DELIVERED (4), SHIPPED (2), CANCELLED (2), PENDING (2)
        for row in data["rows"]:
            assert row["order_count"] >= 2


# === TEST: TRANSFORM OPERATIONS ===

class TestTransformOperations:
    """Tests for field transformation and calculation."""

    def test_simple_expression_transform(self, session_with_ecommerce):
        """Test transform with simple arithmetic expression."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}},
                {
                    "id": "transform",
                    "type": "transform",
                    "order": 1,
                    "config": {
                        "mappings": [
                            {
                                "id": "m1",
                                "expression": "amount * 1.1",
                                "outputField": "amount_with_tax",
                                "mode": "simple"
                            }
                        ]
                    }
                }
            ],
            "children": []
        }

        res = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root"
        })

        assert res.status_code == 200
        data = res.json()

        assert "amount_with_tax" in data["columns"]
        for row in data["rows"]:
            expected = row["amount"] * 1.1
            assert abs(row["amount_with_tax"] - expected) < 0.01


# === TEST: PAGINATION ===

class TestPagination:
    """Tests for pagination functionality."""

    def test_pagination_pages(self, session_with_ecommerce):
        """Test pagination returns correct pages."""
        session_id = session_with_ecommerce

        tree = {
            "id": "root",
            "type": "operation",
            "name": "Root",
            "enabled": True,
            "commands": [
                {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "ecommerce_orders"}}
            ],
            "children": []
        }

        # Page 1
        res1 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "page": 1,
            "pageSize": 3
        })

        # Page 2
        res2 = client.post("/execute", json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "page": 2,
            "pageSize": 3
        })

        assert res1.status_code == 200
        assert res2.status_code == 200

        data1 = res1.json()
        data2 = res2.json()

        assert len(data1["rows"]) == 3
        assert len(data2["rows"]) == 3
        assert data1["totalCount"] == 10

        # Ensure different rows
        ids1 = {r["order_id"] for r in data1["rows"]}
        ids2 = {r["order_id"] for r in data2["rows"]}
        assert ids1.isdisjoint(ids2)


# === TEST: SESSION STATE PERSISTENCE ===

class TestSessionStatePersistence:
    """Tests for session state save and retrieve."""

    def test_save_and_retrieve_state(self):
        """Test saving and retrieving session state."""
        res = client.post("/sessions")
        session_id = res.json()["sessionId"]

        state = {
            "tree": {
                "id": "root",
                "type": "operation",
                "operationType": "root",
                "name": "Test Project",
                "enabled": True,
                "commands": [],
                "children": []
            },
            "datasets": [],
            "sqlHistory": [
                {"id": "1", "timestamp": 1234567890, "query": "SELECT 1", "status": "success"}
            ]
        }

        # Save state
        save_res = client.post(f"/sessions/{session_id}/state", json=state)
        assert save_res.status_code == 200

        # Retrieve state
        get_res = client.get(f"/sessions/{session_id}/state")
        assert get_res.status_code == 200

        retrieved = get_res.json()
        assert retrieved["tree"]["name"] == "Test Project"
        assert len(retrieved["sqlHistory"]) == 1
        assert retrieved["sqlHistory"][0]["query"] == "SELECT 1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
