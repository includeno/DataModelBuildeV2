"""Tests for data lineage tracking (T1.3.4).

Builds source → join → transform → group pipelines and verifies that
compute_lineage() returns correct field origins and transformation chains.
"""

import pandas as pd
import pytest

from engine import ExecutionEngine
from lineage import FieldLineage, LineageStep, LineageTracker
from models import (
    Command,
    CommandConfig,
    MappingRule,
    OperationNode,
    ViewFieldConfig,
)
from storage import storage


# ── Helpers ──────────────────────────────────────────────────────────────────

def _cmd(cmd_type: str, config: dict | None = None, cmd_id: str = "c1", order: int = 0) -> Command:
    return Command(id=cmd_id, type=cmd_type, order=order, config=CommandConfig(**(config or {})))


def _node(
    node_id: str,
    commands: list[Command],
    children: list[OperationNode] | None = None,
) -> OperationNode:
    return OperationNode(
        id=node_id,
        type="operation",
        operationType="process",
        name=node_id,
        enabled=True,
        commands=commands,
        children=children or [],
    )


SESSION = "test_session"


@pytest.fixture(autouse=True)
def clean_storage():
    storage.clear()
    yield
    storage.clear()


def _store(table: str, df: pd.DataFrame):
    storage.add_dataset(SESSION, table, df)


# ── LineageTracker unit tests ─────────────────────────────────────────────────

class TestLineageTracker:
    def test_init_from_source(self):
        t = LineageTracker()
        t.init_from_source("orders", ["id", "amount"], "n1", "c1")
        d = t.to_dict()
        assert "id" in d
        assert d["id"]["originTable"] == "orders"
        assert d["id"]["originField"] == "id"
        assert len(d["id"]["transformations"]) == 1
        assert d["id"]["transformations"][0]["commandType"] == "source"

    def test_record_join_adds_new_fields(self):
        t = LineageTracker()
        t.init_from_source("orders", ["id"], "n1", "c1")
        t.record_join("users", ["name", "email"], "n1", "c2")
        d = t.to_dict()
        assert "name" in d
        assert d["name"]["originTable"] == "users"
        assert d["name"]["transformations"][0]["commandType"] == "join"

    def test_record_join_does_not_overwrite_existing_fields(self):
        t = LineageTracker()
        t.init_from_source("orders", ["id", "name"], "n1", "c1")
        t.record_join("users", ["name", "email"], "n1", "c2")
        # "name" already existed — should not be overwritten
        d = t.to_dict()
        assert d["name"]["originTable"] == "orders"

    def test_record_transform_new_field(self):
        t = LineageTracker()
        t.init_from_source("orders", ["amount"], "n1", "c1")
        t.record_transform("revenue", "amount * 1.2", "n1", "c2")
        d = t.to_dict()
        assert "revenue" in d
        assert d["revenue"]["originTable"] == "computed"
        assert d["revenue"]["transformations"][0]["expression"] == "amount * 1.2"

    def test_record_transform_existing_field_appends_step(self):
        t = LineageTracker()
        t.init_from_source("orders", ["amount"], "n1", "c1")
        t.record_transform("amount", "amount * 1.2", "n1", "c2")
        d = t.to_dict()
        assert len(d["amount"]["transformations"]) == 2
        assert d["amount"]["transformations"][1]["commandType"] == "transform"

    def test_record_group_replaces_field_set(self):
        t = LineageTracker()
        t.init_from_source("orders", ["user_id", "amount", "date"], "n1", "c1")
        t.record_group(
            group_fields=["user_id"],
            agg_aliases=["total"],
            agg_expressions=["sum(amount)"],
            node_id="n1",
            command_id="c2",
        )
        d = t.to_dict()
        assert set(d.keys()) == {"user_id", "total"}
        assert d["total"]["originTable"] == "computed"
        assert d["total"]["transformations"][0]["expression"] == "sum(amount)"
        # user_id retains its original origin but gains a 'group' step
        assert d["user_id"]["originTable"] == "orders"
        assert d["user_id"]["transformations"][-1]["commandType"] == "group"

    def test_record_view_prunes_fields(self):
        t = LineageTracker()
        t.init_from_source("orders", ["id", "amount", "date"], "n1", "c1")
        t.record_view(["id", "amount"], "n1", "c2")
        d = t.to_dict()
        assert set(d.keys()) == {"id", "amount"}
        assert d["id"]["transformations"][-1]["commandType"] == "view"

    def test_empty_tracker_to_dict(self):
        t = LineageTracker()
        assert t.to_dict() == {}


# ── compute_lineage integration tests ────────────────────────────────────────

class TestComputeLineage:
    def test_source_only(self):
        _store("products", pd.DataFrame({"id": [1], "name": ["a"], "price": [10.0]}))
        cmd = _cmd("source", {"mainTable": "products"})
        node = _node("n1", [cmd])
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        assert "id" in result
        assert result["id"]["originTable"] == "products"
        assert result["price"]["originField"] == "price"

    def test_source_then_transform(self):
        _store("orders", pd.DataFrame({"amount": [100]}))
        cmds = [
            _cmd("source", {"mainTable": "orders"}, "c1", 0),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "amount * 1.1", "outputField": "revenue", "mode": "simple"}]
            }, "c2", 1),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        assert "revenue" in result
        assert result["revenue"]["transformations"][0]["expression"] == "amount * 1.1"
        assert result["revenue"]["transformations"][0]["commandType"] == "transform"

    def test_source_then_join(self):
        _store("orders", pd.DataFrame({"order_id": [1], "user_id": [10]}))
        _store("users", pd.DataFrame({"user_id": [10], "name": ["Alice"]}))
        cmds = [
            _cmd("source", {"mainTable": "orders"}, "c1", 0),
            _cmd("join", {
                "joinTable": "users",
                "joinType": "LEFT",
                "on": "orders.user_id = users.user_id",
                "joinSuffix": "_joined",
            }, "c2", 1),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        # "name" comes from users table
        assert "name" in result
        assert result["name"]["originTable"] == "users"
        assert result["name"]["transformations"][0]["commandType"] == "join"
        # "order_id" originates from orders
        assert result["order_id"]["originTable"] == "orders"

    def test_source_join_transform_group_pipeline(self):
        _store("orders", pd.DataFrame({"order_id": [1, 2], "user_id": [1, 1], "amount": [50.0, 75.0]}))
        _store("users", pd.DataFrame({"user_id": [1], "region": ["EU"]}))
        cmds = [
            _cmd("source", {"mainTable": "orders"}, "c1", 0),
            _cmd("join", {
                "joinTable": "users",
                "joinType": "LEFT",
                "on": "orders.user_id = users.user_id",
            }, "c2", 1),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "amount * 1.2", "outputField": "revenue", "mode": "simple"}]
            }, "c3", 2),
            _cmd("group", {
                "groupByFields": ["region"],
                "aggregations": [{"field": "revenue", "func": "sum", "alias": "total_revenue"}],
            }, "c4", 3),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")

        # After group: only region and total_revenue remain
        assert set(result.keys()) == {"region", "total_revenue"}
        assert result["region"]["originTable"] == "users"
        assert result["total_revenue"]["originTable"] == "computed"
        assert "sum(revenue)" in result["total_revenue"]["transformations"][0]["expression"]

    def test_view_prunes_fields_in_lineage(self):
        _store("items", pd.DataFrame({"id": [1], "name": ["x"], "secret": ["s"]}))
        cmds = [
            _cmd("source", {"mainTable": "items"}, "c1", 0),
            _cmd("view", {
                "viewFields": [
                    {"field": "id", "distinct": False},
                    {"field": "name", "distinct": False},
                ]
            }, "c2", 1),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        assert "secret" not in result
        assert "id" in result and "name" in result
        assert result["id"]["transformations"][-1]["commandType"] == "view"

    def test_multi_node_path(self):
        """Lineage traces through parent → child node path."""
        _store("data", pd.DataFrame({"x": [1], "y": [2]}))
        parent_cmd = _cmd("source", {"mainTable": "data"}, "c1", 0)
        child_cmd = _cmd("transform", {
            "mappings": [{"id": "m1", "expression": "x + y", "outputField": "z", "mode": "simple"}]
        }, "c2", 0)
        parent = _node("parent", [parent_cmd], children=[_node("child", [child_cmd])])
        result = ExecutionEngine().compute_lineage(SESSION, parent, "child")
        assert "z" in result
        assert result["z"]["transformations"][0]["expression"] == "x + y"
        # x and y still present (transform adds z, doesn't remove others)
        assert "x" in result
        assert "y" in result

    def test_unknown_node_raises(self):
        _store("data", pd.DataFrame({"x": [1]}))
        cmd = _cmd("source", {"mainTable": "data"})
        node = _node("n1", [cmd])
        with pytest.raises(ValueError, match="Target node not found"):
            ExecutionEngine().compute_lineage(SESSION, node, "nonexistent")

    def test_disabled_node_skipped(self):
        _store("data", pd.DataFrame({"val": [1]}))
        cmd = _cmd("source", {"mainTable": "data"})
        node = OperationNode(
            id="n1", type="operation", operationType="process",
            name="n1", enabled=False, commands=[cmd], children=[]
        )
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        # Disabled node commands not executed → empty lineage
        assert result == {}


# ── Command-level lineage (target_command_id) ─────────────────────────────────

class TestComputeLineageAtCommand:
    def test_stops_after_target_command(self):
        """Lineage stops at c2; c3 (transform) should not appear."""
        _store("orders", pd.DataFrame({"amount": [100], "region": ["EU"]}))
        cmds = [
            _cmd("source", {"mainTable": "orders"}, "c1", 0),
            _cmd("group", {
                "groupByFields": ["region"],
                "aggregations": [{"field": "amount", "func": "sum", "alias": "total"}],
            }, "c2", 1),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "total * 2", "outputField": "double_total", "mode": "simple"}]
            }, "c3", 2),
        ]
        node = _node("n1", cmds)

        # Stop at c2 (group): should have region + total, NOT double_total
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1", target_command_id="c2")
        assert set(result.keys()) == {"region", "total"}
        assert "double_total" not in result

    def test_full_lineage_without_command_id(self):
        """Without target_command_id, all commands are processed."""
        _store("orders", pd.DataFrame({"amount": [100], "region": ["EU"]}))
        cmds = [
            _cmd("source", {"mainTable": "orders"}, "c1", 0),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "amount * 2", "outputField": "double", "mode": "simple"}]
            }, "c2", 1),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1")
        assert "double" in result

    def test_stops_at_first_command(self):
        """Stopping at source command returns source fields only."""
        _store("data", pd.DataFrame({"x": [1], "y": [2]}))
        cmds = [
            _cmd("source", {"mainTable": "data"}, "c1", 0),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "x + y", "outputField": "z", "mode": "simple"}]
            }, "c2", 1),
        ]
        node = _node("n1", cmds)
        result = ExecutionEngine().compute_lineage(SESSION, node, "n1", target_command_id="c1")
        assert "x" in result and "y" in result
        assert "z" not in result

    def test_command_id_only_limits_in_target_node(self):
        """target_command_id only stops in the target node, not in ancestor nodes."""
        _store("data", pd.DataFrame({"val": [1]}))
        # Parent has c1 (source) and c2 (transform that produces derived)
        parent_cmds = [
            _cmd("source", {"mainTable": "data"}, "c1", 0),
            _cmd("transform", {
                "mappings": [{"id": "m1", "expression": "val * 10", "outputField": "big_val", "mode": "simple"}]
            }, "c2", 1),
        ]
        # Child has c3 (transform) and c4 (another transform)
        child_cmds = [
            _cmd("transform", {
                "mappings": [{"id": "m2", "expression": "big_val + 1", "outputField": "x", "mode": "simple"}]
            }, "c3", 0),
            _cmd("transform", {
                "mappings": [{"id": "m3", "expression": "x * 2", "outputField": "y", "mode": "simple"}]
            }, "c4", 1),
        ]
        parent = _node("parent", parent_cmds, children=[_node("child", child_cmds)])
        # Stop at c3 in child: ancestor c2 should still be fully processed
        result = ExecutionEngine().compute_lineage(SESSION, parent, "child", target_command_id="c3")
        assert "big_val" in result  # ancestor transform ran fully
        assert "x" in result        # c3 ran
        assert "y" not in result    # c4 did not run
