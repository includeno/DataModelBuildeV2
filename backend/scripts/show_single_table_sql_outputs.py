import json
import logging
import sys
from pathlib import Path
from copy import deepcopy
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app
from storage import storage


logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("uvicorn").setLevel(logging.WARNING)

client = TestClient(app)


def add_setup_node(tree: dict, tables: list[str]):
    setup_cmds = []
    for idx, table in enumerate(tables):
        setup_cmds.append({
            "id": f"setup_src_{idx}",
            "type": "source",
            "config": {
                "mainTable": table,
                "alias": table,
                "linkId": f"link_{table}",
            },
        })
    setup_node = {
        "id": "setup",
        "type": "operation",
        "operationType": "setup",
        "name": "Data Setup",
        "enabled": True,
        "commands": setup_cmds,
        "children": [],
    }
    children = tree.get("children") or []
    tree["children"] = children + [setup_node]
    return tree


def setup_session_with_data() -> str:
    storage.clear()
    session_res = client.post("/sessions")
    session_res.raise_for_status()
    session_id = session_res.json()["sessionId"]

    csv_content = "id,name,age,role\n1,Alice,30,admin\n2,Bob,25,user\n3,Charlie,35,user"
    upload_res = client.post(
        "/upload",
        files={"file": ("users.csv", csv_content, "text/csv")},
        data={"sessionId": session_id, "name": "users"},
    )
    upload_res.raise_for_status()
    return session_id


def build_tree(target_cmd: dict):
    base_tree = {
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
    return add_setup_node(base_tree, ["users"])


def generate_sql(session_id: str, tree: dict, target_command_id: str = "c3"):
    res = client.post(
        "/generate_sql",
        json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "root",
            "targetCommandId": target_command_id,
            "includeCommandMeta": True,
        },
    )
    res.raise_for_status()
    payload = res.json()
    return payload.get("sql", ""), payload.get("dmb")


def main():
    commands = [
        {
            "name": "filter",
            "cmd": {
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
        },
        {
            "name": "sort",
            "cmd": {
                "id": "c3",
                "type": "sort",
                "config": {"dataSource": "users", "field": "age", "ascending": False},
            },
        },
        {
            "name": "group",
            "cmd": {
                "id": "c3",
                "type": "group",
                "config": {
                    "dataSource": "users",
                    "groupByFields": ["name"],
                    "aggregations": [{"func": "count", "field": "*", "alias": "fresh_cnt"}],
                },
            },
        },
        {
            "name": "transform",
            "cmd": {
                "id": "c3",
                "type": "transform",
                "config": {
                    "dataSource": "users",
                    "mappings": [{"expression": "age + 1", "outputField": "age_plus", "mode": "sql"}],
                },
            },
        },
        {
            "name": "save",
            "cmd": {
                "id": "c3",
                "type": "save",
                "config": {"dataSource": "users", "field": "role", "distinct": True},
            },
        },
        {
            "name": "view",
            "cmd": {
                "id": "c3",
                "type": "view",
                "config": {
                    "dataSource": "users",
                    "viewFields": [{"field": "name"}],
                    "viewSorts": [{"field": "name", "ascending": True}],
                    "viewLimit": 2,
                },
            },
        },
        {
            "name": "filter_noop_stream",
            "cmd": {
                "id": "c3",
                "type": "filter",
                "config": {
                    "dataSource": "stream",
                    "filterRoot": {"id": "root", "type": "group", "logicalOperator": "AND", "conditions": []},
                },
            },
        },
    ]

    print("=== SQL Export Outputs For Single-Table Commands ===")
    print("(Each case has a prior polluted group command to verify reset behavior.)\n")

    for idx, item in enumerate(commands, start=1):
        session_id = setup_session_with_data()
        tree = build_tree(deepcopy(item["cmd"]))
        sql, dmb = generate_sql(session_id, tree)

        print(f"[{idx}] command={item['name']}")
        print("- config:")
        print(json.dumps(item["cmd"]["config"], ensure_ascii=False, indent=2))
        print("- output SQL (without DMB metadata line):")
        print(sql)
        print("- dmb:")
        print(json.dumps(dmb, ensure_ascii=False, indent=2))
        print("- leaked marker present?", "polluted_total_age" in sql)
        print("-" * 80)

    storage.clear()


if __name__ == "__main__":
    main()
