import pandas as pd
from fastapi.testclient import TestClient

from main import app
from storage import storage

client = TestClient(app)


def _build_tree_with_setup_notes():
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {"id": "src", "type": "source", "order": 0, "config": {"mainTable": "users"}}
        ],
        "children": [
            {
                "id": "setup_1",
                "type": "operation",
                "operationType": "setup",
                "name": "Data Setup",
                "enabled": True,
                "commands": [
                    {
                        "id": "setup_src_1",
                        "type": "source",
                        "order": 0,
                        "config": {
                            "mainTable": "users",
                            "alias": "users",
                            "linkId": "link_users",
                            "note": "Primary user table"
                        }
                    },
                    {
                        "id": "setup_var_1",
                        "type": "define_variable",
                        "order": 1,
                        "config": {
                            "variableName": "region",
                            "variableType": "text",
                            "variableValue": "APAC",
                            "note": "Default region variable"
                        }
                    }
                ],
                "children": []
            }
        ]
    }


def test_execute_accepts_setup_notes():
    storage.clear()
    try:
        session_id = client.post("/sessions").json()["sessionId"]
        storage.add_dataset(session_id, "users", pd.DataFrame([{"id": 1, "name": "Alice"}]))
        tree = _build_tree_with_setup_notes()

        res = client.post(
            "/execute",
            json={"sessionId": session_id, "tree": tree, "targetNodeId": "root"},
        )
        assert res.status_code == 200
        assert res.json()["totalCount"] == 1
    finally:
        storage.clear()


def test_diagnostics_returns_source_note():
    storage.clear()
    try:
        session_id = client.post("/sessions").json()["sessionId"]
        storage.add_dataset(session_id, "users", pd.DataFrame([{"id": 1, "name": "Alice"}]))
        tree = _build_tree_with_setup_notes()
        client.post(f"/sessions/{session_id}/state", json={"tree": tree})

        res = client.get(f"/sessions/{session_id}/diagnostics")
        assert res.status_code == 200
        sources = res.json()["sources"]
        src = next(s for s in sources if s["id"] == "setup_src_1")
        assert src["note"] == "Primary user table"
    finally:
        storage.clear()
