import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage

client = TestClient(app)

CASES_PATH = Path(__file__).parent / "test_fixtures" / "sql_export_ui_generated_cases.json"
ORDERS_CSV_PATH = Path(__file__).resolve().parents[1] / "test_data" / "orders.csv"


def _load_ui_cases():
    if not CASES_PATH.exists():
        raise RuntimeError(f"Missing UI-generated fixture file: {CASES_PATH}")
    payload = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    cases = payload.get("cases", [])
    if len(cases) < 50:
        raise RuntimeError(f"Expected at least 50 UI-generated cases, got {len(cases)}")
    unique_keys = {
        json.dumps(
            {"type": c.get("command", {}).get("type"), "config": c.get("command", {}).get("config", {})},
            sort_keys=True,
            ensure_ascii=False,
        )
        for c in cases
    }
    if len(unique_keys) < 50:
        raise RuntimeError(f"Expected at least 50 unique UI-generated command JSON payloads, got {len(unique_keys)}")
    return cases


UI_SQL_EXPORT_CASES = _load_ui_cases()


@pytest.fixture(autouse=True)
def clean_env():
    storage.clear()
    yield
    storage.clear()


@pytest.fixture()
def session_id():
    if not ORDERS_CSV_PATH.exists():
        raise RuntimeError(f"Missing required test data: {ORDERS_CSV_PATH}")
    res = client.post("/sessions")
    assert res.status_code == 200
    sid = res.json()["sessionId"]
    storage.add_dataset(sid, "orders", pd.read_csv(ORDERS_CSV_PATH))
    return sid


def _build_ui_like_tree(cumulative_commands: list[dict], source_ref: str) -> dict:
    return {
        "id": "root",
        "type": "operation",
        "operationType": "root",
        "name": "Project",
        "enabled": True,
        "commands": [],
        "children": [
            {
                "id": "setup_ui",
                "type": "operation",
                "operationType": "setup",
                "name": "Data Setup",
                "enabled": True,
                "commands": [
                    {
                        "id": "setup_src_orders",
                        "type": "source",
                        "order": 1,
                        "config": {
                            "mainTable": "orders",
                            "alias": "orders",
                            "linkId": source_ref,
                        },
                    }
                ],
                "children": [
                    {
                        "id": "op_ui",
                        "type": "operation",
                        "operationType": "process",
                        "name": "UI SQL Export Cases",
                        "enabled": True,
                        "commands": cumulative_commands,
                        "children": [],
                    }
                ],
            }
        ],
    }


@pytest.mark.parametrize(
    "case_index,case",
    [(idx + 1, c) for idx, c in enumerate(UI_SQL_EXPORT_CASES)],
    ids=[c.get("caseId", f"case_{idx + 1}") for idx, c in enumerate(UI_SQL_EXPORT_CASES)],
)
def test_generate_sql_button_real_ui_cases(session_id, case_index, case):
    cumulative_commands = [deepcopy(c["command"]) for c in UI_SQL_EXPORT_CASES[:case_index]]
    source_ref = str((cumulative_commands[0].get("config") or {}).get("dataSource") or "orders")
    tree = _build_ui_like_tree(cumulative_commands, source_ref)

    res = client.post(
        "/generate_sql",
        json={
            "sessionId": session_id,
            "tree": tree,
            "targetNodeId": "op_ui",
            "targetCommandId": case["command"]["id"],
            # Send true intentionally: backend should still return pure SQL.
            "includeCommandMeta": True,
        },
    )

    assert res.status_code == 200
    sql_text = res.json()["sql"]
    assert "-- DMB_COMMAND:" not in sql_text
    assert sql_text == case.get("uiGeneratedSql", "")

    for token in case.get("expectedTokens", []):
        assert token in sql_text

