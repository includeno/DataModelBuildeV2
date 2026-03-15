import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage

client = TestClient(app)

FIXTURE_PATH = Path(__file__).parent / "test_fixtures" / "generate_sql_real_requests.json"
ORDERS_CSV_PATH = Path(__file__).resolve().parents[1] / "test_data" / "orders.csv"


def _load_real_request_cases():
    if not FIXTURE_PATH.exists():
        raise RuntimeError(f"Missing real request fixture: {FIXTURE_PATH}")

    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    requests = payload.get("requests", [])
    if len(requests) < 50:
        raise RuntimeError(f"Expected at least 50 real /generate_sql requests, got {len(requests)}")

    unique_request_json = {
        json.dumps(item.get("request", {}), sort_keys=True, ensure_ascii=False)
        for item in requests
    }
    if len(unique_request_json) < 50:
        raise RuntimeError(f"Expected at least 50 unique request JSON payloads, got {len(unique_request_json)}")

    return payload, requests


REAL_REQUEST_FIXTURE, REAL_REQUEST_CASES = _load_real_request_cases()


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


def test_generate_sql_real_request_fixture_has_50_plus_cases():
    assert REAL_REQUEST_FIXTURE.get("actualCaseCount", 0) >= 50
    assert len(REAL_REQUEST_CASES) >= 50


@pytest.mark.parametrize(
    "case",
    REAL_REQUEST_CASES,
    ids=[c.get("requestId", f"real_request_{idx + 1}") for idx, c in enumerate(REAL_REQUEST_CASES)],
)
def test_generate_sql_real_request_json_cases(session_id, case):
    request_payload = deepcopy(case["request"])
    request_payload["sessionId"] = session_id

    res = client.post("/generate_sql", json=request_payload)
    assert res.status_code == case.get("responseStatus", 200)

    body = res.json()
    assert "sql" in body

    sql_text = body["sql"]
    assert isinstance(sql_text, str)
    assert sql_text == case.get("responseSql", "")

    # Project policy: only pure SQL or explicit unsupported hint.
    assert "-- DMB_COMMAND:" not in sql_text
    if sql_text.startswith("--"):
        assert sql_text.startswith("-- SQL generation not supported")
