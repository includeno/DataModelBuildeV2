import json
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main as main_module
import runtime_config as runtime_config_module
from collab_storage import collab_storage
from generate_openapi_schema import build_openapi_schema, write_openapi_schema
from storage import storage


@pytest.fixture
def client():
    with TestClient(main_module.app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def clean_runtime():
    storage.clear()
    collab_storage.clear()
    main_module.idempotency_store._records.clear()
    main_module.rate_limiter._events.clear()
    yield
    storage.clear()
    collab_storage.clear()
    main_module.idempotency_store._records.clear()
    main_module.rate_limiter._events.clear()


def _auth_header(token: str):
    return {"Authorization": f"Bearer {token}"}


def _register(client: TestClient, email: str, password: str = "Passw0rd!", display_name: str = ""):
    return client.post(
        "/auth/register",
        json={"email": email, "password": password, "displayName": display_name},
    )


def _login(client: TestClient, email: str, password: str = "Passw0rd!"):
    return client.post("/auth/login", json={"email": email, "password": password})


def _simple_state(name: str):
    return {
        "tree": {
            "id": "root",
            "type": "operation",
            "name": name,
            "enabled": True,
            "commands": [],
            "children": [],
        }
    }


def test_v2_envelope_and_request_id(monkeypatch, client: TestClient):
    monkeypatch.setenv("BACKEND_AUTH_ENABLED", "0")

    res = client.post(
        "/v2/projects",
        json={"name": "Contract Project", "description": "v2"},
        headers={"X-Request-ID": "req_contract_001"},
    )
    assert res.status_code == 200
    assert res.headers["X-Request-ID"] == "req_contract_001"

    payload = res.json()
    assert payload["request_id"] == "req_contract_001"
    assert payload["error"] is None
    assert payload["meta"]["api_version"] == "v2"
    assert payload["data"]["name"] == "Contract Project"

    missing = client.get("/v2/projects/prj_missing", headers={"X-Request-ID": "req_contract_404"})
    assert missing.status_code == 404
    assert missing.headers["X-Request-ID"] == "req_contract_404"
    err_payload = missing.json()
    assert err_payload["data"] is None
    assert err_payload["error"]["code"] == "PROJECT_NOT_FOUND"
    assert err_payload["request_id"] == "req_contract_404"


def test_legacy_sessions_emit_deprecation_headers(monkeypatch, client: TestClient):
    monkeypatch.setenv("BACKEND_AUTH_ENABLED", "0")

    session_id = client.post("/sessions").json()["sessionId"]
    res = client.get(f"/sessions/{session_id}/state")

    assert res.status_code == 200
    assert res.headers["Deprecation"] == "true"
    assert res.headers["X-API-Deprecated"] == "true"
    assert "Sunset" in res.headers
    assert "docs/api-contract-strategy.md" in res.headers["Link"]
    assert res.headers["X-Request-ID"]


def test_legacy_session_compat_can_be_disabled(monkeypatch, client: TestClient):
    monkeypatch.setattr(
        runtime_config_module,
        "DEFAULT_RUNTIME_CONFIG",
        replace(runtime_config_module.DEFAULT_RUNTIME_CONFIG, legacy_session_compat_enabled=False),
    )

    res = client.get("/sessions")

    assert res.status_code == 410
    assert res.json()["detail"]["code"] == "LEGACY_SESSION_DISABLED"


def test_legacy_session_bridge_reads_project_state(client: TestClient):
    _register(client, "owner@example.com", display_name="Owner")
    token = _login(client, "owner@example.com").json()["accessToken"]

    project = client.post(
        "/projects",
        json={"name": "Bridge Project", "description": "legacy bridge"},
        headers=_auth_header(token),
    ).json()

    commit = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 0, "state": _simple_state("Bridge Root")},
        headers=_auth_header(token),
    )
    assert commit.status_code == 200

    bridged = client.get(f"/sessions/{project['id']}/state", headers=_auth_header(token))
    assert bridged.status_code == 200
    assert bridged.json()["tree"]["name"] == "Bridge Root"
    assert bridged.headers["Deprecation"] == "true"


def test_commit_idempotency_key_replays_and_rejects_mismatch(monkeypatch, client: TestClient):
    monkeypatch.setenv("BACKEND_AUTH_ENABLED", "0")
    project = client.post("/projects", json={"name": "Idempotent Project", "description": ""}).json()

    first = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 0, "state": _simple_state("Commit Once")},
        headers={"Idempotency-Key": "idem-001"},
    )
    assert first.status_code == 200
    first_payload = first.json()
    assert first_payload["version"] == 1

    replay = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 0, "state": _simple_state("Commit Once")},
        headers={"Idempotency-Key": "idem-001"},
    )
    assert replay.status_code == 200
    assert replay.json()["version"] == 1

    mismatch = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 0, "state": _simple_state("Commit Changed")},
        headers={"Idempotency-Key": "idem-001"},
    )
    assert mismatch.status_code == 409
    assert mismatch.json()["detail"]["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_commit_rate_limit_returns_429(monkeypatch, client: TestClient):
    monkeypatch.setenv("BACKEND_AUTH_ENABLED", "0")
    monkeypatch.setattr(
        runtime_config_module,
        "DEFAULT_RUNTIME_CONFIG",
        replace(
            runtime_config_module.DEFAULT_RUNTIME_CONFIG,
            project_commit_rate_limit_count=1,
            project_commit_rate_limit_window_seconds=60,
        ),
    )

    project = client.post("/projects", json={"name": "Rate Limit Project", "description": ""}).json()

    first = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 0, "state": _simple_state("First")},
    )
    assert first.status_code == 200

    limited = client.post(
        f"/projects/{project['id']}/state/commit",
        json={"baseVersion": 1, "state": _simple_state("Second")},
    )
    assert limited.status_code == 429
    assert limited.json()["detail"]["code"] == "RATE_LIMIT_EXCEEDED"


def test_openapi_generation_includes_v2_contract(tmp_path: Path):
    schema = build_openapi_schema()
    assert "/v2/projects" in schema["paths"]
    assert "/v2/projects/{project_id}/state/commit" in schema["paths"]

    output_path = tmp_path / "openapi.json"
    written = write_openapi_schema(str(output_path))
    assert written == output_path
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["info"]["title"] == "DataFlow Engine API"
    assert "/v2/meta/error-codes" in payload["paths"]
