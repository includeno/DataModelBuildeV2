import json
import logging
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

import main as main_module
import runtime_config as runtime_config_module
from collab_storage import collab_storage
from security import compile_python_transform
from storage import storage

client = TestClient(main_module.app)


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    storage.clear()
    collab_storage.clear()
    monkeypatch.setattr(main_module, "rate_limiter", main_module.RateLimiter())
    monkeypatch.setattr(main_module, "idempotency_store", main_module.IdempotencyStore(ttl_seconds=3600))
    yield
    storage.clear()
    collab_storage.clear()


def _register(email: str, password: str = "Passw0rd!", display_name: str = "User"):
    return client.post(
        "/auth/register",
        json={"email": email, "password": password, "displayName": display_name},
    )


def _login(email: str, password: str = "Passw0rd!"):
    return client.post("/auth/login", json={"email": email, "password": password})


def _auth_header(token: str):
    return {"Authorization": f"Bearer {token}"}


def _create_project(token: str, name: str = "Secure Project") -> str:
    res = client.post("/projects", headers=_auth_header(token), json={"name": name})
    assert res.status_code == 200
    return res.json()["id"]


def _upload_csv(project_id: str, token: str, filename: str = "people.csv", content: bytes = b"id,name\n1,Alice\n2,Bob\n"):
    return client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(token),
        files={"file": (filename, content, "text/csv")},
        data={"name": "people"},
    )


def test_runtime_config_sanitizes_server_and_cors(monkeypatch):
    monkeypatch.setattr(
        runtime_config_module,
        "DEFAULT_RUNTIME_CONFIG",
        replace(
            runtime_config_module.DEFAULT_RUNTIME_CONFIG,
            default_server_override="javascript:alert(1)",
            cors_origins=("http://127.0.0.1:1420", "javascript://bad", "http://127.0.0.1:1420"),
        ),
    )

    config = runtime_config_module.load_runtime_config()
    assert config.default_server == "mockServer"
    assert config.cors_origins == ("http://127.0.0.1:1420",)


def test_login_failure_limit_blocks_bruteforce_attempts(monkeypatch):
    monkeypatch.setattr(
        runtime_config_module,
        "DEFAULT_RUNTIME_CONFIG",
        replace(
            runtime_config_module.DEFAULT_RUNTIME_CONFIG,
            auth_login_attempt_limit=50,
            auth_login_failure_limit=2,
            auth_login_window_seconds=60,
        ),
    )
    assert _register("owner@example.com").status_code == 200

    first = _login("owner@example.com", password="wrong-pass")
    second = _login("owner@example.com", password="wrong-pass")
    third = _login("owner@example.com", password="wrong-pass")

    assert first.status_code == 401
    assert second.status_code == 401
    assert third.status_code == 429
    assert third.json()["detail"]["code"] == "RATE_LIMIT_EXCEEDED"


def test_project_query_rejects_mutating_or_multi_statement_sql():
    assert _register("owner@example.com").status_code == 200
    login = _login("owner@example.com")
    token = login.json()["accessToken"]
    project_id = _create_project(token)
    assert _upload_csv(project_id, token).status_code == 200

    delete_res = client.post(
        f"/projects/{project_id}/query",
        headers=_auth_header(token),
        json={"projectId": project_id, "query": "DELETE FROM people"},
    )
    assert delete_res.status_code == 400
    assert delete_res.json()["detail"]["code"] == "QUERY_INVALID"

    multi_res = client.post(
        f"/projects/{project_id}/query",
        headers=_auth_header(token),
        json={"projectId": project_id, "query": "SELECT * FROM people; DELETE FROM people"},
    )
    assert multi_res.status_code == 400
    assert multi_res.json()["detail"]["code"] == "QUERY_INVALID"


def test_upload_rejects_dangerous_extensions_content_type_and_path_traversal():
    assert _register("owner@example.com").status_code == 200
    login = _login("owner@example.com")
    token = login.json()["accessToken"]
    project_id = _create_project(token)

    dangerous = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(token),
        files={"file": ("customers.exe.csv", b"id,name\n1,Alice\n", "text/csv")},
        data={"name": "customers"},
    )
    assert dangerous.status_code == 400
    assert "Dangerous file extension" in dangerous.json()["detail"]

    mismatch = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(token),
        files={"file": ("customers.csv", b"id,name\n1,Alice\n", "application/javascript")},
        data={"name": "customers"},
    )
    assert mismatch.status_code == 400
    assert "content type" in mismatch.json()["detail"]

    traversal = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(token),
        files={"file": ("../customers.csv", b"id,name\n1,Alice\n", "text/csv")},
        data={"name": "customers"},
    )
    assert traversal.status_code == 400
    assert traversal.json()["detail"] == "Invalid filename"


def test_python_transform_runs_safe_code_and_blocks_dangerous_calls():
    transform = compile_python_transform(
        "def transform(row):\n"
        "    return row.get('amount', 0) * 2\n"
    )
    assert transform({"amount": 21}) == 42

    with pytest.raises(ValueError):
        compile_python_transform(
            "def transform(row):\n"
            "    return open('/tmp/secrets').read()\n"
        )

    with pytest.raises(ValueError):
        compile_python_transform(
            "import os\n"
            "def transform(row):\n"
            "    return os.listdir('.')\n"
        )


def test_audit_logs_include_request_user_and_project(caplog):
    caplog.set_level(logging.INFO, logger="backend")

    assert _register("owner@example.com").status_code == 200
    login = _login("owner@example.com")
    token = login.json()["accessToken"]
    project_id = _create_project(token, name="Audit Project")

    commit = client.post(
        f"/projects/{project_id}/state/commit",
        headers={**_auth_header(token), "X-Request-ID": "req_audit_001"},
        json={"baseVersion": 0, "state": {"tree": {"id": "root"}}},
    )
    assert commit.status_code == 200

    upload = _upload_csv(project_id, token)
    assert upload.status_code == 200

    audit_payloads = []
    for record in caplog.records:
        if record.name != "backend" or "AUDIT " not in record.getMessage():
            continue
        audit_payloads.append(json.loads(record.getMessage().split("AUDIT ", 1)[1]))

    assert any(item["action"] == "project_create" and item["project_id"] == project_id for item in audit_payloads)
    assert any(
        item["action"] == "project_commit"
        and item["project_id"] == project_id
        and item["request_id"] == "req_audit_001"
        and item["user_id"].startswith("usr_")
        for item in audit_payloads
    )
    assert any(item["action"] == "dataset_upload" and item["project_id"] == project_id for item in audit_payloads)


def test_strict_request_validation_rejects_extra_fields():
    bad_register = client.post(
        "/auth/register",
        json={"email": "owner@example.com", "password": "Passw0rd!", "displayName": "Owner", "extra": True},
    )
    assert bad_register.status_code == 422

    assert _register("owner@example.com").status_code == 200
    login = _login("owner@example.com")
    token = login.json()["accessToken"]
    project_id = _create_project(token)

    bad_commit = client.post(
        f"/projects/{project_id}/state/commit",
        headers=_auth_header(token),
        json={"baseVersion": 0, "state": {}, "extra": True},
    )
    assert bad_commit.status_code == 422

    bad_query = client.post(
        f"/projects/{project_id}/query",
        headers=_auth_header(token),
        json={"projectId": project_id, "query": "SELECT 1", "extra": True},
    )
    assert bad_query.status_code == 400
    assert bad_query.json()["detail"]["code"] == "QUERY_INVALID"
