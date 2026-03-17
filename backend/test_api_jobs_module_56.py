import io
import time

import pytest
from fastapi.testclient import TestClient

import main as main_module
from main import app
from storage import storage
from collab_storage import collab_storage


@pytest.fixture(autouse=True)
def clean_data():
    main_module.job_runner.stop()
    storage.clear()
    collab_storage.clear()
    yield
    main_module.job_runner.stop()
    storage.clear()
    collab_storage.clear()


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


def _auth_header(token: str):
    return {"Authorization": f"Bearer {token}"}


def _register(client: TestClient, email: str, password: str = "Passw0rd!", display_name: str = ""):
    return client.post(
        "/auth/register",
        json={"email": email, "password": password, "displayName": display_name},
    )


def _login(client: TestClient, email: str, password: str = "Passw0rd!"):
    return client.post("/auth/login", json={"email": email, "password": password})


def _basic_tree(dataset_name: str = "sales"):
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "src",
                "type": "source",
                "order": 0,
                "config": {"mainTable": dataset_name},
            }
        ],
        "children": [],
    }


def _create_project_with_dataset(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Execution Project", "description": "module-f"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("sales.csv", io.BytesIO(b"id,amount\n1,100\n2,250\n3,300\n"), "text/csv")},
        data={"name": "sales"},
    )
    assert upload.status_code == 200

    tree = _basic_tree("sales")
    commit = client.post(
        f"/projects/{project_id}/state/commit",
        headers=_auth_header(owner_token),
        json={"baseVersion": 0, "state": {"tree": tree}},
    )
    assert commit.status_code == 200
    return project_id, owner_token, tree


def _poll_job(client: TestClient, job_id: str, token: str, *, timeout_s: float = 3.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        res = client.get(f"/jobs/{job_id}", headers=_auth_header(token))
        assert res.status_code == 200
        payload = res.json()
        if payload["status"] in {"completed", "failed", "canceled"}:
            return payload
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish in time")


def test_project_execute_uses_saved_state_and_project_id_context(client: TestClient):
    project_id, owner_token, _tree = _create_project_with_dataset(client)

    executed = client.post(
        f"/projects/{project_id}/execute",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root", "pageSize": 50},
    )
    assert executed.status_code == 200
    payload = executed.json()
    assert payload["totalCount"] == 3
    assert payload["columns"] == ["id", "amount"]
    assert payload["rows"][0]["amount"] == 100


def test_project_execute_enforces_page_and_query_limits(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    project_id, owner_token, _tree = _create_project_with_dataset(client)

    monkeypatch.setattr(main_module, "MAX_SYNC_PAGE_SIZE", 2)
    too_large_page = client.post(
        f"/projects/{project_id}/execute",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root", "pageSize": 3},
    )
    assert too_large_page.status_code == 400
    assert too_large_page.json()["detail"]["code"] == "EXEC_PAGE_SIZE_LIMIT"

    monkeypatch.setattr(main_module, "MAX_SYNC_QUERY_ROWS", 1)
    query_limit = client.post(
        f"/projects/{project_id}/query",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "query": "SELECT * FROM sales", "pageSize": 1},
    )
    assert query_limit.status_code == 413
    assert query_limit.json()["detail"]["code"] == "EXEC_RESULT_TOO_LARGE"


def test_project_execute_times_out_with_standardized_error(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    project_id, owner_token, _tree = _create_project_with_dataset(client)

    def _slow_execute(*_args, **_kwargs):
        time.sleep(0.2)
        return main_module.pd.DataFrame([{"id": 1}])

    monkeypatch.setattr(main_module, "MAX_SYNC_EXECUTION_SECONDS", 0.1)
    monkeypatch.setattr(main_module.engine, "execute", _slow_execute)

    timed_out = client.post(
        f"/projects/{project_id}/execute",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root", "pageSize": 50},
    )
    assert timed_out.status_code == 504
    assert timed_out.json()["detail"]["code"] == "EXEC_TIMEOUT"
    assert timed_out.json()["detail"]["category"] == "timeout"


def test_project_execute_job_completes_and_returns_result(client: TestClient):
    project_id, owner_token, _tree = _create_project_with_dataset(client)
    main_module.job_runner.poll_interval_s = 0.01

    created = client.post(
        f"/projects/{project_id}/jobs/execute",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root", "pageSize": 2},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]

    completed = _poll_job(client, job_id, owner_token)
    assert completed["status"] == "completed"
    assert completed["type"] == "execute"

    result = client.get(f"/jobs/{job_id}/result", headers=_auth_header(owner_token))
    assert result.status_code == 200
    payload = result.json()
    assert payload["totalCount"] == 3
    assert payload["pageSize"] == 2
    assert payload["rows"][0]["id"] == 1


def test_project_export_job_returns_downloadable_csv(client: TestClient):
    project_id, owner_token, _tree = _create_project_with_dataset(client)
    main_module.job_runner.poll_interval_s = 0.01

    created = client.post(
        f"/projects/{project_id}/export",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root"},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]

    completed = _poll_job(client, job_id, owner_token)
    assert completed["status"] == "completed"
    assert completed["downloadUrl"] == f"/jobs/{job_id}/result"

    result = client.get(f"/jobs/{job_id}/result", headers=_auth_header(owner_token))
    assert result.status_code == 200
    assert result.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=" in result.headers["content-disposition"]
    assert "id,amount" in result.text
    assert "3,300" in result.text


def test_project_job_cancel_marks_queued_job_canceled(client: TestClient):
    project_id, owner_token, _tree = _create_project_with_dataset(client)
    main_module.job_runner.stop()

    created = client.post(
        f"/projects/{project_id}/jobs/execute",
        headers=_auth_header(owner_token),
        json={"projectId": project_id, "targetNodeId": "root", "pageSize": 2},
    )
    assert created.status_code == 202
    job_id = created.json()["id"]

    canceled = client.post(f"/jobs/{job_id}:cancel", headers=_auth_header(owner_token))
    assert canceled.status_code == 200
    assert canceled.json()["status"] == "canceled"
    assert canceled.json()["cancelRequested"] is True

    result = client.get(f"/jobs/{job_id}/result", headers=_auth_header(owner_token))
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "JOB_NOT_READY"
