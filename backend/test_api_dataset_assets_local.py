import io
import os

import pytest
from fastapi.testclient import TestClient

import main as main_module
from main import app
from storage import storage, local_file_backend
from collab_storage import collab_storage


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


@pytest.fixture(autouse=True)
def clean_data():
    storage.clear()
    collab_storage.clear()
    yield
    storage.clear()
    collab_storage.clear()


def test_project_upload_persists_dataset_asset_and_supports_runtime_rebuild(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    assert _register(client, "viewer@example.com", display_name="Viewer").status_code == 200

    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    viewer_token = _login(client, "viewer@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Project", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    add_viewer = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "viewer@example.com", "role": "viewer"},
        headers=_auth_header(owner_token),
    )
    assert add_viewer.status_code == 200

    upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("sales.csv", io.BytesIO(b"id,amount\n1,100\n2,250\n"), "text/csv")},
        data={"name": "sales"},
    )
    assert upload.status_code == 200
    upload_payload = upload.json()
    assert upload_payload["id"] == "sales"
    assert upload_payload["datasetVersion"] == 1
    assert upload_payload["status"] == "ready"
    assert upload_payload["storageKey"].startswith(f"project_assets/projects/{project_id}/datasets/")

    owner_datasets = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(owner_token))
    assert owner_datasets.status_code == 200
    assert len(owner_datasets.json()) == 1
    dataset = owner_datasets.json()[0]
    assert dataset["id"] == "sales"
    assert dataset["datasetVersion"] == 1
    assert dataset["storageKey"].startswith(f"project_assets/projects/{project_id}/datasets/")
    assert dataset["fieldTypes"]["amount"]["type"] == "number"

    preview = client.get(
        f"/projects/{project_id}/datasets/sales/preview?limit=5",
        headers=_auth_header(viewer_token),
    )
    assert preview.status_code == 200
    assert preview.json()["totalCount"] == 2
    assert preview.json()["rows"][1]["amount"] == 250

    # Simulate runtime index/db loss; project dataset metadata should rebuild runtime state.
    index_path = storage._get_datasets_index_path(project_id)
    db_path = storage._get_db_path(project_id)
    if os.path.exists(index_path):
        os.remove(index_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    rebuilt_list = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(viewer_token))
    assert rebuilt_list.status_code == 200
    assert rebuilt_list.json()[0]["id"] == "sales"
    assert rebuilt_list.json()[0]["datasetVersion"] == 1

    rebuilt_preview = client.get(
        f"/projects/{project_id}/datasets/sales/preview?limit=5",
        headers=_auth_header(viewer_token),
    )
    assert rebuilt_preview.status_code == 200
    assert rebuilt_preview.json()["rows"][0]["amount"] == 100


def test_project_upload_replace_increments_dataset_version_and_delete_soft_removes_asset(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Versioning", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    first_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n1,10\n"), "text/csv")},
        data={"name": "orders"},
    )
    assert first_upload.status_code == 200
    first_payload = first_upload.json()

    second_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n2,99\n"), "text/csv")},
        data={"name": "orders"},
    )
    assert second_upload.status_code == 200
    second_payload = second_upload.json()
    assert second_payload["datasetVersion"] == 2
    assert second_payload["storageKey"] != first_payload["storageKey"]
    retained_file_path = local_file_backend.resolve_path(second_payload["storageKey"])
    assert os.path.exists(retained_file_path)

    datasets = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(owner_token))
    assert datasets.status_code == 200
    assert len(datasets.json()) == 1
    assert datasets.json()[0]["datasetVersion"] == 2

    preview = client.get(
        f"/projects/{project_id}/datasets/orders/preview?limit=5",
        headers=_auth_header(owner_token),
    )
    assert preview.status_code == 200
    assert preview.json()["rows"] == [{"id": 2, "amount": 99}]

    delete_res = client.delete(
        f"/projects/{project_id}/datasets/orders",
        headers=_auth_header(owner_token),
    )
    assert delete_res.status_code == 200

    list_after_delete = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(owner_token))
    assert list_after_delete.status_code == 200
    assert list_after_delete.json() == []
    assert os.path.exists(retained_file_path)

    preview_after_delete = client.get(
        f"/projects/{project_id}/datasets/orders/preview?limit=5",
        headers=_auth_header(owner_token),
    )
    assert preview_after_delete.status_code == 404

    assert collab_storage.list_active_project_dataset_assets(project_id) == []


def test_project_upload_new_name_strategy_keeps_both_datasets(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Rename Strategy", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    first_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n1,10\n"), "text/csv")},
        data={"name": "orders"},
    )
    assert first_upload.status_code == 200

    second_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n2,20\n"), "text/csv")},
        data={"name": "orders", "duplicateStrategy": "new_name"},
    )
    assert second_upload.status_code == 200
    second_payload = second_upload.json()
    assert second_payload["id"] == "orders_2"
    assert second_payload["datasetVersion"] == 1
    assert second_payload["duplicateStrategy"] == "new_name"

    datasets = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(owner_token))
    assert datasets.status_code == 200
    ids = {item["id"] for item in datasets.json()}
    assert ids == {"orders", "orders_2"}

    first_preview = client.get(
        f"/projects/{project_id}/datasets/orders/preview?limit=5",
        headers=_auth_header(owner_token),
    )
    assert first_preview.status_code == 200
    assert first_preview.json()["rows"] == [{"id": 1, "amount": 10}]

    second_preview = client.get(
        f"/projects/{project_id}/datasets/orders_2/preview?limit=5",
        headers=_auth_header(owner_token),
    )
    assert second_preview.status_code == 200
    assert second_preview.json()["rows"] == [{"id": 2, "amount": 20}]


def test_project_query_rebuilds_runtime_from_dataset_assets(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Query Recovery", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("sales.csv", io.BytesIO(b"id,amount\n1,100\n2,250\n"), "text/csv")},
        data={"name": "sales"},
    )
    assert upload.status_code == 200

    index_path = storage._get_datasets_index_path(project_id)
    db_path = storage._get_db_path(project_id)
    if os.path.exists(index_path):
        os.remove(index_path)
    if os.path.exists(db_path):
        os.remove(db_path)

    query_res = client.post(
        f"/projects/{project_id}/query",
        headers=_auth_header(owner_token),
        json={"query": "SELECT SUM(amount) AS total_amount FROM sales"},
    )
    assert query_res.status_code == 200
    assert query_res.json()["rows"] == [{"total_amount": 350.0}]


def test_project_upload_failure_rolls_back_to_previous_ready_version(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Rollback", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    first_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n1,10\n"), "text/csv")},
        data={"name": "orders"},
    )
    assert first_upload.status_code == 200

    def _broken_finalize(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module.collab_storage, "finalize_project_dataset_asset", _broken_finalize)
    failed_upload = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("orders.csv", io.BytesIO(b"id,amount\n2,99\n"), "text/csv")},
        data={"name": "orders"},
    )
    assert failed_upload.status_code == 500
    assert failed_upload.json()["detail"] == "Project upload failed"

    datasets = client.get(f"/projects/{project_id}/datasets", headers=_auth_header(owner_token))
    assert datasets.status_code == 200
    assert len(datasets.json()) == 1
    assert datasets.json()[0]["id"] == "orders"
    assert datasets.json()[0]["datasetVersion"] == 1

    preview = client.get(
        f"/projects/{project_id}/datasets/orders/preview?limit=5",
        headers=_auth_header(owner_token),
    )
    assert preview.status_code == 200
    assert preview.json()["rows"] == [{"id": 1, "amount": 10}]

    with collab_storage._connect() as conn:
        statuses = [
            row["status"]
            for row in conn.execute(
                "SELECT status FROM dataset_assets WHERE project_id = ? ORDER BY created_at ASC",
                (project_id,),
            ).fetchall()
        ]
    assert statuses == ["ready", "failed"]


def test_project_upload_validation_and_storage_health(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Dataset Validation", "description": "assets"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    invalid_name = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("../evil.csv", io.BytesIO(b"id,amount\n1,10\n"), "text/csv")},
    )
    assert invalid_name.status_code == 400
    assert invalid_name.json()["detail"] == "Invalid filename"

    monkeypatch.setattr(main_module, "MAX_UPLOAD_SIZE_BYTES", 8)
    too_large = client.post(
        f"/projects/{project_id}/upload",
        headers=_auth_header(owner_token),
        files={"file": ("sales.csv", io.BytesIO(b"id,amount\n1,10\n"), "text/csv")},
    )
    assert too_large.status_code == 400
    assert "File is too large" in too_large.json()["detail"]

    health = client.get("/config/storage_health")
    assert health.status_code == 200
    payload = health.json()
    assert payload["exists"] is True
    assert payload["writable"] is True
    assert payload["freeSpaceOk"] is True
    assert payload["tempDir"]["available"] is True
    assert payload["healthy"] is True
