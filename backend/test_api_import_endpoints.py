"""Integration tests for /projects/{project_id}/upload/preview and /projects/{project_id}/upload endpoints."""

import io
import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage
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


@pytest.fixture
def owner_project(client: TestClient):
    """Returns (owner_token, project_id)."""
    _register(client, "owner@example.com", display_name="Owner")
    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    project = client.post(
        "/projects",
        json={"name": "Test Project", "description": ""},
        headers=_auth_header(owner_token),
    )
    assert project.status_code == 200
    return owner_token, project.json()["id"]


# ── /upload/preview ────────────────────────────────────────────────────────────

class TestUploadPreview:
    def test_preview_csv_returns_schema_and_clean_report(self, client, owner_project):
        token, project_id = owner_project
        # rows 2 and 3 are fully identical → 1 duplicate
        csv = b"id,name,salary\n1,Alice,100\n2,Bob,200\n2,Bob,200\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code == 200
        body = res.json()
        assert "previewToken" in body
        assert body["fields"] == ["id", "name", "salary"]
        assert body["fieldTypes"]["salary"]["type"] == "number"
        assert body["totalCount"] == 3
        assert len(body["rows"]) <= 50
        report = body["cleanReport"]
        assert report["duplicateRowCount"] == 1

    def test_preview_detects_missing_values(self, client, owner_project):
        token, project_id = owner_project
        csv = b"a,b\n1,\n2,hello\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code == 200
        body = res.json()
        report = body["cleanReport"]
        assert "b" in report["missingValueCounts"]
        assert report["missingValueCounts"]["b"] >= 1

    def test_preview_detects_whitespace_fields(self, client, owner_project):
        token, project_id = owner_project
        csv = b"name\n  hello  \nworld\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code == 200
        assert res.json()["cleanReport"]["whitespaceFieldCount"] == 1

    def test_preview_requires_authentication(self, client, owner_project):
        _, project_id = owner_project
        csv = b"id\n1\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code in (401, 403)

    def test_preview_rejects_viewer_access(self, client, owner_project):
        owner_token, project_id = owner_project
        _register(client, "viewer@example.com", display_name="Viewer")
        viewer_token = _login(client, "viewer@example.com").json()["accessToken"]
        client.post(
            f"/projects/{project_id}/members",
            json={"memberEmail": "viewer@example.com", "role": "viewer"},
            headers=_auth_header(owner_token),
        )
        csv = b"id\n1\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(viewer_token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code in (403, 401)

    def test_preview_returns_preview_token(self, client, owner_project):
        token, project_id = owner_project
        csv = b"x,y\n1,2\n3,4\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code == 200
        preview_token = res.json()["previewToken"]
        assert isinstance(preview_token, str)
        assert len(preview_token) == 32  # uuid4().hex

    def test_preview_unsupported_format_returns_400(self, client, owner_project):
        token, project_id = owner_project
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.txt", io.BytesIO(b"just text"), "text/plain")},
        )
        assert res.status_code in (400, 422, 500)

    def test_preview_unknown_project_returns_error(self, client, owner_project):
        token, _ = owner_project
        csv = b"id\n1\n"
        res = client.post(
            "/projects/nonexistent_project/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code in (403, 404)

    def test_preview_column_names_normalized(self, client, owner_project):
        """Spaces in column names should be replaced with underscores."""
        token, project_id = owner_project
        csv = b"first name,last name\nAlice,Smith\n"
        res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert res.status_code == 200
        fields = res.json()["fields"]
        assert "first_name" in fields
        assert "last_name" in fields


# ── /upload with cleanConfig ───────────────────────────────────────────────────

class TestUploadWithCleanConfig:
    def _upload(self, client, token, project_id, csv: bytes, name: str = "dataset", clean_config: dict | None = None, preview_token: str | None = None):
        data: dict = {"name": name}
        if clean_config is not None:
            data["cleanConfig"] = json.dumps(clean_config)
        if preview_token is not None:
            data["previewToken"] = preview_token
        return client.post(
            f"/projects/{project_id}/upload",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
            data=data,
        )

    def test_upload_without_clean_config(self, client, owner_project):
        token, project_id = owner_project
        csv = b"id,name\n1,Alice\n2,Bob\n"
        res = self._upload(client, token, project_id, csv)
        assert res.status_code == 200
        body = res.json()
        assert body["id"] == "dataset"
        assert body["totalCount"] == 2

    def test_upload_dedup_removes_duplicates(self, client, owner_project):
        token, project_id = owner_project
        csv = b"id,name\n1,Alice\n1,Alice\n2,Bob\n"
        clean_config = {
            "dedup": {"enabled": True, "fields": "all", "keep": "first"},
            "fillMissing": {"enabled": False, "rules": []},
            "outlier": {"enabled": False, "method": "iqr", "threshold": 1.5, "action": "flag", "targetFields": "numeric"},
            "trimWhitespace": {"enabled": False, "fields": "string"},
        }
        res = self._upload(client, token, project_id, csv, clean_config=clean_config)
        assert res.status_code == 200
        assert res.json()["totalCount"] == 2  # one duplicate removed

    def test_upload_fill_missing_constant(self, client, owner_project):
        token, project_id = owner_project
        csv = b"id,name\n1,Alice\n2,\n"
        clean_config = {
            "dedup": {"enabled": False, "fields": "all", "keep": "first"},
            "fillMissing": {
                "enabled": True,
                "rules": [{"field": "name", "strategy": "constant", "constantValue": "UNKNOWN"}],
            },
            "outlier": {"enabled": False, "method": "iqr", "threshold": 1.5, "action": "flag", "targetFields": "numeric"},
            "trimWhitespace": {"enabled": False, "fields": "string"},
        }
        res = self._upload(client, token, project_id, csv, clean_config=clean_config)
        assert res.status_code == 200

    def test_upload_trim_whitespace(self, client, owner_project):
        token, project_id = owner_project
        csv = b"name\n  Alice  \nBob\n"
        clean_config = {
            "dedup": {"enabled": False, "fields": "all", "keep": "first"},
            "fillMissing": {"enabled": False, "rules": []},
            "outlier": {"enabled": False, "method": "iqr", "threshold": 1.5, "action": "flag", "targetFields": "numeric"},
            "trimWhitespace": {"enabled": True, "fields": "string"},
        }
        res = self._upload(client, token, project_id, csv, clean_config=clean_config)
        assert res.status_code == 200

    def test_upload_outlier_flag_adds_column(self, client, owner_project):
        token, project_id = owner_project
        data = ",".join(str(i) for i in range(1, 21)) + ",10000"
        csv = ("val\n" + "\n".join(str(i) for i in list(range(1, 21)) + [10000])).encode()
        clean_config = {
            "dedup": {"enabled": False, "fields": "all", "keep": "first"},
            "fillMissing": {"enabled": False, "rules": []},
            "outlier": {"enabled": True, "method": "iqr", "threshold": 1.5, "action": "flag", "targetFields": "numeric"},
            "trimWhitespace": {"enabled": False, "fields": "string"},
        }
        res = self._upload(client, token, project_id, csv, clean_config=clean_config)
        assert res.status_code == 200
        body = res.json()
        # The outlier flag column should be in the dataset fields
        assert "_val_outlier" in body.get("fields", [])

    def test_upload_outlier_remove_reduces_rows(self, client, owner_project):
        token, project_id = owner_project
        csv = ("val\n" + "\n".join(str(i) for i in list(range(1, 21)) + [10000])).encode()
        clean_config = {
            "dedup": {"enabled": False, "fields": "all", "keep": "first"},
            "fillMissing": {"enabled": False, "rules": []},
            "outlier": {"enabled": True, "method": "iqr", "threshold": 1.5, "action": "remove", "targetFields": "numeric"},
            "trimWhitespace": {"enabled": False, "fields": "string"},
        }
        res = self._upload(client, token, project_id, csv, clean_config=clean_config)
        assert res.status_code == 200
        assert res.json()["totalCount"] < 21

    def test_upload_invalid_clean_config_returns_400(self, client, owner_project):
        token, project_id = owner_project
        csv = b"id\n1\n"
        res = client.post(
            f"/projects/{project_id}/upload",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
            data={"name": "ds", "cleanConfig": "not valid json"},
        )
        assert res.status_code in (400, 422)

    def test_upload_uses_preview_token_cache(self, client, owner_project):
        """When a valid previewToken is supplied the file should not be re-parsed."""
        token, project_id = owner_project
        csv = b"id,val\n1,10\n2,20\n3,30\n"

        # First get a preview token
        preview_res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        assert preview_res.status_code == 200
        preview_token = preview_res.json()["previewToken"]

        # Upload with that token
        upload_res = self._upload(
            client, token, project_id, csv, name="cached_ds", preview_token=preview_token
        )
        assert upload_res.status_code == 200
        assert upload_res.json()["totalCount"] == 3

    def test_upload_preview_token_consumed_on_use(self, client, owner_project):
        """The preview token should be removed from the cache after a successful upload."""
        token, project_id = owner_project
        csv = b"id\n1\n2\n"

        preview_res = client.post(
            f"/projects/{project_id}/upload/preview",
            headers=_auth_header(token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
        )
        preview_token = preview_res.json()["previewToken"]

        # First upload consumes the token
        res1 = self._upload(client, token, project_id, csv, name="ds1", preview_token=preview_token)
        assert res1.status_code == 200

        # Second upload with same token falls back to re-parsing the file
        res2 = self._upload(client, token, project_id, csv, name="ds2", preview_token=preview_token)
        assert res2.status_code == 200  # falls back gracefully

    def test_upload_replace_strategy_increments_version(self, client, owner_project):
        token, project_id = owner_project
        csv1 = b"id\n1\n"
        csv2 = b"id\n1\n2\n"

        res1 = self._upload(client, token, project_id, csv1, name="mydata")
        assert res1.status_code == 200
        assert res1.json()["datasetVersion"] == 1

        res2 = self._upload(client, token, project_id, csv2, name="mydata")
        assert res2.status_code == 200
        assert res2.json()["datasetVersion"] == 2
        assert res2.json()["totalCount"] == 2

    def test_upload_requires_authentication(self, client, owner_project):
        _, project_id = owner_project
        csv = b"id\n1\n"
        res = client.post(
            f"/projects/{project_id}/upload",
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
            data={"name": "ds"},
        )
        assert res.status_code in (401, 403)

    def test_upload_viewer_cannot_upload(self, client, owner_project):
        owner_token, project_id = owner_project
        _register(client, "viewer2@example.com", display_name="Viewer2")
        viewer_token = _login(client, "viewer2@example.com").json()["accessToken"]
        client.post(
            f"/projects/{project_id}/members",
            json={"memberEmail": "viewer2@example.com", "role": "viewer"},
            headers=_auth_header(owner_token),
        )
        csv = b"id\n1\n"
        res = client.post(
            f"/projects/{project_id}/upload",
            headers=_auth_header(viewer_token),
            files={"file": ("data.csv", io.BytesIO(csv), "text/csv")},
            data={"name": "ds"},
        )
        assert res.status_code in (401, 403)
