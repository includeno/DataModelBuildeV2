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


def test_organization_bootstrap_and_member_management(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    assert _register(client, "member@example.com", display_name="Member").status_code == 200

    owner_login = _login(client, "owner@example.com")
    assert owner_login.status_code == 200
    owner_token = owner_login.json()["accessToken"]

    orgs = client.get("/organizations", headers=_auth_header(owner_token))
    assert orgs.status_code == 200
    # Personal org is created on registration.
    assert len(orgs.json()) >= 1

    create_org = client.post(
        "/organizations",
        json={"name": "Team Alpha"},
        headers=_auth_header(owner_token),
    )
    assert create_org.status_code == 200
    org_id = create_org.json()["id"]

    add_member = client.post(
        f"/organizations/{org_id}/members",
        json={"memberEmail": "member@example.com", "role": "member"},
        headers=_auth_header(owner_token),
    )
    assert add_member.status_code == 200
    member_user_id = add_member.json()["userId"]

    update_member = client.patch(
        f"/organizations/{org_id}/members/{member_user_id}",
        json={"role": "admin"},
        headers=_auth_header(owner_token),
    )
    assert update_member.status_code == 200
    assert update_member.json()["role"] == "admin"

    member_login = _login(client, "member@example.com")
    assert member_login.status_code == 200
    member_token = member_login.json()["accessToken"]

    member_orgs = client.get("/organizations", headers=_auth_header(member_token))
    assert member_orgs.status_code == 200
    assert any(o["id"] == org_id for o in member_orgs.json())

    member_list = client.get(f"/organizations/{org_id}/members", headers=_auth_header(member_token))
    assert member_list.status_code == 200
    assert len(member_list.json()) == 2


def test_project_search_archive_delete_and_org_scoping(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    assert _register(client, "outsider@example.com", display_name="Outsider").status_code == 200

    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    outsider_token = _login(client, "outsider@example.com").json()["accessToken"]

    create_org = client.post(
        "/organizations",
        json={"name": "Workspace A"},
        headers=_auth_header(owner_token),
    )
    assert create_org.status_code == 200
    org_id = create_org.json()["id"]

    p1 = client.post(
        "/projects",
        json={"name": "Sales ETL", "description": "pipeline", "orgId": org_id},
        headers=_auth_header(owner_token),
    )
    assert p1.status_code == 200
    project_id = p1.json()["id"]
    assert p1.json()["orgId"] == org_id

    p2 = client.post(
        "/projects",
        json={"name": "Finance ETL", "description": "pipeline", "orgId": org_id},
        headers=_auth_header(owner_token),
    )
    assert p2.status_code == 200

    query_sales = client.get(
        "/projects/query?page=1&pageSize=10&search=sales",
        headers=_auth_header(owner_token),
    )
    assert query_sales.status_code == 200
    assert query_sales.json()["total"] == 1
    assert query_sales.json()["items"][0]["id"] == project_id

    archive = client.post(
        f"/projects/{project_id}/archive",
        json={"archived": True},
        headers=_auth_header(owner_token),
    )
    assert archive.status_code == 200
    assert archive.json()["archived"] is True

    active_list = client.get("/projects", headers=_auth_header(owner_token))
    assert active_list.status_code == 200
    assert all(p["id"] != project_id for p in active_list.json())

    include_archived = client.get(
        "/projects/query?page=1&pageSize=10&includeArchived=true",
        headers=_auth_header(owner_token),
    )
    assert include_archived.status_code == 200
    ids = {p["id"] for p in include_archived.json()["items"]}
    assert project_id in ids

    outsider_get = client.get(f"/projects/{project_id}", headers=_auth_header(outsider_token))
    assert outsider_get.status_code == 404

    delete_project = client.delete(f"/projects/{project_id}", headers=_auth_header(owner_token))
    assert delete_project.status_code == 200

    after_delete = client.get(f"/projects/{project_id}", headers=_auth_header(owner_token))
    assert after_delete.status_code == 404

    refresh_invalid = client.post("/auth/refresh", json={"refreshToken": "invalid"})
    assert refresh_invalid.status_code == 401


def test_project_scoped_runtime_endpoints_with_permissions(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    assert _register(client, "editor@example.com", display_name="Editor").status_code == 200
    assert _register(client, "viewer@example.com", display_name="Viewer").status_code == 200
    assert _register(client, "outsider@example.com", display_name="Outsider").status_code == 200

    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    editor_token = _login(client, "editor@example.com").json()["accessToken"]
    viewer_token = _login(client, "viewer@example.com").json()["accessToken"]
    outsider_token = _login(client, "outsider@example.com").json()["accessToken"]

    create_project = client.post(
        "/projects",
        json={"name": "Runtime Project", "description": "runtime-scope"},
        headers=_auth_header(owner_token),
    )
    assert create_project.status_code == 200
    project_id = create_project.json()["id"]

    add_editor = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "editor@example.com", "role": "editor"},
        headers=_auth_header(owner_token),
    )
    assert add_editor.status_code == 200

    add_viewer = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "viewer@example.com", "role": "viewer"},
        headers=_auth_header(owner_token),
    )
    assert add_viewer.status_code == 200

    write_meta_editor = client.post(
        f"/projects/{project_id}/metadata",
        json={"displayName": "Editor Updated"},
        headers=_auth_header(editor_token),
    )
    assert write_meta_editor.status_code == 200

    read_meta_viewer = client.get(
        f"/projects/{project_id}/metadata",
        headers=_auth_header(viewer_token),
    )
    assert read_meta_viewer.status_code == 200
    assert read_meta_viewer.json()["displayName"] == "Editor Updated"

    write_meta_viewer = client.post(
        f"/projects/{project_id}/metadata",
        json={"displayName": "Viewer Attempt"},
        headers=_auth_header(viewer_token),
    )
    assert write_meta_viewer.status_code == 403

    list_datasets_member = client.get(
        f"/projects/{project_id}/datasets",
        headers=_auth_header(editor_token),
    )
    assert list_datasets_member.status_code == 200
    assert list_datasets_member.json() == []

    outsider_read = client.get(
        f"/projects/{project_id}/metadata",
        headers=_auth_header(outsider_token),
    )
    assert outsider_read.status_code == 404


def test_state_since_snapshot_and_conflict_replay_acceptance(client: TestClient):
    assert _register(client, "owner@example.com", display_name="Owner").status_code == 200
    assert _register(client, "editor@example.com", display_name="Editor").status_code == 200

    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    editor_token = _login(client, "editor@example.com").json()["accessToken"]

    create_project = client.post(
        "/projects",
        json={"name": "Concurrent Project", "description": "state-versioning"},
        headers=_auth_header(owner_token),
    )
    assert create_project.status_code == 200
    project_id = create_project.json()["id"]

    add_editor = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "editor@example.com", "role": "editor"},
        headers=_auth_header(owner_token),
    )
    assert add_editor.status_code == 200

    first_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 0,
            "clientOpId": "owner_init_1",
            "patches": [{"op": "set_top_level", "key": "title", "value": "v1"}],
        },
        headers=_auth_header(owner_token),
    )
    assert first_commit.status_code == 200
    assert first_commit.json()["version"] == 1

    since_0 = client.get(
        f"/projects/{project_id}/state?sinceVersion=0",
        headers=_auth_header(editor_token),
    )
    assert since_0.status_code == 200
    assert since_0.json()["changed"] is True
    assert since_0.json()["version"] == 1
    assert since_0.json()["events"][0]["version"] == 1

    since_1 = client.get(
        f"/projects/{project_id}/state?sinceVersion=1",
        headers=_auth_header(editor_token),
    )
    assert since_1.status_code == 200
    assert since_1.json()["changed"] is False

    current_version = 1
    for idx in range(2, 51):
        commit = client.post(
            f"/projects/{project_id}/state/commit",
            json={
                "baseVersion": current_version,
                "clientOpId": f"owner_bulk_{idx}",
                "patches": [{"op": "set_top_level", "key": f"k{idx}", "value": idx}],
            },
            headers=_auth_header(owner_token),
        )
        assert commit.status_code == 200
        current_version = commit.json()["version"]

    assert current_version == 50

    snapshot_window = client.get(
        f"/projects/{project_id}/events?fromVersion=49&limit=20",
        headers=_auth_header(owner_token),
    )
    assert snapshot_window.status_code == 200
    assert any(
        event["eventType"] == "state_snapshot" and event["version"] == 50
        for event in snapshot_window.json()["events"]
    )

    stale_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 49,
            "clientOpId": "editor_stale_1",
            "patches": [{"op": "set_top_level", "key": "editor_note", "value": "from-stale"}],
        },
        headers=_auth_header(editor_token),
    )
    assert stale_commit.status_code == 409
    assert stale_commit.json()["detail"]["code"] == "PROJECT_STATE_CONFLICT"
    assert stale_commit.json()["detail"]["data"]["latestVersion"] == 50

    latest_since = client.get(
        f"/projects/{project_id}/state?sinceVersion=49",
        headers=_auth_header(editor_token),
    )
    assert latest_since.status_code == 200
    assert latest_since.json()["changed"] is True
    assert latest_since.json()["version"] == 50

    merged_state = latest_since.json()["state"]
    merged_state["editor_note"] = "replayed"

    replay_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 50,
            "clientOpId": "editor_replay_1",
            "state": merged_state,
        },
        headers=_auth_header(editor_token),
    )
    assert replay_commit.status_code == 200
    assert replay_commit.json()["version"] == 51
    assert replay_commit.json()["state"]["editor_note"] == "replayed"
    assert replay_commit.json()["state"]["k50"] == 50
