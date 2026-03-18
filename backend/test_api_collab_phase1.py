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
    res = client.post(
        "/auth/register",
        json={"email": email, "password": password, "displayName": display_name},
    )
    return res


def _login(client: TestClient, email: str, password: str = "Passw0rd!"):
    return client.post("/auth/login", json={"email": email, "password": password})


@pytest.fixture(autouse=True)
def clean_data():
    storage.clear()
    collab_storage.clear()
    yield
    storage.clear()
    collab_storage.clear()


def test_auth_register_login_and_me(client: TestClient):
    weak = _register(client, "weak@example.com", password="12345678")
    assert weak.status_code == 400

    reg = _register(client, "owner@example.com", display_name="Owner")
    assert reg.status_code == 200
    assert reg.json()["user"]["email"] == "owner@example.com"

    dup = _register(client, "owner@example.com")
    assert dup.status_code == 400

    bad_login = _login(client, "owner@example.com", password="wrong-pass")
    assert bad_login.status_code == 401

    login = _login(client, "owner@example.com")
    assert login.status_code == 200
    token = login.json()["accessToken"]
    refresh = login.json()["refreshToken"]
    assert token
    assert refresh

    me = client.get("/auth/me", headers=_auth_header(token))
    assert me.status_code == 200
    assert me.json()["email"] == "owner@example.com"

    refreshed = client.post("/auth/refresh", json={"refreshToken": refresh})
    assert refreshed.status_code == 200
    assert refreshed.json()["accessToken"]
    assert refreshed.json()["refreshToken"] == refresh

    out = client.post("/auth/logout", headers=_auth_header(token))
    assert out.status_code == 200

    me_after = client.get("/auth/me", headers=_auth_header(token))
    assert me_after.status_code == 401


def test_phase1_project_membership_and_version_commit_conflict(client: TestClient):
    owner = _register(client, "owner@example.com", display_name="Owner")
    editor = _register(client, "editor@example.com", display_name="Editor")
    viewer = _register(client, "viewer@example.com", display_name="Viewer")
    assert owner.status_code == 200
    assert editor.status_code == 200
    assert viewer.status_code == 200

    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    editor_token = _login(client, "editor@example.com").json()["accessToken"]
    viewer_token = _login(client, "viewer@example.com").json()["accessToken"]

    no_auth_projects = client.get("/projects")
    assert no_auth_projects.status_code == 401

    create = client.post(
        "/projects",
        json={"name": "Team Project", "description": "Phase1"},
        headers=_auth_header(owner_token),
    )
    assert create.status_code == 200
    project_id = create.json()["id"]
    assert create.json()["role"] == "owner"

    add_editor = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "editor@example.com", "role": "editor"},
        headers=_auth_header(owner_token),
    )
    assert add_editor.status_code == 200
    assert add_editor.json()["role"] == "editor"

    add_viewer = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "viewer@example.com", "role": "viewer"},
        headers=_auth_header(owner_token),
    )
    assert add_viewer.status_code == 200
    assert add_viewer.json()["role"] == "viewer"

    members = client.get(f"/projects/{project_id}/members", headers=_auth_header(owner_token))
    assert members.status_code == 200
    assert len(members.json()) == 3

    viewer_add_fail = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "owner@example.com", "role": "viewer"},
        headers=_auth_header(viewer_token),
    )
    assert viewer_add_fail.status_code == 403

    viewer_state = client.get(f"/projects/{project_id}/state", headers=_auth_header(viewer_token))
    assert viewer_state.status_code == 200
    assert viewer_state.json()["version"] == 0
    assert viewer_state.json()["state"] == {}

    viewer_commit_fail = client.post(
        f"/projects/{project_id}/state/commit",
        json={"baseVersion": 0, "state": {"tree": {"id": "root"}}},
        headers=_auth_header(viewer_token),
    )
    assert viewer_commit_fail.status_code == 403

    editor_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 0,
            "state": {"tree": {"id": "root", "name": "Project"}},
            "clientOpId": "op_editor_1",
        },
        headers=_auth_header(editor_token),
    )
    assert editor_commit.status_code == 200
    assert editor_commit.json()["version"] == 1
    assert editor_commit.json()["conflict"] is False

    owner_stale_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 0,
            "state": {"tree": {"id": "root", "name": "Old"}},
            "clientOpId": "op_owner_stale",
        },
        headers=_auth_header(owner_token),
    )
    assert owner_stale_commit.status_code == 409
    assert owner_stale_commit.json()["detail"]["code"] == "PROJECT_STATE_CONFLICT"
    assert owner_stale_commit.json()["detail"]["data"]["latestVersion"] == 1

    owner_patch_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 1,
            "clientOpId": "op_owner_patch_1",
            "patches": [{"op": "set_top_level", "key": "note", "value": "added-by-owner"}],
        },
        headers=_auth_header(owner_token),
    )
    assert owner_patch_commit.status_code == 200
    assert owner_patch_commit.json()["version"] == 2
    assert owner_patch_commit.json()["state"]["note"] == "added-by-owner"

    owner_patch_idempotent = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 1,
            "clientOpId": "op_owner_patch_1",
            "patches": [{"op": "set_top_level", "key": "note", "value": "added-by-owner"}],
        },
        headers=_auth_header(owner_token),
    )
    assert owner_patch_idempotent.status_code == 200
    assert owner_patch_idempotent.json()["idempotent"] is True
    assert owner_patch_idempotent.json()["version"] == 2

    update_member = client.patch(
        f"/projects/{project_id}/members/{add_viewer.json()['userId']}",
        json={"role": "editor"},
        headers=_auth_header(owner_token),
    )
    assert update_member.status_code == 200
    assert update_member.json()["role"] == "editor"

    remove_member = client.delete(
        f"/projects/{project_id}/members/{add_viewer.json()['userId']}",
        headers=_auth_header(owner_token),
    )
    assert remove_member.status_code == 200
    assert remove_member.json()["removed"] is True

    members_after_remove = client.get(f"/projects/{project_id}/members", headers=_auth_header(owner_token))
    assert members_after_remove.status_code == 200
    assert len(members_after_remove.json()) == 2

    events = client.get(
        f"/projects/{project_id}/events?fromVersion=0&limit=20",
        headers=_auth_header(editor_token),
    )
    assert events.status_code == 200
    versions = [e["version"] for e in events.json()["events"]]
    assert versions == [1, 2]
    assert events.json()["latestVersion"] == 2
