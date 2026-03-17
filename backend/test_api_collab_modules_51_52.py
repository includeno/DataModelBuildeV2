import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage
from collab_storage import collab_storage


client = TestClient(app)


def _auth_header(token: str):
    return {"Authorization": f"Bearer {token}"}


def _register(email: str, password: str = "Passw0rd!", display_name: str = ""):
    return client.post(
        "/auth/register",
        json={"email": email, "password": password, "displayName": display_name},
    )


def _login(email: str, password: str = "Passw0rd!"):
    return client.post("/auth/login", json={"email": email, "password": password})


@pytest.fixture(autouse=True)
def clean_data():
    storage.clear()
    collab_storage.clear()
    yield
    storage.clear()
    collab_storage.clear()


def test_organization_bootstrap_and_member_management():
    assert _register("owner@example.com", display_name="Owner").status_code == 200
    assert _register("member@example.com", display_name="Member").status_code == 200

    owner_login = _login("owner@example.com")
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

    member_login = _login("member@example.com")
    assert member_login.status_code == 200
    member_token = member_login.json()["accessToken"]

    member_orgs = client.get("/organizations", headers=_auth_header(member_token))
    assert member_orgs.status_code == 200
    assert any(o["id"] == org_id for o in member_orgs.json())

    member_list = client.get(f"/organizations/{org_id}/members", headers=_auth_header(member_token))
    assert member_list.status_code == 200
    assert len(member_list.json()) == 2


def test_project_search_archive_delete_and_org_scoping():
    assert _register("owner@example.com", display_name="Owner").status_code == 200
    assert _register("outsider@example.com", display_name="Outsider").status_code == 200

    owner_token = _login("owner@example.com").json()["accessToken"]
    outsider_token = _login("outsider@example.com").json()["accessToken"]

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

