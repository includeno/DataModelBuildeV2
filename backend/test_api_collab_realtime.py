import pytest
from fastapi.testclient import TestClient

from main import app
from storage import storage
from collab_storage import collab_storage
from realtime import realtime_hub


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


def _receive_non_heartbeat(ws, attempts: int = 5):
    message = None
    for _ in range(attempts):
        message = ws.receive_json()
        if message.get("eventType") != "heartbeat_ping":
            return message
    return message


@pytest.fixture(autouse=True)
def clean_data():
    realtime_hub.reset()
    storage.clear()
    collab_storage.clear()
    yield
    realtime_hub.reset()
    storage.clear()
    collab_storage.clear()


def test_project_realtime_presence_replay_and_commit_broadcast(client: TestClient):
    owner = _register(client, "owner@example.com", display_name="Owner")
    editor = _register(client, "editor@example.com", display_name="Editor")
    assert owner.status_code == 200
    assert editor.status_code == 200

    owner_user_id = owner.json()["user"]["id"]
    editor_user_id = editor.json()["user"]["id"]
    owner_token = _login(client, "owner@example.com").json()["accessToken"]
    editor_token = _login(client, "editor@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Realtime Project", "description": "ws"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    add_editor = client.post(
        f"/projects/{project_id}/members",
        json={"memberEmail": "editor@example.com", "role": "editor"},
        headers=_auth_header(owner_token),
    )
    assert add_editor.status_code == 200

    initial_commit = client.post(
        f"/projects/{project_id}/state/commit",
        json={
            "baseVersion": 0,
            "clientOpId": "owner_init",
            "state": {"tree": {"id": "root", "name": "v1"}},
        },
        headers=_auth_header(owner_token),
    )
    assert initial_commit.status_code == 200
    assert initial_commit.json()["version"] == 1

    with client.websocket_connect(f"/ws/projects/{project_id}?token={owner_token}") as owner_ws:
        owner_ws.send_json(
            {
                "type": "subscribe",
                "projectId": project_id,
                "clientVersion": 0,
                "sessionId": "sess_owner",
            }
        )

        owner_subscribed = owner_ws.receive_json()
        assert owner_subscribed["eventType"] == "subscribed"
        assert owner_subscribed["payload"]["latestVersion"] == 1
        assert owner_subscribed["payload"]["connectionId"]
        assert owner_subscribed["payload"]["presence"][0]["userId"] == owner_user_id

        owner_replay = owner_ws.receive_json()
        assert owner_replay["eventType"] == "state_committed"
        assert owner_replay["version"] == 1
        assert owner_replay["payload"]["state"]["tree"]["name"] == "v1"

        with client.websocket_connect(f"/ws/projects/{project_id}?token={editor_token}") as editor_ws:
            editor_ws.send_json(
                {
                    "type": "subscribe",
                    "projectId": project_id,
                    "clientVersion": 1,
                    "sessionId": "sess_editor",
                }
            )

            editor_subscribed = editor_ws.receive_json()
            assert editor_subscribed["eventType"] == "subscribed"
            assert editor_subscribed["payload"]["latestVersion"] == 1
            presence_user_ids = {item["userId"] for item in editor_subscribed["payload"]["presence"]}
            assert presence_user_ids == {owner_user_id, editor_user_id}

            owner_join = _receive_non_heartbeat(owner_ws)
            assert owner_join["eventType"] == "presence_join"
            assert owner_join["payload"]["member"]["userId"] == editor_user_id

            owner_ws.send_json(
                {
                    "type": "presence_update",
                    "editingNodeId": "node_42",
                    "sessionId": "sess_owner",
                    "lastSeenVersion": 1,
                }
            )

            editor_presence = _receive_non_heartbeat(editor_ws)
            assert editor_presence["eventType"] == "presence_update"
            assert editor_presence["payload"]["member"]["editingNodeId"] == "node_42"
            assert editor_presence["payload"]["member"]["userId"] == owner_user_id

            next_commit = client.post(
                f"/projects/{project_id}/state/commit",
                json={
                    "baseVersion": 1,
                    "clientOpId": "owner_note_2",
                    "patches": [{"op": "set_top_level", "key": "note", "value": "synced"}],
                },
                headers=_auth_header(owner_token),
            )
            assert next_commit.status_code == 200
            assert next_commit.json()["version"] == 2

            owner_state_event = _receive_non_heartbeat(owner_ws)
            editor_state_event = _receive_non_heartbeat(editor_ws)

            assert owner_state_event["eventType"] == "state_committed"
            assert editor_state_event["eventType"] == "state_committed"
            assert owner_state_event["version"] == 2
            assert editor_state_event["version"] == 2
            assert editor_state_event["payload"]["patches"] == [
                {"op": "set_top_level", "key": "note", "value": "synced"}
            ]
            assert owner_state_event["payload"]["state"]["note"] == "synced"


def test_project_realtime_conflict_notice_on_invalid_version_and_session(client: TestClient):
    reg = _register(client, "owner@example.com", display_name="Owner")
    assert reg.status_code == 200
    owner_token = _login(client, "owner@example.com").json()["accessToken"]

    created = client.post(
        "/projects",
        json={"name": "Realtime Guardrails", "description": "ws"},
        headers=_auth_header(owner_token),
    )
    assert created.status_code == 200
    project_id = created.json()["id"]

    with client.websocket_connect(f"/ws/projects/{project_id}?token={owner_token}") as ws:
        ws.send_json(
            {
                "type": "subscribe",
                "projectId": project_id,
                "clientVersion": 9,
                "sessionId": "sess_main",
            }
        )

        conflict = ws.receive_json()
        assert conflict["eventType"] == "conflict_notice"
        assert conflict["payload"]["latestVersion"] == 0

        subscribed = ws.receive_json()
        assert subscribed["eventType"] == "subscribed"

        ws.send_json(
            {
                "type": "presence_update",
                "editingNodeId": "node_wrong",
                "sessionId": "sess_other",
            }
        )

        session_conflict = ws.receive_json()
        assert session_conflict["eventType"] == "conflict_notice"
        assert "session mismatch" in session_conflict["payload"]["message"].lower()
