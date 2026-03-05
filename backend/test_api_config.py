import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main as main_module
import storage as storage_module
from storage import storage

client = TestClient(main_module.app)


@pytest.fixture(autouse=True)
def clean_env():
    storage.clear()
    yield
    storage.clear()


def test_default_server_missing_file(monkeypatch, tmp_path: Path):
    missing = tmp_path / "missing.json"
    monkeypatch.setattr(main_module, "DEFAULT_SERVER_FILE", str(missing))

    res = client.get("/config/default_server")
    assert res.status_code == 200
    payload = res.json()
    assert payload["server"] == "mockServer"
    assert payload["isMock"] is True


def test_default_server_from_file(monkeypatch, tmp_path: Path):
    cfg = tmp_path / "default_server.json"
    cfg.write_text(json.dumps({"server": "http://localhost:8000"}), encoding="utf-8")
    monkeypatch.setattr(main_module, "DEFAULT_SERVER_FILE", str(cfg))

    res = client.get("/config/default_server")
    assert res.status_code == 200
    payload = res.json()
    assert payload["server"] == "http://localhost:8000"
    assert payload["isMock"] is False


def test_dataset_preview_endpoint():
    session_id = client.post("/sessions").json()["sessionId"]
    csv_content = "id,name\n1,Alice\n2,Bob"
    upload_res = client.post(
        "/upload",
        files={"file": ("people.csv", csv_content, "text/csv")},
        data={"sessionId": session_id, "name": "people"},
    )
    assert upload_res.status_code == 200

    preview_res = client.get(f"/sessions/{session_id}/datasets/people/preview?limit=1")
    assert preview_res.status_code == 200
    payload = preview_res.json()
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["id"] == 1


def test_datasets_index_no_rows_field():
    session_id = client.post("/sessions").json()["sessionId"]
    csv_content = "id,name\n1,Alice\n2,Bob"
    client.post(
        "/upload",
        files={"file": ("people.csv", csv_content, "text/csv")},
        data={"sessionId": session_id, "name": "people"},
    )

    datasets_path = Path(storage._get_session_path(session_id)) / "datasets.json"
    assert datasets_path.exists()
    data = json.loads(datasets_path.read_text(encoding="utf-8"))
    assert isinstance(data, list) and len(data) == 1
    assert "rows" not in data[0]


def test_session_storage_endpoints(tmp_path: Path, monkeypatch):
    data_root = tmp_path / "data"
    sessions_default = data_root / "sessions"
    sessions_alt = data_root / "sessions_alt"

    monkeypatch.setattr(storage_module, "DATA_ROOT", str(data_root))
    monkeypatch.setattr(storage_module, "DEFAULT_SESSIONS_DIR", str(sessions_default))
    monkeypatch.setattr(storage_module, "SESSION_STORAGE_CONFIG", str(tmp_path / "session_storage.json"))

    os.makedirs(data_root, exist_ok=True)
    storage.set_sessions_dir(str(sessions_default))

    res = client.get("/config/session_storage")
    assert res.status_code == 200
    assert res.json()["relative"] in ("sessions", "")

    create_res = client.post("/config/session_storage/create", json={"path": "sessions_alt"})
    assert create_res.status_code == 200
    assert sessions_alt.exists()

    list_res = client.get("/config/session_storage/list?path=")
    assert list_res.status_code == 200
    folders = list_res.json()["folders"]
    assert any(f["path"] == "sessions_alt" for f in folders)

    select_res = client.post("/config/session_storage/select", json={"path": "sessions_alt"})
    assert select_res.status_code == 200
    assert storage.sessions_dir.endswith("sessions_alt")


def test_session_storage_switches_sessions(tmp_path: Path, monkeypatch):
    data_root = tmp_path / "data"
    sessions_a = data_root / "sessions_a"
    sessions_b = data_root / "sessions_b"

    monkeypatch.setattr(storage_module, "DATA_ROOT", str(data_root))
    monkeypatch.setattr(storage_module, "DEFAULT_SESSIONS_DIR", str(sessions_a))
    monkeypatch.setattr(storage_module, "SESSION_STORAGE_CONFIG", str(tmp_path / "session_storage.json"))

    os.makedirs(data_root, exist_ok=True)
    storage.set_sessions_dir(str(sessions_a))

    try:
        # Create session in A
        res_a = client.post("/sessions")
        assert res_a.status_code == 200
        session_a = res_a.json()["sessionId"]

        list_a = client.get("/sessions").json()
        assert any(s["sessionId"] == session_a for s in list_a)

        # Switch to B and ensure A is not listed
        os.makedirs(sessions_b, exist_ok=True)
        select_res = client.post("/config/session_storage/select", json={"path": "sessions_b"})
        assert select_res.status_code == 200

        list_b = client.get("/sessions").json()
        assert all(s["sessionId"] != session_a for s in list_b)

        # Create session in B
        res_b = client.post("/sessions")
        session_b = res_b.json()["sessionId"]

        list_b2 = client.get("/sessions").json()
        assert any(s["sessionId"] == session_b for s in list_b2)

        # Switch back to A and ensure only A sessions appear
        select_res = client.post("/config/session_storage/select", json={"path": "sessions_a"})
        assert select_res.status_code == 200
        list_a2 = client.get("/sessions").json()
        assert any(s["sessionId"] == session_a for s in list_a2)
        assert all(s["sessionId"] != session_b for s in list_a2)
    finally:
        storage.set_sessions_dir(str(sessions_a))
