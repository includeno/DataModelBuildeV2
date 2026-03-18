import json
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main as main_module
import runtime_config as runtime_config_module
from storage import storage

client = TestClient(main_module.app)


@pytest.fixture(autouse=True)
def clean_env():
    storage.clear()
    yield
    storage.clear()


def _simple_tree():
    return {
        "id": "root",
        "type": "operation",
        "name": "Root",
        "enabled": True,
        "commands": [],
        "children": [],
    }


def test_load_default_server_string_and_invalid_json(monkeypatch, tmp_path: Path):
    cfg = tmp_path / "default_server.json"
    cfg.write_text(json.dumps("mock"), encoding="utf-8")
    monkeypatch.setattr(
        runtime_config_module,
        "DEFAULT_RUNTIME_CONFIG",
        replace(runtime_config_module.DEFAULT_RUNTIME_CONFIG, default_server_file=str(cfg)),
    )
    assert main_module.load_default_server() == "mockServer"

    cfg.write_text(json.dumps(["not_supported_type"]), encoding="utf-8")
    assert main_module.load_default_server() == "mockServer"

    cfg.write_text("{bad-json", encoding="utf-8")
    assert main_module.load_default_server() == "mockServer"


def test_session_storage_invalid_paths_return_400():
    res_list = client.get("/config/session_storage/list?path=../bad")
    assert res_list.status_code == 400

    res_create = client.post("/config/session_storage/create", json={"path": "../bad"})
    assert res_create.status_code == 400

    res_select_invalid = client.post("/config/session_storage/select", json={"path": "../bad"})
    assert res_select_invalid.status_code == 400


def test_session_storage_select_missing_folder_returns_404():
    res = client.post("/config/session_storage/select", json={"path": "folder_not_exists_123"})
    assert res.status_code == 404
    assert res.json()["detail"] == "Folder not found"


def test_delete_dataset_success_path():
    session_id = client.post("/sessions").json()["sessionId"]
    upload = client.post(
        "/upload",
        files={"file": ("people.csv", "id,name\n1,Alice", "text/csv")},
        data={"sessionId": session_id, "name": "people"},
    )
    assert upload.status_code == 200

    res = client.delete(f"/sessions/{session_id}/datasets/people")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_diagnostics_no_tree_warning():
    session_id = client.post("/sessions").json()["sessionId"]
    client.post(f"/sessions/{session_id}/state", json={})
    res = client.get(f"/sessions/{session_id}/diagnostics")
    assert res.status_code == 200
    report = res.json()
    assert "No tree found in session state." in report["warnings"]


def test_diagnostics_parse_tree_failure_warning():
    session_id = client.post("/sessions").json()["sessionId"]
    # tree is present, but invalid for OperationNode parsing
    client.post(f"/sessions/{session_id}/state", json={"tree": {"id": "broken"}})

    res = client.get(f"/sessions/{session_id}/diagnostics")
    assert res.status_code == 200
    report = res.json()
    assert any("Failed to parse tree:" in w for w in report["warnings"])


def test_diagnostics_missing_data_source_and_missing_dataset_warnings():
    session_id = client.post("/sessions").json()["sessionId"]
    tree = {
        "id": "root",
        "type": "operation",
        "operationType": "process",
        "name": "Root",
        "enabled": True,
        "commands": [
            {
                "id": "cmd_no_source",
                "type": "filter",
                "order": 1,
                "config": {"field": "id", "operator": "=", "value": 1},
            },
            {
                "id": "cmd_blank_source",
                "type": "filter",
                "order": 2,
                "config": {"dataSource": "", "field": "id", "operator": "=", "value": 2},
            },
            {
                "id": "cmd_missing_dataset",
                "type": "filter",
                "order": 3,
                "config": {"dataSource": "ghost_link", "field": "id", "operator": "=", "value": 3},
            },
        ],
        "children": [],
    }
    client.post(f"/sessions/{session_id}/state", json={"tree": tree})

    res = client.get(f"/sessions/{session_id}/diagnostics")
    assert res.status_code == 200
    report = res.json()

    assert any("Missing data source: command cmd_no_source" in w for w in report["warnings"])
    assert any("Missing data source: command cmd_blank_source" in w for w in report["warnings"])
    assert any("references dataSource 'ghost_link'" in w for w in report["warnings"])
    assert any("No source commands found." in w for w in report["warnings"])
    assert any("No datasets found in storage." in w for w in report["warnings"])


def test_upload_csv_excel_parquet_parse_errors():
    session_id = client.post("/sessions").json()["sessionId"]

    bad_csv = client.post(
        "/upload",
        files={"file": ("bad.csv", b"\x80\x81\x82", "text/csv")},
        data={"sessionId": session_id},
    )
    assert bad_csv.status_code == 200
    assert "Could not parse CSV" in bad_csv.json()["error"]

    bad_xlsx = client.post(
        "/upload",
        files={"file": ("bad.xlsx", b"not-an-excel", "application/octet-stream")},
        data={"sessionId": session_id},
    )
    assert bad_xlsx.status_code == 200
    assert "Could not parse Excel file" in bad_xlsx.json()["error"]

    bad_parquet = client.post(
        "/upload",
        files={"file": ("bad.parquet", b"not-a-parquet", "application/octet-stream")},
        data={"sessionId": session_id},
    )
    assert bad_parquet.status_code == 200
    assert "Could not parse Parquet file" in bad_parquet.json()["error"]


def test_upload_outer_exception_branch(monkeypatch):
    session_id = client.post("/sessions").json()["sessionId"]

    def boom(*args, **kwargs):
        raise RuntimeError("forced-add-dataset-error")

    monkeypatch.setattr(main_module.storage, "add_dataset", boom)

    res = client.post(
        "/upload",
        files={"file": ("ok.csv", "id\n1", "text/csv")},
        data={"sessionId": session_id, "name": "ok"},
    )
    assert res.status_code == 200
    assert "forced-add-dataset-error" in res.json()["error"]


def test_export_analyze_query_error_branches():
    session_id = client.post("/sessions").json()["sessionId"]
    tree = _simple_tree()

    export_res = client.post(
        "/export",
        json={"sessionId": session_id, "tree": tree, "targetNodeId": "missing_node"},
    )
    assert export_res.status_code == 500

    analyze_res = client.post(
        "/analyze",
        json={"sessionId": session_id, "tree": tree, "parentNodeId": "missing_node"},
    )
    assert analyze_res.status_code == 500

    query_res = client.post("/query", json={"sessionId": session_id, "query": "SELECT * FROM not_exists"})
    assert query_res.status_code == 400
