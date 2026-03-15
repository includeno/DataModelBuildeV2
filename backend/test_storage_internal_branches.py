import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import storage as storage_module


def _patch_storage_env(monkeypatch, tmp_path: Path, is_test_env: bool):
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(storage_module, "DATA_ROOT", str(data_root))
    monkeypatch.setattr(storage_module, "DEFAULT_SESSIONS_DIR", str(data_root / "sessions"))
    monkeypatch.setattr(storage_module, "SESSION_STORAGE_CONFIG", str(tmp_path / "session_storage.json"))
    monkeypatch.setattr(storage_module, "IS_TEST_ENV", is_test_env)
    return data_root


def test_path_helpers_and_config_reading(monkeypatch, tmp_path: Path):
    data_root = _patch_storage_env(monkeypatch, tmp_path, is_test_env=False)

    with pytest.raises(ValueError):
        storage_module._normalize_relative_path("../bad")
    with pytest.raises(ValueError):
        storage_module._normalize_relative_path("./bad")

    resolved = storage_module.resolve_data_subdir("a/b")
    assert resolved == str(data_root / "a" / "b")

    with pytest.raises(ValueError):
        storage_module.to_data_relative(str(tmp_path))

    cfg = Path(storage_module.SESSION_STORAGE_CONFIG)
    cfg.write_text(json.dumps("sessions_alt"), encoding="utf-8")
    assert storage_module._read_session_storage_config() == "sessions_alt"

    cfg.write_text(json.dumps({"sessionsDir": "sessions_alt_2"}), encoding="utf-8")
    assert storage_module._read_session_storage_config() == "sessions_alt_2"

    cfg.write_text("{bad-json", encoding="utf-8")
    assert storage_module._read_session_storage_config() is None


def test_load_sessions_dir_and_save_sessions_dir(monkeypatch, tmp_path: Path):
    data_root = _patch_storage_env(monkeypatch, tmp_path, is_test_env=False)

    monkeypatch.setenv("SESSION_STORAGE_DIR", "sessions_x")
    assert storage_module.load_sessions_dir() == str(data_root / "sessions_x")

    abs_sessions = str(tmp_path / "abs_sessions")
    monkeypatch.setenv("SESSION_STORAGE_DIR", abs_sessions)
    assert storage_module.load_sessions_dir() == abs_sessions

    monkeypatch.delenv("SESSION_STORAGE_DIR", raising=False)
    cfg = Path(storage_module.SESSION_STORAGE_CONFIG)
    cfg.write_text(json.dumps({"sessionsDir": "sessions_cfg"}), encoding="utf-8")
    assert storage_module.load_sessions_dir() == str(data_root / "sessions_cfg")

    cfg.write_text(json.dumps({"sessionsDir": str(data_root / "sessions_abs_cfg")}), encoding="utf-8")
    assert storage_module.load_sessions_dir() == str(data_root / "sessions_abs_cfg")

    target = data_root / "saved_sessions"
    target.mkdir(parents=True, exist_ok=True)
    storage_module.save_sessions_dir(str(target))
    saved = json.loads(Path(storage_module.SESSION_STORAGE_CONFIG).read_text(encoding="utf-8"))
    assert saved["sessionsDir"] == "saved_sessions"

    monkeypatch.setattr(storage_module, "IS_TEST_ENV", True)
    cfg_before = Path(storage_module.SESSION_STORAGE_CONFIG).read_text(encoding="utf-8")
    storage_module.save_sessions_dir(str(data_root / "ignored_in_test_env"))
    assert Path(storage_module.SESSION_STORAGE_CONFIG).read_text(encoding="utf-8") == cfg_before


def test_session_storage_json_fallbacks_and_helpers(monkeypatch, tmp_path: Path):
    _patch_storage_env(monkeypatch, tmp_path, is_test_env=True)
    ss = storage_module.SessionStorage()
    sid = "sess_a"
    ss.create_session(sid)

    # Broken schema overrides should fallback to {}
    overrides_path = Path(ss._get_schema_overrides_path(sid))
    overrides_path.write_text("{broken", encoding="utf-8")
    assert ss._load_schema_overrides(sid) == {}

    # Broken index/history files should fallback to []
    Path(ss._get_datasets_index_path(sid)).write_text("{broken", encoding="utf-8")
    Path(ss._get_import_history_path(sid)).write_text("{broken", encoding="utf-8")
    assert ss._load_datasets_index(sid) == []
    assert ss._load_import_history(sid) == []

    ss.append_import_history(sid, {"x": 1})
    assert ss.get_import_history(sid)[0]["x"] == 1

    ss._save_datasets_index(sid, [{"id": "t1", "rows": [{"a": 1}]}, "invalid-entry"])
    saved_index = json.loads(Path(ss._get_datasets_index_path(sid)).read_text(encoding="utf-8"))
    assert saved_index == [{"id": "t1"}]

    assert ss._resolve_dataset_file_path(sid, {}) is None
    assert ss._resolve_dataset_file_path(sid, {"file": "/tmp/a.parquet"}) == "/tmp/a.parquet"
    rel = ss._resolve_dataset_file_path(sid, {"file": "datasets/a.parquet"})
    assert rel.endswith("/datasets/a.parquet")


def test_clear_metadata_and_state_branches(monkeypatch, tmp_path: Path):
    _patch_storage_env(monkeypatch, tmp_path, is_test_env=True)
    ss = storage_module.SessionStorage()

    # clear() else branch when sessions_dir does not exist
    missing_dir = tmp_path / "missing_sessions"
    ss.set_sessions_dir(str(missing_dir))
    if missing_dir.exists():
        os.rmdir(missing_dir)
    ss.clear()
    assert missing_dir.exists()

    sid = "sess_meta"
    ss.create_session(sid)

    meta_path = Path(ss._get_session_path(sid)) / "metadata.json"
    meta_path.write_text("{broken", encoding="utf-8")
    fallback_meta = ss.get_session_metadata(sid)
    assert fallback_meta["settings"]["cascadeDisable"] is False

    # save_session_state branch: existing broken state json
    state_path = Path(ss._get_session_path(sid)) / "state.json"
    state_path.write_text("{broken", encoding="utf-8")
    ss.save_session_state(
        sid,
        {
            "tree": {"id": "root"},
            "datasets": [{"id": "should_not_persist"}],
            "sqlHistory": [{"query": "select 1"}],
            "customKey": "kept",
        },
    )
    saved_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved_state["tree"]["id"] == "root"
    assert saved_state["customKey"] == "kept"
    assert "datasets" not in saved_state
    assert "sqlHistory" not in saved_state

    # get_session_state branch: invalid sql history json
    Path(ss._get_sql_history_path(sid)).write_text("{broken", encoding="utf-8")
    restored = ss.get_session_state(sid)
    assert restored["sqlHistory"] == []
    assert isinstance(restored["datasets"], list)


def test_legacy_dataset_listing_preview_and_query_branches(monkeypatch, tmp_path: Path):
    _patch_storage_env(monkeypatch, tmp_path, is_test_env=True)
    ss = storage_module.SessionStorage()
    sid = "sess_legacy"
    ss.create_session(sid)

    # Build a legacy table directly in DuckDB (without datasets index entries)
    db_path = ss._get_db_path(sid)
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE TABLE legacy_tbl AS SELECT 1 AS id, 'a' AS val")
    finally:
        con.close()

    listed = ss.list_datasets(sid)
    assert any(d["id"] == "legacy_tbl" for d in listed)

    # Trigger list_datasets exception branch
    class _BrokenCon:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("boom")

        def close(self):
            pass

    monkeypatch.setattr(storage_module.duckdb, "connect", lambda *_a, **_k: _BrokenCon())
    assert ss.list_datasets(sid) == []


def test_preview_full_dataset_execute_sql_and_delete_dataset(monkeypatch, tmp_path: Path):
    _patch_storage_env(monkeypatch, tmp_path, is_test_env=True)
    ss = storage_module.SessionStorage()
    sid = "sess_ops"
    ss.create_session(sid)

    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    ss.add_dataset(sid, "orders", df)
    ss.save_dataset_field_types(sid, "orders", {"id": {"type": "number"}})

    with pytest.raises(ValueError):
        ss.get_dataset_preview(sid, "select")

    assert ss.get_dataset_preview(sid, "missing_table") is None

    with pytest.raises(ValueError):
        ss.execute_sql("missing_session", "select 1")

    with pytest.raises(ValueError):
        ss.get_full_dataset(sid, "select")

    assert ss.get_full_dataset(sid, "missing_table") is None

    # delete_dataset should remove schema override and data/index entry
    assert ss.delete_dataset(sid, "orders") is True
    overrides = ss._load_schema_overrides(sid)
    assert "orders" not in overrides

    # Legacy remove path: table exists only in db, not in datasets index
    sid2 = "sess_legacy_drop"
    ss.create_session(sid2)
    con = duckdb.connect(ss._get_db_path(sid2))
    try:
        con.execute("CREATE VIEW legacy_only AS SELECT 1 AS id")
    finally:
        con.close()
    assert ss.delete_dataset(sid2, "legacy_only") is True
    assert ss.delete_dataset(sid2, "not_exists_anywhere") is False
