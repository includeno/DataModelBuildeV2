
import pandas as pd
import duckdb
import os
import shutil
import re
import json
import time
import uuid
import tempfile
from typing import List, Dict, Optional, Any

from sql_utils import quote_identifier, is_reserved_identifier

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(REPO_ROOT, "data")
BACKEND_ENV = (os.environ.get("BACKEND_ENV") or "production").strip().lower()
IS_TEST_ENV = BACKEND_ENV == "test" or bool(os.environ.get("PYTEST_CURRENT_TEST"))
DEFAULT_SESSIONS_DIR = os.path.join(DATA_ROOT, "sessions_test" if IS_TEST_ENV else "sessions")
SESSION_STORAGE_CONFIG = os.environ.get(
    "SESSION_STORAGE_CONFIG_PATH",
    os.path.join(
        os.path.dirname(__file__),
        "session_storage_test.json" if IS_TEST_ENV else "session_storage.json",
    ),
)

DATASETS_DIRNAME = "datasets"
DATASETS_INDEX = "datasets.json"
SQL_HISTORY_FILE = "sql_history.json"
IMPORT_HISTORY_FILE = "import_history.json"
PROJECT_ASSETS_DIRNAME = "project_assets"
PROJECT_ASSETS_TRASH_DIRNAME = ".trash"
LOCAL_STORAGE_MIN_FREE_BYTES = int(os.environ.get("LOCAL_STORAGE_MIN_FREE_BYTES", str(64 * 1024 * 1024)))


def _ensure_data_root():
    os.makedirs(DATA_ROOT, exist_ok=True)


def _normalize_relative_path(path: str) -> str:
    clean = (path or "").replace("\\", "/").strip().strip("/")
    if not clean:
        return ""
    parts = [p for p in clean.split("/") if p]
    if any(p in (".", "..") for p in parts):
        raise ValueError("Invalid path")
    return "/".join(parts)


def resolve_data_subdir(path: str) -> str:
    _ensure_data_root()
    rel = _normalize_relative_path(path)
    abs_path = os.path.abspath(os.path.join(DATA_ROOT, rel))
    if os.path.commonpath([DATA_ROOT, abs_path]) != DATA_ROOT:
        raise ValueError("Path must be under data directory")
    return abs_path


def to_data_relative(path: str) -> str:
    _ensure_data_root()
    abs_path = os.path.abspath(path)
    if os.path.commonpath([DATA_ROOT, abs_path]) != DATA_ROOT:
        raise ValueError("Path must be under data directory")
    rel = os.path.relpath(abs_path, DATA_ROOT)
    return "" if rel == "." else rel.replace("\\", "/")


def _read_session_storage_config() -> Optional[str]:
    if not os.path.exists(SESSION_STORAGE_CONFIG):
        return None
    try:
        with open(SESSION_STORAGE_CONFIG, "r") as f:
            data = json.load(f)
        if isinstance(data, str):
            return data.strip()
        if isinstance(data, dict):
            return str(data.get("sessionsDir") or data.get("path") or "").strip() or None
    except Exception:
        return None
    return None


def load_sessions_dir() -> str:
    _ensure_data_root()

    env_override = os.environ.get("SESSION_STORAGE_DIR")
    if env_override:
        return resolve_data_subdir(env_override) if not os.path.isabs(env_override) else env_override

    if IS_TEST_ENV:
        return DEFAULT_SESSIONS_DIR

    cfg = _read_session_storage_config()
    if cfg:
        if os.path.isabs(cfg):
            try:
                return resolve_data_subdir(to_data_relative(cfg))
            except Exception:
                pass
        else:
            try:
                return resolve_data_subdir(cfg)
            except Exception:
                pass

    return DEFAULT_SESSIONS_DIR


def save_sessions_dir(path: str):
    if IS_TEST_ENV:
        return
    rel = to_data_relative(path)
    with open(SESSION_STORAGE_CONFIG, "w") as f:
        json.dump({"sessionsDir": rel or "sessions"}, f, indent=2)


class StorageBackend:
    def resolve_path(self, storage_key: str) -> str:
        raise NotImplementedError

    def put(self, storage_key: str, data: bytes) -> str:
        raise NotImplementedError

    def open(self, storage_key: str, mode: str = "rb"):
        raise NotImplementedError

    def exists(self, storage_key: str) -> bool:
        raise NotImplementedError

    def delete(self, storage_key: str) -> bool:
        raise NotImplementedError

    def move_atomic(self, source_path: str, storage_key: str) -> str:
        raise NotImplementedError

    def healthcheck(self) -> Dict[str, Any]:
        raise NotImplementedError


class LocalFileBackend(StorageBackend):
    def __init__(self, root_dir: Optional[str] = None):
        base_root = root_dir or os.path.join(DATA_ROOT, PROJECT_ASSETS_DIRNAME)
        self.root_dir = os.path.abspath(base_root)
        os.makedirs(self.root_dir, exist_ok=True)

    def resolve_path(self, storage_key: str) -> str:
        rel_key = _normalize_relative_path(storage_key)
        if not rel_key:
            raise ValueError("storage_key is required")
        abs_path = resolve_data_subdir(rel_key)
        if os.path.commonpath([self.root_dir, abs_path]) != self.root_dir:
            raise ValueError("storage_key must be under local storage root")
        return abs_path

    def exists(self, storage_key: str) -> bool:
        return os.path.exists(self.resolve_path(storage_key))

    def put(self, storage_key: str, data: bytes) -> str:
        target_path = self.resolve_path(storage_key)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(target_path))
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            return self.move_atomic(tmp_path, storage_key)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

    def open(self, storage_key: str, mode: str = "rb"):
        return open(self.resolve_path(storage_key), mode)

    def delete(self, storage_key: str) -> bool:
        path = self.resolve_path(storage_key)
        if not os.path.exists(path):
            return False
        os.remove(path)
        self._prune_empty_dirs(os.path.dirname(path))
        return True

    def stage_delete(self, storage_key: str) -> Optional[str]:
        path = self.resolve_path(storage_key)
        if not os.path.exists(path):
            return None
        rel_key = _normalize_relative_path(storage_key)
        trash_key = "/".join(
            [
                PROJECT_ASSETS_DIRNAME,
                PROJECT_ASSETS_TRASH_DIRNAME,
                str(int(time.time() * 1000)),
                uuid.uuid4().hex[:8],
                rel_key,
            ]
        )
        target_path = self.resolve_path(trash_key)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        os.replace(path, target_path)
        self._prune_empty_dirs(os.path.dirname(path))
        return trash_key

    def move_atomic(self, source_path: str, storage_key: str) -> str:
        target_path = self.resolve_path(storage_key)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        os.replace(source_path, target_path)
        return target_path

    def healthcheck(self) -> Dict[str, Any]:
        os.makedirs(self.root_dir, exist_ok=True)
        writable = os.access(self.root_dir, os.W_OK)
        exists = os.path.exists(self.root_dir)
        total_bytes = None
        free_bytes = None
        free_space_ok = False
        try:
            usage = shutil.disk_usage(self.root_dir)
            total_bytes = usage.total
            free_bytes = usage.free
            free_space_ok = usage.free >= LOCAL_STORAGE_MIN_FREE_BYTES
        except Exception:
            free_space_ok = False
        temp_dir = tempfile.gettempdir()
        temp_ok = False
        temp_probe = None
        try:
            fd, temp_probe = tempfile.mkstemp(dir=temp_dir)
            os.close(fd)
            temp_ok = True
        except Exception:
            temp_ok = False
        finally:
            if temp_probe and os.path.exists(temp_probe):
                os.remove(temp_probe)
        return {
            "root": self.root_dir,
            "exists": exists,
            "writable": writable,
            "totalBytes": total_bytes,
            "freeBytes": free_bytes,
            "minFreeBytes": LOCAL_STORAGE_MIN_FREE_BYTES,
            "freeSpaceOk": free_space_ok,
            "tempDir": {
                "path": temp_dir,
                "available": temp_ok,
            },
            "healthy": exists and writable and free_space_ok and temp_ok,
        }

    def clear(self) -> None:
        if os.path.exists(self.root_dir):
            shutil.rmtree(self.root_dir, ignore_errors=True)
        os.makedirs(self.root_dir, exist_ok=True)

    def _prune_empty_dirs(self, path: str) -> None:
        current = os.path.abspath(path)
        while current.startswith(self.root_dir) and current != self.root_dir:
            if os.path.isdir(current) and not os.listdir(current):
                os.rmdir(current)
                current = os.path.dirname(current)
                continue
            break


local_file_backend = LocalFileBackend()

class SessionStorage:
    def __init__(self):
        self.sessions_dir = load_sessions_dir()
        os.makedirs(self.sessions_dir, exist_ok=True)

    def set_sessions_dir(self, sessions_dir: str):
        self.sessions_dir = sessions_dir
        os.makedirs(self.sessions_dir, exist_ok=True)

    def clear(self):
        """Remove all session data (used by tests for isolation)."""
        if os.path.exists(self.sessions_dir):
            for name in os.listdir(self.sessions_dir):
                path = os.path.join(self.sessions_dir, name)
                try:
                    if os.path.isdir(path):
                        shutil.rmtree(path)
                    else:
                        os.remove(path)
                except Exception:
                    pass
        else:
            os.makedirs(self.sessions_dir, exist_ok=True)
        local_file_backend.clear()

    def _get_session_path(self, session_id: str) -> str:
        return os.path.join(self.sessions_dir, session_id)

    def _get_db_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), "database.db")

    def _get_datasets_dir(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), DATASETS_DIRNAME)

    def _get_datasets_index_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), DATASETS_INDEX)

    def _get_sql_history_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), SQL_HISTORY_FILE)

    def _get_import_history_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), IMPORT_HISTORY_FILE)

    def _get_schema_overrides_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), "schema_overrides.json")

    def _load_schema_overrides(self, session_id: str) -> Dict[str, Dict]:
        path = self._get_schema_overrides_path(session_id)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
            except Exception:
                pass
        return {}

    def _derive_table_name(self, name: str) -> str:
        base_name = os.path.splitext(name or "")[0]
        table_name = base_name or "uploaded_table"
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please choose another name.")
        return table_name

    def save_dataset_field_types(self, session_id: str, dataset_id: str, field_types: Dict):
        self.create_session(session_id)
        overrides = self._load_schema_overrides(session_id)
        overrides[dataset_id] = field_types or {}
        with open(self._get_schema_overrides_path(session_id), "w") as f:
            json.dump(overrides, f, indent=2)

    def create_session(self, session_id: str):
        path = self._get_session_path(session_id)
        if not os.path.exists(path):
            os.makedirs(path)
            # Initialize default metadata
            self.save_session_metadata(session_id, {
                "displayName": "",
                "settings": {
                    "cascadeDisable": False
                }
            })
        os.makedirs(self._get_datasets_dir(session_id), exist_ok=True)
        datasets_index = self._get_datasets_index_path(session_id)
        if not os.path.exists(datasets_index):
            with open(datasets_index, "w") as f:
                json.dump([], f, indent=2)
        sql_history = self._get_sql_history_path(session_id)
        if not os.path.exists(sql_history):
            with open(sql_history, "w") as f:
                json.dump([], f, indent=2)
        import_history = self._get_import_history_path(session_id)
        if not os.path.exists(import_history):
            with open(import_history, "w") as f:
                json.dump([], f, indent=2)

    def _escape_sql_literal(self, value: str) -> str:
        return value.replace("'", "''")

    def _load_datasets_index(self, session_id: str) -> List[Dict[str, Any]]:
        path = self._get_datasets_index_path(session_id)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
        return []

    def _load_import_history(self, session_id: str) -> List[Dict[str, Any]]:
        path = self._get_import_history_path(session_id)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
        return []

    def append_import_history(self, session_id: str, record: Dict[str, Any]):
        self.create_session(session_id)
        history = self._load_import_history(session_id)
        history.append(record)
        with open(self._get_import_history_path(session_id), "w") as f:
            json.dump(history, f, indent=2)

    def get_import_history(self, session_id: str) -> List[Dict[str, Any]]:
        return self._load_import_history(session_id)

    def _save_datasets_index(self, session_id: str, datasets: List[Dict[str, Any]]):
        self.create_session(session_id)
        # Avoid storing preview rows in index
        sanitized = []
        for ds in datasets:
            if not isinstance(ds, dict):
                continue
            cleaned = {k: v for k, v in ds.items() if k != "rows"}
            sanitized.append(cleaned)
        path = self._get_datasets_index_path(session_id)
        with open(path, "w") as f:
            json.dump(sanitized, f, indent=2)

    def _resolve_dataset_file_path(self, session_id: str, dataset_entry: Dict[str, Any]) -> Optional[str]:
        storage_key = dataset_entry.get("storageKey")
        if storage_key:
            try:
                return local_file_backend.resolve_path(storage_key)
            except Exception:
                return None
        rel_path = dataset_entry.get("file")
        if not rel_path:
            return None
        if os.path.isabs(rel_path):
            return rel_path
        return os.path.join(self._get_session_path(session_id), rel_path)

    def _remove_dataset_file(self, session_id: str, dataset_entry: Dict[str, Any]) -> None:
        storage_key = dataset_entry.get("storageKey")
        if storage_key:
            try:
                local_file_backend.delete(storage_key)
                return
            except Exception:
                pass
        file_path = self._resolve_dataset_file_path(session_id, dataset_entry)
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

    def _build_dataset_index_entry(
        self,
        session_id: str,
        table_name: str,
        df: pd.DataFrame,
        *,
        file_path: str,
        storage_key: Optional[str] = None,
        created_at: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        resolved_file_path = file_path if os.path.isabs(file_path) else os.path.join(self._get_session_path(session_id), file_path)
        payload: Dict[str, Any] = {
            "id": table_name,
            "name": table_name,
            "file": file_path,
            "fields": df.columns.tolist(),
            "totalCount": len(df),
            "createdAt": created_at or int(time.time() * 1000),
            "format": "parquet",
            "fileSize": os.path.getsize(resolved_file_path) if os.path.exists(resolved_file_path) else None,
        }
        if storage_key:
            payload["storageKey"] = storage_key
        if extra:
            payload.update(extra)
        return payload

    def _ensure_duckdb_views(self, session_id: str):
        datasets = self._load_datasets_index(session_id)
        if not datasets:
            return
        db_path = self._get_db_path(session_id)
        con = duckdb.connect(db_path)
        try:
            for ds in datasets:
                table_name = ds.get("id") or ds.get("name")
                file_path = self._resolve_dataset_file_path(session_id, ds)
                if not table_name or not file_path or not os.path.exists(file_path):
                    continue
                safe_path = self._escape_sql_literal(file_path)
                table_sql = quote_identifier(table_name)
                con.execute(f"CREATE OR REPLACE VIEW {table_sql} AS SELECT * FROM read_parquet('{safe_path}')")
        finally:
            con.close()

    def sync_runtime_datasets(self, session_id: str, datasets: List[Dict[str, Any]]) -> None:
        self.create_session(session_id)
        next_index: List[Dict[str, Any]] = []
        next_overrides: Dict[str, Dict] = {}
        for ds in datasets or []:
            table_name = ds.get("tableName") or ds.get("id") or ds.get("name")
            storage_key = ds.get("storageKey")
            if not table_name or not storage_key:
                continue
            file_path = local_file_backend.resolve_path(storage_key)
            schema_json = ds.get("schema")
            if not isinstance(schema_json, dict):
                schema_json = {}
            fields = ds.get("fields")
            if not isinstance(fields, list):
                fields = list(schema_json.keys())
            next_index.append(
                {
                    "id": table_name,
                    "name": ds.get("name") or table_name,
                    "file": file_path,
                    "storageKey": storage_key,
                    "fields": fields,
                    "fieldTypes": schema_json,
                    "totalCount": ds.get("rows"),
                    "createdAt": ds.get("createdAt"),
                    "datasetVersion": ds.get("datasetVersion"),
                    "status": ds.get("status"),
                    "format": ds.get("format") or "parquet",
                    "fileSize": ds.get("fileSize"),
                    "assetId": ds.get("id"),
                }
            )
            if schema_json:
                next_overrides[table_name] = schema_json
        self._save_datasets_index(session_id, next_index)
        if next_overrides:
            with open(self._get_schema_overrides_path(session_id), "w") as f:
                json.dump(next_overrides, f, indent=2)
        self._ensure_duckdb_views(session_id)

    def get_session_metadata(self, session_id: str) -> Dict:
        path = os.path.join(self._get_session_path(session_id), "metadata.json")
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except:
                pass
        return {"displayName": "", "settings": {"cascadeDisable": False}}

    def save_session_metadata(self, session_id: str, metadata: Dict):
        path = os.path.join(self._get_session_path(session_id), "metadata.json")
        # Merge with existing to prevent data loss if partial update
        current = self.get_session_metadata(session_id)
        current.update(metadata)
        
        with open(path, "w") as f:
            json.dump(current, f, indent=2)

    def list_sessions(self) -> List[Dict]:
        sessions = []
        if os.path.exists(self.sessions_dir):
            for name in os.listdir(self.sessions_dir):
                path = os.path.join(self.sessions_dir, name)
                if os.path.isdir(path):
                    # Get metadata for display name
                    meta = self.get_session_metadata(name)
                    
                    sessions.append({
                        "sessionId": name,
                        "displayName": meta.get("displayName", ""),
                        "createdAt": os.path.getctime(path) * 1000 # Convert to ms for JS
                    })
        # Sort by newest
        sessions.sort(key=lambda x: x["createdAt"], reverse=True)
        return sessions

    def delete_session(self, session_id: str):
        for ds in self._load_datasets_index(session_id):
            self._remove_dataset_file(session_id, ds)
        path = self._get_session_path(session_id)
        if os.path.exists(path):
            shutil.rmtree(path)

    def add_dataset(
        self,
        session_id: str,
        name: str,
        df: pd.DataFrame,
        *,
        storage_key: Optional[str] = None,
        extra_index_fields: Optional[Dict[str, Any]] = None,
    ):
        self.create_session(session_id)
        db_path = self._get_db_path(session_id)
        table_name = self._derive_table_name(name)

        datasets_index = self._load_datasets_index(session_id)
        existing = next((d for d in datasets_index if d.get("id") == table_name), None)

        if storage_key:
            dataset_file_path = local_file_backend.resolve_path(storage_key)
            os.makedirs(os.path.dirname(dataset_file_path), exist_ok=True)
        else:
            datasets_dir = self._get_datasets_dir(session_id)
            os.makedirs(datasets_dir, exist_ok=True)
            dataset_file = f"ds_{uuid.uuid4().hex[:12]}.parquet"
            dataset_file_path = os.path.join(datasets_dir, dataset_file)

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(dataset_file_path))
        os.close(tmp_fd)

        con = duckdb.connect(db_path)
        replace_succeeded = False
        try:
            # Register dataframe, write parquet, and create a view
            con.register('temp_df', df)
            safe_path = self._escape_sql_literal(tmp_path)
            con.execute(f"COPY temp_df TO '{safe_path}' (FORMAT PARQUET)")
            if storage_key:
                local_file_backend.move_atomic(tmp_path, storage_key)
            else:
                os.replace(tmp_path, dataset_file_path)
            table_sql = quote_identifier(table_name)
            con.execute(f"DROP VIEW IF EXISTS {table_sql}")
            con.execute(f"DROP TABLE IF EXISTS {table_sql}")
            final_safe_path = self._escape_sql_literal(dataset_file_path)
            con.execute(f"CREATE OR REPLACE VIEW {table_sql} AS SELECT * FROM read_parquet('{final_safe_path}')")
            replace_succeeded = True
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            con.close()

        # Update dataset index
        datasets_index = [d for d in datasets_index if d.get("id") != table_name]
        index_entry = self._build_dataset_index_entry(
            session_id,
            table_name,
            df,
            file_path=dataset_file_path if storage_key else os.path.join(DATASETS_DIRNAME, os.path.basename(dataset_file_path)),
            storage_key=storage_key,
            extra=extra_index_fields,
        )
        datasets_index.append(index_entry)
        self._save_datasets_index(session_id, datasets_index)
        if replace_succeeded and existing and not existing.get("storageKey"):
            self._remove_dataset_file(session_id, existing)

        return table_name

    def get_dataset_entry(self, session_id: str, dataset_id: str) -> Optional[Dict[str, Any]]:
        datasets_index = self._load_datasets_index(session_id)
        return next(
            (
                d for d in datasets_index
                if d.get("id") == dataset_id or d.get("name") == dataset_id or d.get("assetId") == dataset_id
            ),
            None,
        )

    def list_datasets(self, session_id: str) -> List[Dict]:
        datasets_index = self._load_datasets_index(session_id)
        schema_overrides = self._load_schema_overrides(session_id)

        if datasets_index:
            result = []
            for ds in datasets_index:
                table_name = ds.get("id") or ds.get("name")
                if not table_name:
                    continue
                fields = ds.get("fields") or []
                total_count = ds.get("totalCount")
                result.append({
                    "id": table_name,
                    "name": ds.get("name") or table_name,
                    "rows": [],
                    "fields": fields,
                    "fieldTypes": ds.get("fieldTypes") or schema_overrides.get(table_name),
                    "totalCount": total_count,
                    "format": ds.get("format"),
                    "fileSize": ds.get("fileSize"),
                    "storageKey": ds.get("storageKey"),
                    "datasetVersion": ds.get("datasetVersion"),
                    "status": ds.get("status"),
                    "assetId": ds.get("assetId"),
                })
            return result

        # Legacy fallback: list from DuckDB if index is missing
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
            return []

        con = duckdb.connect(db_path)
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            result = []
            for t in tables:
                t_name = t[0]
                table_sql = quote_identifier(t_name)
                count = con.execute(f"SELECT count(*) FROM {table_sql}").fetchone()[0]
                cols = con.execute(f"DESCRIBE {table_sql}").fetchall()
                fields = [c[0] for c in cols]

                result.append({
                    "id": t_name,
                    "name": t_name,
                    "rows": [],
                    "fields": fields,
                    "fieldTypes": schema_overrides.get(t_name),
                    "totalCount": count
                })
            return result
        except Exception as e:
            print(f"Error listing datasets: {e}")
            return []
        finally:
            con.close()

    def get_dataset_preview(self, session_id: str, table_name: str, limit: int = 50) -> Optional[pd.DataFrame]:
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please rename or re-import.")

        self._ensure_duckdb_views(session_id)
        db_path = self._get_db_path(session_id)
        con = duckdb.connect(db_path)
        try:
            table_sql = quote_identifier(table_name)
            df = con.execute(f"SELECT * FROM {table_sql} LIMIT {limit}").df()
            return df
        except:
            return None
        finally:
            con.close()

    def execute_sql(self, session_id: str, query: str) -> pd.DataFrame:
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
             raise ValueError("Session database not found")
        self._ensure_duckdb_views(session_id)
        con = duckdb.connect(db_path)
        try:
            df = con.execute(query).df()
            return df
        finally:
            con.close()
            
    def get_full_dataset(self, session_id: str, table_name: str) -> Optional[pd.DataFrame]:
        # Used by engine to load into pandas for complex ops
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please rename or re-import.")
        self._ensure_duckdb_views(session_id)
        db_path = self._get_db_path(session_id)
        con = duckdb.connect(db_path)
        try:
            table_sql = quote_identifier(table_name)
            return con.execute(f"SELECT * FROM {table_sql}").df()
        except:
            return None
        finally:
            con.close()
            
    def save_session_state(self, session_id: str, state: Dict):
        path = self._get_session_path(session_id)
        if not os.path.exists(path):
            self.create_session(session_id)

        tree = state.get("tree")
        sql_history = state.get("sqlHistory")

        # Persist tree (and any non-dataset/non-sql fields)
        state_payload: Dict[str, Any] = {}
        if os.path.exists(os.path.join(path, "state.json")):
            try:
                with open(os.path.join(path, "state.json"), "r") as f:
                    state_payload = json.load(f) or {}
            except Exception:
                state_payload = {}
        if tree is not None:
            state_payload["tree"] = tree

        # Preserve any extra keys except datasets/sqlHistory
        for key, value in state.items():
            if key in ("tree", "datasets", "sqlHistory"):
                continue
            state_payload[key] = value

        with open(os.path.join(path, "state.json"), "w") as f:
            json.dump(state_payload, f, indent=2)

        # Persist SQL history separately
        if sql_history is not None:
            with open(self._get_sql_history_path(session_id), "w") as f:
                json.dump(sql_history, f, indent=2)

    def get_session_state(self, session_id: str) -> Optional[Dict]:
        state_path = os.path.join(self._get_session_path(session_id), "state.json")
        state: Dict[str, Any] = {}
        if os.path.exists(state_path):
            with open(state_path, "r") as f:
                state = json.load(f) or {}

        sql_history_path = self._get_sql_history_path(session_id)
        if os.path.exists(sql_history_path):
            try:
                with open(sql_history_path, "r") as f:
                    state["sqlHistory"] = json.load(f) or []
            except Exception:
                state["sqlHistory"] = []

        # Always attach datasets from index/legacy fallback
        state["datasets"] = self.list_datasets(session_id)

        return state

    def delete_dataset(self, session_id: str, dataset_id: str, *, delete_file: bool = True) -> bool:
        datasets_index = self._load_datasets_index(session_id)
        target = next((d for d in datasets_index if d.get("id") == dataset_id or d.get("name") == dataset_id), None)
        removed = False

        if target:
            if delete_file:
                self._remove_dataset_file(session_id, target)
            datasets_index = [d for d in datasets_index if d.get("id") != target.get("id")]
            self._save_datasets_index(session_id, datasets_index)
            removed = True

        # Remove schema overrides
        overrides = self._load_schema_overrides(session_id)
        if dataset_id in overrides:
            overrides.pop(dataset_id, None)
            with open(self._get_schema_overrides_path(session_id), "w") as f:
                json.dump(overrides, f, indent=2)

        # Drop table/view in DuckDB if present
        db_path = self._get_db_path(session_id)
        if os.path.exists(db_path):
            con = duckdb.connect(db_path)
            try:
                # Check existence for legacy datasets not in index
                if not removed:
                    try:
                        tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
                        if dataset_id in tables:
                            removed = True
                    except Exception:
                        pass
                dataset_sql = quote_identifier(dataset_id)
                con.execute(f"DROP VIEW IF EXISTS {dataset_sql}")
                con.execute(f"DROP TABLE IF EXISTS {dataset_sql}")
            finally:
                con.close()

        return removed

storage = SessionStorage()
