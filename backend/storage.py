
import pandas as pd
import duckdb
import os
import shutil
import re
import json
import time
import uuid
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
        rel_path = dataset_entry.get("file")
        if not rel_path:
            return None
        if os.path.isabs(rel_path):
            return rel_path
        return os.path.join(self._get_session_path(session_id), rel_path)

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
        path = self._get_session_path(session_id)
        if os.path.exists(path):
            shutil.rmtree(path)

    def add_dataset(self, session_id: str, name: str, df: pd.DataFrame):
        self.create_session(session_id)
        db_path = self._get_db_path(session_id)
        
        # Sanitize table name: remove extension, replace bad chars, ensure starts with letter
        base_name = os.path.splitext(name)[0]
        # Replace non-alphanumeric chars with underscore
        # Preserve user-provided name for SQL usage (identifiers will be quoted when needed)
        table_name = base_name or "uploaded_table"
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please choose another name.")
        
        datasets_dir = self._get_datasets_dir(session_id)
        os.makedirs(datasets_dir, exist_ok=True)
        dataset_file = f"ds_{uuid.uuid4().hex[:12]}.parquet"
        dataset_file_path = os.path.join(datasets_dir, dataset_file)

        # Remove previous file if replacing an existing dataset
        datasets_index = self._load_datasets_index(session_id)
        existing = next((d for d in datasets_index if d.get("id") == table_name), None)
        if existing:
            old_path = self._resolve_dataset_file_path(session_id, existing)
            if old_path and os.path.exists(old_path):
                try:
                    os.remove(old_path)
                except Exception:
                    pass

        con = duckdb.connect(db_path)
        try:
            # Register dataframe, write parquet, and create a view
            con.register('temp_df', df)
            safe_path = self._escape_sql_literal(dataset_file_path)
            con.execute(f"COPY temp_df TO '{safe_path}' (FORMAT PARQUET)")
            table_sql = quote_identifier(table_name)
            con.execute(f"DROP VIEW IF EXISTS {table_sql}")
            con.execute(f"DROP TABLE IF EXISTS {table_sql}")
            con.execute(f"CREATE OR REPLACE VIEW {table_sql} AS SELECT * FROM read_parquet('{safe_path}')")
        finally:
            con.close()

        # Update dataset index
        datasets_index = [d for d in datasets_index if d.get("id") != table_name]
        datasets_index.append({
            "id": table_name,
            "name": table_name,
            "file": os.path.join(DATASETS_DIRNAME, dataset_file),
            "fields": df.columns.tolist(),
            "totalCount": len(df),
            "createdAt": int(time.time() * 1000)
        })
        self._save_datasets_index(session_id, datasets_index)

        return table_name

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
                    "fieldTypes": schema_overrides.get(table_name),
                    "totalCount": total_count
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
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
            return None
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please rename or re-import.")

        self._ensure_duckdb_views(session_id)
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
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
            return None
        if is_reserved_identifier(table_name):
            raise ValueError(f"Dataset name '{table_name}' is a reserved keyword. Please rename or re-import.")
        self._ensure_duckdb_views(session_id)
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

    def delete_dataset(self, session_id: str, dataset_id: str) -> bool:
        datasets_index = self._load_datasets_index(session_id)
        target = next((d for d in datasets_index if d.get("id") == dataset_id or d.get("name") == dataset_id), None)
        removed = False

        if target:
            file_path = self._resolve_dataset_file_path(session_id, target)
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
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
