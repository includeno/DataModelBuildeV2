from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

from security import DEFAULT_LOCAL_CORS_ORIGINS, normalize_server, parse_cors_origins

DEFAULT_SERVER_FILE = Path(__file__).with_name("default_server.json")


@dataclass(frozen=True)
class RuntimeConfig:
    auth_enabled: bool = True
    default_server_file: str = str(DEFAULT_SERVER_FILE)
    default_server_override: Optional[str] = None
    cors_origins: Tuple[str, ...] = DEFAULT_LOCAL_CORS_ORIGINS
    legacy_session_compat_enabled: bool = True
    legacy_session_project_bridge_enabled: bool = True
    deprecation_headers_enabled: bool = True
    auth_login_attempt_limit: int = 500
    auth_login_failure_limit: int = 8
    auth_login_window_seconds: int = 300
    project_commit_rate_limit_count: int = 120
    project_commit_rate_limit_window_seconds: int = 60
    project_execute_rate_limit_count: int = 60
    project_execute_rate_limit_window_seconds: int = 60
    project_upload_rate_limit_count: int = 20
    project_upload_rate_limit_window_seconds: int = 60
    sql_read_only_enforced: bool = True
    unsafe_python_transform_enabled: bool = False
    audit_logs_enabled: bool = True

    @property
    def auth_required(self) -> bool:
        return self.auth_enabled

    @property
    def auth_mode(self) -> str:
        return "required" if self.auth_enabled else "disabled"

    @property
    def default_server(self) -> str:
        if self.default_server_override and str(self.default_server_override).strip():
            return normalize_server(self.default_server_override)
        return _load_default_server_file(self.default_server_file)

    @property
    def is_mock_server(self) -> bool:
        return self.default_server == "mockServer"


# Local development defaults live here so `uvicorn --reload` picks them up when
# this file changes. Environment variables still take precedence.
DEFAULT_RUNTIME_CONFIG = RuntimeConfig()


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


def _parse_int(value: Any, default: int, *, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), parsed)


def _load_default_server_file(path: str) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return "mockServer"
    try:
        with file_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, str):
            return normalize_server(data)
        if isinstance(data, dict):
            return normalize_server(
                data.get("server") or data.get("defaultServer") or data.get("baseUrl") or ""
            )
        return "mockServer"
    except Exception:
        return "mockServer"


def load_runtime_config() -> RuntimeConfig:
    base = DEFAULT_RUNTIME_CONFIG
    auth_raw = os.environ.get("BACKEND_AUTH_ENABLED")
    if auth_raw is None:
        auth_raw = os.environ.get("BACKEND_AUTH_REQUIRED")
    default_server_override = os.environ.get("BACKEND_DEFAULT_SERVER")
    cors_origins_raw = os.environ.get("BACKEND_CORS_ORIGINS")
    legacy_session_compat_raw = os.environ.get("BACKEND_LEGACY_SESSION_COMPAT_ENABLED")
    legacy_session_bridge_raw = os.environ.get("BACKEND_LEGACY_SESSION_PROJECT_BRIDGE_ENABLED")
    deprecation_headers_raw = os.environ.get("BACKEND_DEPRECATION_HEADERS_ENABLED")
    auth_login_attempt_limit_raw = os.environ.get("BACKEND_AUTH_LOGIN_ATTEMPT_LIMIT")
    auth_login_failure_limit_raw = os.environ.get("BACKEND_AUTH_LOGIN_FAILURE_LIMIT")
    auth_login_window_raw = os.environ.get("BACKEND_AUTH_LOGIN_WINDOW_SECONDS")
    project_commit_rate_limit_count_raw = os.environ.get("BACKEND_PROJECT_COMMIT_RATE_LIMIT_COUNT")
    project_commit_rate_limit_window_raw = os.environ.get("BACKEND_PROJECT_COMMIT_RATE_LIMIT_WINDOW_SECONDS")
    project_execute_rate_limit_count_raw = os.environ.get("BACKEND_PROJECT_EXECUTE_RATE_LIMIT_COUNT")
    project_execute_rate_limit_window_raw = os.environ.get("BACKEND_PROJECT_EXECUTE_RATE_LIMIT_WINDOW_SECONDS")
    project_upload_rate_limit_count_raw = os.environ.get("BACKEND_PROJECT_UPLOAD_RATE_LIMIT_COUNT")
    project_upload_rate_limit_window_raw = os.environ.get("BACKEND_PROJECT_UPLOAD_RATE_LIMIT_WINDOW_SECONDS")
    sql_read_only_raw = os.environ.get("BACKEND_SQL_READ_ONLY_ENFORCED")
    unsafe_python_transform_raw = os.environ.get("BACKEND_UNSAFE_PYTHON_TRANSFORM_ENABLED")
    audit_logs_raw = os.environ.get("BACKEND_AUDIT_LOGS_ENABLED")

    return RuntimeConfig(
        auth_enabled=_parse_bool(auth_raw, base.auth_enabled),
        default_server_file=base.default_server_file,
        default_server_override=default_server_override if default_server_override is not None else base.default_server_override,
        cors_origins=parse_cors_origins(cors_origins_raw, base.cors_origins),
        legacy_session_compat_enabled=_parse_bool(legacy_session_compat_raw, base.legacy_session_compat_enabled),
        legacy_session_project_bridge_enabled=_parse_bool(legacy_session_bridge_raw, base.legacy_session_project_bridge_enabled),
        deprecation_headers_enabled=_parse_bool(deprecation_headers_raw, base.deprecation_headers_enabled),
        auth_login_attempt_limit=_parse_int(auth_login_attempt_limit_raw, base.auth_login_attempt_limit),
        auth_login_failure_limit=_parse_int(auth_login_failure_limit_raw, base.auth_login_failure_limit),
        auth_login_window_seconds=_parse_int(auth_login_window_raw, base.auth_login_window_seconds),
        project_commit_rate_limit_count=_parse_int(project_commit_rate_limit_count_raw, base.project_commit_rate_limit_count),
        project_commit_rate_limit_window_seconds=_parse_int(project_commit_rate_limit_window_raw, base.project_commit_rate_limit_window_seconds),
        project_execute_rate_limit_count=_parse_int(project_execute_rate_limit_count_raw, base.project_execute_rate_limit_count),
        project_execute_rate_limit_window_seconds=_parse_int(project_execute_rate_limit_window_raw, base.project_execute_rate_limit_window_seconds),
        project_upload_rate_limit_count=_parse_int(project_upload_rate_limit_count_raw, base.project_upload_rate_limit_count),
        project_upload_rate_limit_window_seconds=_parse_int(project_upload_rate_limit_window_raw, base.project_upload_rate_limit_window_seconds),
        sql_read_only_enforced=_parse_bool(sql_read_only_raw, base.sql_read_only_enforced),
        unsafe_python_transform_enabled=_parse_bool(unsafe_python_transform_raw, base.unsafe_python_transform_enabled),
        audit_logs_enabled=_parse_bool(audit_logs_raw, base.audit_logs_enabled),
    )


def load_default_server(config: Optional[RuntimeConfig] = None) -> str:
    runtime_config = config or load_runtime_config()
    return runtime_config.default_server
