from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

DEFAULT_SERVER_FILE = Path(__file__).with_name("default_server.json")


@dataclass(frozen=True)
class RuntimeConfig:
    auth_enabled: bool = True
    default_server_file: str = str(DEFAULT_SERVER_FILE)
    default_server_override: Optional[str] = None
    legacy_session_compat_enabled: bool = True
    legacy_session_project_bridge_enabled: bool = True
    deprecation_headers_enabled: bool = True

    @property
    def auth_required(self) -> bool:
        return self.auth_enabled

    @property
    def auth_mode(self) -> str:
        return "required" if self.auth_enabled else "disabled"

    @property
    def default_server(self) -> str:
        if self.default_server_override and str(self.default_server_override).strip():
            return _normalize_server(self.default_server_override)
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


def _normalize_server(value: Any) -> str:
    normalized = str(value or "").strip()
    if normalized.lower() in {"mock", "mockserver"}:
        return "mockServer"
    return normalized or "mockServer"


def _load_default_server_file(path: str) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return "mockServer"
    try:
        with file_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, str):
            return _normalize_server(data)
        if isinstance(data, dict):
            return _normalize_server(
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
    legacy_session_compat_raw = os.environ.get("BACKEND_LEGACY_SESSION_COMPAT_ENABLED")
    legacy_session_bridge_raw = os.environ.get("BACKEND_LEGACY_SESSION_PROJECT_BRIDGE_ENABLED")
    deprecation_headers_raw = os.environ.get("BACKEND_DEPRECATION_HEADERS_ENABLED")

    return RuntimeConfig(
        auth_enabled=_parse_bool(auth_raw, base.auth_enabled),
        default_server_file=base.default_server_file,
        default_server_override=default_server_override if default_server_override is not None else base.default_server_override,
        legacy_session_compat_enabled=_parse_bool(legacy_session_compat_raw, base.legacy_session_compat_enabled),
        legacy_session_project_bridge_enabled=_parse_bool(legacy_session_bridge_raw, base.legacy_session_project_bridge_enabled),
        deprecation_headers_enabled=_parse_bool(deprecation_headers_raw, base.deprecation_headers_enabled),
    )


def load_default_server(config: Optional[RuntimeConfig] = None) -> str:
    runtime_config = config or load_runtime_config()
    return runtime_config.default_server
