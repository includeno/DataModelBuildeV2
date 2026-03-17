
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Body, Depends, Header, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import io
import uuid
import os
import json
import tempfile
import duckdb
import time
import logging
import re
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

from models import (
    ExecuteRequest,
    ExecuteSqlRequest,
    AnalyzeRequest,
    OperationNode,
    RegisterRequest,
    LoginRequest,
    CreateProjectRequest,
    CreateOrganizationRequest,
    AddOrganizationMemberRequest,
    UpdateOrganizationMemberRequest,
    AddProjectMemberRequest,
    UpdateProjectMemberRequest,
    CommitProjectStateRequest,
    RefreshTokenRequest,
)
import storage as storage_module
from storage import storage, resolve_data_subdir, to_data_relative, save_sessions_dir, PROJECT_ASSETS_DIRNAME, local_file_backend
from engine import ExecutionEngine
from collab_storage import collab_storage
from realtime import realtime_hub

LOG_PATH = os.environ.get(
    "BACKEND_LOG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "logs", "backend.log")
)
Path(os.path.dirname(LOG_PATH)).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()]
)
logger = logging.getLogger("backend")

MAX_UPLOAD_SIZE_BYTES = int(os.environ.get("MAX_UPLOAD_SIZE_BYTES", str(64 * 1024 * 1024)))
ALLOWED_UPLOAD_EXTENSIONS = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".parquet": "parquet",
    ".pq": "parquet",
}
VALID_DUPLICATE_STRATEGIES = {"replace", "new_name"}
SAFE_DATASET_NAME_RE = re.compile(r"[^\w.\- ]+", re.UNICODE)

app = FastAPI()


def _load_cors_origins() -> List[str]:
    raw = (os.environ.get("BACKEND_CORS_ORIGINS") or "").strip()
    if raw:
        origins = [item.strip() for item in raw.split(",") if item.strip()]
        if origins:
            return origins
    # Safe defaults for local development with credentials.
    return [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:1420",
        "http://localhost:1420",
        "http://127.0.0.1:4173",
        "http://localhost:4173",
    ]


_cors_origins = _load_cors_origins()
logger.info("CORS allow_origins=%s", _cors_origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = ExecutionEngine()

DEFAULT_SERVER_FILE = os.path.join(os.path.dirname(__file__), "default_server.json")


def _is_auth_enabled() -> bool:
    raw = (
        os.environ.get("BACKEND_AUTH_ENABLED")
        or os.environ.get("BACKEND_AUTH_REQUIRED")
        or "1"
    )
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def _ensure_auth_enabled() -> None:
    if _is_auth_enabled():
        return
    raise HTTPException(
        status_code=404,
        detail={"code": "AUTH_DISABLED", "message": "Authentication endpoints are disabled"},
    )

def load_default_server() -> str:
    if not os.path.exists(DEFAULT_SERVER_FILE):
        return "mockServer"
    try:
        with open(DEFAULT_SERVER_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, str):
            value = data.strip()
        elif isinstance(data, dict):
            value = str(data.get("server") or data.get("defaultServer") or data.get("baseUrl") or "").strip()
        else:
            value = ""
        if value.lower() in ("mock", "mockserver"):
            return "mockServer"
        return value or "mockServer"
    except Exception:
        return "mockServer"

def clean_df_for_json(df: pd.DataFrame) -> List[dict]:
    """
    Replace NaN, Infinity, -Infinity with None for valid JSON serialization.
    """
    # Replace infinite values with NaN
    df = df.copy()
    num_cols = df.select_dtypes(include=[np.number]).columns
    if len(num_cols) > 0:
        df[num_cols] = df[num_cols].replace([np.inf, -np.inf], np.nan)
    
    # Preprocessing to handle NaNs (restored as requested)
    df = df.where(pd.notnull(df), None)
    
    # Final cleanup and serialization
    return df.replace({np.nan: None}).to_dict(orient='records')


def _walk_sources(node: Dict, sources: List[Dict]) -> None:
    for cmd in node.get("commands") or []:
        if cmd.get("type") == "source":
            sources.append(cmd)
    for child in node.get("children") or []:
        _walk_sources(child, sources)


def _walk_operations(node: Dict, operations: List[Dict]) -> None:
    if node.get("type") == "operation":
        commands = []
        for cmd in node.get("commands") or []:
            cfg = cmd.get("config") or {}
            commands.append({
                "id": cmd.get("id"),
                "type": cmd.get("type"),
                "order": cmd.get("order", 0),
                "dataSource": cfg.get("dataSource")
            })
        operations.append({
            "id": node.get("id"),
            "name": node.get("name"),
            "operationType": node.get("operationType"),
            "commands": commands
        })
    for child in node.get("children") or []:
        _walk_operations(child, operations)

def paginate_df(df: pd.DataFrame, page: int, page_size: int) -> pd.DataFrame:
    start = (page - 1) * page_size
    end = start + page_size
    return df.iloc[start:end]


def _extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    raw = authorization.strip()
    if not raw:
        return None
    if raw.lower().startswith("bearer "):
        return raw[7:].strip() or None
    return raw


def _extract_ws_token(websocket: WebSocket) -> Optional[str]:
    token = websocket.query_params.get("token")
    if token:
        return token.strip() or None
    auth_header = websocket.headers.get("authorization")
    return _extract_bearer_token(auth_header)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def get_current_user(authorization: Optional[str] = Header(None)) -> Dict[str, Any]:
    if not _is_auth_enabled():
        return {
            "id": "usr_auth_disabled",
            "email": "auth-disabled@local",
            "displayName": "Auth Disabled",
            "authDisabled": True,
        }
    token = _extract_bearer_token(authorization)
    user = collab_storage.get_user_by_token(token or "")
    if not user:
        raise HTTPException(status_code=401, detail={"code": "AUTH_UNAUTHORIZED", "message": "Unauthorized"})
    return user


def _require_project_access(project_id: str, user_id: str, need_write: bool = False, need_manage: bool = False) -> Dict[str, Any]:
    project = collab_storage.get_project_for_user(project_id, user_id)
    if not project:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": "Project not found"})
    role = str(project.get("role") or "")
    if need_manage and role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_MANAGE", "message": "Insufficient project permission"})
    if need_write and role not in ("owner", "admin", "editor"):
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_WRITE", "message": "Insufficient project permission"})
    return project


def _infer_schema_field(dtype: Any) -> Dict[str, Any]:
    raw = str(dtype).lower()
    if "int" in raw or "float" in raw or "double" in raw or "decimal" in raw:
        return {"type": "number"}
    if "bool" in raw:
        return {"type": "boolean"}
    if "date" in raw or "time" in raw:
        return {"type": "date"}
    if "json" in raw or "dict" in raw or "struct" in raw:
        return {"type": "json"}
    return {"type": "string"}


def _build_dataframe_schema(df: pd.DataFrame) -> Dict[str, Any]:
    return {str(col): _infer_schema_field(dtype) for col, dtype in df.dtypes.items()}


def _build_project_storage_key(project_id: str, asset_id: str, dataset_version: int) -> str:
    return (
        f"{PROJECT_ASSETS_DIRNAME}/projects/{project_id}/datasets/"
        f"{asset_id}/v{int(dataset_version)}.parquet"
    )


def _normalize_duplicate_strategy(raw: Optional[str]) -> str:
    strategy = (raw or "replace").strip().lower() or "replace"
    if strategy not in VALID_DUPLICATE_STRATEGIES:
        raise ValueError("duplicateStrategy must be one of: replace, new_name")
    return strategy


def _normalize_upload_filename(filename: Optional[str]) -> str:
    raw = (filename or "").strip()
    if not raw:
        raise ValueError("Uploaded file must include a filename")
    if "/" in raw or "\\" in raw or "\x00" in raw:
        raise ValueError("Invalid filename")
    return os.path.basename(raw)


def _normalize_dataset_name(raw_name: Optional[str], fallback_name: str) -> str:
    raw = (raw_name or "").strip() or fallback_name
    if "/" in raw or "\\" in raw or "\x00" in raw:
        raise ValueError("Invalid dataset name")
    collapsed = re.sub(r"\s+", " ", raw).strip()
    cleaned = SAFE_DATASET_NAME_RE.sub("_", collapsed).strip("._ ")
    if not cleaned:
        raise ValueError("Dataset name is required")
    return cleaned[:120]


def _build_unique_project_dataset_name(project_id: str, user_id: str, dataset_name: str) -> str:
    active_assets = collab_storage.list_project_dataset_assets(project_id, user_id)
    existing_tables = {
        str(item.get("tableName") or item.get("name") or item.get("id") or "").strip()
        for item in active_assets
    }
    base_table_name = storage._derive_table_name(dataset_name)
    if base_table_name not in existing_tables:
        return dataset_name
    suffix = 2
    while True:
        candidate = f"{base_table_name}_{suffix}"
        if candidate not in existing_tables:
            return candidate
        suffix += 1


def _merge_project_dataset_payload(
    runtime_datasets: List[Dict[str, Any]],
    asset_datasets: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    asset_by_table = {
        str(item.get("tableName") or item.get("name") or item.get("id")): item
        for item in asset_datasets or []
    }
    merged: List[Dict[str, Any]] = []
    seen_tables = set()

    for ds in runtime_datasets or []:
        table_name = str(ds.get("id") or ds.get("name") or "").strip()
        if not table_name:
            continue
        asset = asset_by_table.get(table_name)
        payload = dict(ds)
        if asset:
            payload.update(
                {
                    "assetId": asset.get("id"),
                    "storageKey": asset.get("storageKey"),
                    "datasetVersion": asset.get("datasetVersion"),
                    "status": asset.get("status"),
                    "format": asset.get("format"),
                    "fileSize": asset.get("fileSize"),
                    "createdAt": asset.get("createdAt") or payload.get("createdAt"),
                    "fieldTypes": asset.get("schema") or payload.get("fieldTypes"),
                    "totalCount": asset.get("rows") if asset.get("rows") is not None else payload.get("totalCount"),
                }
            )
        merged.append(payload)
        seen_tables.add(table_name)

    for asset in asset_datasets or []:
        table_name = str(asset.get("tableName") or asset.get("name") or asset.get("id") or "").strip()
        if not table_name or table_name in seen_tables:
            continue
        merged.append(
            {
                "id": table_name,
                "name": asset.get("name") or table_name,
                "rows": [],
                "fields": asset.get("fields") or [],
                "fieldTypes": asset.get("schema") or {},
                "totalCount": asset.get("rows"),
                "assetId": asset.get("id"),
                "storageKey": asset.get("storageKey"),
                "datasetVersion": asset.get("datasetVersion"),
                "status": asset.get("status"),
                "format": asset.get("format"),
                "fileSize": asset.get("fileSize"),
                "createdAt": asset.get("createdAt"),
            }
        )

    return merged


def _ensure_project_runtime_session(project_id: str) -> None:
    # Keep runtime engine compatibility by mapping project_id to session storage namespace.
    storage.create_session(project_id)
    dataset_assets = collab_storage.list_active_project_dataset_assets(project_id)
    if dataset_assets:
        storage.sync_runtime_datasets(project_id, dataset_assets)


# ---- Phase 1: local SQLite collaboration/auth APIs ----

@app.post("/auth/register")
async def register(payload: RegisterRequest):
    _ensure_auth_enabled()
    try:
        user = collab_storage.register_user(payload.email, payload.password, payload.displayName or "")
        return {"user": user}
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "AUTH_REGISTER_INVALID", "message": str(e)})


@app.post("/auth/login")
async def login(payload: LoginRequest):
    _ensure_auth_enabled()
    user = collab_storage.authenticate_user(payload.email, payload.password)
    if not user:
        raise HTTPException(status_code=401, detail={"code": "AUTH_INVALID_CREDENTIALS", "message": "Invalid email or password"})
    tokens = collab_storage.issue_auth_tokens(user["id"])
    return {**tokens, "user": user}


@app.post("/auth/refresh")
async def refresh_token(payload: RefreshTokenRequest):
    _ensure_auth_enabled()
    try:
        return collab_storage.refresh_access_token(payload.refreshToken)
    except ValueError as e:
        raise HTTPException(status_code=401, detail={"code": "AUTH_REFRESH_INVALID", "message": str(e)})


@app.post("/auth/logout")
async def logout(current_user: Dict[str, Any] = Depends(get_current_user), authorization: Optional[str] = Header(None)):
    _ensure_auth_enabled()
    token = _extract_bearer_token(authorization)
    if token:
        collab_storage.revoke_token(token)
    return {"status": "ok", "userId": current_user["id"]}


@app.get("/auth/me")
async def get_auth_me(current_user: Dict[str, Any] = Depends(get_current_user)):
    _ensure_auth_enabled()
    return current_user


@app.post("/organizations")
async def create_organization(payload: CreateOrganizationRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    try:
        return collab_storage.create_organization(current_user["id"], payload.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "ORG_CREATE_INVALID", "message": str(e)})


@app.get("/organizations")
async def list_organizations(current_user: Dict[str, Any] = Depends(get_current_user)):
    return collab_storage.list_organizations_for_user(current_user["id"])


@app.get("/organizations/{organization_id}")
async def get_organization(organization_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    org = collab_storage.get_organization_for_user(organization_id, current_user["id"])
    if not org:
        raise HTTPException(status_code=404, detail={"code": "ORG_NOT_FOUND", "message": "Organization not found"})
    return org


@app.get("/organizations/{organization_id}/members")
async def list_organization_members(organization_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    try:
        return collab_storage.list_organization_members(organization_id, current_user["id"])
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_ORG_READ", "message": "Insufficient organization permission"})


@app.post("/organizations/{organization_id}/members")
async def add_organization_member(
    organization_id: str,
    payload: AddOrganizationMemberRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        return collab_storage.add_organization_member(
            organization_id,
            current_user["id"],
            payload.memberEmail,
            payload.role,
        )
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_ORG_MANAGE", "message": "Insufficient organization permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "ORG_MEMBER_INVALID", "message": str(e)})


@app.patch("/organizations/{organization_id}/members/{member_user_id}")
async def update_organization_member(
    organization_id: str,
    member_user_id: str,
    payload: UpdateOrganizationMemberRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        return collab_storage.update_organization_member_role(
            organization_id,
            current_user["id"],
            member_user_id,
            payload.role,
        )
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_ORG_MANAGE", "message": "Insufficient organization permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "ORG_MEMBER_INVALID", "message": str(e)})


@app.post("/projects")
async def create_project(payload: CreateProjectRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    try:
        project = collab_storage.create_project(
            current_user["id"],
            payload.name,
            payload.description or "",
            payload.orgId,
        )
        _ensure_project_runtime_session(project["id"])
        return project
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_ORG_WRITE", "message": "Insufficient organization permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "PROJECT_CREATE_INVALID", "message": str(e)})


@app.get("/projects")
async def list_projects(current_user: Dict[str, Any] = Depends(get_current_user)):
    return collab_storage.list_projects_for_user(current_user["id"])


@app.get("/projects/query")
async def query_projects(
    page: int = Query(1),
    page_size: int = Query(20, alias="pageSize"),
    search: str = Query(""),
    include_archived: bool = Query(False, alias="includeArchived"),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    return collab_storage.search_projects_for_user(
        current_user["id"],
        page=page,
        page_size=page_size,
        search=search,
        include_archived=include_archived,
    )


@app.get("/projects/{project_id}")
async def get_project(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    project = collab_storage.get_project_for_user(project_id, current_user["id"])
    if not project:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": "Project not found"})
    return project


@app.post("/projects/{project_id}/archive")
async def archive_project(project_id: str, payload: dict = Body(...), current_user: Dict[str, Any] = Depends(get_current_user)):
    archived = bool(payload.get("archived", True))
    try:
        return collab_storage.archive_project(project_id, current_user["id"], archived)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_MANAGE", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": str(e)})


@app.delete("/projects/{project_id}")
async def delete_project(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    try:
        collab_storage.soft_delete_project(project_id, current_user["id"])
        storage.delete_session(project_id)
        return {"status": "ok"}
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_MANAGE", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_NOT_FOUND", "message": str(e)})


@app.get("/projects/{project_id}/members")
async def list_project_members(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"])
    try:
        return collab_storage.list_project_members(project_id, current_user["id"])
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_READ", "message": "Insufficient project permission"})


@app.post("/projects/{project_id}/members")
async def add_project_member(project_id: str, payload: AddProjectMemberRequest, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"], need_manage=True)
    try:
        return collab_storage.add_project_member(project_id, current_user["id"], payload.memberEmail, payload.role)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_MANAGE", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "PROJECT_MEMBER_INVALID", "message": str(e)})


@app.patch("/projects/{project_id}/members/{member_user_id}")
async def update_project_member(
    project_id: str,
    member_user_id: str,
    payload: UpdateProjectMemberRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"], need_manage=True)
    try:
        return collab_storage.update_project_member_role(project_id, current_user["id"], member_user_id, payload.role)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_MANAGE", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "PROJECT_MEMBER_INVALID", "message": str(e)})


@app.get("/projects/{project_id}/state")
async def get_project_state(
    project_id: str,
    since_version: Optional[int] = Query(None, alias="sinceVersion"),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    try:
        if since_version is not None:
            return collab_storage.get_project_state_since(project_id, current_user["id"], since_version)
        return collab_storage.get_project_state(project_id, current_user["id"])
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_READ", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=404, detail={"code": "PROJECT_STATE_NOT_FOUND", "message": str(e)})


@app.post("/projects/{project_id}/state/commit")
async def commit_project_state(
    project_id: str,
    payload: CommitProjectStateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"], need_write=True)

    raw_patches: Optional[List[Dict[str, Any]]] = None
    if payload.patches is not None:
        raw_patches = []
        for patch in payload.patches:
            if hasattr(patch, "model_dump"):
                raw_patches.append(patch.model_dump(exclude_none=True))
            else:
                raw_patches.append(patch.dict(exclude_none=True))

    try:
        result = collab_storage.commit_project_state(
            project_id=project_id,
            user_id=current_user["id"],
            base_version=payload.baseVersion,
            state=payload.state,
            client_op_id=payload.clientOpId,
            patches=raw_patches,
        )
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_WRITE", "message": "Insufficient project permission"})
    except ValueError as e:
        raise HTTPException(status_code=400, detail={"code": "PROJECT_COMMIT_INVALID", "message": str(e)})

    if result.get("conflict"):
        raise HTTPException(status_code=409, detail={"code": "PROJECT_STATE_CONFLICT", "message": "Version conflict", "data": result})

    broadcast_patches = raw_patches
    if broadcast_patches is None:
        broadcast_patches = [{"op": "replace_state", "state": result.get("state") or {}}]
    await realtime_hub.broadcast_state_committed(
        project_id,
        version=int(result.get("version") or 0),
        payload={
            "baseVersion": payload.baseVersion,
            "clientOpId": payload.clientOpId,
            "patches": broadcast_patches,
            "state": result.get("state") or {},
            "updatedBy": current_user["id"],
            "updatedAt": result.get("updatedAt"),
        },
    )
    return result


@app.get("/projects/{project_id}/events")
async def list_project_events(
    project_id: str,
    from_version: int = Query(0, alias="fromVersion"),
    limit: int = Query(100),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    try:
        return collab_storage.list_project_events(project_id, current_user["id"], from_version=from_version, limit=limit)
    except PermissionError:
        raise HTTPException(status_code=403, detail={"code": "PERM_PROJECT_READ", "message": "Insufficient project permission"})


@app.websocket("/ws/projects/{project_id}")
async def project_realtime_socket(websocket: WebSocket, project_id: str):
    token = _extract_ws_token(websocket)
    user = collab_storage.get_user_by_token(token or "")
    if not user:
        await websocket.close(code=4401, reason="unauthorized")
        return

    try:
        project = _require_project_access(project_id, user["id"])
    except HTTPException as exc:
        close_code = 4404 if exc.status_code == 404 else 4403
        await websocket.close(code=close_code, reason=str(exc.detail.get("message") if isinstance(exc.detail, dict) else exc.detail))
        return

    ctx = await realtime_hub.accept_connection(websocket, project_id, user, str(project.get("role") or "viewer"))
    try:
        while True:
            raw_message = await websocket.receive_text()
            try:
                message = json.loads(raw_message or "{}")
            except json.JSONDecodeError:
                await realtime_hub.send_conflict_notice(
                    ctx,
                    ctx.last_seen_version,
                    "Invalid realtime payload.",
                )
                continue

            message_type = str(message.get("type") or "").strip().lower()

            if message_type == "subscribe":
                requested_project_id = str(message.get("projectId") or project_id).strip() or project_id
                if requested_project_id != project_id:
                    await realtime_hub.send_conflict_notice(
                        ctx,
                        ctx.last_seen_version,
                        "Project mismatch in realtime subscribe payload.",
                    )
                    continue

                client_version = max(_to_int(message.get("clientVersion"), 0), 0)
                if message.get("sessionId") is not None:
                    ctx.session_id = str(message.get("sessionId") or "").strip() or None
                ctx.last_pong_at = int(time.time() * 1000)
                events = collab_storage.list_project_events(
                    project_id,
                    user["id"],
                    from_version=client_version,
                    limit=500,
                )
                latest_version = _to_int(events.get("latestVersion"), 0)
                if client_version > latest_version:
                    await realtime_hub.send_conflict_notice(
                        ctx,
                        latest_version,
                        "Client version is ahead of server state; resyncing to latest.",
                    )
                    client_version = latest_version
                ctx.last_seen_version = client_version
                await realtime_hub.subscribe(ctx, latest_version, events.get("events") or [])
                continue

            if message_type == "pong":
                realtime_hub.mark_pong(ctx, _to_int(message.get("lastSeenVersion"), ctx.last_seen_version))
                continue

            if message_type == "ack_version":
                realtime_hub.acknowledge_version(ctx, _to_int(message.get("version"), ctx.last_seen_version))
                continue

            if message_type == "presence_update":
                incoming_session_id = str(message.get("sessionId") or "").strip() or None
                if ctx.session_id and incoming_session_id and incoming_session_id != ctx.session_id:
                    await realtime_hub.send_conflict_notice(
                        ctx,
                        ctx.last_seen_version,
                        "Realtime session mismatch.",
                    )
                    continue
                await realtime_hub.update_presence(
                    ctx,
                    editing_node_id=message.get("editingNodeId"),
                    last_seen_version=_to_int(message.get("lastSeenVersion"), ctx.last_seen_version),
                )
                continue

            await realtime_hub.send_conflict_notice(
                ctx,
                ctx.last_seen_version,
                f"Unsupported realtime message type: {message_type or 'unknown'}",
            )
    except WebSocketDisconnect:
        logger.info("project realtime disconnected project=%s user=%s", project_id, user["id"])
    finally:
        await realtime_hub.disconnect(ctx)


# ---- Project-scoped runtime endpoints (replacement for /sessions/*) ----

@app.get("/projects/{project_id}/datasets")
async def project_list_datasets(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    runtime_datasets = storage.list_datasets(project_id)
    asset_datasets = collab_storage.list_project_dataset_assets(project_id, current_user["id"])
    return _merge_project_dataset_payload(runtime_datasets, asset_datasets)


@app.get("/projects/{project_id}/imports")
async def project_list_imports(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    return storage.get_import_history(project_id)


@app.get("/projects/{project_id}/datasets/{dataset_id}/preview")
async def project_dataset_preview(
    project_id: str,
    dataset_id: str,
    limit: int = 50,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    asset = collab_storage.get_project_dataset_asset(project_id, current_user["id"], dataset_id)
    runtime_dataset_id = asset["tableName"] if asset else dataset_id
    df = storage.get_dataset_preview(project_id, runtime_dataset_id, limit=limit)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"rows": clean_df_for_json(df), "totalCount": len(df)}


@app.delete("/projects/{project_id}/datasets/{dataset_id}")
async def project_delete_dataset(project_id: str, dataset_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"], need_write=True)
    _ensure_project_runtime_session(project_id)
    asset = collab_storage.get_project_dataset_asset(project_id, current_user["id"], dataset_id)
    runtime_dataset_id = asset["tableName"] if asset else dataset_id
    removed = storage.delete_dataset(project_id, runtime_dataset_id, delete_file=not bool(asset))
    if asset:
        collab_storage.soft_delete_project_dataset_asset(project_id, current_user["id"], dataset_id)
    elif not removed:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"status": "ok"}


@app.post("/projects/{project_id}/datasets/update")
async def project_update_dataset_schema(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"], need_write=True)
    _ensure_project_runtime_session(project_id)
    dataset_id = payload.get("datasetId")
    field_types = payload.get("fieldTypes")
    if not dataset_id:
        raise HTTPException(status_code=400, detail="datasetId is required")
    if field_types is None:
        raise HTTPException(status_code=400, detail="fieldTypes is required")
    asset = collab_storage.get_project_dataset_asset(project_id, current_user["id"], dataset_id)
    runtime_dataset_id = asset["tableName"] if asset else dataset_id
    storage.save_dataset_field_types(project_id, runtime_dataset_id, field_types)
    if asset:
        collab_storage.update_project_dataset_schema(project_id, current_user["id"], dataset_id, field_types)
    return {"status": "ok"}


@app.get("/projects/{project_id}/metadata")
async def project_get_metadata(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    return storage.get_session_metadata(project_id)


@app.post("/projects/{project_id}/metadata")
async def project_update_metadata(
    project_id: str,
    metadata: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"], need_write=True)
    _ensure_project_runtime_session(project_id)
    storage.save_session_metadata(project_id, metadata)
    return {"status": "ok"}


@app.get("/projects/{project_id}/diagnostics")
async def project_diagnostics(project_id: str, current_user: Dict[str, Any] = Depends(get_current_user)):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    return await get_session_diagnostics(project_id)


@app.post("/projects/{project_id}/upload")
async def project_upload_file(
    project_id: str,
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
    duplicateStrategy: str = Form("replace"),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"], need_write=True)
    _ensure_project_runtime_session(project_id)
    pending_asset_id: Optional[str] = None
    runtime_dataset_id: Optional[str] = None
    try:
        df, _source_format, normalized_filename = await _read_uploaded_dataframe(file)
        df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
        duplicate_strategy = _normalize_duplicate_strategy(duplicateStrategy)
        requested_name = _normalize_dataset_name(name, normalized_filename)
        dataset_name = requested_name
        if duplicate_strategy == "new_name":
            dataset_name = _build_unique_project_dataset_name(project_id, current_user["id"], requested_name)
        table_name = storage._derive_table_name(dataset_name)
        current_asset = collab_storage.get_project_dataset_asset(project_id, current_user["id"], table_name)
        next_version = int(current_asset["datasetVersion"]) + 1 if current_asset and duplicate_strategy == "replace" else 1
        asset_id = f"ast_{uuid.uuid4().hex[:12]}"
        storage_key = _build_project_storage_key(project_id, asset_id, next_version)
        schema = _build_dataframe_schema(df)
        pending_asset = collab_storage.prepare_project_dataset_asset(
            project_id,
            current_user["id"],
            name=dataset_name,
            table_name=table_name,
            storage_key=storage_key,
            format="parquet",
            dataset_version=next_version,
            asset_id=asset_id,
            status="uploading",
        )
        pending_asset_id = pending_asset["id"]

        runtime_dataset_id = storage.add_dataset(
            project_id,
            dataset_name,
            df,
            storage_key=storage_key,
            extra_index_fields={
                "datasetVersion": next_version,
                "status": "uploading",
                "storageKey": storage_key,
                "fieldTypes": schema,
                "assetId": asset_id,
            },
        )
        entry = storage.get_dataset_entry(project_id, runtime_dataset_id) or {}
        file_path = storage._resolve_dataset_file_path(project_id, entry) or ""
        asset = collab_storage.finalize_project_dataset_asset(
            project_id,
            current_user["id"],
            pending_asset_id,
            file_size=os.path.getsize(file_path) if file_path and os.path.exists(file_path) else None,
            rows=len(df),
            schema=schema,
            status="ready",
            replace_existing=duplicate_strategy == "replace",
        )
        entry["assetId"] = asset["id"]
        storage.sync_runtime_datasets(project_id, collab_storage.list_active_project_dataset_assets(project_id))

        storage.append_import_history(project_id, {
            "timestamp": int(time.time() * 1000),
            "originalFileName": normalized_filename,
            "datasetName": dataset_name,
            "tableName": runtime_dataset_id,
            "rows": len(df)
        })

        preview_rows = clean_df_for_json(df.head(50))
        return {
            "id": runtime_dataset_id,
            "assetId": asset["id"],
            "name": runtime_dataset_id,
            "fields": df.columns.tolist(),
            "rows": preview_rows,
            "totalCount": len(df),
            "storageKey": storage_key,
            "datasetVersion": asset["datasetVersion"],
            "status": asset["status"],
            "format": asset["format"],
            "duplicateStrategy": duplicate_strategy,
        }
    except PermissionError:
        if pending_asset_id:
            collab_storage.mark_project_dataset_asset_failed(project_id, current_user["id"], pending_asset_id)
        if runtime_dataset_id:
            storage.delete_dataset(project_id, runtime_dataset_id)
            storage.sync_runtime_datasets(project_id, collab_storage.list_active_project_dataset_assets(project_id))
        raise HTTPException(status_code=403, detail="Insufficient project permission")
    except ValueError as e:
        if pending_asset_id:
            collab_storage.mark_project_dataset_asset_failed(project_id, current_user["id"], pending_asset_id)
        if runtime_dataset_id:
            storage.delete_dataset(project_id, runtime_dataset_id)
            storage.sync_runtime_datasets(project_id, collab_storage.list_active_project_dataset_assets(project_id))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        if pending_asset_id:
            collab_storage.mark_project_dataset_asset_failed(project_id, current_user["id"], pending_asset_id)
        if runtime_dataset_id:
            storage.delete_dataset(project_id, runtime_dataset_id)
            storage.sync_runtime_datasets(project_id, collab_storage.list_active_project_dataset_assets(project_id))
        logger.exception("Project upload error")
        raise HTTPException(status_code=500, detail="Project upload failed")


@app.post("/projects/{project_id}/execute")
async def project_execute(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    body = dict(payload or {})
    body["sessionId"] = project_id
    req = ExecuteRequest(**body)
    return await execute(req)


@app.post("/projects/{project_id}/export")
async def project_export(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    body = dict(payload or {})
    body["sessionId"] = project_id
    req = ExecuteRequest(**body)
    return await export_data(req)


@app.post("/projects/{project_id}/generate_sql")
async def project_generate_sql(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    body = dict(payload or {})
    body["sessionId"] = project_id
    req = ExecuteRequest(**body)
    return await generate_sql(req)


@app.post("/projects/{project_id}/analyze")
async def project_analyze(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    body = dict(payload or {})
    body["sessionId"] = project_id
    req = AnalyzeRequest(**body)
    return await analyze_overlap(req)


@app.post("/projects/{project_id}/query")
async def project_query(
    project_id: str,
    payload: dict = Body(...),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    _require_project_access(project_id, current_user["id"])
    _ensure_project_runtime_session(project_id)
    body = dict(payload or {})
    body["sessionId"] = project_id
    req = ExecuteSqlRequest(**body)
    return await execute_sql(req)

@app.get("/sessions")
async def list_sessions():
    return storage.list_sessions()

@app.get("/config/default_server")
async def get_default_server():
    server = load_default_server()
    auth_enabled = _is_auth_enabled()
    return {
        "server": server,
        "isMock": server == "mockServer",
        "authEnabled": auth_enabled,
        "authRequired": auth_enabled,
    }


@app.get("/config/auth")
async def get_auth_config():
    auth_enabled = _is_auth_enabled()
    return {
        "authEnabled": auth_enabled,
        "authRequired": auth_enabled,
        "mode": "required" if auth_enabled else "disabled",
    }

@app.get("/config/session_storage")
async def get_session_storage():
    return {
        "dataRoot": storage_module.DATA_ROOT,
        "sessionsDir": storage.sessions_dir,
        "relative": to_data_relative(storage.sessions_dir)
    }


@app.get("/config/storage_health")
async def get_storage_health():
    return local_file_backend.healthcheck()

@app.get("/config/session_storage/list")
async def list_session_storage(path: str = ""):
    try:
        target = resolve_data_subdir(path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    folders = []
    if os.path.exists(target):
        for name in os.listdir(target):
            full = os.path.join(target, name)
            if os.path.isdir(full):
                rel = to_data_relative(full)
                folders.append({"name": name, "path": rel})
    folders.sort(key=lambda x: x["name"].lower())
    return {"path": to_data_relative(target), "folders": folders}

@app.post("/config/session_storage/create")
async def create_session_storage(payload: dict = Body(...)):
    rel_path = payload.get("path") or payload.get("name") or ""
    try:
        target = resolve_data_subdir(rel_path)
        os.makedirs(target, exist_ok=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"path": to_data_relative(target)}

@app.post("/config/session_storage/select")
async def select_session_storage(payload: dict = Body(...)):
    rel_path = payload.get("path") or ""
    try:
        target = resolve_data_subdir(rel_path)
        if not os.path.exists(target):
            raise HTTPException(status_code=404, detail="Folder not found")
        storage.set_sessions_dir(target)
        save_sessions_dir(target)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "dataRoot": storage_module.DATA_ROOT,
        "sessionsDir": storage.sessions_dir,
        "relative": to_data_relative(storage.sessions_dir)
    }

@app.post("/sessions")
async def create_session():
    new_id = f"sess_{uuid.uuid4().hex[:8]}"
    storage.create_session(new_id)
    return {"sessionId": new_id}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    storage.delete_session(session_id)
    return {"status": "ok"}

@app.get("/sessions/{session_id}/datasets")
async def list_datasets(session_id: str):
    return storage.list_datasets(session_id)

@app.get("/sessions/{session_id}/imports")
async def list_imports(session_id: str):
    return storage.get_import_history(session_id)

@app.get("/sessions/{session_id}/datasets/{dataset_id}/preview")
async def get_dataset_preview(session_id: str, dataset_id: str, limit: int = 50):
    df = storage.get_dataset_preview(session_id, dataset_id, limit=limit)
    if df is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {
        "rows": clean_df_for_json(df),
        "totalCount": len(df)
    }

@app.delete("/sessions/{session_id}/datasets/{dataset_id}")
async def delete_dataset(session_id: str, dataset_id: str):
    removed = storage.delete_dataset(session_id, dataset_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {"status": "ok"}

@app.post("/sessions/{session_id}/datasets/update")
async def update_dataset_schema(session_id: str, payload: dict = Body(...)):
    dataset_id = payload.get("datasetId")
    field_types = payload.get("fieldTypes")
    if not dataset_id:
        raise HTTPException(status_code=400, detail="datasetId is required")
    if field_types is None:
        raise HTTPException(status_code=400, detail="fieldTypes is required")

    storage.save_dataset_field_types(session_id, dataset_id, field_types)
    return {"status": "ok"}

@app.get("/sessions/{session_id}/state")
async def get_session_state(session_id: str):
    state = storage.get_session_state(session_id)
    return state or {}

@app.post("/sessions/{session_id}/state")
async def save_session_state(session_id: str, state: dict = Body(...)):
    storage.save_session_state(session_id, state)
    return {"status": "ok"}

@app.get("/sessions/{session_id}/metadata")
async def get_session_metadata(session_id: str):
    return storage.get_session_metadata(session_id)

@app.post("/sessions/{session_id}/metadata")
async def update_session_metadata(session_id: str, metadata: dict = Body(...)):
    storage.save_session_metadata(session_id, metadata)
    return {"status": "ok"}

@app.get("/sessions/{session_id}/diagnostics")
async def get_session_diagnostics(session_id: str):
    state = storage.get_session_state(session_id) or {}
    tree = state.get("tree")
    datasets = storage.list_datasets(session_id)

    report = {
        "sessionId": session_id,
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources": [],
        "sourceMap": [],
        "datasets": [],
        "operations": [],
        "dataSourceResolution": [],
        "warnings": []
    }

    report["datasets"] = [
        {
            "id": d.get("id"),
            "name": d.get("name"),
            "totalCount": d.get("totalCount"),
            "fieldCount": len(d.get("fields") or [])
        }
        for d in datasets
    ]

    if not tree:
        report["warnings"].append("No tree found in session state.")
        return report

    sources: List[Dict] = []
    _walk_sources(tree, sources)
    report["sources"] = [
        {
            "id": s.get("id"),
            "mainTable": (s.get("config") or {}).get("mainTable"),
            "alias": (s.get("config") or {}).get("alias"),
            "linkId": (s.get("config") or {}).get("linkId"),
            "note": (s.get("config") or {}).get("note")
        }
        for s in sources
    ]

    operations: List[Dict] = []
    _walk_operations(tree, operations)
    report["operations"] = operations

    try:
        node = OperationNode(**tree)
        _, source_map, _ = engine._collect_setup_sources(node)
    except Exception as e:
        report["warnings"].append(f"Failed to parse tree: {e}")
        source_map = {}
        node = None

    report["sourceMap"] = [
        {"identifier": k, "table": v}
        for k, v in sorted(source_map.items(), key=lambda item: item[0])
    ]

    dataset_names = set()
    for ds in datasets:
        if ds.get("name"):
            dataset_names.add(ds.get("name"))
        if ds.get("id"):
            dataset_names.add(ds.get("id"))

    missing_sources = []
    for op in operations:
        op_name = op.get("name") or op.get("id") or "unknown-operation"
        for cmd in op.get("commands") or []:
            cmd_type = cmd.get("type") or "unknown"
            if cmd_type in ("source", "multi_table"):
                continue
            data_source = cmd.get("dataSource")
            if data_source is None:
                missing_sources.append((cmd.get("id"), cmd_type, op_name))
                continue
            if isinstance(data_source, str) and data_source.strip() == "":
                missing_sources.append((cmd.get("id"), cmd_type, op_name))
                continue
            if data_source == "stream":
                continue
            resolved = None
            if node is not None:
                resolved = engine._resolve_table_from_link_id(node, data_source)
            if not resolved:
                resolved = source_map.get(data_source, data_source)
            status = "ok" if resolved in dataset_names else "missing"
            report["dataSourceResolution"].append({
                "commandId": cmd.get("id"),
                "dataSource": data_source,
                "resolved": resolved,
                "status": status
            })
            if status == "missing":
                report["warnings"].append(
                    f"Command {cmd.get('id')} ({cmd_type}) in operation {op_name} "
                    f"references dataSource '{data_source}' → '{resolved}', but dataset not found."
                )

    for cmd_id, cmd_type, op_name in missing_sources:
        report["warnings"].append(
            f"Missing data source: command {cmd_id} ({cmd_type}) in operation {op_name}."
        )

    if not report["sources"]:
        report["warnings"].append("No source commands found.")
    if not report["datasets"]:
        report["warnings"].append("No datasets found in storage.")
    report["storageHealth"] = local_file_backend.healthcheck()

    return report


async def _read_uploaded_dataframe(file: UploadFile) -> Tuple[pd.DataFrame, str, str]:
    content = await file.read()
    normalized_filename = _normalize_upload_filename(file.filename)
    filename = normalized_filename.lower()
    _, extension = os.path.splitext(filename)
    if extension not in ALLOWED_UPLOAD_EXTENSIONS:
        raise ValueError("Unsupported file format. Please upload CSV, Excel, or Parquet.")
    if len(content) > MAX_UPLOAD_SIZE_BYTES:
        raise ValueError(f"File is too large. Max size is {MAX_UPLOAD_SIZE_BYTES} bytes.")

    if filename.endswith('.csv'):
        try:
            return pd.read_csv(io.BytesIO(content)), "csv", normalized_filename
        except Exception:
            raise ValueError("Could not parse CSV")
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        try:
            return pd.read_excel(io.BytesIO(content)), "excel", normalized_filename
        except Exception:
            raise ValueError("Could not parse Excel file")
    if filename.endswith('.parquet') or filename.endswith('.pq'):
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            con = duckdb.connect(":memory:")
            try:
                escaped = tmp_path.replace("'", "''")
                return con.execute(f"SELECT * FROM read_parquet('{escaped}')").df(), "parquet", normalized_filename
            finally:
                con.close()
        except Exception as e:
            raise ValueError(f"Could not parse Parquet file: {e}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    raise ValueError("Unsupported file format. Please upload CSV, Excel, or Parquet.")

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...), 
    sessionId: str = Form(...),
    name: Optional[str] = Form(None)
):
    try:
        df, _source_format, normalized_filename = await _read_uploaded_dataframe(file)
            
        # Clean col names
        df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
        
        # Determine dataset name
        dataset_name = _normalize_dataset_name(name, normalized_filename)
        
        table_name = storage.add_dataset(sessionId, dataset_name, df)

        storage.append_import_history(sessionId, {
            "timestamp": int(time.time() * 1000),
            "originalFileName": normalized_filename,
            "datasetName": dataset_name,
            "tableName": table_name,
            "rows": len(df)
        })
        
        # Get preview
        preview_rows = clean_df_for_json(df.head(50))
        
        return {
            "id": table_name,
            "name": table_name,
            "fields": df.columns.tolist(),
            "rows": preview_rows,
            "totalCount": len(df)
        }
    except Exception as e:
        logger.exception("Upload error")
        return {"error": str(e)}

@app.post("/execute")
async def execute(req: ExecuteRequest):
    try:
        logger.info(
            "execute session=%s node=%s view=%s cmd=%s page=%s pageSize=%s",
            req.session_id,
            req.targetNodeId,
            req.viewId,
            req.targetCommandId,
            req.page,
            req.pageSize,
        )
        # Pass viewId and targetCommandId to engine
        df = engine.execute(req.session_id, req.tree, req.targetNodeId, req.viewId, req.targetCommandId)
        
        total_count = len(df)
        paginated_df = paginate_df(df, req.page, req.pageSize)
        clean_rows = clean_df_for_json(paginated_df)
        
        return {
            "rows": clean_rows,
            "totalCount": total_count,
            "columns": df.columns.tolist(),
            "page": req.page,
            "pageSize": req.pageSize,
            "activeViewId": req.viewId
        }
    except Exception as e:
        logger.exception("Execution Error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export")
async def export_data(req: ExecuteRequest):
    try:
        # Execute logic to get full dataframe 
        df = engine.execute(req.session_id, req.tree, req.targetNodeId, req.viewId, req.targetCommandId)
        
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=export_full.csv"
        return response
    except Exception as e:
        logger.exception("Export Error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate_sql")
async def generate_sql(req: ExecuteRequest):
    try:
        if not req.targetCommandId:
            raise HTTPException(status_code=400, detail="targetCommandId is required")
        
        sql = engine.generate_sql(
            req.session_id,
            req.tree,
            req.targetNodeId,
            req.targetCommandId,
            False,
        )
        return {"sql": sql}
    except HTTPException as e:
        # Preserve explicit HTTP errors (e.g., missing targetCommandId)
        raise e
    except Exception as e:
        logger.exception("SQL Generation Error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze")
async def analyze_overlap(req: AnalyzeRequest):
    try:
        report = engine.calculate_overlap(req.session_id, req.tree, req.parentNodeId)
        return {"report": report}
    except Exception as e:
        logger.exception("Analysis Error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def execute_sql(req: ExecuteSqlRequest):
    try:
        logger.info(
            "query session=%s page=%s pageSize=%s query=%s",
            req.session_id,
            req.page,
            req.pageSize,
            (req.query or "")[:500],
        )
        df = storage.execute_sql(req.session_id, req.query)
        
        total_count = len(df)
        paginated_df = paginate_df(df, req.page, req.pageSize)
        clean_rows = clean_df_for_json(paginated_df)
        
        return {
            "rows": clean_rows,
            "totalCount": total_count,
            "columns": df.columns.tolist(),
            "page": req.page,
            "pageSize": req.pageSize
        }
    except Exception as e:
        logger.exception("SQL Query Error")
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
