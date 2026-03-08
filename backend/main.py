
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Body
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
from pathlib import Path
from typing import List, Optional, Dict

from models import ExecuteRequest, ExecuteSqlRequest, AnalyzeRequest
from models import OperationNode
import storage as storage_module
from storage import storage, resolve_data_subdir, to_data_relative, save_sessions_dir
from engine import ExecutionEngine

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

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = ExecutionEngine()

DEFAULT_SERVER_FILE = os.path.join(os.path.dirname(__file__), "default_server.json")

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

@app.get("/sessions")
async def list_sessions():
    return storage.list_sessions()

@app.get("/config/default_server")
async def get_default_server():
    server = load_default_server()
    return {"server": server, "isMock": server == "mockServer"}

@app.get("/config/session_storage")
async def get_session_storage():
    return {
        "dataRoot": storage_module.DATA_ROOT,
        "sessionsDir": storage.sessions_dir,
        "relative": to_data_relative(storage.sessions_dir)
    }

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
            "linkId": (s.get("config") or {}).get("linkId")
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

    return report

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...), 
    sessionId: str = Form(...),
    name: Optional[str] = Form(None)
):
    try:
        content = await file.read()
        filename = file.filename.lower() if file.filename else ""
        
        if filename.endswith('.csv'):
            try:
                df = pd.read_csv(io.BytesIO(content))
            except:
                return {"error": "Could not parse CSV"}
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            try:
                df = pd.read_excel(io.BytesIO(content))
            except:
                return {"error": "Could not parse Excel file"}
        elif filename.endswith('.parquet') or filename.endswith('.pq'):
            tmp_path = None
            try:
                # Use DuckDB to read parquet without extra deps
                with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                    tmp.write(content)
                    tmp_path = tmp.name
                con = duckdb.connect(":memory:")
                try:
                    escaped = tmp_path.replace("'", "''")
                    df = con.execute(f"SELECT * FROM read_parquet('{escaped}')").df()
                finally:
                    con.close()
            except Exception as e:
                return {"error": f"Could not parse Parquet file: {e}"}
            finally:
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
        else:
             return {"error": "Unsupported file format. Please upload CSV, Excel, or Parquet."}
            
        # Clean col names
        df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
        
        # Determine dataset name
        dataset_name = name if name and name.strip() else (file.filename or "uploaded_file")
        
        table_name = storage.add_dataset(sessionId, dataset_name, df)

        storage.append_import_history(sessionId, {
            "timestamp": int(time.time() * 1000),
            "originalFileName": file.filename or "",
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
        
        sql = engine.generate_sql(req.session_id, req.tree, req.targetNodeId, req.targetCommandId)
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
