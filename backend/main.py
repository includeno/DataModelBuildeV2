from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import uuid
from pathlib import Path
import re
import json
import math
import logging

from .models import ExecuteRequest
from .storage import storage
from .engine import ExecutionEngine

app = FastAPI()
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = ExecutionEngine()

SESSION_FALLBACK_ID = "default"

def _sanitize_filename(filename: str) -> str:
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    return safe_name or "dataset.csv"

def _build_session_path(session_id: str, dataset_id: str, filename: str) -> Path:
    safe_filename = _sanitize_filename(filename)
    return storage.get_session_dir(session_id) / f"{dataset_id}__{safe_filename}"

def _normalize_records(df: pd.DataFrame, limit: int):
    non_finite_tokens = {
        "nan",
        "nan(ind)",
        "inf",
        "+inf",
        "-inf",
        "infinity",
        "+infinity",
        "-infinity",
    }

    def _sanitize_value(value):
        if isinstance(value, str) and value.strip().lower() in non_finite_tokens:
            return None
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        if isinstance(value, (float, np.floating)) and not math.isfinite(float(value)):
            return None
        if pd.isna(value):
            return None
        return value

    df_preview = df.head(limit).copy()
    df_preview = df_preview.apply(lambda series: series.map(_sanitize_value))
    records = df_preview.to_dict(orient='records')
    return json.dumps(records, ensure_ascii=False, allow_nan=False)

def _session_metadata():
    sessions = []
    for session_id in storage.list_sessions():
        datasets = []
        for record in storage.get_dataset_records(session_id):
            datasets.append({
                "id": record.dataset_id,
                "name": record.name,
                "fields": record.df.columns.tolist(),
                "totalCount": int(len(record.df))
            })
        sessions.append({"sessionId": session_id, "datasets": datasets})
    return sessions

@app.get("/sessions")
async def list_sessions():
    return {"sessions": _session_metadata()}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), session_id: str = Form(SESSION_FALLBACK_ID)):
    try:
        content = await file.read()
        session_id = session_id or SESSION_FALLBACK_ID
        dataset_id = str(uuid.uuid4())
        file_path = _build_session_path(session_id, dataset_id, file.filename)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
        try:
            df = pd.read_csv(file_path)
        except Exception:
            file_path.unlink(missing_ok=True)
            logger.warning("Failed to parse CSV upload", exc_info=True)
            return {"error": "Could not parse CSV"}
            
        name = file.filename
        
        storage.add_dataset(session_id, dataset_id, name, df, file_path)
        
        return {
            "id": dataset_id,
            "sessionId": session_id,
            "name": name,
            "fields": df.columns.tolist(),
            "rows": _normalize_records(df, 50),
            "totalCount": int(len(df))
        }
    except Exception as e:
        logger.exception("Upload failed", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute")
async def execute(req: ExecuteRequest):
    try:
        session_id = req.sessionId or SESSION_FALLBACK_ID
        df = engine.execute(req.tree, req.targetNodeId, session_id)
        
        return {
            "rows": _normalize_records(df, 100),
            "totalCount": int(len(df))
        }
    except Exception as e:
        logger.exception("Execution failed", exc_info=e)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
