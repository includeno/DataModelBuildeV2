from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Body
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import io
import uuid
from typing import List, Optional

from models import ExecuteRequest, ExecuteSqlRequest
from storage import storage
from engine import ExecutionEngine

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = ExecutionEngine()

def clean_df_for_json(df: pd.DataFrame) -> List[dict]:
    """
    Replace NaN, Infinity, -Infinity with None for valid JSON serialization.
    """
    # Replace infinite values with NaN
    df = df.replace([np.inf, -np.inf], np.nan)
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient='records')

@app.get("/sessions")
async def list_sessions():
    return storage.list_sessions()

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
        else:
             return {"error": "Unsupported file format. Please upload CSV or Excel."}
            
        # Clean col names
        df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]
        
        # Determine dataset name
        dataset_name = name if name and name.strip() else (file.filename or "uploaded_file")
        
        table_name = storage.add_dataset(sessionId, dataset_name, df)
        
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
        print(f"Upload error: {e}")
        return {"error": str(e)}

@app.post("/execute")
async def execute(req: ExecuteRequest):
    try:
        df = engine.execute(req.session_id, req.tree, req.targetNodeId)
        
        clean_rows = clean_df_for_json(df.head(100))
        
        return {
            "rows": clean_rows,
            "totalCount": len(df)
        }
    except Exception as e:
        print(f"Execution Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def execute_sql(req: ExecuteSqlRequest):
    try:
        df = storage.execute_sql(req.session_id, req.query)
        clean_rows = clean_df_for_json(df)
        return {
            "rows": clean_rows,
            "totalCount": len(df),
            "columns": df.columns.tolist()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)