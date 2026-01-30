
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Body
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import io
import uuid
from typing import List, Optional, Dict

from models import ExecuteRequest, ExecuteSqlRequest, AnalyzeRequest
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
    
    # Preprocessing to handle NaNs (restored as requested)
    df = df.where(pd.notnull(df), None)
    
    # Final cleanup and serialization
    return df.replace({np.nan: None}).to_dict(orient='records')

def paginate_df(df: pd.DataFrame, page: int, page_size: int) -> pd.DataFrame:
    start = (page - 1) * page_size
    end = start + page_size
    return df.iloc[start:end]

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
        print(f"Execution Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/export")
async def export_data(req: ExecuteRequest):
    try:
        # Execute logic to get full dataframe (ignoring pagination params in req for fetching, 
        # but using them if we wanted to slice. Here we want full.)
        df = engine.execute(req.session_id, req.tree, req.targetNodeId)
        
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=export_full.csv"
        return response
    except Exception as e:
        print(f"Export Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze")
async def analyze_overlap(req: AnalyzeRequest):
    try:
        report = engine.calculate_overlap(req.session_id, req.tree, req.parentNodeId)
        return {"report": report}
    except Exception as e:
        print(f"Analysis Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def execute_sql(req: ExecuteSqlRequest):
    try:
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
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
