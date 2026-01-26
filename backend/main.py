from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import uuid

from .models import ExecuteRequest
from .storage import storage
from .engine import ExecutionEngine

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = ExecutionEngine()

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        try:
            df = pd.read_csv(io.BytesIO(content))
        except:
            return {"error": "Could not parse CSV"}
            
        ds_id = str(uuid.uuid4())
        name = file.filename
        
        # Replace NaN with None for valid JSON serialization
        df_clean = df.where(pd.notnull(df), None)
        
        storage.add_dataset(name, df)
        
        return {
            "id": ds_id,
            "name": name,
            "fields": df.columns.tolist(),
            "rows": df_clean.head(50).to_dict(orient='records'),
            "totalCount": len(df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/execute")
async def execute(req: ExecuteRequest):
    try:
        df = engine.execute(req.tree, req.targetNodeId)
        
        # Clean up for JSON response
        clean_df = df.where(pd.notnull(df), None)
        
        return {
            "rows": clean_df.head(100).to_dict(orient='records'),
            "totalCount": len(df)
        }
    except Exception as e:
        # In production, log error
        print(f"Execution Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
