import pandas as pd
import duckdb
import os
import shutil
import re
from typing import List, Dict, Optional

SESSIONS_DIR = "sessions"

class SessionStorage:
    def __init__(self):
        if not os.path.exists(SESSIONS_DIR):
            os.makedirs(SESSIONS_DIR)

    def _get_session_path(self, session_id: str) -> str:
        return os.path.join(SESSIONS_DIR, session_id)

    def _get_db_path(self, session_id: str) -> str:
        return os.path.join(self._get_session_path(session_id), "database.db")

    def create_session(self, session_id: str):
        path = self._get_session_path(session_id)
        if not os.path.exists(path):
            os.makedirs(path)

    def list_sessions(self) -> List[Dict]:
        sessions = []
        if os.path.exists(SESSIONS_DIR):
            for name in os.listdir(SESSIONS_DIR):
                path = os.path.join(SESSIONS_DIR, name)
                if os.path.isdir(path):
                    # In a real app, read metadata.json for creation time
                    sessions.append({
                        "sessionId": name,
                        "createdAt": os.path.getctime(path) # Returns timestamp
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
        safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
        
        # Ensure it doesn't start with a number or is empty
        if not safe_name:
            table_name = "uploaded_table"
        elif safe_name[0].isdigit():
            table_name = f"t_{safe_name}"
        else:
            table_name = safe_name
        
        con = duckdb.connect(db_path)
        try:
            # Register dataframe and create table
            con.register('temp_df', df)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        finally:
            con.close()
            
        return table_name

    def list_datasets(self, session_id: str) -> List[Dict]:
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
            return []
            
        con = duckdb.connect(db_path)
        try:
            # Get table info
            tables = con.execute("SHOW TABLES").fetchall()
            result = []
            for t in tables:
                t_name = t[0]
                count = con.execute(f"SELECT count(*) FROM {t_name}").fetchone()[0]
                # Get schema
                cols = con.execute(f"DESCRIBE {t_name}").fetchall()
                fields = [c[0] for c in cols]
                
                result.append({
                    "id": t_name, # Use table name as ID for simplicity
                    "name": t_name,
                    "rows": [], # Don't return rows in list to save bandwidth
                    "fields": fields,
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
            
        con = duckdb.connect(db_path)
        try:
            df = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").df()
            return df
        except:
            return None
        finally:
            con.close()

    def execute_sql(self, session_id: str, query: str) -> pd.DataFrame:
        db_path = self._get_db_path(session_id)
        if not os.path.exists(db_path):
             raise ValueError("Session database not found")
             
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
        
        con = duckdb.connect(db_path)
        try:
            return con.execute(f"SELECT * FROM {table_name}").df()
        except:
            return None
        finally:
            con.close()

storage = SessionStorage()