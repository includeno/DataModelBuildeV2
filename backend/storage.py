from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

@dataclass
class DatasetRecord:
    dataset_id: str
    name: str
    path: Path
    df: pd.DataFrame

class DatasetStorage:
    def __init__(self, base_dir: Optional[Path] = None):
        self._datasets: Dict[str, Dict[str, DatasetRecord]] = {}
        self._base_dir = base_dir or Path(__file__).resolve().parent / "data" / "sessions"
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._load_from_disk()

    def add_dataset(self, session_id: str, dataset_id: str, name: str, df: pd.DataFrame, path: Path) -> str:
        session_bucket = self._datasets.setdefault(session_id, {})
        session_bucket[name] = DatasetRecord(dataset_id=dataset_id, name=name, path=path, df=df)
        self._write_manifest(session_id)
        return name

    def get_dataset(self, session_id: str, name: str) -> Optional[pd.DataFrame]:
        session_bucket = self._datasets.get(session_id, {})
        record = session_bucket.get(name)
        return record.df if record else None

    def list_datasets(self, session_id: str) -> List[str]:
        return list(self._datasets.get(session_id, {}).keys())

    def list_sessions(self) -> List[str]:
        return list(self._datasets.keys())

    def get_dataset_records(self, session_id: str) -> List[DatasetRecord]:
        return list(self._datasets.get(session_id, {}).values())

    def clear(self):
        self._datasets.clear()

    def _session_dir(self, session_id: str) -> Path:
        return self._base_dir / session_id

    def get_session_dir(self, session_id: str) -> Path:
        return self._session_dir(session_id)

    def _manifest_path(self, session_id: str) -> Path:
        return self._session_dir(session_id) / "manifest.json"

    def _write_manifest(self, session_id: str) -> None:
        session_dir = self._session_dir(session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        manifest = [
            {
                "id": record.dataset_id,
                "name": record.name,
                "filename": record.path.name,
            }
            for record in self._datasets.get(session_id, {}).values()
        ]
        self._manifest_path(session_id).write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

    def _load_from_disk(self) -> None:
        for session_dir in self._base_dir.iterdir():
            if not session_dir.is_dir():
                continue
            session_id = session_dir.name
            manifest_path = session_dir / "manifest.json"
            records: List[Dict[str, str]] = []
            if manifest_path.exists():
                try:
                    records = json.loads(manifest_path.read_text())
                except json.JSONDecodeError:
                    records = []
            if not records:
                records = []
                for file in session_dir.glob("*.csv"):
                    if "__" in file.name:
                        dataset_id, original_name = file.name.split("__", 1)
                    else:
                        dataset_id = file.stem
                        original_name = file.name
                    records.append({"id": dataset_id, "name": original_name, "filename": file.name})
            for record in records:
                filename = record.get("filename")
                name = record.get("name")
                dataset_id = record.get("id")
                if not filename or not name or not dataset_id:
                    continue
                file_path = session_dir / filename
                if not file_path.exists():
                    continue
                try:
                    df = pd.read_csv(file_path)
                except Exception:
                    continue
                self.add_dataset(session_id, dataset_id, name, df, file_path)

# Global instance
storage = DatasetStorage()
