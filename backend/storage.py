import pandas as pd
from typing import Dict, Optional

class DatasetStorage:
    def __init__(self):
        self._datasets: Dict[str, pd.DataFrame] = {}

    def add_dataset(self, name: str, df: pd.DataFrame) -> str:
        # Simple unique ID generation or use name as ID for simplicity in this demo
        # In a real app, generate a UUID and map it
        self._datasets[name] = df
        return name

    def get_dataset(self, name: str) -> Optional[pd.DataFrame]:
        return self._datasets.get(name)

    def list_datasets(self):
        return list(self._datasets.keys())

    def clear(self):
        self._datasets.clear()

# Global instance
storage = DatasetStorage()
