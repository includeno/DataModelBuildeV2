from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from main import app

DEFAULT_OUTPUT = Path(__file__).with_name("openapi.generated.json")


def build_openapi_schema() -> Dict[str, Any]:
    return app.openapi()


def write_openapi_schema(output_path: Optional[str] = None) -> Path:
    target = Path(output_path) if output_path else DEFAULT_OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    schema = build_openapi_schema()
    target.write_text(json.dumps(schema, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return target


if __name__ == "__main__":
    write_openapi_schema()
