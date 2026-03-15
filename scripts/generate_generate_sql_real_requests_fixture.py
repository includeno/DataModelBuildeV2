import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx


BASE_URL = os.environ.get("DMB_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
SOURCE_CASES_PATH = Path("backend/test_fixtures/sql_export_ui_generated_cases.json")
OUTPUT_PATH = Path("backend/test_fixtures/generate_sql_real_requests.json")
ORDERS_CSV_PATH = Path("test_data/orders.csv")

TARGET_CASES = 60


def _load_source_cases() -> List[Dict[str, Any]]:
    if not SOURCE_CASES_PATH.exists():
        raise RuntimeError(f"Missing source fixture: {SOURCE_CASES_PATH}")
    payload = json.loads(SOURCE_CASES_PATH.read_text(encoding="utf-8"))
    cases = payload.get("cases", [])
    if len(cases) < 50:
        raise RuntimeError(f"Need at least 50 source UI cases, got {len(cases)}")
    return cases


def _create_session(client: httpx.Client) -> str:
    res = client.post(f"{BASE_URL}/sessions", timeout=30)
    res.raise_for_status()
    return res.json()["sessionId"]


def _upload_orders(client: httpx.Client, session_id: str) -> None:
    if not ORDERS_CSV_PATH.exists():
        raise RuntimeError(f"Missing CSV: {ORDERS_CSV_PATH}")
    with ORDERS_CSV_PATH.open("rb") as f:
        files = {"file": ("orders.csv", f, "text/csv")}
        data = {"sessionId": session_id, "name": "orders"}
        res = client.post(f"{BASE_URL}/upload", files=files, data=data, timeout=60)
    res.raise_for_status()


def _build_ui_like_tree(cumulative_commands: List[Dict[str, Any]], source_ref: str) -> Dict[str, Any]:
    return {
        "id": "root",
        "type": "operation",
        "operationType": "root",
        "name": "Project",
        "enabled": True,
        "commands": [],
        "children": [
            {
                "id": "setup_ui",
                "type": "operation",
                "operationType": "setup",
                "name": "Data Setup",
                "enabled": True,
                "commands": [
                    {
                        "id": "setup_src_orders",
                        "type": "source",
                        "order": 1,
                        "config": {
                            "mainTable": "orders",
                            "alias": "orders",
                            "linkId": source_ref,
                        },
                    }
                ],
                "children": [
                    {
                        "id": "op_ui",
                        "type": "operation",
                        "operationType": "process",
                        "name": "UI SQL Export Cases",
                        "enabled": True,
                        "commands": cumulative_commands,
                        "children": [],
                    }
                ],
            }
        ],
    }


def main() -> None:
    source_cases = _load_source_cases()
    collected: List[Dict[str, Any]] = []

    with httpx.Client() as client:
        session_id = _create_session(client)
        _upload_orders(client, session_id)

        for idx, case in enumerate(source_cases, start=1):
            cumulative_commands = [deepcopy(c["command"]) for c in source_cases[:idx]]
            source_ref = str((cumulative_commands[0].get("config") or {}).get("dataSource") or "orders")
            tree = _build_ui_like_tree(cumulative_commands, source_ref)

            # Keep all source 50 payloads + add 10 meta-flag variants to exceed 50.
            include_meta_variants = [False]
            if idx <= 10:
                include_meta_variants.append(True)

            for include_meta in include_meta_variants:
                request_payload = {
                    "sessionId": session_id,
                    "tree": tree,
                    "targetNodeId": "op_ui",
                    "targetCommandId": case["command"]["id"],
                }
                if include_meta:
                    request_payload["includeCommandMeta"] = True

                res = client.post(f"{BASE_URL}/generate_sql", json=request_payload, timeout=60)
                response_sql = ""
                response_body: Dict[str, Any] = {}
                try:
                    response_body = res.json()
                    response_sql = response_body.get("sql", "")
                except Exception:
                    response_body = {"raw": res.text}

                collected.append(
                    {
                        "requestId": f"{case['caseId']}_{'meta_on' if include_meta else 'meta_off'}",
                        "sourceCaseId": case["caseId"],
                        "sourceSql": case.get("sourceSql", ""),
                        "request": request_payload,
                        "responseStatus": res.status_code,
                        "responseSql": response_sql,
                        "responseBody": response_body,
                    }
                )
                print(f"[collect] {collected[-1]['requestId']} ({len(collected)}/{TARGET_CASES}) status={res.status_code}")
                if len(collected) >= TARGET_CASES:
                    break
            if len(collected) >= TARGET_CASES:
                break

    if len(collected) < TARGET_CASES:
        raise RuntimeError(f"Not enough collected real requests: {len(collected)} < {TARGET_CASES}")

    output = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "generatedBy": "scripts/generate_generate_sql_real_requests_fixture.py",
        "baseUrl": BASE_URL,
        "sourceCasesPath": str(SOURCE_CASES_PATH),
        "targetCaseCount": TARGET_CASES,
        "actualCaseCount": len(collected),
        "requests": collected,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(collected)} real /generate_sql request cases to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
