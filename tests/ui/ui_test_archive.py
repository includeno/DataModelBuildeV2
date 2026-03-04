#!/usr/bin/env python3
"""
UI test archive generator for DataModelBuildeV2.

This script writes a JSON report capturing the UI test steps, datasets,
operations, and observations. It does not execute UI automation; it
records what was validated during a manual/assisted session.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any


@dataclass
class Step:
    id: str
    name: str
    status: str
    details: str


def build_report() -> Dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    steps: List[Step] = [
        Step(
            id="S1",
            name="Backend Connection",
            status="pass",
            details="Switched backend to http://localhost:8000 and verified status in header."
        ),
        Step(
            id="S2",
            name="Session Selection",
            status="pass",
            details="Selected existing session 'sess_demo'."
        ),
        Step(
            id="S3",
            name="Dataset Import",
            status="pass",
            details="Imported ecommerce_orders.csv and mock_retail_transactions.csv from test_data."
        ),
        Step(
            id="S4",
            name="Setup Sources",
            status="pass",
            details="Configured sources: HR_Employees, ecommerce_orders, mock_retail_transactions."
        ),
        Step(
            id="S5",
            name="Filter Operation",
            status="pass",
            details="Filter on ecommerce_orders: status = DELIVERED."
        ),
        Step(
            id="S6",
            name="Join Operation",
            status="pass",
            details="Join ecommerce_orders with mock_retail_transactions on customer_id."
        ),
        Step(
            id="S7",
            name="Sort Operation",
            status="pass",
            details="Sort ecommerce_orders by amount DESC."
        ),
        Step(
            id="S8",
            name="Mapping Operation",
            status="pass",
            details="Mapping: amount * 1.1 -> amount_plus."
        ),
        Step(
            id="S9",
            name="Group Operation",
            status="pass",
            details="Group by status, metric count(*) as order_count, output table order_summary."
        ),
        Step(
            id="S10",
            name="View Operation",
            status="pass",
            details="View table order_summary and run pipeline up to step #6."
        ),
        Step(
            id="S11",
            name="SQL Studio",
            status="pass",
            details="Ran SELECT * FROM ecommerce_orders via top-right Run button."
        ),
    ]

    layout_observations = [
        "Top bar is compact; Run is only visible in SQL Studio.",
        "Workflow editor is dense but scannable; long command lists require scrolling.",
        "Right preview panel is clearly separated; table toolbars are discoverable.",
        "Dataset list and operations tree stay visible for context during edits.",
    ]

    potential_ux_issues = [
        "Command list grows tall quickly; consider collapsing steps or mini-map.",
        "Join condition input is free-form; lack of field-picker may increase error rate."
    ]

    return {
        "generated_at": now,
        "environment": {
            "frontend_url": "http://localhost:1420",
            "backend_url": "http://localhost:8000",
        },
        "datasets": [
            "HR_Employees",
            "ecommerce_orders",
            "mock_retail_transactions",
        ],
        "operations_validated": [
            "filter",
            "join",
            "sort",
            "mapping",
            "group",
            "view",
            "sql_query",
        ],
        "steps": [asdict(s) for s in steps],
        "layout_observations": layout_observations,
        "potential_ux_issues": potential_ux_issues,
    }


def main() -> None:
    report = build_report()
    out_dir = Path(__file__).resolve().parent
    out_path = out_dir / f"ui_test_archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote UI test archive to {out_path}")


if __name__ == "__main__":
    main()
