#!/usr/bin/env python3
"""Backend diagnostics for DataModelBuildeV2.

Usage examples:
  python scripts/diagnose_backend.py --state data/test_sessions/sess_xxx/state.json
  python scripts/diagnose_backend.py --session-id sess_xxx --root data/test_sessions

What it does:
- Load state.json and print source commands (id, mainTable, alias, linkId)
- Build linkId/alias -> table name mapping
- List datasets present for the session
- Validate command dataSource references resolve to existing datasets
- List operations and commands (type/order/id/dataSource)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"

sys.path.append(str(BACKEND_DIR))

from engine import ExecutionEngine  # type: ignore
from models import OperationNode  # type: ignore
from storage import storage  # type: ignore


def load_state(state_path: Path) -> Dict[str, Any]:
    return json.loads(state_path.read_text())


def walk_sources(node: Dict[str, Any], sources: List[Dict[str, Any]]) -> None:
    for cmd in node.get("commands") or []:
        if cmd.get("type") == "source":
            sources.append(cmd)
    for child in node.get("children") or []:
        walk_sources(child, sources)


def walk_operations(node: Dict[str, Any], ops: List[Dict[str, Any]]) -> None:
    if node.get("type") == "operation":
        ops.append(node)
    for child in node.get("children") or []:
        walk_operations(child, ops)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose backend session data issues.")
    parser.add_argument("--state", help="Path to state.json", default="")
    parser.add_argument("--session-id", help="Session ID (e.g., sess_xxx)", default="")
    parser.add_argument("--root", help="Sessions root (default: data/test_sessions)", default="data/test_sessions")
    args = parser.parse_args()

    state_path: Optional[Path] = None
    if args.state:
        state_path = Path(args.state)
    elif args.session_id:
        state_path = ROOT / args.root / args.session_id / "state.json"
    else:
        parser.error("Provide --state or --session-id")

    if not state_path.exists():
        print(f"State file not found: {state_path}")
        return 1

    data = load_state(state_path)
    tree = data.get("tree")
    if not tree:
        print("No tree found in state.json")
        return 1

    session_id = args.session_id or state_path.parent.name

    print(f"State: {state_path}")
    print(f"Session: {session_id}")

    # Parse with models for engine helpers
    engine = ExecutionEngine()
    node = OperationNode(**tree)

    # Sources
    sources: List[Dict[str, Any]] = []
    walk_sources(tree, sources)
    print("\nSources:")
    for s in sources:
        cfg = s.get("config") or {}
        print(f"- {s.get('id')} mainTable={cfg.get('mainTable')} alias={cfg.get('alias')} linkId={cfg.get('linkId')}")

    # Mappings
    allowed, source_map, table_to_ids = engine._collect_setup_sources(node)
    print("\nSource map (identifier -> table):")
    for k, v in sorted(source_map.items()):
        print(f"- {k} -> {v}")

    # Datasets present
    print("\nDatasets in storage:")
    datasets = storage.list_datasets(session_id)
    for ds in datasets:
        print(f"- {ds.get('name')} rows={ds.get('totalCount')} fields={len(ds.get('fields') or [])}")

    # Operations and commands
    ops: List[Dict[str, Any]] = []
    walk_operations(tree, ops)
    print("\nOperations:")
    for op in ops:
        print(f"- {op.get('name')} ({op.get('id')})")
        for cmd in op.get("commands") or []:
            cfg = cmd.get("config") or {}
            ds = cfg.get("dataSource")
            print(f"  - cmd {cmd.get('id')} type={cmd.get('type')} order={cmd.get('order')} dataSource={ds}")

    # Validate dataSource resolution
    print("\nDataSource resolution:")
    for op in ops:
        for cmd in op.get("commands") or []:
            cfg = cmd.get("config") or {}
            ds = cfg.get("dataSource")
            if not ds or ds == "stream":
                continue
            resolved = engine._resolve_table_from_link_id(node, ds) or source_map.get(ds, ds)
            exists = any(d.get("name") == resolved for d in datasets)
            status = "OK" if exists else "MISSING"
            print(f"- {cmd.get('id')} dataSource={ds} -> {resolved} [{status}]")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
