#!/usr/bin/env python3
"""Sync test sessions between data/ and test_data/ for git-friendly snapshots.

Usage:
  python scripts/sync_test_sessions.py export   # data/test_sessions -> test_data/test_sessions
  python scripts/sync_test_sessions.py import   # test_data/test_sessions -> data/test_sessions
  python scripts/sync_test_sessions.py export --clean

Notes:
- data/ is not committed; test_data/ can be committed.
- Use --clean to remove destination before copying.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_TEST_SESSIONS = ROOT / "data" / "test_sessions"
SNAPSHOT_TEST_SESSIONS = ROOT / "test_data" / "test_sessions"


def copy_tree(src: Path, dst: Path, clean: bool) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Source directory not found: {src}")
    if clean and dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)

    # Copy children to preserve destination root
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync test_sessions between data/ and test_data/.")
    parser.add_argument("direction", choices=["export", "import"], help="export: data -> test_data; import: test_data -> data")
    parser.add_argument("--clean", action="store_true", help="Remove destination before copying")
    args = parser.parse_args()

    if args.direction == "export":
        copy_tree(DATA_TEST_SESSIONS, SNAPSHOT_TEST_SESSIONS, args.clean)
        print(f"Exported: {DATA_TEST_SESSIONS} -> {SNAPSHOT_TEST_SESSIONS}")
    else:
        copy_tree(SNAPSHOT_TEST_SESSIONS, DATA_TEST_SESSIONS, args.clean)
        print(f"Imported: {SNAPSHOT_TEST_SESSIONS} -> {DATA_TEST_SESSIONS}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
