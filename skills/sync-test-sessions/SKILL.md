---
name: sync-test-sessions
description: Sync DataModelBuildeV2 test session snapshots between `data/test_sessions` (runtime) and `test_data/test_sessions` (git-tracked). Use when exporting sessions for git, restoring sessions for UI tests, or resetting session data from a snapshot.
---

# Sync Test Sessions

## Overview

Use the project script `scripts/sync_test_sessions.py` to export or import test sessions between the runtime data directory and a git-friendly snapshot directory.

## Quick Start

- Export runtime sessions to the git snapshot directory:
  - `python scripts/sync_test_sessions.py export`
- Import snapshot sessions back into runtime:
  - `python scripts/sync_test_sessions.py import`
- Replace the destination before copying:
  - `python scripts/sync_test_sessions.py export --clean`
  - `python scripts/sync_test_sessions.py import --clean`

## Workflow

1. Decide direction:
   - `export`: `data/test_sessions` -> `test_data/test_sessions`
   - `import`: `test_data/test_sessions` -> `data/test_sessions`
2. Run the command.
3. Verify the destination folder contents.

## Notes

- `data/` is not committed. Use `test_data/test_sessions` for git snapshots.
- Use `--clean` when you want an exact mirror of the source.
- If the backend is actively writing session files, consider stopping it before syncing to avoid partial copies.
