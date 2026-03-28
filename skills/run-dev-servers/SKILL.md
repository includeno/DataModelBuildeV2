---
name: run-dev-servers
description: Start and troubleshoot DataModelBuildeV2 frontend/backend development servers with the correct auth mode, ports, and CORS settings. Use when a user asks to run UI manually, recover from "8000 disconnected", switch between auth enabled/disabled, or prepare headed/headless browser flows.
---

# Run Dev Servers

## Quick Start

1. Start backend in terminal A: `npm run backend`
2. Start frontend in terminal B: `npm run dev -- --host 127.0.0.1 --port 1420`
3. Open UI at `http://127.0.0.1:1420`
4. Verify backend auth mode: `curl -s http://localhost:8000/config/auth`

## Backend Commands

### Auth enabled (default)

- `npm run backend`
- Equivalent direct command:
`cd backend && BACKEND_ENV=production BACKEND_AUTH_ENABLED=1 python -m uvicorn main:app --reload --port 8000`

### Auth disabled (skip login/register)

- `cd backend && BACKEND_ENV=production BACKEND_AUTH_ENABLED=0 python -m uvicorn main:app --reload --port 8000`
- Validate mode:
`curl -s http://localhost:8000/config/auth` and confirm `"authEnabled": false`

### Test environment

- `npm run backend:test`
- Auth-disabled test mode:
`cd backend && BACKEND_ENV=test BACKEND_AUTH_ENABLED=0 python -m uvicorn main:app --reload --port 8000`

## Frontend Commands

### Standard UI dev server

- `npm run dev -- --host 127.0.0.1 --port 1420`

### Alternate port when `1420` is occupied

- `npm run dev -- --host 127.0.0.1 --port 1425`
- If frontend is on a custom port, start backend with matching CORS origins:
`BACKEND_CORS_ORIGINS=http://127.0.0.1:1425,http://localhost:1425 python -m uvicorn main:app --reload --port 8000`

## Connectivity Checks

- Backend reachable:
`curl -sS http://localhost:8000/config/auth`
- Frontend reachable:
`curl -sS http://127.0.0.1:1420 | head -n 3`
- Port owner checks:
`lsof -nP -iTCP:8000 -sTCP:LISTEN`
`lsof -nP -iTCP:1420 -sTCP:LISTEN`

## Recover "8000 Disconnected"

1. Confirm backend is listening on `8000`.
2. In app settings, select `http://localhost:8000` (not `mockServer`).
3. If auth is enabled, login before expecting projects/realtime to connect.
4. If frontend uses non-default port, restart backend with matching `BACKEND_CORS_ORIGINS`.

## Manual Browser Walkthrough Prep (No Automated Tests)

1. Start backend with the requested auth mode.
2. Start frontend on the requested port.
3. Switch app connection to `http://localhost:8000`.
4. Login if prompted.
5. Create/select a project, import dataset, then run `Run this operation`.

## Optional Full Smoke Wrappers

- Headless full bootstrap:
`npm run smoke:cross-browser:full:headless`
- Headed full bootstrap:
`npm run smoke:cross-browser:full:headed`
- Force UI bootstrap path:
`node scripts/run_cross_browser_collab_smoke.mjs --headless --project-bootstrap ui`
