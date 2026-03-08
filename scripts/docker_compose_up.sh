#!/usr/bin/env bash
set -euo pipefail

if docker compose version >/dev/null 2>&1; then
  docker compose up -d --build
elif command -v docker-compose >/dev/null 2>&1; then
  docker-compose up -d --build
else
  echo "Neither 'docker compose' nor 'docker-compose' is available."
  exit 1
fi
