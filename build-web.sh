#!/usr/bin/env bash
set -euo pipefail

echo "Building frontend..."
npm install
npm run build
