#!/usr/bin/env bash
set -euo pipefail

echo "Building Tauri app..."
npm install
npm run tauri build
