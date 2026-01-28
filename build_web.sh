#!/bin/bash
set -e

echo "📦 Installing dependencies..."
npm install

echo "🏗️  Building Web Application..."
# Runs 'tsc && vite build' as defined in package.json
npm run build:web

echo "✅ Web build successful! Artifacts are located in the 'dist' directory."