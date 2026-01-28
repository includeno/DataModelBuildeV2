#!/bin/bash
set -e

# Determine python command
if command -v python3 &> /dev/null; then
    PYTHON=python3
else
    PYTHON=python
fi

echo "📦 Installing/Updating Python dependencies..."
$PYTHON -m pip install -r backend/requirements.txt

echo "🚀 Starting Backend Server (FastAPI)..."
echo "   Listening on http://localhost:8000"

cd backend
$PYTHON -m uvicorn main:app --reload --port 8000