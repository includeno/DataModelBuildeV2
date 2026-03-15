#!/bin/bash
set -e

MODE="${1:-production}"
case "$MODE" in
    production|prod)
        export BACKEND_ENV=production
        ENV_LABEL="production"
        ;;
    test)
        export BACKEND_ENV=test
        export TEST_SESSION_CONFIG_PATH="${TEST_SESSION_CONFIG_PATH:-$(pwd)/backend/session_test_config.json}"
        ENV_LABEL="test"
        ;;
    *)
        echo "Usage: $0 [production|test]"
        exit 1
        ;;
esac

# Determine python command
if command -v python3 &> /dev/null; then
    PYTHON=python3
else
    PYTHON=python
fi

echo "📦 Installing/Updating Python dependencies..."
$PYTHON -m pip install -r backend/requirements.txt

echo "🚀 Starting Backend Server (FastAPI)..."
echo "   Environment: ${ENV_LABEL}"
echo "   Listening on http://localhost:8000"

cd backend
$PYTHON -m uvicorn main:app --reload --port 8000
