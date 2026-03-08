#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-dataflow-backend}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-backend/Dockerfile}"
CONTEXT_PATH="${CONTEXT_PATH:-backend}"
IMAGE_REF="${IMAGE_NAME}:${IMAGE_TAG}"

echo "Building Docker image: ${IMAGE_REF}"
docker build -f "${DOCKERFILE_PATH}" -t "${IMAGE_REF}" "${CONTEXT_PATH}"
echo "Docker image ready: ${IMAGE_REF}"
