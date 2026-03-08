#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-dataflow-backend}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_REF="${IMAGE_NAME}:${IMAGE_TAG}"
OUTPUT_DIR="${OUTPUT_DIR:-artifacts/docker}"
OUTPUT_FILE="${OUTPUT_FILE:-${IMAGE_NAME//\//_}-${IMAGE_TAG}.tar.gz}"
OUTPUT_PATH="${OUTPUT_DIR}/${OUTPUT_FILE}"

mkdir -p "${OUTPUT_DIR}"

if ! docker image inspect "${IMAGE_REF}" >/dev/null 2>&1; then
  echo "Docker image ${IMAGE_REF} not found locally. Building it now..."
  IMAGE_NAME="${IMAGE_NAME}" IMAGE_TAG="${IMAGE_TAG}" ./scripts/docker_build_backend.sh
fi

echo "Packaging Docker image to ${OUTPUT_PATH}"
docker save "${IMAGE_REF}" | gzip > "${OUTPUT_PATH}"
echo "Package created: ${OUTPUT_PATH}"
