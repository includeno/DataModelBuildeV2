#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-dataflow-backend}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_REF="${IMAGE_NAME}:${IMAGE_TAG}"
CONTAINER_NAME="${CONTAINER_NAME:-dataflow-backend}"
HOST_PORT="${HOST_PORT:-8000}"
CONTAINER_PORT="${CONTAINER_PORT:-8000}"
DATA_DIR="${DATA_DIR:-$(pwd)/data}"
LOG_DIR="${LOG_DIR:-$(pwd)/logs}"
DETACH="${DETACH:-0}"

mkdir -p "${DATA_DIR}" "${LOG_DIR}"

if ! docker image inspect "${IMAGE_REF}" >/dev/null 2>&1; then
  echo "Docker image ${IMAGE_REF} not found locally. Building it now..."
  IMAGE_NAME="${IMAGE_NAME}" IMAGE_TAG="${IMAGE_TAG}" ./scripts/docker_build_backend.sh
fi

docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

RUN_ARGS=(
  --name "${CONTAINER_NAME}"
  -p "${HOST_PORT}:${CONTAINER_PORT}"
  -v "${DATA_DIR}:/app/data"
  -v "${LOG_DIR}:/app/logs"
  -e "BACKEND_LOG_PATH=/app/logs/backend.log"
)

if [[ "${DETACH}" == "1" ]]; then
  RUN_ARGS=(-d "${RUN_ARGS[@]}")
else
  RUN_ARGS=(--rm "${RUN_ARGS[@]}")
fi

echo "Starting container ${CONTAINER_NAME} from ${IMAGE_REF} on port ${HOST_PORT}"
docker run "${RUN_ARGS[@]}" "${IMAGE_REF}"
