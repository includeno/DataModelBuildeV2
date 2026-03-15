#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:-$(date +%Y%m%d-%H%M%S)}"
RELEASE_DIR="${RELEASE_DIR:-artifacts/release/${VERSION}}"
IMAGE_NAME="${IMAGE_NAME:-dataflow-backend}"
IMAGE_TAG="${IMAGE_TAG:-${VERSION}}"
INCLUDE_DOCKER="${INCLUDE_DOCKER:-1}"

mkdir -p "${RELEASE_DIR}"

echo "Building web bundle..."
npm run build:web

WEB_ARCHIVE="${RELEASE_DIR}/dataflow-web-${VERSION}.tar.gz"
tar -czf "${WEB_ARCHIVE}" dist
echo "Web package created: ${WEB_ARCHIVE}"

if [[ "${INCLUDE_DOCKER}" == "1" ]]; then
  if command -v docker >/dev/null 2>&1; then
    IMAGE_NAME="${IMAGE_NAME}" IMAGE_TAG="${IMAGE_TAG}" OUTPUT_DIR="${RELEASE_DIR}" ./scripts/docker_package_backend.sh
  else
    echo "Docker is not installed. Skipping Docker package."
  fi
fi

echo "Release artifacts ready in: ${RELEASE_DIR}"
