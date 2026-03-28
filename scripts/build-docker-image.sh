#!/usr/bin/env bash
set -euo pipefail

# Build a Docker image from an Obelisk binary.
#
# Usage:
#   build.sh <obelisk-binary> <dockerfile> [tag ...]
#
# Arguments:
#   obelisk-binary  Path to the obelisk binary
#   dockerfile      Path to the Dockerfile
#   tag ...         One or more image tags (e.g. getobelisk/obelisk:ubuntu)
#
# Environment:
#   PUSH  If set to "true" value, push all tags after building.

if [ $# -lt 2 ]; then
  echo "Usage: $0 <obelisk-binary> <dockerfile> [tag ...]" >&2
  exit 1
fi

OBELISK_BINARY="$1"
DOCKERFILE="$2"
shift 2
TAGS=("$@")

# Validate inputs
for f in "$OBELISK_BINARY" "$DOCKERFILE"; do
  if [ ! -f "$f" ]; then
    echo "Error: file not found: $f" >&2
    exit 1
  fi
done

# Create a temporary build context
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT

cp "$OBELISK_BINARY" "$BUILD_DIR/obelisk"
chmod +x "$BUILD_DIR/obelisk"

# Build tag arguments
TAG_ARGS=()
for tag in "${TAGS[@]}"; do
  TAG_ARGS+=(--tag "$tag")
done

echo "Building docker image from $DOCKERFILE"
docker build --progress=plain -f "$DOCKERFILE" "${TAG_ARGS[@]}" "$BUILD_DIR"

# Push if requested
if [ "${PUSH:-}" == "true" ]; then
  for tag in "${TAGS[@]}"; do
    echo "Pushing $tag"
    docker push "$tag"
  done
fi
