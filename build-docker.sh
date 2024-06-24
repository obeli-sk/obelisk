#!/usr/bin/env bash
set -e; set -o pipefail;

nix build '.#docker'
image=$((docker load < result) | sed -n '$s/^Loaded image: //p')
echo "Built $image"
if [ -n "$1" ]; then
    docker image tag "$image" "$1"
fi
