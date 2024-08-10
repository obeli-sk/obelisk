#!/usr/bin/env bash

# Builds two docker images. WIP.

set -exuo pipefail
cd $(dirname "$0")/..

nix build '.#docker' '.#dockerBinSh'
image=$((docker load < result) | sed -n '$s/^Loaded image: //p')
echo -e "Built .#docker\n$image"
image=$((docker load < result-1) | sed -n '$s/^Loaded image: //p')
echo -e "Built .#dockerBinSh\n$image"
