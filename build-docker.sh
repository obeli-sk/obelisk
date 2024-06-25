#!/usr/bin/env bash
set -e; set -o pipefail;

nix build '.#docker' '.#dockerBinSh'
image=$((docker load < result) | sed -n '$s/^Loaded image: //p')
echo -e "Built .#docker\n$image"
image=$((docker load < result-1) | sed -n '$s/^Loaded image: //p')
echo -e "Built .#dockerBinSh\n$image"
