#!/bin/sh
set -eu

cargo_target_dir=${CARGO_TARGET_DIR:-target}
tools_dir="$cargo_target_dir/release"
mkdir -p "$tools_dir"
output="$tools_dir/obelisk-activity-vm-socket-shim.so"
mkdir -p "$(dirname "$output")"
cc -O2 -fPIC -shared \
  -Wall -Wextra -Werror \
  -o "$output" \
  crates/activity-vm-runner/guest/socket-shim.c

printf 'Guest tools written to %s\n' "$(dirname "$output")"
