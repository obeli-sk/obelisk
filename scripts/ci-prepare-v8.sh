#!/usr/bin/env bash

# Materializes v8's native archive before a cached CI build. The `v8` build script
# downloads `librusty_v8.a` into `target/<profile>/gn_out/obj` and emits an absolute
# `rustc-link-search` for it, but no build cache preserves that path: `rust-cache`
# strips everything but `build`, `.fingerprint` and `deps`, and `kache` can restore a
# compiled `v8` whose build script never ran. Either way the next build of `v8` fails
# with "could not find native static library `rusty_v8`". Dropping v8's artifacts and
# rebuilding it with the compiler wrapper disabled forces the build script to run.
#
# Run this after restoring any build cache and before the first cargo build of a job.

set -exuo pipefail
cd "$(dirname "$0")/.."

cargo clean --package v8
RUSTC_WRAPPER= cargo build --package v8
