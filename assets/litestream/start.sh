#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")"

OBELISK_VERSION=${OBELISK_VERSION:-$(../target/debug/obelisk -v | cut -d ' ' -f 2)}

docker run --rm -it --net=host \
--name obelisk \
-v $(pwd):/etc/obelisk \
-e 'OBELISK__WASM__CACHE_DIRECTORY=/cache/obelisk/wasm' \
-v ~/.cache/obelisk/wasm:/cache/obelisk/wasm \
--entrypoint=/etc/obelisk/entrypoint.sh \
getobelisk/obelisk:${OBELISK_VERSION}-ubuntu-litestream
