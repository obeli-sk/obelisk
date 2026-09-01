#!/usr/bin/env bash

set -euo pipefail

litestream restore -if-replica-exists --config litestream.yml ~/.local/share/obelisk/obelisk-sqlite/obelisk.sqlite
exec litestream replicate --config litestream.yml --exec 'obelisk server run --litestream-socket /var/run/litestream.sock'
