#!/usr/bin/env bash

set -euo pipefail

litestream restore -if-replica-exists --config /etc/obelisk/litestream.yml /obelisk-sqlite/obelisk.sqlite
exec litestream replicate --config /etc/obelisk/litestream.yml --exec 'obelisk server run'
