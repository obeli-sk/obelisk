#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/update-schema-toml.sh
scripts/update-schema-openapi.sh
scripts/update-schema-db.sh
scripts/update-schema-cli.sh
scripts/update-schema-oci-metadata-annotation.sh
