#!/usr/bin/env bash

set -exuo pipefail
cd "$(dirname "$0")/.."

scripts/update-toml-schema.sh
scripts/update-openapi-schema.sh
scripts/update-db-schema.sh
scripts/update-cli-schema.sh
scripts/update-schema-oci-metadata-annotation.sh
