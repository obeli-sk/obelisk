#!/usr/bin/env bash
# $1 is a JSON string e.g. "World" (with quotes). Strip quotes for raw value.

raw=$(echo $1 | jq -r .)
echo "\"Hello, $raw!\""
