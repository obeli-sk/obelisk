# Migrating to the next breaking release

This guide describes unreleased changes after 0.42.0. gRPC and gRPC-web remain
available, and short bearer tokens remain accepted with a warning.

## API token environment variable

Replace `OBELISK__API__TOKEN` with `OBELISK_API_TOKEN` and unset the old variable.
The CLI no longer reads the old variable as an authentication fallback, and server
startup rejects it. `--api-token` and persistent server `api.token_hashes` remain
supported. The old variable name remains reserved as sensitive.

This removes compatibility earlier than the previously announced after-0.43
timeline. Update startup environments before upgrading.

## Unauthenticated server startup

Replace `server run --allow-unauthenticated-api` with `server run --no-auth`.
The old alias is no longer accepted, bringing its removal forward from the
previously announced after-0.43 timeline.

## Minimum server version for CLI response decoding

Use the next CLI with Obelisk servers version 0.42 or newer. Replay responses must
include `replayed_event_count`, `replay_duration_ms`, and `replay_version`; structured
missing-runtime-config responses must include `optional_public_env` and
`required_public_env`. The optional replay failure detail remains optional.
