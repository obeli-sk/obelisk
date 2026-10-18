# Migrating to the next breaking release

This guide describes unreleased changes after 0.42.0. gRPC and gRPC-web remain
available, and short bearer tokens remain accepted with a warning.

## API token environment variable

Use `OBELISK_API_TOKEN` for CLI authentication. `--api-token` and persistent
server `api.token_hashes` remain supported.

Apps needing their server's API credential should declare an internal secret:

```toml
[secrets]
INTERNAL_TOKEN = { type = "internal" }
```

Replace deployment secret references to the operator token with `INTERNAL_TOKEN`
and review its exposure grants as usual. Obelisk supplies its startup token,
which grants full API access and changes on restart. Internal secrets require an
enabled authenticated API and need no environment value or cloud upload.

Platform admins can set `app_env_prefix` in server.toml to isolate app public env
and env-backed secrets from admin variables. The default is empty for local use.
When set, source lookup prepends the prefix and never falls back to an unprefixed
value. App and deployment names remain unchanged.

## Unauthenticated server startup

Replace `server run --allow-unauthenticated-api` with `server run --no-auth`.
The old alias is no longer accepted, bringing its removal forward from the
previously announced after-0.43 timeline.

## Minimum server version for CLI response decoding

Use the next CLI with Obelisk servers version 0.42 or newer. Replay responses must
include `replayed_event_count`, `replay_duration_ms`, and `replay_version`; structured
missing-runtime-config responses must include `optional_public_env` and
`required_public_env`. The optional replay failure detail remains optional.

## System-event retention

REST requests to `POST /v1/admin/system-events/retain` must include `dry_run`:
use `true` to preview and `false` to delete. Omitted values return HTTP 422 and
leave events unchanged. The CLI already supplies the field. gRPC is unchanged.

## JavaScript webhook source environment variables

Webhooks no longer receive `__OBELISK_JS_SOURCE__` or `__OBELISK_JS_FILE_NAME__`
automatically. Pass application metadata through deployment `env_vars` when
needed. Module loading continues to use the module graph in both Boa and V8.

## Structured REST responses

Use `Accept: application/json` for structured resources and actions. The old
human-readable `text/plain` representations have been removed earlier than the
previously announced after-0.43 timeline. Endpoints that negotiated structured text
now return HTTP 406 when the Accept header allows only text. JSON remains the
default when no Accept header is sent. Replace pipelines that parse formatted
text with JSON consumers such as `jq`.

WIT documents, source files, execution and deployment IDs, node-run IDs and
app-config digests retain their text representations. Execution-log streams and
SSE follow modes remain available; SSE clients can send `Accept: text/event-stream`.
The CLI still formats human-readable output locally.
