# HTTP database client

`HttpPool` implements Obelisk's database interfaces by sending complete operations
to the storage service in [`db-http-server`](../db-http-server). The service owns one native SQLite pool, using its existing
migrations, WAL durability, transaction batching and commit notifications.
Execution, deployment, CAS, log, system event and maintenance operations all use
the remote database.

See the server package for running the SQLite storage service.

Point application nodes at it in `server.toml`:

```toml
[database.http]
url = "http://127.0.0.1:8081"
token = "${OBELISK_STORAGE_TOKEN}"
```

The URL and token support startup environment interpolation. HTTPS is supported
by the client; terminate TLS at a proxy in front of the storage service. The
library accepts an injected `reqwest::Client` so applications choose their TLS
provider. `db_http::client()` disables redirects and automatic retries and sets
connection and request deadlines; injected clients should preserve those policies.

One storage service owns the SQLite file. Application nodes can overlap during
`obelisk deployment apply --empty` handover. Closing an application pool only
closes that client's operations. The storage service remains available to other
nodes, and graceful service shutdown drains writes before SQLite closes. This
architecture does not replicate the storage database.

Operations use authenticated `POST /v1/{interface}/{operation}` requests with
JSON tuples and typed `Result` responses. One entry in the method table defines
both sides of each operation. CAS uploads and downloads use binary bodies at
`/v1/blobs`, with `HEAD` for existence checks. Incompatible operation signatures
or encodings require a new protocol version; deployed clients and servers must
support the same version.

Notifications use native subscriptions with bounded long polls. Pending waits
return on notifications or the caller's future. Response subscriptions preserve
the response cursor and interruption reason. Finished-result waits check existing
results before racing the caller's timeout, then renew polls until completion.
Polls last at most 30 seconds, and service shutdown interrupts them. Queries and
cursor reads remain authoritative if notifications are missed.

A failed HTTP reply may follow a committed write. The client returns a database
error and does not replay writes automatically. Existing version checks and
idempotent operations retain their native behavior, including version and stub
conflict errors. There is no request journal or universal exactly-once retry
contract. Reconcile an uncertain outcome through the database before deciding to
retry. The service caps request bodies at 128 MiB, in-flight requests at 256 and
simultaneous long polls at 128; excess requests receive HTTP 503. Service requests have a 60-second deadline.

The shared database test matrix runs HTTP tests against a separate local server
and SQLite file for each test. Service tests additionally cover independent
clients, claims, persistence and reconnection after restart, notifications,
shutdown, authentication, binary CAS and lost committed replies.

This client package owns the protocol and generates the published
[OpenAPI schema](../../assets/schemas/storage-openapi.json). Server implementations
should generate their transport bindings from that schema. Regenerate it with
`scripts/update-schemas.sh`; CI checks it against the client operation table and
DTOs, without depending on any server package.
