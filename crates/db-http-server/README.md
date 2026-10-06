# HTTP storage server

`StorageServer` serves database operations from the [`db-http`](../db-http)
client. The `obelisk storage-serve` command supplies a native SQLite pool, using
its existing migrations, transaction batching, WAL durability and notifications.

```sh
export OBELISK_STORAGE_TOKEN='<shared-secret>'
obelisk storage-serve --token "$OBELISK_STORAGE_TOKEN" --database /var/lib/obelisk/storage.sqlite --listen 127.0.0.1:8081
```

One service owns the SQLite file. Closing an application client leaves the service
available to other nodes. Close the service before closing the native pool so
shutdown drains writes and interrupts long polls. The service does not replicate
the database. HTTPS clients can connect through a TLS termination proxy.

The integration tests cover independent clients, claims, persistence and
reconnection after restart, notifications, shutdown, authentication, binary CAS
and lost committed replies. The shared database test matrix starts a separate
local service and SQLite file for each HTTP test.

See the client README for configuration and protocol behavior.

The [`db-http`](../db-http) client owns the protocol and publishes its
[OpenAPI schema](../../assets/schemas/storage-openapi.json) for server bindings.
Schema generation and compatibility checks live in that client package.
