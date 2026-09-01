Set up MinIO
```sh
docker run -d --net=host --name minio minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1 server /data --console-address :9001
```
Inside MinIO container, run:
```sh
mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin
mc mb myminio/litestream-bucket
```

## Start Obelisk

Use the same command on an empty VM and for regular restarts:

```sh
litestream replicate -restore-if-db-not-exists --config litestream.yml --exec 'obelisk server run'
```

On an empty VM, Litestream restores the database from the replica before starting Obelisk. If no
backup exists, it starts with a fresh database. On a regular restart, the database already exists,
so Litestream leaves it unchanged and starts replication normally.
