Set up MinIO
```sh
docker run -d --net=host --name minio minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1 server /data --console-address :9001
```
Inside MinIO container, run:
```sh
mc alias set myminio http://127.0.0.1:9000 minioadmin minioadmin
mc mb myminio/{MINIO_BUCKET_NAME
```

Run Obelisk container set up with Litestream:
```sh
./start.sh
```
