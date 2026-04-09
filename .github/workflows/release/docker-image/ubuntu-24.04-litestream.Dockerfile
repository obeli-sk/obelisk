FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY obelisk /obelisk/obelisk
COPY server.toml /obelisk/server.toml

# Install Litestream
COPY --from=litestream/litestream:0.5.11 /usr/local/bin/litestream /usr/local/bin/litestream

ENV PATH="/obelisk:${PATH}"

ENTRYPOINT ["obelisk"]
CMD ["server", "run", "--server-config", "/obelisk/server.toml"]
