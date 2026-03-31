FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY obelisk /obelisk/obelisk
COPY server.toml /obelisk/server.toml

ENV PATH="/obelisk:${PATH}"

ENTRYPOINT ["obelisk"]
CMD ["server", "run", "--server-config", "/obelisk/server.toml"]

