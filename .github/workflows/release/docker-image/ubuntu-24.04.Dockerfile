FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY obelisk /obelisk/obelisk
COPY obelisk.toml /etc/obelisk/obelisk.toml

ENV PATH="/obelisk:${PATH}"
WORKDIR /etc/obelisk
ENTRYPOINT ["obelisk"]

