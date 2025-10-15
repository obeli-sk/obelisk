FROM ubuntu:24.04
WORKDIR /obelisk

# Install dependencies for .deb installation
RUN apt-get update && apt-get install -y wget gnupg ca-certificates && rm -rf /var/lib/apt/lists/*

ADD obelisk .
ADD obelisk.toml /etc/obelisk/obelisk.toml

# Install Litestream
COPY --from=litestream/litestream:0.5.2 /usr/local/bin/litestream /usr/local/bin/litestream


ENV PATH="/obelisk:${PATH}"
ENTRYPOINT ["obelisk"]
CMD ["server", "run"]
