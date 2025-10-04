FROM ubuntu:24.04
WORKDIR /obelisk

# Install dependencies for .deb installation
RUN apt-get update && apt-get install -y wget gnupg ca-certificates && rm -rf /var/lib/apt/lists/*

ADD obelisk .
ADD obelisk.toml /etc/obelisk/obelisk.toml

# Install Litestream
RUN wget https://github.com/benbjohnson/litestream/releases/download/v0.5.0/litestream-0.5.0-linux-x86_64.deb \
    && dpkg -i litestream-0.5.0-linux-x86_64.deb \
    && rm litestream-0.5.0-linux-x86_64.deb

ENV PATH="/obelisk:${PATH}"
ENTRYPOINT ["obelisk"]
CMD ["server", "run"]
