FROM ubuntu:24.04
WORKDIR /obelisk

ADD obelisk .
ADD obelisk.toml /etc/obelisk/obelisk.toml

# Install Litestream
COPY --from=litestream/litestream:0.5.5 /usr/local/bin/litestream /usr/local/bin/litestream


ENV PATH="/obelisk:${PATH}"
ENTRYPOINT ["obelisk"]
CMD ["server", "run"]
