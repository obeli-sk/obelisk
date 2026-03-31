FROM alpine:3.23

RUN apk add --no-cache ca-certificates \
 && update-ca-certificates

COPY obelisk /obelisk/obelisk
COPY server.toml /obelisk/server.toml

ENV PATH="/obelisk:${PATH}"

ENTRYPOINT ["obelisk"]
CMD ["server", "run", "--server-config", "/obelisk/server.toml"]
