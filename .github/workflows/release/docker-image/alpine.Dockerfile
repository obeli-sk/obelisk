FROM alpine:3.23

RUN apk add --no-cache ca-certificates \
 && update-ca-certificates

COPY obelisk /obelisk/obelisk
COPY obelisk.toml /etc/obelisk/obelisk.toml

ENV PATH="/obelisk:${PATH}"
WORKDIR /etc/obelisk
ENTRYPOINT ["obelisk"]
