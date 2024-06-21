# syntax=docker/dockerfile:1
FROM alpine:latest
ARG TAG
ARG SOURCE=https://github.com/obeli-sk/obeli-sk/releases/download/${TAG}/obelisk-x86_64-unknown-linux-musl.tar.gz
ADD ${SOURCE} /obelisk/
WORKDIR /obelisk/
RUN tar xvzf *.tar.gz && rm *.tar.gz

FROM alpine:latest
COPY --from=0 /obelisk/obelisk /obelisk/obelisk
ENTRYPOINT ["/obelisk/obelisk"]
