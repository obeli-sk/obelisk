FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY obelisk /obelisk/obelisk

ENV PATH="/obelisk:${PATH}"
ENV OBELISK__API__LISTENING_ADDR=0.0.0.0:5005
ENV OBELISK__WEBUI__LISTENING_ADDR=0.0.0.0:8080
ENV OBELISK__EXTERNAL__LISTENING_ADDR=0.0.0.0:9090

ENTRYPOINT ["obelisk"]

