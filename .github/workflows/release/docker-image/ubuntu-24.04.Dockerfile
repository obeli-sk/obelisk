FROM ubuntu:24.04
RUN apt update && apt install wget -y
WORKDIR /obelisk
ADD obelisk-x86_64-unknown-linux-gnu.tar.gz .
ADD obelisk.toml .
ENTRYPOINT ./obelisk daemon serve
