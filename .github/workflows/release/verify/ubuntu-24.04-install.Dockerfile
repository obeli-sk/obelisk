FROM ubuntu:24.04
ARG TAG

RUN apt update && apt install wget gcc protobuf-compiler libprotobuf-dev -y
WORKDIR /root
RUN wget https://sh.rustup.rs -O rustup.sh \
    && chmod u+x ./rustup.sh \
    && ./rustup.sh -y

RUN cargo install obelisk@$TAG --locked --root .
RUN ./obelisk --version
RUN ./obelisk server generate-config
RUN ./obelisk server verify
