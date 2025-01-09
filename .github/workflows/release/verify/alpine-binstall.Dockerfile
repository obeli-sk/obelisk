FROM alpine:latest
ARG TAG

WORKDIR /root
RUN wget https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-x86_64-unknown-linux-musl.tgz \
    && tar xvfz cargo-binstall-x86_64-unknown-linux-musl.tgz
RUN echo $TAG
RUN ./cargo-binstall obelisk@$TAG --no-track --install-path . -y
RUN ldd obelisk
RUN ./obelisk --version
RUN ./obelisk server generate-config
RUN ./obelisk server verify
