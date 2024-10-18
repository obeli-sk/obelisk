FROM alpine:latest
WORKDIR /root
RUN wget https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-x86_64-unknown-linux-musl.tgz \
    && \
    tar xvfz cargo-binstall-x86_64-unknown-linux-musl.tgz
RUN ./cargo-binstall obelisk --no-track --install-path . -y
RUN ldd obelisk
RUN ./obelisk --version
