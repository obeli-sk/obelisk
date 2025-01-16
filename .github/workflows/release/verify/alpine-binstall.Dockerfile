FROM alpine:latest
ARG VERSION

WORKDIR /root
RUN wget https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-x86_64-unknown-linux-musl.tgz \
    && tar xvfz cargo-binstall-x86_64-unknown-linux-musl.tgz
RUN ./cargo-binstall \
    --strategies "crate-meta-data,quick-install" \
    --install-path . \
    -y \
    obelisk@$VERSION
RUN ldd obelisk
RUN ./obelisk --version
RUN ./obelisk server generate-config
RUN ./obelisk server verify
