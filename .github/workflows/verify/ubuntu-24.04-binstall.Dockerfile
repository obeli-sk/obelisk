FROM ubuntu:24.04
RUN apt update && apt install wget -y
RUN wget https://github.com/cargo-bins/cargo-binstall/releases/latest/download/cargo-binstall-x86_64-unknown-linux-musl.tgz \
    && \
    tar xvfz cargo-binstall-x86_64-unknown-linux-musl.tgz
WORKDIR /root/libc
RUN /cargo-binstall obeli-sk --no-track --install-path . -y
RUN ldd obelisk
RUN ./obelisk --version
WORKDIR /root/musl
RUN /cargo-binstall obeli-sk --targets x86_64-unknown-linux-musl --no-track --install-path . -y
RUN ldd obelisk
RUN ./obelisk --version
