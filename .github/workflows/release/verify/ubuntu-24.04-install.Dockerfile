FROM ubuntu:24.04
RUN apt update && apt install wget gcc protobuf-compiler libprotobuf-dev -y
WORKDIR /root
RUN wget https://sh.rustup.rs -O rustup.sh \
    && chmod u+x ./rustup.sh \
    && ./rustup.sh -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install obelisk --locked
RUN obelisk --version
