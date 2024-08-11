FROM alpine
WORKDIR /obelisk
ADD obelisk .
ADD obelisk.toml /etc/obelisk/obelisk.toml
ENV PATH="/obelisk:${PATH}"
ENTRYPOINT ["obelisk"]
CMD ["server", "run"]