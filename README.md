## Setting up environment
```sh
nix develop # or direnv allow
```

## Benchmarks
```sh
cargo bench -p benches
```

## Tests
```sh
cargo nextest run --workspace --tests
```

### Integration tests
```sh
RUST_LOG=info,runtime=debug RUST_BACKTRACE=1 cargo test -p tests --tests -- --nocapture
```

### Simulation testing
Enable Madsim by setting the `RUSTFLAGS` envvar or by editing `.cargo/config.toml`:
```toml
rustflags = ["--cfg", "tokio_unstable", "--cfg", "madsim"]
```
