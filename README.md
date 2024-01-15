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
