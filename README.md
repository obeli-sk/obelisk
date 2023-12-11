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
cargo test --workspace  --tests
```

### Integration tests
```sh
RUST_LOG=info,runtime=debug RUST_BACKTRACE=1 cargo test -p tests --tests -- --nocapture
```

Note: changes in `test-programs` are not always reflected in the built WASM files. Run
```sh
cargo clean -p test-programs-builder # --release
```
to regenerate them.
