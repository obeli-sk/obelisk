```sh
nix develop # or direnv allow
./rebuild-wasm.sh
cargo run --release -- \
  target/wasm32-unknown-unknown/release/wasm_email_provider.wasm \
  target/wasm32-unknown-unknown/release/hello_world.wasm \
  noop
```

## Benchmarks
```sh
cargo bench -p benches
```

## Integration tests
```sh
RUST_LOG=info,runtime=debug RUST_BACKTRACE=1 cargo test -p tests --tests -- --nocapture
```

Note: changes in `test-programs` are not always reflected in the built WASM files. Run
```sh
cargo clean -p test-programs-builder
```
to regenerate them.
