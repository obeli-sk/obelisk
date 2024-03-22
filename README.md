## Setting up environment
```sh
nix develop # or direnv allow
```

## Building
```sh
cargo build --workspace --no-default-features
```

## Tests
```sh
RUSTFLAGS="--cfg tokio_unstable --cfg tracing_unstable" cargo test --workspace
```

### Simulation testing
```sh
RUSTFLAGS="--cfg madsim --cfg tokio_unstable --cfg tracing_unstable" cargo test --workspace
```
