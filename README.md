```
nix shell # or direnv allow
./rebuild-wasm.sh
cargo run --release -- \
  target/wasm32-unknown-unknown/release/wasm_email_provider.wasm \
  target/wasm32-unknown-unknown/release/hello_world.wasm \
  noop
```
