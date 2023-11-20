```
cargo component build --target=wasm32-unknown-unknown -p hello-world
# cargo run -p runtime -- target/wasm32-unknown-unknown/debug/hello_world.wasm execute

cargo component build --target=wasm32-unknown-unknown -p wasm-email-provider
cargo run -p runtime -- target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm send
```
