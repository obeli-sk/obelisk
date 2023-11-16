```
cargo component build --target=wasm32-unknown-unknown -p hello-world

cargo run -p runtime -- target/wasm32-unknown-unknown/debug/hello_world.wasm
```
