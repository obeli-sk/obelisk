# Notes from creating this project
I started with generating a new project using [cargo comopnent](https://github.com/bytecodealliance/cargo-component):
```sh
cargo component new --lib trigger-http-hello
cd trigger-http-hello/
```
Then I copied the `wasi:http` package and its dependencies from
[wasmtime](https://github.com/bytecodealliance/wasmtime/tree/v24.0.0/crates/wasi-http/wit/deps/http)
and added the packages to `Cargo.toml` as target dependencies:

```sh
cargo component add --target --path wit/deps/http wasi:http@0.2.0
cargo component add --target --path wit/deps/random wasi:random@0.2.0
cargo component add --target --path wit/deps/cli wasi:cli@0.2.0
cargo component add --target --path wit/deps/io wasi:io@0.2.0
cargo component add --target --path wit/deps/clocks wasi:clocks@0.2.0
cargo component add --target --path wit/deps/filesystem wasi:filesystem@0.2.0
cargo component add --target --path wit/deps/sockets wasi:sockets@0.2.0
```
To implement the contract in `src/lib.rs`
I adapted code from this [repo](https://github.com/sunfishcode/hello-wasi-http/blob/9f0e7081b987ff9bfbb71f08106b00217eed9d7e/src/lib.rs)
and built the component:
```sh
cargo component build --release
```
To test, I used `wasmtime` binary:
```sh
wasmtime serve trigger_http_hello.wasm
curl localhost:8080
```
