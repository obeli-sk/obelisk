# Building the proxy
```sh
cargo build --package webui-proxy --target=wasm32-wasip2 --profile=release_trunk
```

## Running in obelisk
Add the `webui-proxy` as a webhook endpoint to `obelisk.toml`:
```toml
[[http_server]]
name = "webui"
listening_addr = "127.0.0.1:8080"

[[webhook_endpoint]]
name = "webui"
http_server = "webui"
location.path = "target/wasm32-wasip2/release/webui_proxy.wasm"
routes = [""]
env_vars = ["TARGET_URL=http://127.0.0.1:5005"]
forward_stdout = "stderr"
forward_stderr = "stderr"
```
