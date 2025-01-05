# Web UI
A [yew](https://yew.rs) based UI for Obelisk.

## Developing locally
Run the Obelisk server. Execute `trunk` in development mode:
```sh
trunk --config Trunk-dev.toml serve
```
Navigate to [localhost:8081](http://127.0.0.1:8081)

All gRPC requests go through the `webui-proxy` listening
on localhost:8080 .
If no interaction with `webui-proxy` is desired, change the `backend`
to use Obelisk gRPC port directly:
```toml
backend = "http://127.0.0.1:5005"
```

## Building the release
```sh
trunk build
```
