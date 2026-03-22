# Development Guide

## Repository Structure

```
├── src/                    # Main binary (obelisk CLI + server)
│   ├── command/            # CLI commands
│   ├── config/             # Configuration handling
│   └── server/             # gRPC and HTTP API servers
├── crates/
│   ├── concepts/           # Core types, traits, storage interfaces
│   ├── db-sqlite/          # SQLite implementation
│   ├── db-postgres/        # PostgreSQL implementation
│   ├── db-mem/             # In-memory implementation (limited, for testing)
│   ├── grpc/               # gRPC codegen and mappings
│   ├── wasm-workers/       # WASM execution runtime
│   ├── executor/           # Work-stealing executor
│   ├── utils/              # Shared utilities
│   ├── val-json/           # JSON value handling
│   └── testing/            # Test infrastructure
│       ├── db-tests/       # Database integration tests
│       ├── db-macro/       # Test macro for multi-DB testing
│       ├── test-utils/     # Test helpers (SimClock, etc.)
│       └── test-programs/  # WASM test components
├── proto/
│   └── obelisk.proto       # gRPC API definition
└── scripts/                # Development scripts
```

## Setup

Set up dependencies via Nix:
```sh
cp .envrc-example .envrc
$EDITOR .envrc
direnv allow
# If direnv is not available use `nix develop`
```
Or manually install dependencies (see [dev-deps.txt](dev-deps.txt)).

## Checking Compilation

```sh
# Check all crates and targets (including tests)
scripts/check.sh
```

## Running Tests

Postgres must be running. See `.envrc-example` for environment variables.
```sh
# All tests
scripts/test.sh

# Tests with locally built activity, workflow and webhook JavaScript runtimes
scripts/test-js-local.sh

# Specific test crate
cargo test --package obeli-db-tests --test deployment_pagination

# Specific test in main package
cargo test --package obelisk grpc_server::tests
```

## Key Files

| Task | Files |
|------|-------|
| TOML | `src/config/toml.rs`, `obelisk-help-server.toml`, `obelisk-help-deployment.toml` |
| Database schema/queries | `crates/db-sqlite/src/sqlite_dao.rs`, `crates/db-postgres/src/postgres_dao.rs` |
| Storage traits | `crates/concepts/src/storage.rs` |
| gRPC API | `proto/obelisk.proto`, `src/server/grpc_server.rs`,`crates/grpc/src/grpc_mapping.rs` |
| REST API | `src/server/web_api_server.rs` |
| Server | `src/command/server.rs`, `crates/wasm-workers/src/registry.rs` |
| Activities | `crates/wasm-workers/src/activity/` |
| Workflows | `crates/wasm-workers/src/workflow/` |
| Webhooks | `crates/wasm-workers/src/webhook/` |
| Test utilities | `crates/testing/test-utils/src/` |

## Build System Notes

- `cargo metadata --format-version 1 --no-deps | python3 -c "import sys,json; print(json.load(sys.stdin)['target_directory'])"` gets the current Cargo target directory.
- **gRPC and REST are multiplexed on the same port** (`api.listening_addr`, default `9080`). gRPC clients connect to the same address as REST clients.
