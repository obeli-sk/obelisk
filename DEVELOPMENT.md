# Development Guide

## Repository Structure

```
├── src/                    # Main binary (obelisk CLI + server)
│   ├── command/            # CLI commands
│   ├── config/             # Configuration handling
│   └── server/             # gRPC and HTTP API servers
├── crates/
│   ├── adhoc-js-workflow/  # Ad-hoc JS workflow component (Boa JS engine)
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

## Before Committing

Always run:
```sh
./scripts/clippy.sh
```
This runs clippy with pedantic settings and formats code.

## Running Tests

Postgres must be running. See `.envrc-example` for environment variables.
```sh
# All tests
./scripts/test.sh

# Specific test crate
cargo test --package obeli-db-tests --test deployment_pagination

# Specific test in main package
cargo test --package obelisk grpc_server::tests
```

### Database Tests

Tests in `crates/testing/db-tests/` use `#[expand_enum_database]` to run against
multiple backends (SQLite, PostgreSQL, Memory). The Memory backend has limited functionality.

```rust
#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn my_test(database: Database) {
    if database == Database::Memory {
        return; // Skip if not supported
    }
    // ...
}
```

## Code Patterns

### Pagination

Cursor-based pagination via `Pagination<T>`:
```rust
pub enum Pagination<T> {
    NewerThan { length: u16, cursor: T, including_cursor: bool },
    OlderThan { length: u16, cursor: T, including_cursor: bool },
}
```
Results are always returned in **descending order** (newest first), even for `NewerThan`.
The DB layer reverses `NewerThan` results.

### WASM Component Error Variants

WASM components returning `result<T, E>` where `E` is a variant type must include an
`execution-failed` case with no payload. `E` can also be a `string` or an empty type
(see `enum ReturnType`).

### Adding Workflow Host Functions

WIT definitions:
- Types: `wit/obelisk_types@X.Y.Z/types.wit` (symlinked as `@latest`)
- Workflow support: `wit/obelisk_workflow@X.Y.Z/workflow-support.wit` (symlinked as `@latest`)

Implementation pattern:
1. Define function signature in WIT with `@since` annotation
2. Implement in `crates/wasm-workers/src/workflow/workflow_ctx.rs`
3. Link via `add_to_linker_workflow_support()` using `func_wrap` or `func_wrap_async`
4. Type conversions in `host_exports.rs` (WIT ↔ Rust types via wasmtime bindgen)
5. Test with programs in `crates/testing/test-programs/`

## Key Files

| Task | Files |
|------|-------|
| Database schema/queries | `crates/db-sqlite/src/sqlite_dao.rs`, `crates/db-postgres/src/postgres_dao.rs` |
| Storage traits | `crates/concepts/src/storage.rs` |
| gRPC API | `proto/obelisk.proto`, `src/server/grpc_server.rs` |
| REST API | `src/server/web_api_server.rs` |
| Type conversions (gRPC) | `crates/grpc/src/grpc_mapping.rs` |
| Test utilities | `crates/testing/test-utils/src/` |

## Ad-hoc JavaScript Workflows

The `crates/adhoc-js-workflow/` crate provides a WASM component that executes JavaScript
as workflows using the [Boa](https://boajs.dev/) JS engine.

```sh
cargo build -p adhoc-js-workflow --target wasm32-wasip2 --release
```

See `crates/adhoc-js-workflow/README.md` for the JavaScript API.

## Commit Messages

Use conventional commit style with scope:
- `fix(grpc):` — gRPC fixes
- `r:` or `refactor:` — Refactoring
- `test:` — Test additions/changes
- `chore:` — Maintenance tasks
