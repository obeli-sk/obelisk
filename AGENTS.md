# Agent Guidelines for Obelisk

## Project Overview

Obelisk is a **deterministic workflow engine** built on the WASM Component Model. It executes workflows, activities, and webhook endpoints, persisting execution steps in SQLite or PostgreSQL.

### Key Concepts
- **Workflows**: Deterministic, replayable executions with persistent logs, see https://obeli.sk/docs/latest/concepts/workflows/join-sets/ for details
- **Activities**: Idempotent, retriable tasks with automatic error handling
- **Webhooks**: HTTP endpoints that can spawn child executions

## Repository Structure

```
/workspace
├── src/                    # Main binary (obelisk CLI + server)
│   ├── command/            # CLI commands
│   ├── config/             # Configuration handling
│   └── server/             # gRPC and HTTP API servers
│       ├── grpc_server.rs  # gRPC API implementation
│       ├── web_api_server.rs # REST API implementation
│       └── mod.rs          # Shared server utilities
├── crates/
│   ├── adhoc-js-workflow/  # Ad-hoc JS workflow component (Boa JS engine)
│   ├── concepts/           # Core types, traits, storage interfaces
│   │   └── src/storage.rs  # Database trait definitions (DbPool, DbConnection, etc.)
│   ├── db-sqlite/          # SQLite implementation
│   ├── db-postgres/        # PostgreSQL implementation
│   ├── db-mem/             # In-memory implementation (limited, for testing)
│   ├── grpc/               # gRPC codegen and mappings
│   ├── wasm-workers/       # WASM execution runtime
│   │   ├── activity/       # Activity worker
│   │   ├── workflow/       # Workflow worker
│   │   └── webhook/        # Webhook handler
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
    ├── clippy.sh           # Lint check (run before committing)
    └── test.sh             # Run tests
```

## Development Workflow

### Before Committing
Always run:
```bash
./scripts/clippy.sh
```

This runs clippy with pedantic settings and formats code.

### Running Tests
```bash
# All tests
./scripts/test.sh

# Specific test crate
cargo test --package obeli-db-tests --test deployment_pagination

# Specific test in main package
cargo test --package obelisk grpc_server::tests
```

### Database Tests
Tests in `crates/testing/db-tests/` use the `#[expand_enum_database]` macro to run against multiple database backends (SQLite, PostgreSQL, Memory). The Memory backend has limited functionality.

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
The codebase uses a `Pagination<T>` enum for cursor-based pagination:
```rust
pub enum Pagination<T> {
    NewerThan { length: u16, cursor: T, including_cursor: bool },
    OlderThan { length: u16, cursor: T, including_cursor: bool },
}
```

**Important**: Results are always returned in **descending order** (newest first) for UI consistency, even when using `NewerThan` pagination. The DB layer reverses `NewerThan` results.

### gRPC to Internal Type Mapping
The gRPC layer (`grpc_server.rs`) converts proto types to internal types. Be careful with enum mappings - ensure `NewerThan` maps to `Pagination::NewerThan`, not `OlderThan`.

### Adding Current Deployment
When listing deployments, the current deployment may need to be added if it has no executions yet. Use `should_add_current_deployment()` in `src/server/mod.rs`.

## Key Files for Common Tasks

| Task | Files |
|------|-------|
| Database schema/queries | `crates/db-sqlite/src/sqlite_dao.rs`, `crates/db-postgres/src/postgres_dao.rs` |
| Storage traits | `crates/concepts/src/storage.rs` |
| gRPC API | `proto/obelisk.proto`, `src/server/grpc_server.rs` |
| REST API | `src/server/web_api_server.rs` |
| Type conversions (gRPC) | `crates/grpc/src/grpc_mapping.rs` |
| Test utilities | `crates/testing/test-utils/src/` |

## WASM Component Error Variants

WASM components returning `result<T, E>` where `E` is a variant type must include an `execution-failed` case with no payload. This allows the runtime to map execution failures and
pass them to callers. `E` can also be a `string` or an empty type, see `enum ReturnType`.


## Adding Workflow Host Functions

WIT definitions:
- Types: `wit/obelisk_types@X.Y.Z/types.wit` (symlinked as `@latest`)
- Workflow support: `wit/obelisk_workflow@X.Y.Z/workflow-support.wit` (symlinked as `@latest`)

Implementation pattern:
1. Define function signature in WIT with `@since` annotation
2. Implement in `crates/wasm-workers/src/workflow/workflow_ctx.rs`
3. Link via `add_to_linker_workflow_support()` using `func_wrap` or `func_wrap_async`
4. Type conversions in `host_exports.rs` (WIT ↔ Rust types via wasmtime bindgen)
5. Test with programs in `crates/testing/test-programs/`

JSON value handling uses the `val-json` crate for serialization/deserialization.

## Ad-hoc JavaScript Workflows

The `crates/adhoc-js-workflow/` crate provides a WASM component that executes JavaScript code as workflows:

- Uses [Boa](https://boajs.dev/) JS engine embedded in WASM
- Exposes `obelisk` global with workflow-support functions (createJoinSet, sleep, submit, etc.)
- Build: `cargo build -p adhoc-js-workflow --target wasm32-wasip2 --release`
- Output: `target/wasm32-wasip2/release/adhoc_js_workflow.wasm` (~4.2MB)

See `crates/adhoc-js-workflow/README.md` for JavaScript API documentation.

**Future**: Permanent JS/TS workflows via `[[workflow-js]]` / `[[workflow-ts]]` config sections.

## Commit Messages
Use conventional commit style with scope:
- `fix(grpc):` - gRPC fixes
- `r:` or `refactor:` - Refactoring
- `test:` - Test additions/changes
- `chore:` - Maintenance tasks
