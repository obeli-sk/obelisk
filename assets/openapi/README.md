# Authoritative API contracts

`lifecycle.json` is the maintained OpenAPI source for server info, app policy,
and deployment switch. Edit this file first when changing their HTTP contract.
Other endpoints still use Rust annotations during the incremental migration.

Run inside `nix develop`:

```sh
scripts/update-api-types.sh
scripts/update-api-types.sh --check
scripts/clippy.sh
scripts/test.sh -p obelisk -E 'test(app_policy_webapi_reports_running_policy) | test(deploy_local_wasm_to_empty_server)'
scripts/test.sh -p obelisk --run-ignored only -E 'test(update_openapi_schema)'
```

The locked generator in `tools/obelisk-api-codegen` uses Typify to follow the
selected operations' component references. Generated Rust is checked in under
`crates/rest-types/src/lifecycle_generated.rs`; ordinary builds do not run the
generator. CI checks regeneration. Generated maps use `BTreeMap` for stable
serialization.

Handlers use generated JSON request and response types. Internal deployment IDs
are converted to strings at the HTTP boundary. Internal app policy is converted
through Serde into the generated response model; its digest still uses the
internal representation. TOML responses preserve the existing serializer.

The server merges these operations and schemas into its published OpenAPI
document and the generated `assets/schemas/openapi.json` that clients pin.
They must not also have Rust operation annotations. The deprecated
app-config response references the authoritative policy schema.

HTTP integration tests validate responses against the source contract, including
status codes, media types, JSON Schema constraints and date-time formats. They
also check that the published definitions match the source. Switch flags default
to false when omitted and reject null, preserving the existing handler behavior.
The contract describes the existing 400/503 switch errors instead of the former
incorrect 409 annotation. Switch rejects text-only Accept headers with 406;
policy accepts JSON or TOML. Middleware errors remain outside this initial slice.
