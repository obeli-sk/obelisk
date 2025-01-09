# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.11.0](https://github.com/obeli-sk/obelisk/compare/v0.10.0...v0.11.0) - 2025-01-09


### Fixed

- Build glibc artifact natively

## [0.10.0](https://github.com/obeli-sk/obelisk/compare/v0.8.0...v0.10.0) - 2025-01-08

### Added

- Render function links inside the WIT
- *(ui)* Highlight submittable functions in WIT
- *(ui)* Hide unrelated functions on the submit page
- *(ui)* Filter the WIT to required interfaces in the submit page
- *(ui)* Add syntax highlighting for WIT
- *(grpc)* Implement `GetWit`
- *(ui)* Render interfaces as a code block in `<ComponentListPage />`
- *(toml)* [**breaking**] Rename `api_listening_addr` to `api.listening_addr`
- *(toml)* [**breaking**] Rename `oci.wasm_directory` to `wasm_cache_directory`
- Hide webui config details behind `webui.listening_addr`
- Add `convert_core_module` to `component push`
- Add `convert_core_module` switch to workflows
- Transform Core Module to a Component in `inspect`
- Make the component builder publishable
- Add error detail to the `ExecutionFailure` message
- Add error detail to the `TemporarilyFailed` message
- Increase timeouts exponentially when limit is reached
- *(grpc)* Propagate `Unblock` reason
- *(ui)* Show submission and parameter parsing errors
- *(ui)* Type check parameters before submission

### Fixed

- *(ci)* Save the `blueprint.css` in `webui-builder`
- *(logging)* Skip ansi escape codes when logging to a file
- Explicitly convert from `ConfigIdType` to `grpc::ComopnentType`
- Make router match '/path/' with pattern '/path/*'
- Update `clippy.sh` after `cargo-component` removal
- Fix `null` deserialization into `WastValWithType`
- *(sqlite)* Fix broken attribute ordering in `JsonWrapper`
- *(ui)* Stop leaking connections in `<ExecutionStatus />`

### Other

- Move the `wasm-tools` fork declaration to the `patch section
- Revert "chore: release v0.9.0"
- Add `release-plz` to the dev shell
- Update the disclaimer
- Remove `blueprint.css` from git
- Update the readme
- Turn `EngineConfig` into a struct
- Fix clippy
- *(toml)* [**breaking**] Enable file logger with `enabled` flag
- *(toml)* [**breaking**] Enable stdout logger with `log.stdout.enabled`
- *(toml)* [**breaking**] Require `otlp.enabled`, bump dependencies
- *(grpc)* [**breaking**] Make parameter names mandatory
- Bump `wasmtime` to v28
- Upgrade dependencies
- Run `cargo update`
- Bump `Cargo.lock`
- Move all webui deps to the webui section
- Bump `wasm-tools` after PR merge
- Bump `flake.lock`
- Bump `wasm-tools` fork revision
- Bump fork of `wasm-tools` with the new `WitPrinter` API
- release v0.9.0
- Bump OCI reference of `webui`
- Rename `ConfigId` to `ComponentId`, `ConfigIdType` to `ComponentType`
- Rename `Webhook` to `WebhookEndpoint`
- Move the WIT manipulation to the server side
- *(ui)* Print `-submit` in `print_all`
- Readd `-nosim` suffix
- Bump the `webui` OCI reference
- Rename `ConfigIdType::WebhookWasm` to `WebhookEndpoint`
- Add `build.yml`
- *(grpc)* [**breaking**] Rename `WEBHOOK_WASM` to `WEBHOOK_ENDPOINT`
- Update the roadmap
- Switch to `WitPrinterExt`
- Show only types on nested interfaces
- *(ui)* Remove `yewprint-css`
- *(grpc)* [**breaking**] Rename `function` to `function_name`
- *(grpc)* [**breaking**] Rename `ConfigId` to `ComponentId`
- Bump `flake.lock`
- Extract roadmap from the readme
- *(toml)* [**breaking**] Rename `webhook_wasm` to `webhook_endpoint`
- Change the commented out `external` port
- Simplify the fibo webhook using `waki`
- Update the readme
- Determine architecture of the `macos-13` runner
- Change the the arm architecture for MacOS
- Add Mac OS 13 x64 to the list of supported platforms
- Rename macos artifacts for `cargo-binstall`
- Use `gnu-tar` instead of BSD `tar`
- Remove `jq` as it is already present
- Fix `protobuf` installation on MacOS
- Replace `grep -oP` with MacOS-compatible `jq`
- Add support for building on MacOS
- Mention `protoc` in the instructions
- Fix clippy
- *(toml)* Enable stdout logger by default
- *(toml)* Make `codegen_cache` enabled by default
- *(toml)* [**breaking**] Rename `webhook_component` to `webhook_wasm`
- Remove `wasm32-wasip1`
- Add `cargo-generate` to `flake.nix`
- Update `dev-deps.txt`
- Remove `tracing-chrome`
- Fix clippy
- Update `rustc` to 1.83
- Bump component versions in docker hub
- Stop using `cargo-component` in `webui-proxy`
- Remove `cargo-component`
- Switch workflows away from `cargo-component`
- Remove `cargo-component` from activities, webhooks
- Add `wasm32-wasip2` target
- Bump `wasmtime` to v27
- Add `WasmComponent.wit()`
- Fix missing delimiter
- Build test workflows without `cargo-component`
- Lower error to a warning on unsupported component elements
- *(test)* Simplify tests by extracting `new_activity_worker`

## [0.8.0](https://github.com/obeli-sk/obelisk/compare/v0.7.0...v0.8.0) - 2024-11-25

### Added

- [**breaking**] Remove `request_timeout` configuration from webhooks
- *(grpc)* Implement `ListResponses`
- *(ui)* Show time since scheduling in the execution detail page
- *(grpc)* [**breaking**] send `scheduled_at` in `FinishedStatus`
- *(grpc)* Send `scheduled_at` in `ExecutionSummary`
- *(sqlite)* Persist `t_state.schedule_at`, pass it in `ExecutionWithState`
- *(grpc)* [**breaking**] Remove `WitType.internal`
- *(ui)* Show parameter names in `<CreateEvent />`
- *(grpc)* [**breaking**] Allow sorting executions by creation date, execution ID
- *(sqlite)* [**breaking**] Add `t_state.created_at`
- *(grpc)* Implement `list_execution_events`
- *(grpc)* Add stub for `GetExecutionLog` rpc
- *(grpc)* Add definitions for execution history
- *(sqlite)* Implement `list_responses` with pagination
- *(db)* Add `DbConnection::get_execution_events`
- [**breaking**] Change format for derived `ExecutionId`

### Fixed

- *(grpc)* Fix version in `list_execution_events`
- *(sqlite)* Fix sql statement of `list_responses`

### Other

- Bump the webui OCI image
- *(sqlite)* [**breaking**] Rename `Intermittently...` to `Temporarily...`
- *(grpc)* [**breaking**] Rename `Intermittently...` to `Temporarily...`
- Bump the webui OCI image
- Update the documentation
- Run `cargo upgrade`
- Bump `flake.lock`
- Fix clippy
- Fix clippy
- *(concepts)* Implement `FromSql` and `ToSql` for `ExecutionId`
- Bump `flake.lock`
- *(grpc)* [**breaking**] Change `ListExecutions` pagination to date-based
- *(grpc)* Rename `ListExecutionEvents`
- Update the readme
- *(grpc)* [**breaking**] Rename `FunctionDetail`
- *(grpc)* [**breaking**] Introduce ` ResultDetail` message
- Make `get_finished_result` return `Ok(None)` on unfinished executions
- *(ui)* Build webui with `release_trunk` profile
- Bump `trunk` to v0.21.1
- Run `cargo upgrade`
- Rename `wait_for_pending`
- Fix clippy warnings
- *(concepts)* Change `append_batch`,`append_batch_respond_to_parent`
- *(concepts)* [**breaking**] Change signature of `append_batch_create_new_execution`
- *(db)* Rename `list_execution_events`
- Convert `Version(usize)` to `Version(u32)`
- *(concepts)* [**breaking**] Rename `ExecutionEventInner::IntermittentTimedOut`
- *(concepts)* [**breaking**] `ExecutionEventInner::IntermittentlyFailed`
- *(concepts)* [**breaking**] Rename `JoinNext.run_expires_at`
- Extract `SupportedFunctionReturnValue::FallibleResultErr`
- Rename `list_responses`
- *(sqlite)* [**breaking**] Introduce `t_join_set_response.id`
- *(sqlite)* Add index to `t_state.created_at`
- Remove `ExecutionIdW`
- *(sqlite)* Revert adding `t_ffqn`
- *(sqlite)* Add more stats to the dumped json
- update Cargo.toml dependencies
- Fix clippy

## [0.7.0](https://github.com/obeli-sk/obelisk/compare/v0.6.1...v0.7.0) - 2024-11-09

### Added

- *(ui)* Print status with chrono formatted dates
- Mark traps on workflows as permanent failures, allow overriding via `retry_on_trap`
- [**breaking**] Introduce `ListExecutions` RPC
- Add execution submit page
- Switch component list to `<ComponentTree />`
- Add `submittable` to `FunctionDetails` proto message
- Implement `webui-proxy`, enable it in the TOML
- Validate name of `http_server`
- Add `obelisk-webui`
- Introduce `FunctionExtension`
- *(wit)* [**breaking**] Remove `id` function from `join-set-id`
- Add execution id hierachy for workflows - `-submit` and direct call
- Add execution id hierarchy to direct calls from webhook

### Fixed

- *(grpc)* Send the finished result when requested at all times
- Sync `wasm-bindgen`,`wasm-bindgen-futures` with `wasm-bindgen-cli`
- [**breaking**] Disallow zero `retry_exp_backoff` for workflows
- Disallow submitting ext functions
- Filter out ext fns in `ComponentConfigRegistryRO`
- Return non-ext functions in `Worker::exported_functions`
- Panic when `increment` is called on the top level `ExecutionId`
- Check for multiple '.' separators when parsing FFQN
- *(sqlite)* Rollback wrong sleep when a pending execution is found
- Convert some `ValidationFailed` errors in sqlite to `panic!`
- Sleep until timeout in `subscribe_to_pending` on empty pending list
- Exclude state `BlockedByJoinSet` in sqlite's `get_pending`
- [**breaking**] Use snake_case for `TypeWrapper` serialization
- [**breaking**] Serialize `Enum` into string
- [**breaking**] Serialize unit-like variants as strings
- Serialize result variants into snake_case
- Fix `bindgen!` macro use for v26.0

### Other

- Add `hdrmetrics` to sqlite
- Bump webui
- Improve startup traces
- Update the readme
- *(db)* [**breaking**] Remove `topmost_parent`, add `scheduled_by`
- Fix a typo
- *(gRPC)* [**breaking**] Remove `execution_id` from `SubmitRequest`
- [**breaking**] Make `ExecutionId` hierarchical
- *(grpc)* [**breaking**] Rename pagination messages
- *(db)* [**breaking**] Remove `return_type` from `CreateRequest`, `t_state`
- *(ui)* Move the dev trunk build directory to `dist-dev`
- Increase default pagination size to 20
- [**breaking**] Merge `Scheduler` into `ExecutionRepository` service
- Add `send_finished_status` flag to `GetStatus`
- Simplify return type of `get_finished_result`
- Fix clippy issues
- Query `ListComponents` only once, sort components,ifcs,fns
- Bump `waki`
- Bump `oci-client`,`oci-wasm`
- Run `cargo upgrade`
- Remove `tailwindcss`
- Run `nix flake update`
- Bump `webui` version in TOML
- Bump `wasmtime` to v26.0
- Use `indexmap` for grouping fns by interfaces
- [**breaking**] Extract `ComponentType` in the protobuf schema
- Extract webui components to their own modules
- Add logging to `webui`
- Add `tailwindcss`
- Fix `wasm-bindgen` version based on flakes
- Track binarien version in `dev-deps`
- Add `trunk`,`wasm-bindgen` to dev-deps
- Add `webui-builder`
- Add `http://` prefix when starting http servers
- Remove CORS filter
- Remove unused `prost-build`
- Add and enable `tonic-web`
- Update the readme
- Bump `flake.lock`
- [**breaking**] Rename `max_inflight_instances` default to `unlimited`
- [**breaking**] Drop `default_` from `max_retries`,`retry_exp_backoff`
- Update the milestones
- Fix `release-3-upload-artifacts.yml`
- Fix clippy
- Return executions in a specific order in `list_executions`
- Use `can_be_retried_after` in `expired_timers_watcher`
- Fix clippy
- Fix clippy
- Add name to `tx_begin` and `tx_commit` spans
- *(db)* [**breaking**] Change index and query for `get_pending`
- *(db)* [**breaking**] Add `t_ffqn`
- *(sqlite)* Avoid looking up `ffqn` when possible in `update_state`
- *(sqlite)* Add index on `(id,variant)` of `t_execution_log`
- *(sqlite)* Add index on `(id,version)` of `t_execution_log`
- Make spans usable on debug level
- *(sqlite)* Tweak pragma settings
- Change level for a trace message
- *(sqlite)* [**breaking**] Add `state` column to `t_state`
- Do not bail out `lock_pending` if a row fails to lock
- Verify JSON representation
- Replace an `unwrap` with `expect`
- Fix clippy issues

## [0.6.1](https://github.com/obeli-sk/obelisk/compare/v0.6.0...v0.6.1) - 2024-10-19

### Other

- Rollback to a previous `flake.lock` to unblock musl build
- Use libc variant in `ubuntu-24.04-binstall`
- Disable `fail-fast`
- Make `latest` configurable in `release-5`
- Decouple yaml source from target ref for `release-3`
- Disable musl build
- Update the readme

## [0.6.0](https://github.com/obeli-sk/obelisk/compare/v0.5.0...v0.6.0) - 2024-10-18

### Added

- [**breaking**] Remove `default_max_retries` config for workflows
- Add retry config to workflows
- Remove retry override in workflows
- Remove retry override in webhooks
- [**breaking**] Add `closing` flag to `BlockedByJoinSet`
- [**breaking**] Limit webhook calls to direct and `-schedule`
- Add leeway to `TimersWatcherConfig`
- Allow configuring limits for `max_inflight_*`
- Allow disabling `retry_on_err` in activities
- [**breaking**] Add `result_kind` to `PendingStateFinished`
- [**breaking**] Track `config_id` in `Locked` state
- [**breaking**] Implement structured concurency for child executions

### Fixed

- Subject joinset closing to the execution timeout
- Fix serde of `PendingStateFinishedError`
- Set sqlite's `cache_size` to 10MB

### Other

- Change type of `join-set-id` to a `Resource`
- Update the readme
- Update version of `oci-wasm`
- Run `cargo upgrade`
- Fix clippy
- Bump rust to 1.82
- [**breaking**] Rename `non-determinism` to `nondeterminism`
- Remove `ImportableType`
- Update docs
- Add `sleep-activity-submit` for join set testing
- Add `server_verify` test
- Removev `ConfigStore`
- Update the milestones
- Extract `webhook` module
- Extract `activity` module
- Extract `workflow` module
- Change sqlite's `mmap_size`, `page_size`
- Set sqlite's `temp_store` to `MEMORY`
- Merge `ChildExecutionRequest`,`DelayRequest` into `DbUpdatedByWorker`
- [**breaking**] Use newtypes for `execution_id`,`join-set-id`
- Fix clippy
- Extract `WorkflowWorker::convert_result`
- Extract `ApplyError`
- Remove `FunctionMetadataNotFound`
- Extract `WorkflowWorker::race_func_with_timeout`
- Ignore post return fn errors in workflow worker
- Extract `WorkflowWorker::prepare_func`
- Extract `WorkflowWorker::call_func`
- Log `execution_id` upon processing the request
- Extract `get_called_function_metadata`
- Add `http_get_fallible_err`

## [0.5.0](https://github.com/obeli-sk/obelisk/compare/v0.4.0...v0.5.0) - 2024-10-11

### Added

- Filter out redundant pending state changes
- [**breaking**] Use `__` as separator in envvars overriding the TOML
- Do not require native TLS certs on HTTP
- Retry when an OCI registry returns an error
- [**breaking**] Rename table `wasm_activity` to `activity_wasm`
- [**breaking**] Remove abbreviations in `Duration`
- Change nanos-based `Duration` to a `Variant`
- [**breaking**] Expose execution id and execution errors in `-await-next`
- Expose webhooks via `component list`
- [**breaking**] Do not convert permanent timeouts to `Err`
- Implement Tuple de/serialization
- Implement Flags de/serialization
- Implement `Enum` de/serialization
- Implement Variant de-serialization

### Fixed

- Forwarding of std streams in `[[webhook_component]]` must be optional
- Do not block server shutdown with `get_status`
- Include activity settings in `config_id` computation
- Set `PendingAt` with `scheduled_at` set to `lock_expires_at`
- Verify version consistency in `t_state`

### Other

- Remove `expect(dead_code)` in `env_vars`
- Fix clippy
- Bump non-conflicting breaking dependencies
- Bump `flake.nix`
- Run `cargo update`
- Run `cargo upgrade`
- Warn on missing metadata digest
- [**breaking**] Replace `get` with `get_execution_event`
- Remove logging from fibo workflow
- Unify docker build target in `flake.nix`
- Update milestones
- Tweak `fibo_start_fiboas` to start `fiboa_concurrent`
- Update milestones
- Bump test components
- Update the project plan
- Add more specific logging on OCI pull failure
- Use `ConfigStore` solely for `ConfigId` computation
- Extract `ComponentConfigImportable`
- Rename `ImportableType`
- Rename `ComponentConfig`
- Replace `allow` with `expect`
- Rename `ImportType`
- Fix finished exec log discrepancy in mem db
- Fix clippy
- Fix clippy
- Use `ChildReturnValue` to avoid a db lookup
- [**breaking**] Remove details from `PermanentFailure`
- Format sources
- Fix a clippy warning
- Add index `idx_t_state_expired_timers`
- Fix clippy
- Update test snapshots
- Update test snapshots
- Move `execution_error_tuple` back to `wasm_tools`
- Use `ActivityConfig` in `ActivityWorker`

## [0.4.0](https://github.com/obeli-sk/obelisk/compare/v0.3.0...v0.4.0) - 2024-10-02

### Added

- Verify webhook imports
- Preinstantiate in `WorkflowWorkerLinked`
- Add `server verify`
- Allow `component list` filtering out the extensions
- Verify component imports during startup
- Compile webhook in parallel to other components
- [**breaking**] Reintroduce topmost parent
- Introduce business spans
- Add tracing ctx forwarding on the gRPC boundary
- Link scheduled execution to its origin
- Propagate trace ctx from server submit to executor
- [**breaking**] Replace `correlation_id` with `ExecutionMetadata`
- [**breaking**] Remove `recycle_instances` setting from activities
- Allow setting or forwarding envvars to activities, webhooks
- Add `server generate-config`
- Allow specifying TOML config path
- Allow forwarding std streams for activities
- Make std stream forwarding configurable
- Log webhook guest out,err to host's stderr
- Add `correlation_id` to gRPC definition, CLI switch
- [**breaking**] Change `topmost_parent_id` to `correlation_id`
- [**breaking**] Replace dynamic `schedule` with `-schedule` suffix for workflows and webhooks
- Show obelisk types in function signatures
- [**breaking**] Extract `types.wit`, use `duration` instead of millis
- Add logging support for activities
- Add logging capability to workflows and webhooks

### Fixed

- Fail on `start_webhooks` error
- Parse `obelisk.toml` only when starting the server
- Skip `wasi:` namespace from mocking in webhooks
- Verify activity imports in `new_with_config`
- Reenable epoch interruption in webhook components

### Other

- Use `StrVariant` in `ParameterType`, `ReturnType`
- Rename `ComponentType::ActivityWasm`
- Mock webhook imports based on available exports
- Extract `WebhookCompiled`
- Rename `ConfigIdType::ActivityWasm`
- Rename `ConfigIdType::WebhookWasm`
- Fix clippy
- Bump sleep workflow and activity
- Link workflows during verification
- Fix clippy
- Extract `ExIm::enrich_exports_with_extensions`
- Remove `Optional` from `Component.exports`
- Fix clippy
- Simplify `inspect_fns`
- Bump `example_workflow_http_get`
- Extract `ConfigIdType` from `ComponentType`
- Remove `Component` from `WebhookInstanceAndRoutes`
- Move workflow linking to `into_worker`
- Rename `ConfigVerified`
- Fix clippy
- Extract `WorkflowWorkerPre`, `ComponentConfigRegistryRO`
- Update the readme
- Introduce `ComponentConfigRegistryRO`
- Remove long lived `executor` span
- Unifiy SIGINT handling
- Rename `component_to_instance` to `prespawn_webhook_instance`
- Fix clippy
- Bump `shadow-rs`
- Bump `oci-wasm` to v0.0.5
- Fix deprecation warning in `tonic_build`
- Update milestones
- Run `cargo update`
- Run `cargo upgrade`
- Bump test components
- Filter out everything on an empty `level`
- Enable trace level for component logs by default
- Enable backtrace in test scripts
- Update the readme
- *(deps)* Bump cachix/install-nix-action from V28 to 29
- Replace `SpanKind` with a single span
- Add sleep workflow and activity to the default TOML
- Update the readme
- Add `config_id` to the `worker` span
- Add execution related attributes to `submit`
- Fix clippy
- Bump version of `example_workflow_fibo` in TOML
- Propagate context to child executions
- Extract `ServerInit`
- Revert `vscode-check.sh`
- Update the readme
- Use `DbPool` in `expired_timers_watcher`
- Move component parsing to `webhook_trigger::component_to_instance`
- Block output of `rustfmt` in `vscode-check.sh`
- Move retry override to `WebhookInstance`
- Make sure the generated bindings are formatted
- Regenerate bindings in `clippy.sh`
- Add span around webhook instantiation
- Bump `wasmtime` to version 25
- Require `rustc` version 1.81.0
- Bump flake.nix
- Lower spans to debug for tx functions
- Instrument engine initialization
- Make `write_codegen_config` async
- Fix parent span when calling `WebhookComponent::fetch_and_verify`
- Disable `server generate-config` for debug builds
- Replace `Clone` impl with `derivative`
- Move sqlite config to TOML
- [**breaking**] Move sqlite configuration into a new table
- Add `None` to toml's `StdOutput`
- Update the milestones
- Update readme and TOML configuration comments
- Use `StrVariant` in `ConfigId`
- Bump to latest `example_activity_http_get`
- Push the latest `example_webhook_fibo`
- Rename `ComponentType::WebhookComponent`
- Log failed tests on DEBUG level
- Add `name` to `ConfigId`
- [**breaking**] Rename `webhook` to `webhook_component`
- Update the readme
- Fix clippy
- [**breaking**] Switch `correlation_id` type to `StrVariant`
- Update comments in the TOML configuration file
- Fail curl on non-successful status codes
- Change `_private` marker to `non_exhaustive`
- Rename `ConfigId::dummy_activity()`
- Work around `Span::current` giving a disabled span
- Rename `Params::default()` to `Params::empty()`
- Remove `CancelRequest`
- [**breaking**] Serialize `ConfigId` to a string
- Rename `ConfigIdParseError`
- Fix clippy
- Fix clippy
- Print notification send status to logs
- Lower `sqlite_dao` spans by one level
- Lower level of `notify_` spans
- Connect transaction func to the current span directly
- Lower the span level for `conn_low_prio`
- Lower log level of the shutdown message
- Get rid of `expired_timers_watcher` span
- Update test snapshots
- Fix `exports_imports`
- Check that `-obelisk-ext` is not exported by a WASM component
- Move `SUFFIX_PKG_EXT` to `wasm_tools`
- Rename `component` to `wasmtime_component`
- Update snapshots
- Turn `instance.get_func` not found into a panic
- Turn `exported_ffqn_to_index` miss into a panic
- Log component messages with a separate target
- Introduce `component_logger`
- Fix `workflow_ctx`
- Fix clippy
- Set `millis` to u32 to work around integer overflow
- Fix renaming the local variable
- Rename local variable
- Use `InstancePre` in the activity worker
- Fix clippy
- Simplify `wasi:http` linking in `activity_worker`
- Remove `WebhookComponentInstantiationError`
- Fix `write_codegen_config` for madsim
- Fix clippy

## [0.3.0](https://github.com/obeli-sk/obelisk/compare/v0.2.2...v0.3.0) - 2024-09-13

### Added

- Add `--clean-codegen-cache` flag
- Implement request timeouts for webhook servers
- Implement `-submit`, `-await-next` extensions for webhooks
- Add host activities, `-submit` function handling to webhooks
- [**breaking**] Rename generated function suffix from `future` to `-submit`
- Expose webhooks as gRPC `Component` items
- Implement fetching webhooks from OCI registries
- [**breaking**] Stop exposing `file_path` of a `Component` over gRPC
- Spawn webhook triggers on http servers, configured via TOML
- Make retry overrides on workflows optional
- Implement route matching for the webhook trigger
- Allow `webhook_trigger` to submit child executions and await results
- Make `api_listening_addr` mandatory in the TOML configuration
- Display prettified JSON output of the execution result
- Remove `enabled` key from TOML config
- Handle Ctrl-C signal while downloading the images
- Allow cleaning db and caches with separate flags

### Fixed

- Fix a small race in `subscribe_to_pending`
- Add index on `t_state` for `lock_pending` performance
- Disable epoch interruption for webhooks temporarily
- Re-enable epoch interruption for activity engine
- Keep the epoch ticker for the whole server lifetime
- Do not allow retries on child workflows
- Make retry configuration specific for `wasm_activity` TOML table
- Configure rolling appender using its own properties
- Enable tls for OTEL endpoint
- Delete `-wal` and `-shm` files on `--clean`

### Other

- Merge pull request [#24](https://github.com/obeli-sk/obelisk/pull/24) from obeli-sk/dependabot/github_actions/cachix/install-nix-action-V28
- Add `build_webhook` to `cargo-component-builder`
- Update scripts and config
- Fix clippy issues
- Introduce priorities for sqlite commands
- Replace `async-sqlite` with a dedicated thread
- Make all test steps share the same settings to improve compile times
- Allow system thread for `wasmtime_cache`
- Fix tests for madsim
- Disable parallel execution in the "Populate the codegen cache" step
- Populate the codegen cache in a separate step
- Add `populate_codegen_cache` for activities,workflows,webhooks
- Disable parallel test execution
- Extract `RequestHandler`
- Update TOML configuration documentation
- Turn the epoch ticker from a task to a thread
- Improve test executor logging by (ab)using `config_id`
- Update fibo webhook to use `-submit`, `-await-next`
- Fix messages after `-submit` change
- Rename fibo workflow extension wit function to use `-submit` suffix
- Reorganize `test-programs` wit files
- Make tests write logs with level info and above
- Move `fibo-ext.wit` to `activity/wit/ext`
- Move fibo webhook to `testing/test-programs/fibo/webhook`
- Fix typo, clean up `component list` output
- Rename `ObeliskConfig` fields to plural
- Rename `Workflow` to `WorkflowToml`
- Rename `WasmActivityToml`
- Rename `ActivityConfigVerified`, `WasmActivityConfig`
- Rename `WorkflowConfigVerified`
- Rename `webhook_trigger::RetryConfig` to `RetryConfigOverride`
- Fix madsim tests
- Make `Component::exports` optional to accomodate for webhooks
- Extract `ExecConfigToml` from `ComponentCommon` to accomodate for webhooks
- Add `ConfigStore::WebhookV1`
- Add `ComponentType::Webhook`
- Rename `ComponentConfigHash` to `ConfigId`
- Extract `VerifiedConfig`
- Move webhook validation to `fetch_and_verify_all`
- Rename `ComponentType::WasmWorkflow` to `Workflow`
- Add `git-cliff` to dev shell dependencies
- Check the whole workspace with clippy
- Fix clippy attributes on generated code
- Swap clippy `allow`ances to `expect`ations
- Bump `dev-deps.txt`
- Bump `rustc` to 1.81
- Fix clippy issues
- Use error 503 when number of core instances is exceeded
- Use params instead of named wildcards in the webhook routing test
- Fix clippy issues
- Rename `replay_or_interrupt` to `apply`
- Update the milestones
- Add a hardcoded `call_imported_fn` to `webhook_trigger`
- Rename http trigger to webhook trigger
- Add builder for `trigger-http-simple`
- Remove dead `patch` test activity and workflow
- Reorganize fibo's wit files
- Removev `http_trigger` server test
- Fix clippy issues
- Update milestones
- Add incomplete `http_trigger`  with hardcoded configuration
- Enable all default features of `wasmtime`
- Update docs, todos, draft webhook configuration
- Update milestones
- Sync `wit-bindgen-rt` version with current `cargo-component`
- Add `trigger-http-simple`
- Add `wasmtime` to the dev shell
- Update the milestones
- Add license to `wasi_http`
- Patch `with_printer`
- Update `wit_printer` from upstream
- Bump `wasmtime` to version 24.0.0
- Bump `derive_more`
- Add `cargo-edit`
- Run `cargo upgrade`
- Bump `Cargo.lock`
- Bump `flake.lock`
- Update the milestones
- Update the readme
- Simplify printing of the execution result
- Update TOML documentation
- Serialize `JoinNextBlockingStrategy` using snake case
- Update documentation of the TOML configuration file
- Remove deprecated `component_detector`
- Move sha256 checksum to the `obeli-sk` package
- Fix madsim, clippy issues
- Update the readme
- Extract `prespawn_all` to separate compilation from spawing executors
- Use `Arc<dyn Worker>` in `executor` instead of generic `W`
- Make `[otlp]` enabled if the table is present
- Do not repeat sha256sum for files stored in cache on every startup
- Start components in parallel if possible
- Remove double file hash calculation if the hash is correct
- Add more spans around startup
- Update milestones
- Trace `append`, extract `append_batch_create_new_execution_inner`
- Lock `ffqn_to_pending_subscription` only once when batching
- Move `join_all` to `select!`
- Warn on component name clashes
- Allow passing nested configs via envvars separated by `_`
- Replace `JoinSet` with `join_all` due to a madsim limitation
- Allow pulling the images in parallel
- Extract `Digest` from `ContentDigest`
- Track metadata mapping using separate files
- Remove `content_digest` from `ComponentCommon`
- Fetch and verify components before spawning
- Update the readme
- Add `component_name` and `config_id` to `toml`

## [0.2.2](https://github.com/obeli-sk/obelisk/compare/v0.2.1...v0.2.2) - 2024-08-24

### Added
- Track the topmost parent
- Add rolling file appender configurable via TOML
- Configure OTLP level using `EnvFilter` directive in TOML
- Configure stdout logger via TOML
- Make OTLP configurable via toml
- Make Wasmtime's pooling allocator configurable via toml
- Make `OTEL_ENDPOINT` configurable via envvar
- Allow pushing WASM files to OCI registries

### Fixed
- Print the metadata digest on OCI push
- Verify that pulled image's digest matches specified

### Other
- Remove rust cache
- Run `nix flake update`
- Run `cargo update`
- Fix clippy warnings
- Update the readme
- Rename `component_name` clashing with span name
- Fix clippy warnings
- Rename `[activity]` to `[[wasm_activity]]`
- Rename `fetch_and_verify`
- Rename `SupportedFunctionResult` to `SupportedFunctionReturnValue`
- Calculate config store id from `Hash`
- Shorten display of config id
- Rename `obtain_wasm_from_oci` to `pull_to_cache_dir`
- Inform when the gRPC sever is ready
- Make `otlp` feature default
- Make OTLP disabled by default
- Update `obelisk.toml`
- Extract config dumps into separate log fields
- Fix clippy issues
- Rename TOML table `otlp_config` to `otlp`
- Extract module `config_holder`
- Avoid a subsequent `get` in `get_status`
- Instrument `GetStatus`, `Submit`
- Fix clippy warnings
- Move `to_channel` to `grpc_util`
- Rename feature `otel` to `otlp`
- Make `tracing-chrome` optional
- Refactor `load_components` into `spawn_tasks`
- Shutdown otel provider in `Guard::drop`
- Make OTEL optional
- Remove failing version `tracing-opentelemetry=0.25`
- Lower level of some spans
- Record executor id, name, config id in spans
- Remove `ExecutorId` from `obelisk.proto`
- Move exec spans to top level so that the init span finishes on startup
- Downgrade, add `shutdown_tracer_provider`
- Integrate with latest tracing-otlp - missing root spans on shutdown
- Update the readme
- Update the readme
- Move test component images to `getobelisk`
- Remove `wkg`
- Tag docker image with version-os to allow sorting
- Fix version tagging
- Allow specifying target ref in `release-5-push-docker-image`
- Quiet down wget
- Log commands to stdout

## [0.2.1](https://github.com/obeli-sk/obelisk/compare/v0.2.0...v0.2.1) - 2024-08-11

### Added
- Enable TLS on the client

### Other
- Tag docker images with current version
- Report correct name when printing the version

## [0.2.0](https://github.com/obeli-sk/obelisk/compare/v0.1.15...v0.2.0) - 2024-08-11

### Added
- [**breaking**] Use `server run` instead of `daemon serve`
- Make client url configurable, separate client commands

### Other
- Push alpine-based image
- Fix github token in `release-2-plz-release`
- Push the image to docker hub

## [0.1.15](https://github.com/obeli-sk/obelisk/compare/v0.1.14...v0.1.15) - 2024-08-10

### Added
- Support `api_listening_addr`

### Other
- Extract binary from tgz outside of docker build
- Rename workflows
- Build ubuntu-based docker image
- Rename release workflows
- Update `dev-deps.txt`
- Update `flake.lock`
- Check dev-deps.txt in the `check` workflow
- Add and run `dev-deps.sh`
- Add docs and unify bash settings for scripts
- Add `release-verify`

## [0.1.14](https://github.com/obeli-sk/obelisk/compare/v0.1.13...v0.1.14) - 2024-08-09

### Other
- Trigger `release-artifacts` on release
- Build the release artifacts on ubuntu 20.04

## [0.1.13](https://github.com/obeli-sk/obelisk/compare/v0.1.12...v0.1.13) - 2024-08-09

### Other
- Fix elf interpreter in `obelisk-patch-for-generic-linux`
- Update the readme
- Add `tar.gz` suffix to binary artifacts

## [0.1.12](https://github.com/obeli-sk/obelisk/compare/v0.1.11...v0.1.12) - 2024-08-08

### Other
- update Cargo.toml dependencies

## [0.1.11](https://github.com/obeli-sk/obelisk/compare/v0.1.10...v0.1.11) - 2024-08-07

### Added
- Make `non_blocking_event_batching` configurable
- Enable codegen cache by default
- Expose functions and components via gRPC
- Show function return type in WIT format
- Show parameter types in WIT format
- Add `--follow` flag to `schedule` and `get`
- Show pending state stream when submitting new execution
- Add `StreamStatus` rpc
- Type check `params` in `submit` of the execution
- Enable codegen cache
- Make `oci.wasm_directory` configurable

### Fixed
- Add index on `t_join_set_response`
- Allow partial configuration of `CodegenCache`, `ExecConfig`
- Return `NotFound` in `SqlitePool::get`
- Check params and types cardinality in `Params::as_vals`

### Other
- Tweak logs of intermittent failures
- Rename `execution schedule` to `execution submit`
- Fix snapshots broken by adding `fibo-start-fiboas`
- Add `machine_readable_logs` switch
- Rename `executor` command to `daemon`
- Update the readme
- Add seconds handling to `jq-analyze-span-close.sh`
- Remove `first_locked_at` from the `Finished` message
- Add `fibo-start-fiboas` to test workflow hierarchies
- Replace db-backed component registry with `ComponentConfigRegistry`
- Replace panics with Err in `enrich_function_params`
- Add `scripts` folder
- Fix clippy issues
- Merge `StreamStatus` into `GetStatus`
- Update the readme
- Rename `location.file` to `location.path` in toml
- Measure WASM parsing time
- Simplify `VecVisitor`
- Use `Params::deserialize` instead of `Params::from_json_value`
- Represent Params as `Vec<Value>` instead of `Value`
- Switch execution submission to gRPC
- Update the readme
- Extract `Engines::configure_common`
- Update readme
- Fix a clippy issue
- Remove single thread constraint from `nextest`
- Speed up test by replacing `wit_parser` with a dummy fn_registry
- Move `write_codegen_config` to `wasm-workers`
- Add allowance for `cfg(madsim)`
- Bump Rust to 1.80
- Put the sqlite file to the data dir by default
- Remove unused import
- Extract `ComponentLocation::obtain_wasm`
- Simplify sqlite initialization
- Remove unused clippy allowances
- Move `indexmap` feature selection to workspace deps

## [0.1.10](https://github.com/obeli-sk/obelisk/compare/v0.1.9...v0.1.10) - 2024-07-23

### Other
- update Cargo.toml dependencies

## [0.1.9](https://github.com/obeli-sk/obelisk/compare/v0.1.8...v0.1.9) - 2024-07-23

### Other
- update Cargo.toml dependencies
- Lookup exported function indexes upfront
- Bump `wasmtime` to 23.0.1

## [0.1.8](https://github.com/obeli-sk/obelisk/compare/v0.1.7...v0.1.8) - 2024-07-22

### Added
- Put the database to the user's data directory
- Clean wasm cache using `--clean` switch
- Pull wasm files from an OCI registry
- Make pooling allocator configurable via envvars

### Fixed
- Specify `wasm-pkg-common-0.4.1` hash in `flake.nix`
- Use `wasm32-wasip1` when building activities
- Drop all futures after `select!`
- Create the workdir in the docker image
- Do not deactivate component on instantiation error

### Other
- Fix madsim test
- Fix clippy issues
- Add section about building the docker image
- Unify `wasm-pkg-common` revision between cargo and nix
- Improve configuration file logging
- Read `max_retries` from db when submitting
- Update readme
- Improve instrumentation for `expired_timers_watcher`
- Move cache dir creation to `server`
- Simplify `ExecTask::new`
- Add parent span to `ExecutorTaskHandle`
- Compact log messages, putting span fields at the end
- Move back `wkg` to `packages` to improve `nix develop` build time
- Switch to `wasm32-wasip1` to satisfy `cargo-component` v0.14.0
- Fix a clippy issue
- Bump dependencies
- Switch to `bytecodealliance/wasm-pkg-tools`
- Bump `flake.nix`
- Bump `Cargo.lock`
- Fix clippy issues
- Reenable submitting executions from CLI
- Remove config watcher
- Disable old components on startup
- Enable configured components on startup
- Replace `ConfigId` and `ComponentId` with `ComponentConfigHash`
- Readd component persistence to sqlite
- Cache the oci metadata fetch into a json file
- Clean up unused dependencies in `flake.nix`
- Add more workflow configuration settings
- Add config support for `ExecConfig`
- Use snake_case for configuration keys
- Filter components by `enabled`
- Update the readme
- Spawn executors for components without refresh
- Remove unnecessary pinning in `activity_worker`
- Merge loading and watching into `load_config_watch_changes`
- Add support for `obelisk.toml` live reload
- Fix madsim tests
- Introduce `FunctionRegistry`, remove `component_*` from `storage`
- Bump revision of `wasm-pkg-tools`
- Add `ComponentLocation`
- Sort component list by update date, generalize `component_toggle`
- Compute the sha256 digest using `wasm-pkg-common`
- Introduce `ComponentToggle`
- Rename component's `file_name` to `name`
- Remove unused Dockerfile
- Add `wkg` build to nix
- Increase timeout to 40s
- Show component compilation times
- Test with a single thread to get more accurate durations
- Add `http_get_simple`
- Show exported function types in `component inspect`
- Lower connection timeout in http activity to pass CI
- Remove `wasm` feature, supress clippy warnings
- Simplify http testing wit signatures
- Switch http test activity client to `waki`
- Update the readme
- Add docker image containing `/bin/sh`
- Auto-detect whether to use pooling allocator
- Update the readme
- Extract `engines`
- Update the readme
- Fix clippy issues
- Bump Wasmtime to 22.0.0
- Dynamically determine instance allocation strategy
- Add `/bin/obelisk` symlink, set workdir to `/data`
- Build static executable with musl for docker image
- Build docker image with nix
- Add `Dockerfile`
- Improve build times by disabling debug info
- Simplify `lock_pending` error reporting in `executor`
- Add `component_id` to `CreateRequest`
- Detect workflows based on interface fqn
- Add `cargo-binstall` to the nix shell
- Fix a typo
- Bump rust to 1.79
- Disable test check in flake package
- Update readme
- Add flake package
- Clean up Cargo.toml after removing cargo-dist
- Replace madsim patch with new upstream version
- Delete the `cargo-dist` based workflow
- update Cargo.toml dependencies

## [0.1.7](https://github.com/obeli-sk/obeli-sk/compare/v0.1.6...v0.1.7) - 2024-06-13

### Other
- Add binstall metadata

## [0.1.6](https://github.com/obeli-sk/obeli-sk/compare/v0.1.5...v0.1.6) - 2024-06-13

### Other
- Use fine-grained token to create the GH release

## [0.1.5](https://github.com/obeli-sk/obeli-sk/compare/v0.1.4...v0.1.5) - 2024-06-13

### Other
- Switch `release-artifacts` trigger to `released`

## [0.1.4](https://github.com/obeli-sk/obeli-sk/compare/v0.1.3...v0.1.4) - 2024-06-13

### Other
- Add `publish-artifacts.yml`

## [0.1.3](https://github.com/obeli-sk/obeli-sk/compare/v0.1.2...v0.1.3) - 2024-06-12

### Other
- Remove tag creation from `release.yml`
- Fix dependencies for release
- Add `--allow-dirty` to `cargo dist`

## [0.1.2](https://github.com/obeli-sk/obeli-sk/compare/v0.1.1...v0.1.2) - 2024-06-12

### Other
- Pass tag to `cargo dist`
- Remove `debug-assertions` from release profile
- Pass tag to checkout steps of `release.yml`
- Add cargo-dist
- Run `nix flake update`

## [0.1.1](https://github.com/obeli-sk/obeli-sk/releases/tag/obeli-sk-v0.1.1) - 2024-06-11

### Other
- Switch to published `async-sqlite` fork

## [0.1.0](https://github.com/obeli-sk/obeli-sk/releases/tag/obeli-sk-v0.1.0) - 2024-06-11
Initial release - yanked
