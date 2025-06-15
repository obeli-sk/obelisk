# Changelog
All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.23.1](https://github.com/obeli-sk/obelisk/compare/v0.23.0...v0.23.1)

### ⛰️ Features

- Allow missing JSON fields for optional record fields - ([f5c5049](https://github.com/obeli-sk/obelisk/commit/f5c5049ff69ff7690942beee47ac3c4daafcff8c))

### 🐛 Bug Fixes

- Fix order of record field deserialization - ([7c5109c](https://github.com/obeli-sk/obelisk/commit/7c5109c5eb8762fada62faaddeed72ed20abbd29))
- Report missing record fields on deser - ([c54a366](https://github.com/obeli-sk/obelisk/commit/c54a36640d7f698df2408393055c9ba9ef5ed9d5))

### 🚜 Refactor

- Add `AbortOnDropHandle::new` - ([3574253](https://github.com/obeli-sk/obelisk/commit/3574253f98b043bcc2ee36a8eaa006922351571c))


## [0.23.0](https://github.com/obeli-sk/obelisk/compare/v0.22.5...v0.23.0)

Adds support for process spawning and disk IO in activities. Introduces WIT for local process control, and refactors TOML configuration. Includes a breaking change in cache/allocator config.

### ⛰️ Features

- *(activity)* Use process groups - ([659d8d5](https://github.com/obeli-sk/obelisk/commit/659d8d5fdee7b43cf5a64d3828a260f67fbedaa5))
- *(activity)* Implement stdio for child processes - ([372c2f7](https://github.com/obeli-sk/obelisk/commit/372c2f794c4c5197e6af69ed13fce18239ef1ea5))
- *(activity)* Allow spawning of local processes - ([a5a84f7](https://github.com/obeli-sk/obelisk/commit/a5a84f7832f456b7817b11c7e395245b160bae46))
- *(activity)* Allow disabling the preopened directories cleaner - ([6d840d1](https://github.com/obeli-sk/obelisk/commit/6d840d12e799277530f8e0e2f160ad3cb37eef9f))
- *(toml)* Allow enabling local process spawning - ([443619b](https://github.com/obeli-sk/obelisk/commit/443619bff3cfde0493f63067928e7e2a6ab94856))
- *(toml,activity)* Add periodic preopen dir cleanup - ([d449831](https://github.com/obeli-sk/obelisk/commit/d449831356eeb89853437a925d8c734c7ba01702))
- *(wit)* Add `subscribe-wait` - ([47c6ad5](https://github.com/obeli-sk/obelisk/commit/47c6ad52fa9e1dc64e98fab5647afde1055d5095))
- Add preopened dir support to activities - ([d868e71](https://github.com/obeli-sk/obelisk/commit/d868e71a1189d1724b4e4d1ee72233207b3bad80))

### 🚜 Refactor

- *(toml)* [**breaking**] Move cache and allocator config to `wasm` - ([1c58141](https://github.com/obeli-sk/obelisk/commit/1c581413a9870ad53596e8cce114a84186300b39))
- *(toml)* Change default `cleanup.older_than` to 5 mins - ([ea342f2](https://github.com/obeli-sk/obelisk/commit/ea342f2d7ec30fe68da44bf35c5d99774c2b4d71))
- *(wit)* Change type of `child-process.id` to `u32` - ([ece7de7](https://github.com/obeli-sk/obelisk/commit/ece7de7e24753418a09ecea0b0fb97a3dfb0abb1))
b967706217968d3f1381e34ef58a240b506f957c))
- Allow sqlite shutdown during batch processing - ([01e11a1](https://github.com/obeli-sk/obelisk/commit

## [0.22.5](https://github.com/obeli-sk/obelisk/compare/v0.22.4...v0.22.5)

### ⛰️ Features

- *(cli)* Remove backtrace printing - ([fd957a5](https://github.com/obeli-sk/obelisk/commit/fd957a5ef8613a696ad8491eba6767989c3a843f))

### 🐛 Bug Fixes

- Lower `verify_imports` error to a warning - ([e3a1909](https://github.com/obeli-sk/obelisk/commit/e3a1909a8db1ceb2d413b2bcdd93e3c78eace4d2))

### 📚 Documentation

- Update the roadmap - ([ad74fb1](https://github.com/obeli-sk/obelisk/commit/ad74fb1039efa71d4a308d039a76c99b1cf37879))
- Describe `stub_wasi` - ([8eac345](https://github.com/obeli-sk/obelisk/commit/8eac34528120a22be3e12985f622d3fa3d61ef9c))

### 🚜 Refactor

- Drop "com" from `ProjectDir`'s qualifier - ([ee16178](https://github.com/obeli-sk/obelisk/commit/ee16178b4f93e454d92a610e83fec36b07fa6fe6))
- Use `tokio::task::yield_now` on epoch interruption - ([c3b9799](https://github.com/obeli-sk/obelisk/commit/c3b979975946e25bec38b3feabd9e3b07d1b9244))


## [0.22.4](https://github.com/obeli-sk/obelisk/compare/v0.22.3...v0.22.4)

Adds experimental support for creating workflows in TinyGo.

### ⛰️ Features

- *(workflow)* Add preliminary support for `stub_wasi` - ([ed8a297](https://github.com/obeli-sk/obelisk/commit/ed8a297e3e812b2fa3428768e013e83d59097f79))

### 🐛 Bug Fixes

- *(cli)* Work around dots in function name - ([c382787](https://github.com/obeli-sk/obelisk/commit/c382787fd1a72c6cd145c7fb34ffa90ca4959468))
- *(workflow)* Switch to minimal impl for supporting  `stub_wasi` - ([2babfec](https://github.com/obeli-sk/obelisk/commit/2babfec04afe9fc44cea1eb79c17d59105e99734))

### 📚 Documentation

- Update README.md - ([8532630](https://github.com/obeli-sk/obelisk/commit/8532630a8fa78d382ec87e07b7cdbd334ed10073))

### 🚜 Refactor

- *(worker)* Rename `extract_exported_ffqns_noext` - ([6c27f6c](https://github.com/obeli-sk/obelisk/commit/6c27f6cc958824901ec0e4c712a8d42eecb78faa))
- Remove `Worker.imported_functions` - ([8204459](https://github.com/obeli-sk/obelisk/commit/8204459bad8ce029bdc183776fafea2f33c0a6e5))


## [0.22.3](https://github.com/obeli-sk/obelisk/compare/v0.22.2...v0.22.3)

Fixes instantiation error that blocked starting components built by TinyGo.

### 🐛 Bug Fixes

- *(runtime)* Configure epoch callback before instantiation - ([8056afe](https://github.com/obeli-sk/obelisk/commit/8056afee78894c011699ab034d04d57713e67a6a))


## [0.22.2](https://github.com/obeli-sk/obelisk/compare/v0.22.1...v0.22.2)

Fixes missing host export that blocked components generated by [ComponentizeJs](https://github.com/bytecodealliance/ComponentizeJS) from running.

### 🐛 Bug Fixes

- *(webhook)* Link `obelisk:types@1.1.0` in webhook - ([b47c826](https://github.com/obeli-sk/obelisk/commit/b47c826aa36de4ee2b9fdcf383c07a228430899f))


## [0.22.1](https://github.com/obeli-sk/obelisk/compare/v0.22.0...v0.22.1)

### 🐛 Bug Fixes

- Catch webhooks importing `workflow-support` in `verify_imports` - ([39f7a05](https://github.com/obeli-sk/obelisk/commit/39f7a05f7b3ae448c26a3a6f77a11c06d8816d9c))
- Allow workflows to use `join-set-id`.`id()` - ([e14c71d](https://github.com/obeli-sk/obelisk/commit/e14c71d2246aa5d1acefe1e9bc01b12f556bc8d9))


## [0.22.0](https://github.com/obeli-sk/obelisk/compare/v0.21.0...v0.22.0)

This release requires components to upgrade to version 1.1 of `obelisk:types` and `obelisk:workflow`.
The main motivation is to work around a bug in [ComponentizeJs](https://github.com/bytecodealliance/ComponentizeJS),
which enables writing workflows and activities in JavaScript.
There are two changes in the WIT files:
* Removed `closing-strategy` variant `cancel`, which was not implemeted yet
* Added `id` function to `join-set-id` resource. This is the workaround.

### ⛰️ Features

- *(cli)* Skip printing the error twice when reconnect is enabled - ([8e9850f](https://github.com/obeli-sk/obelisk/commit/8e9850faa504e007f4a7cc8cbe9dab0c4da9f310))
- *(wit)* [**breaking**] Switch `types`,`workflow-support` to 1.1.0 - ([fc031ab](https://github.com/obeli-sk/obelisk/commit/fc031ab7700096984263433f70d4c7df4c4bfdf0))
- *(wit)* Add `types`,`workflow-support` version 1.1.0 - ([51d7367](https://github.com/obeli-sk/obelisk/commit/51d736751d4384aff54d6031bc620a185fb790ee))

### 🚜 Refactor

- *(workflow)* Disregard `closing-strategy` - ([6822d09](https://github.com/obeli-sk/obelisk/commit/6822d0946a3f2375940eae93cf92f3aea99f51b5))
- *(workflow)* Move generated types and support to `v1_0_0` - ([4ac8b09](https://github.com/obeli-sk/obelisk/commit/4ac8b09809e8d72945323d6087bd35d355cbfaac))


## [0.21.0](https://github.com/obeli-sk/obelisk/compare/v0.20.0...v0.21.0)

Performance: Workflows that make progress after being locked are now unlocked and allowed to continue immediately. Previously they were sometimes marked as timed out
and became pending only after backoff period.

**Breaking (sqlite)**: This release fixes performance regression in sqlite introduced in v0.20.0, and changes the SQL schema. Use `--clean-db` to wipe out the old
sqlite database files on first run if affected.

**Breaking (toml)**: Setting `workflows.backtrace.persist` was renamed to `wasm.backtrace.persist`.

### ⛰️ Features

- *(db)* [**breaking**] Unlock workflows with progress instead of timing out - ([f193858](https://github.com/obeli-sk/obelisk/commit/f193858b6962b04915d697615368a432cf5f20ef))
- *(toml,sqlite)* Allow customizing PRAGMA statements - ([5d1cbd2](https://github.com/obeli-sk/obelisk/commit/5d1cbd2df82a7921f80448cb19d032fa50a33f4a))
- Decouple backtrace capture from persisting - ([6bd7244](https://github.com/obeli-sk/obelisk/commit/6bd7244014bd1a341a0d97db90663be8b5788c1e))
- Do not capture nor persit backtrace if disabled - ([11c9d4d](https://github.com/obeli-sk/obelisk/commit/11c9d4d41953dcf7aeba9160a32af997f2182c66))

### 🐛 Bug Fixes

- *(sqlite)* Fix `PRAGMA` settings broken by 316ee7a - ([859d335](https://github.com/obeli-sk/obelisk/commit/859d33597321e4f1c965e7ef67d48f2cc3de6578))
- *(toml)* Make `SqliteConfigToml` defaults consistent - ([0f0d8b4](https://github.com/obeli-sk/obelisk/commit/0f0d8b4c699582cab724d8ea99a0bfc603f9ebf5))

### 🚜 Refactor

- *(toml)* [**breaking**] Rename key `workflows` to `wasm` - ([c4f6a3a](https://github.com/obeli-sk/obelisk/commit/c4f6a3a37ff14914967d023888e22f67169c7097))
- Merge `TemporaryTimeoutHandledByWatcher` into `DbUpdatedByWorkerOrWatcher` - ([3572301](https://github.com/obeli-sk/obelisk/commit/3572301d9b3ffe588ec9ce69eb37eca040b06885))
- Remove `interrupt_on_timeout_container` - ([e5031e7](https://github.com/obeli-sk/obelisk/commit/e5031e78e6cb28f0994db234a4e1541bf0f59283))


## [0.20.0](https://github.com/obeli-sk/obelisk/compare/v0.19.3...v0.20.0)

### ⛰️ Features

- *(cli)* Add `generate config-schema` - ([d3f5506](https://github.com/obeli-sk/obelisk/commit/d3f55063d08a397f406f34ca3149f9d5eece200c))
- *(cli)* Add `no-reconnect` and `no-backtrace` to `get` and `submit` - ([eb96ae7](https://github.com/obeli-sk/obelisk/commit/eb96ae78fcb2baf2e53645fbd1af644116134028))

### 🐛 Bug Fixes

- *(cli)* Fix reconnect while polling status stream - ([4a1ef58](https://github.com/obeli-sk/obelisk/commit/4a1ef58d974a1487693453502b9fe11a78464355))

### 📚 Documentation

- Update roadmap - ([860232c](https://github.com/obeli-sk/obelisk/commit/860232c8bcbf3905ce7bcd803ef44e5492d8014d))

### 🚜 Refactor

- *(cli)* Remove `TonicClientResultExt` - ([7cfcba1](https://github.com/obeli-sk/obelisk/commit/7cfcba13db446127035b57c055fb71d876c7ac47))
- *(toml)* [**breaking**] Simplify config attribute `blocking_strategy` - ([d8c216c](https://github.com/obeli-sk/obelisk/commit/d8c216c3fcf2ccf649c9c82ca5e113e210933f33))
- *(toml)* [**breaking**] Make `non_blocking_event_batching` part of `await` strategy - ([3cac492](https://github.com/obeli-sk/obelisk/commit/3cac4925f4ce959c13316d864ead015ea4f26f29))
- Replace `continue` with `filter` when skipping already linked fns - ([7bd8579](https://github.com/obeli-sk/obelisk/commit/7bd8579b0157668cd1fa9083525d89282816c1d5))


## [0.19.3](https://github.com/obeli-sk/obelisk/compare/v0.19.2...v0.19.3)

### 🐛 Bug Fixes

- Use `release` profile when building MacOS artifacts - ([401cf19](https://github.com/obeli-sk/obelisk/commit/401cf193739eaf8600971275a8e7afc9df0416e5))


## [0.19.2](https://github.com/obeli-sk/obelisk/compare/v0.19.1...v0.19.2)

### 🐛 Bug Fixes

- Make sure the runtime is deterministic when using `simd` - ([47728cd](https://github.com/obeli-sk/obelisk/commit/47728cde20d1c2cdd0edafc390feb726adffc00e))

### 📚 Documentation

- Add screencast - ([1e00057](https://github.com/obeli-sk/obelisk/commit/1e0005745a32651827fd1593bb80c28f33dac5e1))

### 🚜 Refactor

- Cross-compile Linux and MacOS targets from `nix` using [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild).
- Remove git sha from version - ([b575c5e](https://github.com/obeli-sk/obelisk/commit/b575c5e07159a0463e0c5389954956ede76ae54e))
- Remove `shadow-rs` - ([7aa0847](https://github.com/obeli-sk/obelisk/commit/7aa084763ab6cbc0a45860d6626cf380692f36bf))
- Rename devShell `release` to `publish` - ([6b7a852](https://github.com/obeli-sk/obelisk/commit/6b7a85233daab669c0238027e9a144e9036d3f42))


## [0.19.1](https://github.com/obeli-sk/obelisk/compare/v0.19.0...v0.19.1)

### ⛰️ Features

- *(toml)* Allow specifying backtrace sources for webhooks - ([d0563fa](https://github.com/obeli-sk/obelisk/commit/d0563fa318303c215ea55d905fed54d8e336de60))
- *(webhook)* Capture and persist backtrace for direct and scheduled child executions - ([29dac20](https://github.com/obeli-sk/obelisk/commit/29dac20d5ade05f3f599332296dbf5a824540273))
- *(workflow)* Persist backtrace for all `workflow-support` functions - ([30849a6](https://github.com/obeli-sk/obelisk/commit/30849a62c61cb336266ef8d7d29c5b48cd008f70))

### 🐛 Bug Fixes

- *(workflow)* Replace panics with errors in host functions - ([923ace7](https://github.com/obeli-sk/obelisk/commit/923ace71749b3cd2abba9783095daf1c663c6dfc))
- Stop sharing the same one-off join set in webhook - ([7c85d13](https://github.com/obeli-sk/obelisk/commit/7c85d13b64e900307b170a63122e3e08b0e959ad))

### 📚 Documentation

- Update readme, add logo - ([8d302b1](https://github.com/obeli-sk/obelisk/commit/8d302b1687e25304ccffd08b8f585ce1b07eabb7))


## [0.19.0](https://github.com/obeli-sk/obelisk/compare/v0.18.2...v0.19.0)

### ⛰️ Features

- *(db)* Allow querying backtrace by version - ([13910c1](https://github.com/obeli-sk/obelisk/commit/13910c1c9a8cb86d3d7d1269a5657a4a7bd58266))
- *(db)* [**breaking**] Track and persist `HttpClientTrace` - ([6d6fa62](https://github.com/obeli-sk/obelisk/commit/6d6fa62f545e52667f693d6d2c586f05dcdb0c94))
- *(db,grpc)* [**breaking**] Allow listing top level executions only - ([6c72057](https://github.com/obeli-sk/obelisk/commit/6c720576875371c51190950825f155eafa16a758))
- *(grpc)* [**breaking**] Rename `GetLastBacktrace` to `GetBacktrace` - ([1a25e4e](https://github.com/obeli-sk/obelisk/commit/1a25e4eaeda1f40cfb851db954fcd4b46c780650))
- *(grpc)* Allow requesting `backtrace_id` for events - ([1a0bb9b](https://github.com/obeli-sk/obelisk/commit/1a0bb9bc7faa56ac91d6fcbc396d456e71b971dc))
- *(grpc)* Add RPC `ListExecutionEventsAndResponses` - ([9b04d94](https://github.com/obeli-sk/obelisk/commit/9b04d9460c0a621a9ba21f3ff2a3010e2d25bd40))
- *(grpc)* Send the http client traces in rpc `ListExecutionEvents` - ([8931b89](https://github.com/obeli-sk/obelisk/commit/8931b897033db306fd52b84aa5623ce7961774db))
- *(grpc,db)* Add http client traces to `TemporarilyTimedOut` - ([4526ada](https://github.com/obeli-sk/obelisk/commit/4526ada66b6ee8d21892eac16c98f45ac7a6ccd5))
- *(tracing)* Instrument `send_request` - ([72454c0](https://github.com/obeli-sk/obelisk/commit/72454c03962f1c313ae823877469af8cacc90a5c))
- *(webui)* Add backtrace browser - ([6bdca4b](https://github.com/obeli-sk/obelisk/commit/6bdca4b9144952bec9a3b385283a7ebc4ba1aabe))
- *(webui)* Fetch the whole log in trace view - ([0be53fe](https://github.com/obeli-sk/obelisk/commit/0be53fece75045052db08aedd300c0cd4d04303a))
- Start one-off and generated joinsets with index 1 - ([ef63277](https://github.com/obeli-sk/obelisk/commit/ef632772d56ce54293efe8bc5fba33b03fddc819))

### 🐛 Bug Fixes

- *(sqlite)* Fix statement and parameter name in `get_backtrace` - ([9661dcf](https://github.com/obeli-sk/obelisk/commit/9661dcf9cae054c5be5d0ac8e9f372a42b50a3e8))
- Persist `http_client_trace` on activity trap - ([4e1e72a](https://github.com/obeli-sk/obelisk/commit/4e1e72a2b56e7ed51606687966ec8b17f4577157))

### 📚 Documentation

- Update roadmap - ([5bebda2](https://github.com/obeli-sk/obelisk/commit/5bebda2d9e128732058dc646aa220c67931b7804))
- Remove mention of `drop` in `closing-strategy` - ([4b643bc](https://github.com/obeli-sk/obelisk/commit/4b643bcd1176153e1ce41649f6e2abbd32dfd09e))

### ⚡ Performance

- *(webui)* Move syntax highlighting to `source_code_state` - ([b1f3a45](https://github.com/obeli-sk/obelisk/commit/b1f3a45fc7812b977cd1f2908e25c2ee6c90c8ec))

### 🚜 Refactor

- *(cli)* Increase the indent when printing the backtrace - ([ad50a49](https://github.com/obeli-sk/obelisk/commit/ad50a498a9738486b658b175abd4e4867b82c115))
- *(concepts)* [**breaking**] Change derive execution id format - ([83e4b37](https://github.com/obeli-sk/obelisk/commit/83e4b3736ab954c50f883e75039242da3ab397fe))
- *(grpc)* Allow getting first backtrace - ([a9519b7](https://github.com/obeli-sk/obelisk/commit/a9519b735fbcb6619b14b7608fff22f1313ab9ed))
- *(grpc)* [**breaking**] Add `temporarily_` to `failed` and `timed_out` - ([ac2ba90](https://github.com/obeli-sk/obelisk/commit/ac2ba90f28a639422b96659f776f1699f5775ea1))
- *(grpc,db)* Rename `http_client_trace` to `http_client_traces` - ([48f2a1e](https://github.com/obeli-sk/obelisk/commit/48f2a1ee6fad3c67a16b33fce369e9fba24c2efc))
- *(logging)* Change grpc failure logging level to debug - ([798105b](https://github.com/obeli-sk/obelisk/commit/798105b4240358ac86c98fc0b72108ba9853379e))
- *(webui)* Move serialized JSON tree to `json_tree` - ([c3b020e](https://github.com/obeli-sk/obelisk/commit/c3b020e97dbb650fe831395cf9441bae0ce34d33))
- *(webui)* Add `BusyIntervalStatus` - ([d95ecf2](https://github.com/obeli-sk/obelisk/commit/d95ecf2d0bae7431f797913ed7a5f0705f7cb348))
- *(webui)* Extract the effect hook in tracing - ([35c7849](https://github.com/obeli-sk/obelisk/commit/35c784908ec27f2752ce8b95ebbfbfcb764fad28))


## [0.18.2](https://github.com/obeli-sk/obelisk/compare/v0.18.1...v0.18.2)

### ⛰️ Features

- *(cli)* Send execution error via exit code on json output - ([d02e574](https://github.com/obeli-sk/obelisk/commit/d02e574b6db02f62feee32e1befaad499ed14791))

### 🐛 Bug Fixes

- *(cli)* Reconnect only on tonic errors - ([23fa4b5](https://github.com/obeli-sk/obelisk/commit/23fa4b56ebf647bae6f72d1adc24c392017f346e))


## [0.18.1](https://github.com/obeli-sk/obelisk/compare/v0.18.0...v0.18.1)

### ⛰️ Features

- Support path prefixes on both sides of `backtrace.sources` - ([521c489](https://github.com/obeli-sk/obelisk/commit/521c489441b62069f2a42a7b652079851774cdf6))


## [0.18.0](https://github.com/obeli-sk/obelisk/compare/v0.17.0...v0.18.0)

### ⛰️ Features

- *(cli)* Reconnect TUI on status polling - ([fdcf6c2](https://github.com/obeli-sk/obelisk/commit/fdcf6c2adb6f020f6c627ce47ab218e15788b342))
- *(cli)* Print source hunk in `submit` and `get` - ([72f9237](https://github.com/obeli-sk/obelisk/commit/72f9237fe8537811be8ffd438d524d333f265706))
- *(cli)* Show the whole source file of the first frame with file and line - ([eacb121](https://github.com/obeli-sk/obelisk/commit/eacb121115c5efa8efff97ca3db3dce8942b21a2))
- *(cli)* Display last backtrace - ([6354e9b](https://github.com/obeli-sk/obelisk/commit/6354e9b4b7bebd9959ff83dd76a2c4cfea137b9e))
- *(db)* Implement `get_last_backtrace` - ([fd98793](https://github.com/obeli-sk/obelisk/commit/fd9879305ca9fc94d8a334fb5c835b9247b188db))
- *(grpc)* Include version in `GetLastBacktraceResponse` - ([c0409b9](https://github.com/obeli-sk/obelisk/commit/c0409b926224d0b60662057d5e8953fb37e93fdf))
- *(grpc)* Demangle function names, make `FrameInfo.func_name` required - ([2e91456](https://github.com/obeli-sk/obelisk/commit/2e91456618c3e4fe2f4eb2d6d7f64933f2d2dc3e))
- *(grpc)* Implement `rpc GetBacktraceSource` - ([fcef6b5](https://github.com/obeli-sk/obelisk/commit/fcef6b51cbb87b9003ecd3c51522cbba24b04163))
- *(grpc)* Implement `rpc GetLastBacktrace` - ([77adffc](https://github.com/obeli-sk/obelisk/commit/77adffc8ac916cb80677c86cd51d142c63f55f88))
- *(grpc,db)* Add `ComponentDigest` to `BacktraceInfo` and `GetLastBacktraceResponse` - ([1d93c49](https://github.com/obeli-sk/obelisk/commit/1d93c49cdb83eea2230392f2e2ebea15233df8b5))
- *(toml)* Introduce `OBELISK_TOML_DIR` path prefix - ([d58db23](https://github.com/obeli-sk/obelisk/commit/d58db2374ae059b04cae4eb0d1a32c7ae084f313))
- Enable path prefixes on mapped backtrace sources - ([539c9f4](https://github.com/obeli-sk/obelisk/commit/539c9f46232e10121ae774a1385d360d5d0170a5))
- Allow workflows to share name with activities - ([e7d570a](https://github.com/obeli-sk/obelisk/commit/e7d570ad4d6b02f29c02de477919b8e4a4fd81ce))
- Allow turning off backtrace capture - ([69c958f](https://github.com/obeli-sk/obelisk/commit/69c958f0bace8937dbd852ff5f0e4d42ece12f36))
- Enable `forward_unhandled_child_errors_in_completing_join_set_close` by default - ([aacdfa7](https://github.com/obeli-sk/obelisk/commit/aacdfa7357344581f1e209e6c6e4672e0b3d67da))
- Capture and persist backtrace for direct, submit, await-next, schedule calls - ([2a260bc](https://github.com/obeli-sk/obelisk/commit/2a260bca756e1473f4bec6847c6de70336698eb5))
- Enable wasm backtrace details for workflow engine - ([9265458](https://github.com/obeli-sk/obelisk/commit/9265458272606c671461062358aa0f81bcf00d17))

### 🐛 Bug Fixes

- *(cli)* Display the full join set id - ([668bd2a](https://github.com/obeli-sk/obelisk/commit/668bd2a77de4ecc95b488317e3c56a8df2992db3))
- Downgrade missing backtrace source to a warning - ([2f31659](https://github.com/obeli-sk/obelisk/commit/2f31659e1f400d93faf89fc1fa5da32c8397aeae))
- Fix parsing ffqn containing version - ([7f1072e](https://github.com/obeli-sk/obelisk/commit/7f1072e8c24aebbadbf8c94b4db3b2b9524eb50e))

### 📚 Documentation

- Expand roadmap todos - ([cef24a0](https://github.com/obeli-sk/obelisk/commit/cef24a012741f257783309b6360d7b0bfbb08c5e))

### 🚜 Refactor

- *(cli)* Simplify backtrace output - ([24a1f2e](https://github.com/obeli-sk/obelisk/commit/24a1f2e76dafaf813a9de36a283fe7d5774d5304))
- *(cli)* Get rid of `old_pending_status` - ([88b857b](https://github.com/obeli-sk/obelisk/commit/88b857b96900fc89a01a64d76770ee6408c17150))
- *(cli)* Simplify screen cleaning - ([6824ec4](https://github.com/obeli-sk/obelisk/commit/6824ec41d850d8b6520afc8da1af0dca04800048))
- *(cli)* Cache sources - ([bfa25e9](https://github.com/obeli-sk/obelisk/commit/bfa25e9d160d0f7f2cacc6d63f6d97722fbc35e1))
- *(cli)* Extract `get_json` - ([9396d9a](https://github.com/obeli-sk/obelisk/commit/9396d9a5e3cd4dc1ef40e33fa262b2af55c83f43))
- *(cli)* Clean up source file formatting - ([7ebc1ef](https://github.com/obeli-sk/obelisk/commit/7ebc1ef50ec4362dc14fde22589bfa9d8b2981cc))
- *(db)* [**breaking**] Rename `HistoryEvent::JoinSet` to `JoinSetCreate` - ([d8d3f20](https://github.com/obeli-sk/obelisk/commit/d8d3f20c819f6ecc2174639c923f1b4603223aba))
- *(grpc, db)* Replace `ComponentDigest` with `ComponentId` in backtrace support - ([6627759](https://github.com/obeli-sk/obelisk/commit/6627759e8eb8bc8d5e0291840bf13f71962800a8))
- *(toml)* Add `#[serde(deny_unknown_fields)]` to all structs - ([ce64955](https://github.com/obeli-sk/obelisk/commit/ce649558c1f35d2f26317951276649b9f30435b6))
- *(toml)* Rename `workflows.backtrace.persist` - ([be23736](https://github.com/obeli-sk/obelisk/commit/be23736fba39f42eb24a8ee64c3fe58da320a103))
- Wait 1s when polling for execution status - ([7ffe506](https://github.com/obeli-sk/obelisk/commit/7ffe506cd953b01e4d0de7a9b47dd7ab22283b8a))
- Record `content_id` at beginning of each `fetch_and_verify` - ([8f90818](https://github.com/obeli-sk/obelisk/commit/8f90818a1a24d3b8d907ddee68e36e9013ef880f))
- Remove configuration hash from `ComponentId` - ([3375ee4](https://github.com/obeli-sk/obelisk/commit/3375ee40a31d22f7961f21f96682cc20f757fc59))
- Rename toml structs - ([cd6c0bb](https://github.com/obeli-sk/obelisk/commit/cd6c0bb39a2ec32894274f0a234c079a02fc4fff))


## [0.17.0](https://github.com/obeli-sk/obelisk/compare/v0.16.3...v0.17.0)

### ⛰️ Features

- *(wit)* [**breaking**] Add `closing-strategy` - ([cfd27ad](https://github.com/obeli-sk/obelisk/commit/cfd27adc231faefa4ad2ddeb60b7d65cfd87581c))

### 📚 Documentation

- Add description to scripts - ([d166811](https://github.com/obeli-sk/obelisk/commit/d16681107b735ec0d98358791be973acd1d410cd))


## [0.16.3](https://github.com/obeli-sk/obelisk/compare/v0.16.2...v0.16.3)

### ⛰️ Features

- Compare `wit_type` in `ReturnType` and `ParameterType` - ([da3faf3](https://github.com/obeli-sk/obelisk/commit/da3faf36b71ab5264f37d4b1d974a0b45672330e))

### 📚 Documentation

- Update the roadmap - ([72ad832](https://github.com/obeli-sk/obelisk/commit/72ad8323be92884b91b7f6d82e97fde4b39f178f))

### 🚜 Refactor

- *(webui)* Replace `futurs-util` with `futures-concurrency` - ([e7ea856](https://github.com/obeli-sk/obelisk/commit/e7ea8569bb09a801953ac26762817b4c25991b41))
- Replace `derivative` with `derive-where` - ([e5c4e14](https://github.com/obeli-sk/obelisk/commit/e5c4e14754c255ad3addc0f79efbc3cd1ad3f07d))


## [0.16.1](https://github.com/obeli-sk/obelisk/compare/v0.16.0...v0.16.1)

### ⛰️ Features

- Disable determinism checking for unfinished executions - ([f94f862](https://github.com/obeli-sk/obelisk/commit/f94f8623c3e2b9c57cd5d18f4302015a70c95c72))

### 🐛 Bug Fixes

- *(sqlite)* Return delays in `nth_response`, `get_responses_with_offset` - ([de53521](https://github.com/obeli-sk/obelisk/commit/de5352126e09f4871d878b3340e7bf2982e4f475))
- *(sqlite)* `list_responses` must return delays - ([86be046](https://github.com/obeli-sk/obelisk/commit/86be04657ceac1cd239a3d5a68351ca8eebfc33e))
- *(sqlite)* Set PendingAt to max(now, lock_expirey) - ([01c9501](https://github.com/obeli-sk/obelisk/commit/01c95015acecea394dfabc3336d3a450bc596275))
- *(workflow)* Account for join set kinds when checking name uniqueness - ([f625496](https://github.com/obeli-sk/obelisk/commit/f625496ac7dd3aa3c8599151084d7e511713a48a))
- Rollback dependency updates as `yew` needs `getrandom` 0.2 - ([1f2911f](https://github.com/obeli-sk/obelisk/commit/1f2911f15b5696eda5bfad559a30284baa33c405))
- Gracefully handle deadline before start of execution in workflow worker - ([8f246d7](https://github.com/obeli-sk/obelisk/commit/8f246d713cc52e4a40b893f82a7068b8fc55c1c3))
- Gracefully handle deadline before start of execution in activity worker - ([3d012b1](https://github.com/obeli-sk/obelisk/commit/3d012b143923edcf3620fba03746f041bc97eb8f))

### 🚜 Refactor

- *(webui)* Switch `webui-proxy` to `wstd` - ([dfad85c](https://github.com/obeli-sk/obelisk/commit/dfad85c94feedd13a42cc1732192707e2c8e6d66))
- Make `sqlite_dao` use `Sleep` trait - ([bdf319d](https://github.com/obeli-sk/obelisk/commit/bdf319d21cb6ac38b5f3c0c048aa8d6fdda09f1d))
- Introduce `Sleep` - ([b12d2ea](https://github.com/obeli-sk/obelisk/commit/b12d2ea3393068c0df84684e8c202acf28c673d1))
- Skip `Persist.value` in debug output - ([c124345](https://github.com/obeli-sk/obelisk/commit/c124345f76eae47b6476d39ea6bea80b2550c280))
- Return `Option<ChildReturnValue>` in `find_matching_atomic` - ([a2d55bb](https://github.com/obeli-sk/obelisk/commit/a2d55bbcab806e9596cf709b1d0b169f9d9c30dd))


## [0.16.0](https://github.com/obeli-sk/obelisk/compare/v0.15.1...v0.16.0)

### ⛰️ Features

- *(toml)* [**breaking**] Rename duration variants `secs`,`millis` to `seconds`,`milliseconds` - ([2f1a3f9](https://github.com/obeli-sk/obelisk/commit/2f1a3f9f61a3487fbce388b6561e42ea537aaafe))


## [0.15.1](https://github.com/obeli-sk/obelisk/compare/v0.15.0...v0.15.1)

### ⛰️ Features

- *(cli)* Add `--json` parameter to `submit` and `get` - ([e437ef9](https://github.com/obeli-sk/obelisk/commit/e437ef9c1c050dcceec1d71097b63cea0eecf5f2))
- *(toml)* Add `~` prefix expansion - ([43050f5](https://github.com/obeli-sk/obelisk/commit/43050f57e05e98b6ec4745b4aad46d8de0bb6826))
- Print join set name in CLI - ([1c9225d](https://github.com/obeli-sk/obelisk/commit/1c9225de47104b0db28d41f7d11ff79447b3e34d))

### 🚜 Refactor

- Add "Server is ready" info message - ([bfa966f](https://github.com/obeli-sk/obelisk/commit/bfa966f1a7a78bd6f8086d5faa6bfd57e9fbb9ed))


## [0.15.0](https://github.com/obeli-sk/obelisk/compare/v0.14.2...v0.15.0)

### ⛰️ Features

- *(db)* [**breaking**] Change format of `JoinSetId` - introduce `JoinSetKind` - ([ed83a5f](https://github.com/obeli-sk/obelisk/commit/ed83a5fca4d1fcf19cee738acfeabc3d6ec9b7d2))
- Remove `.` from join set name charset - ([4524fef](https://github.com/obeli-sk/obelisk/commit/4524fefce40045f6b68eaeb842f49870f0f50e48))
- Allow more characters to be used in join set name - ([7ae009b](https://github.com/obeli-sk/obelisk/commit/7ae009b3d8ce69ef6cd8139778780fe37d50b3df))

### 🐛 Bug Fixes

- *(db)* [**breaking**] Change serde format of `PendingStateFinishedResultKind` to snake_case - ([2f9343d](https://github.com/obeli-sk/obelisk/commit/2f9343d483b4bdc856804ecdd0b6138dd7a257d5))

### 🧪 Testing

- Use -submit ffqn for `WorkflowStep::SubmitWithoutAwait` - ([6ad9f8e](https://github.com/obeli-sk/obelisk/commit/6ad9f8e65c4f1002a914d8022a661acf31f54599))

### 🚜 Refactor

- *(doc)* Rename `install.sh` to `download.sh` - ([8b0f07c](https://github.com/obeli-sk/obelisk/commit/8b0f07caa231349b1c4395dcc8539344be3e887d))
- *(grpc)* [**breaking**] Remove "user defined" prefix from `JoinSetId` - ([6a22e25](https://github.com/obeli-sk/obelisk/commit/6a22e253cf3a07344be0775e26ca9c063241bba9))
- *(grpc)* [**breaking**] Remove execution id part from join set id - ([55a8108](https://github.com/obeli-sk/obelisk/commit/55a8108b92da073403895e9d5061a3b0cd32f76d))
- *(grpc)* [**breaking**] Rename join set kind suffix `random` to `generated` - ([947ff57](https://github.com/obeli-sk/obelisk/commit/947ff57c103661e9e98c1f7be11411e57ba412e5))
- *(grpc)* [**breaking**] Add `kind` to `JoinSetId` message - ([9190b2a](https://github.com/obeli-sk/obelisk/commit/9190b2a893d6fa97fb482b658db81a89ea0ac61d))
- *(sqlite)* [**breaking**] Add join set id to the child execution id - ([ee811ba](https://github.com/obeli-sk/obelisk/commit/ee811ba96127d678b66ba3f9c289e4f366118af4))
- *(sqlite)* [**breaking**] Store child version in `t_join_set_response` - ([aba3019](https://github.com/obeli-sk/obelisk/commit/aba301970a5432f3ca53cebe0b6cd604e73ba277))
- *(sqlite)* [**breaking**] Remove `t_join_set_response.result` - ([f9e91e1](https://github.com/obeli-sk/obelisk/commit/f9e91e1863c70ad0a5e082802b2d7f8cfdbbf4f5))
- *(ui)* Display join set kind in join sets - ([925f1ec](https://github.com/obeli-sk/obelisk/commit/925f1ece86dfa0f0cf9af4cd560232c4b8d880cd))
- *(wit)* [**breaking**] Rename join set creation functions to `-named` and `-generated` - ([64f0877](https://github.com/obeli-sk/obelisk/commit/64f0877652c95e0694ce0df3220f25580339d805))
- *(wit,grpc)* [**breaking**] Remove unhandled errors from `execution-error` - ([97136fd](https://github.com/obeli-sk/obelisk/commit/97136fd6f709399b3d1cfc6e4b327cba1f82cc74))
- Use all parts of execution id for random seed - ([f68dd7c](https://github.com/obeli-sk/obelisk/commit/f68dd7caea63c4e3d582774fc0c6a2af2bab49b9))
- Remove `UserDefined` from `JoinSetKind` variants - ([54acfc3](https://github.com/obeli-sk/obelisk/commit/54acfc3e4fa13c60b637be924d3074def16ccd01))
- Wrap names in `ConfigName` - ([8b1727f](https://github.com/obeli-sk/obelisk/commit/8b1727f53a27e071a2599ec5be8c4ef3842d16c3))
- Use auto-incremented index for unnamed join sets - ([a60e9e0](https://github.com/obeli-sk/obelisk/commit/a60e9e0c11c69983d31bde7de4655a9efd737f7a))
- Shorten `JoinSetKind` string representation - ([8f520be](https://github.com/obeli-sk/obelisk/commit/8f520be110141d7135793ed0ead9630c50b5e118))
- Externalize special characters used in `check_name` - ([5108652](https://github.com/obeli-sk/obelisk/commit/5108652cc63e09a1aa8c9c3a4b70044334015ba9))


## [0.14.2](https://github.com/obeli-sk/obelisk/compare/v0.14.1...v0.14.2)

### 🐛 Bug Fixes

- *(wit)* Readd missing `new-` prefix to join set WIT functions - ([aa17e42](https://github.com/obeli-sk/obelisk/commit/aa17e4213fd5ac3be008a72c55960d6c55de809c))


## [0.14.1](https://github.com/obeli-sk/obelisk/compare/v0.14.0...v0.14.1)

### 🐛 Bug Fixes

- Symlink wit path in `utils` - ([51c85c1](https://github.com/obeli-sk/obelisk/commit/51c85c111a3d133a0068436e57d3323afe9877c4))


## [0.14.0](https://github.com/obeli-sk/obelisk/compare/v0.13.3...v0.14.0)

### ⛰️ Features

- *(grpc,db,wit)* [**breaking**] Add support for random values in workflows - ([098351e](https://github.com/obeli-sk/obelisk/commit/098351e35173dd2b1a28aee60005f9bca203e9ae))
- *(toml)* [**breaking**] Replace `sqlite.file` with `sqlite.directory` - ([57c37f8](https://github.com/obeli-sk/obelisk/commit/57c37f8622a52d87ce1398adf89edb237d277fdb))
- *(webhook)* Persist `JoinNext` - ([f27eb35](https://github.com/obeli-sk/obelisk/commit/f27eb35fa966e6658cf8202f9f0ce9c0d020e7c1))
- *(webhook)* Persist finished status - ([8a8dbc7](https://github.com/obeli-sk/obelisk/commit/8a8dbc724f951c55e838b39c1f203051f7c7a63d))
- *(wit)* [**breaking**] Rename `new-join-set()` to `join-set(name:string)` - ([ae1a67f](https://github.com/obeli-sk/obelisk/commit/ae1a67fca0fb387c8cd36a4370002ffa02779805))
- *(wit)* [**breaking**] Add version 1.0.0 to Obelisk WIT files - ([b05b586](https://github.com/obeli-sk/obelisk/commit/b05b58636bb99e8e0f95d54aeb612fcd0be5d5f5))
- *(wit,grpc,sqlite)* [**breaking**] Implement named join sets - ([c99d71f](https://github.com/obeli-sk/obelisk/commit/c99d71fcdb420bb38a65ea01cf5e1aab91d7b82e))
- Get rid of `convert_core_module` in `client component push` - ([0943ada](https://github.com/obeli-sk/obelisk/commit/0943ada140ff66018caf97f8a2fc9af324b993a9))
- Create a dummy join set in webhook - ([5a40379](https://github.com/obeli-sk/obelisk/commit/5a40379512f9a6c2e3d93f7f840f8ec20082f477))

### 🚜 Refactor

- Remove `ConfigIdError` - ([50c5b26](https://github.com/obeli-sk/obelisk/commit/50c5b260ebbc0af0cdb572a5e3f53904ffdd5d8c))


## [0.13.3](https://github.com/obeli-sk/obelisk/compare/v0.13.2...v0.13.3)

### 📚 Documentation

- Add arm64-linux to supported targets - ([0ee1e67](https://github.com/obeli-sk/obelisk/commit/0ee1e677fb44361f0d3e2d31d172bdcb416ef57a))


## [0.13.2](https://github.com/obeli-sk/obelisk/compare/v0.13.1...v0.13.2)

### 🐛 Bug Fixes

- *(sqlite)* Flush the cache before writing `CreateJoinSet` - ([a2a6787](https://github.com/obeli-sk/obelisk/commit/a2a6787e4e1110e97da9bc39197123ded4909368))
- Fix duplicate `JoinNext` if the response did not arrive yet - ([84b6f29](https://github.com/obeli-sk/obelisk/commit/84b6f29354e7f9de1d6fbda3710cfad2c3a3f17c))
- Make `close_opened_join_set` deterministic - ([398148d](https://github.com/obeli-sk/obelisk/commit/398148d14834e873b43f601600b1e755c0aa5182))

### 📚 Documentation

- Add screenshot - ([a7519ce](https://github.com/obeli-sk/obelisk/commit/a7519ce608c74b636f7346a481a32000fa88558a))

### 🧪 Testing

- Replace `join_set_requests` with `find_join_set_request` - ([2a2ab2e](https://github.com/obeli-sk/obelisk/commit/2a2ab2e59cbbedcc4ec469384210506da1a7da4c))

### 🚜 Refactor

- Use `derive_more` for `Debug` instead of `derivative` - ([2858f82](https://github.com/obeli-sk/obelisk/commit/2858f82637598ea568054f03c15be391fbf612b9))
- Replace `ValToJoinSetIdError` with a `String` - ([2212ec2](https://github.com/obeli-sk/obelisk/commit/2212ec2e7893279c212001cd0f5bf5a8365feaf0))
- Close all join sets in `event_history` - ([cd1ef49](https://github.com/obeli-sk/obelisk/commit/cd1ef49f63cf6003c259c7764f381ecf08df6d88))


## [0.13.1](https://github.com/obeli-sk/obelisk/compare/v0.13.0...v0.13.1)

### ⛰️ Features

- *(sqlite)* Add `t_metadata` table - ([69788e8](https://github.com/obeli-sk/obelisk/commit/69788e84f71993e3fca6c8a922732850f42cabc0))

### 🚜 Refactor

- *(sqlite)* Extract `init_thread`,`connection_rpc` - ([691ff8e](https://github.com/obeli-sk/obelisk/commit/691ff8e0815e1e2a35df4510cf0aa6d9ddd590ec))


## [0.13.0](https://github.com/obeli-sk/obelisk/compare/v0.12.0...v0.13.0)

### ⛰️ Features

- *(cli)* Use `--imports` instead of `-v` to show imports - ([c803ed4](https://github.com/obeli-sk/obelisk/commit/c803ed426eb82b19e11c8bda9f2acf2bb930b0aa))
- Propagate the execution error into exit code - ([4992b1a](https://github.com/obeli-sk/obelisk/commit/4992b1afb7b775543fb311db7846a861c5e61abd))
- Add `install.sh` - ([fccf514](https://github.com/obeli-sk/obelisk/commit/fccf5144e3295b5e75427d6ba3f33d94be86f439))

### 🐛 Bug Fixes

- *(cli)* Handle `None` in `ResultDetail::ResultDetail::return_value` - ([c482775](https://github.com/obeli-sk/obelisk/commit/c4827753b59f10a18eae5a375dae1e863796d0c0))
- Avoid equality footgun of `indexmap` in `TypeWrapper`, `WastVal` - ([9285c74](https://github.com/obeli-sk/obelisk/commit/9285c743bf072682e851db0733407243f649fe80))
- Do not drop `EpochTicker` prematurely - ([3c9d639](https://github.com/obeli-sk/obelisk/commit/3c9d63924794cc743ade782719a80604eea135f5))
- Remove all `serde_json::Value` occurrences from `sqlite_dao` - ([1b33e6d](https://github.com/obeli-sk/obelisk/commit/1b33e6d80a2b4858b32f1dd71f151b036e38fe3e))
- Print references of types in original interface in -ext interface's WIT - ([5c73859](https://github.com/obeli-sk/obelisk/commit/5c73859d28fa764bd47b6cc1ca3b2fdfad5a1105))
- Sort nested packages when printing WIT - ([0f19019](https://github.com/obeli-sk/obelisk/commit/0f190194b88310e1cf5d3e35e583c9a14059b280))
- Improve error when input contains more tuple elements than expected - ([ba2e52c](https://github.com/obeli-sk/obelisk/commit/ba2e52c558e2bd11976e1b2a4af30b6db8ac1240))

### 📚 Documentation

- Add a security note to `obelisk.toml` - ([1479b25](https://github.com/obeli-sk/obelisk/commit/1479b25496fb12d692666e1c8e5b4cc542714651))
- Improve readme - ([23365f4](https://github.com/obeli-sk/obelisk/commit/23365f46317e50ef49d2161d1f391d960c0b73a7))
- Simplify the readme - ([73750d7](https://github.com/obeli-sk/obelisk/commit/73750d7e1ae065008de7311e84ff7725f1915fcf))
- Remove note about madsim - ([d7e04ab](https://github.com/obeli-sk/obelisk/commit/d7e04ab40dda02ad8d216aa74c8fa80710289e5b))
- Expand stargazers section - ([332da14](https://github.com/obeli-sk/obelisk/commit/332da1486f39addd13fbb45ac98231daac6a3b82))
- Update the readme - ([d4d58fa](https://github.com/obeli-sk/obelisk/commit/d4d58fa5c26d6d186850ba821397aa8ba9e6f4a1))

### 🧪 Testing

- Verify `obelisk-local.toml` - ([20c7ae4](https://github.com/obeli-sk/obelisk/commit/20c7ae451859b704851d174ebc9d4022505aa4fe))
- Move wit tests to `wit` module - ([e84d85c](https://github.com/obeli-sk/obelisk/commit/e84d85c2861fbdb9047a44a270878371ccf3ef97))
- Stop using `serde_json::Value` in `wast_val_ser` tests - ([a74ca39](https://github.com/obeli-sk/obelisk/commit/a74ca3998fb1f3e046c5ee10f34d8259ee1c39f2))

### 🚜 Refactor

- *(wit)* [**breaking**] Rename `host-activities` to `workflow-support` - ([b51a389](https://github.com/obeli-sk/obelisk/commit/b51a389825dfe895e86c7510232c1c2e2d17b927))
- Move obelisk WIT files to the `wit` folder - ([7dbdaac](https://github.com/obeli-sk/obelisk/commit/7dbdaac43141c7f2380f7a2bba76d1e62ca508f7))
- Instrument the webhook server task - ([7e1df07](https://github.com/obeli-sk/obelisk/commit/7e1df0730b1cec7bfe0554f8b4fe8cbc895c7f9d))
- Improve error messages - ([d73866b](https://github.com/obeli-sk/obelisk/commit/d73866ba3c780793d923229876e235c6811aefd7))


## [0.12.0](https://github.com/obeli-sk/obelisk/compare/v0.11.0...v0.12.0) - 2025-01-15

### Added

- Make `-schedule` submittable
- *(grpc!)* Make `Submit` rpc idempotent
- Add `ignore_missing_env_vars` to `server verify`
- Wait 1,2,... seconds on OCI pull failure
- Make wasmtime allocator configurable
- *(cli)* Display the detail of an execution failure
- *(grpc)* Add `GenerateExecutionId` rpc
- Parse reason and detail of `ChildExecutionError`
- Move activity error details away from `reason`
- Do not ignore `post-return` trap in workflows

### Fixed

- Run `obelisk` using `cargo run` in `webui-bump.sh`
- *(sqlite)* Ignore expired locks in `get_pending`
- Use snake case in `JoinNextBlockingStrategy` serde

### Other

- Set up garnix
- Add `push-webui` workflow
- Optimize release build for size
- Bump `rustc` to 1.84
- Update the roadmap
- Replace `SKIP_WEBUI_BUILDER` with `RUN_TRUNK`
- Fix `ubuntu-24.04-install`
- Fix comparison when building `x86_64-unknown-linux-gnu`
- Log execution error details on `debug` level

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
