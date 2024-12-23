# Obelisk
Deterministic workflow engine built on top of the WASM Component Model.

> ⚠️ This is a **pre-release**: Expect changes in the CLI, gRPC, WIT or database schema.

## What's Included
* **Obelisk runtime**: A single binary executing your deterministic workflows, activities and webhook endpoints, persisting each step in an event log using SQLite.
* **Control Interfaces**: Access and manage your components and executions through:
  - **CLI**: Command-line interface using the `obelisk` executable.
  - **gRPC API**: Programmatic interaction.
  - **Web UI**: Visual and interactive interface, available at [localhost:8080](http://127.0.0.1:8080) by default.

## How It Works
* **Schema-first design with end-to-end type safety**: Uses the [WASM Component Model](https://component-model.bytecodealliance.org/) and [WIT IDL](https://component-model.bytecodealliance.org/design/wit.html) to generate API bindings between components.
* **Resilient Activities**: Automatically retries on errors and timeouts, with input parameters and execution results persistently stored.
* **Replayable Workflows**: Deterministic execution and a persisted event log make workflows replayable, enabling reliable recovery, debugging, and auditing of processes.

## Use Cases
* **Periodic Tasks**: Automate checks like customer limits, triggering activities such as transactional emails or project suspensions.
* **Background Job Processing**: Offload and manage background tasks with built-in error handling and retries.
* **Mass Deployments**: Manage large-scale deployments across systems efficiently.
* **End-to-End Testing**: Automate tests with workflows, providing detailed logs for identifying and debugging issues (e.g., GraphQL API testing).
* **Webhook Integrations**: Build webhook endpoints (e.g., GitHub) that trigger long-running workflows and interact with external APIs.

## Concepts and features
* **Activities**
    * Must be idempotent (retriable). This contract must be fulfilled by the activity itself.
    * WASI activities are executed in a WASM sandbox
        * Able to contact HTTP servers using the WASI 0.2 HTTP client.
    * Max execution duration support, after which the execution is suspended into temporary timeout.
    * Retries on errors - on WASM traps (panics), or when returning an Error result.
    * Retries on timeouts with exponential backoff.
    * Execution result is persisted.

* **Deterministic workflows**
    * Replayable: Execution is persisted at every state change, so that it can be replayed after an interrupt or an error.
    * Running in a WASM sandbox, isolated from the environment
    * Automatically retried on failures like persistence errors, timeouts or even traps(panics).
    * Able to spawn child workflows or activities, either blocking until result arrives or awaiting the result asynchronously.
    * Join sets allow for structured concurrency, either blocking until child executions are done, or cancelling those that were not awaited (planned).
    * Distributed sagas (planned).

* **Webhook Endpoints**
    * Mounted as a URL path, serving HTTP traffic.
    * Running in a WASM sandbox
    * Able to spawn child workflows or activities.

* **Work stealing executor**
    * Periodically locking a batch of currently pending executions, starts/continues their execution
    * Cleaning up old hanging executions with expired locks. Executions that have the budget will be retried (planned).
    * Concurrency control - limit on the number of workers that can run simultaneously.

## Installation
### Supported platforms
* Linux x64
* Mac OS 13 x64

### Docker
```sh
CONTAINER_ID=$(docker run -d getobelisk/obelisk)
docker logs --follow $CONTAINER_ID | grep "Serving gRPC requests"
```
```sh
docker exec $CONTAINER_ID obelisk client component list
# See Usage for more details
```

### Pre-built binary
Download [latest release](https://github.com/obeli-sk/obelisk/releases/latest) from the GitHub Release page.

### Compiling from source
The compilation requires `protoc` [Protocol Buffers compiler](https://protobuf.dev/downloads/).

#### Using latest version from crates.io
Download using [cargo-binstall](https://crates.io/crates/cargo-binstall)
```sh
cargo binstall --locked obelisk
```
or build using [cargo](https://rustup.rs/)
```sh
cargo install --locked obelisk
```

#### Nix flakes
```sh
nix run github:obeli-sk/obelisk
```

## Getting Started

### Creating sample components from a template
See [obelisk-templates](https://github.com/obeli-sk/obelisk-templates/)

### Configuration
See [obelisk.toml](obelisk.toml) for details.

### Generating a sample configuration file
```sh
obelisk server generate-config
```

### Starting the server
```sh
obelisk server run
```

### Getting the list of loaded functions
```sh
obelisk client component list
```

### Submitting a function to execute (either workflow or activity)
```sh
# Call fibonacci(10) activity from the workflow 500 times in series.
obelisk client execution submit testing:fibo-workflow/workflow.fiboa '[10, 500]' --follow
```

# Contributing
This project has a [roadmap](ROADMAP.md) and features are added in a certain order.
If you would like to contribute a feature, please [discuss](https://github.com/obeli-sk/obelisk/discussions) the feature on GitHub.
In order for us to accept patches and other contributions, you need to adopt our Contributor License Agreement (the "CLA").
The current version of the CLA can be found [here](https://cla-assistant.io/obeli-sk/obelisk).

# Local development
Set up the development dependencies using nix flakes:
```sh
nix develop
# or `direnv allow`, after simlinking .envrc-example -> .envrc
```
Or manually download all dependencies, see [dev-deps.txt](dev-deps.txt) and [Ubuntu based verification Dockerfile](.github/workflows/release/verify/ubuntu-24.04-install.Dockerfile)
Run the program
```sh
cargo run --release
```

## Running Tests
```sh
./scripts/test.sh
```

## Deterministic tests using the [madsim simulator](https://github.com/madsim-rs/madsim)
```sh
./scripts/test-madsim.sh
```
