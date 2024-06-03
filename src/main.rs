mod args;
mod command;
mod init;

use args::{Args, Executor, Subcommand};
use clap::Parser;
use command::execution::ExecutionVerbosity;
use concepts::Params;
use executor::executor::ExecConfig;
use std::path::PathBuf;
use wasm_workers::activity_worker::ActivityConfig;
use wasm_workers::workflow_worker::WorkflowConfig;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let _guard = init::init();
    let db_file =
        // TODO: XDG specs or ~/.obelisk/obelisk.sqlite
        PathBuf::from("obelisk.sqlite");
    match Args::parse().command {
        Subcommand::Executor(Executor::Serve { clean }) => {
            command::server::run(db_file, clean).await.unwrap();
        }
        Subcommand::Component(args::Component::Inspect { wasm_path, verbose }) => {
            command::component::inspect(
                wasm_path,
                if verbose {
                    FunctionMetadataVerbosity::WithTypes
                } else {
                    FunctionMetadataVerbosity::FfqnOnly
                },
            )
            .unwrap();
        }
        Subcommand::Component(args::Component::Add { replace, wasm_path }) => {
            command::component::add(replace, wasm_path, db_file)
                .await
                .unwrap();
        }
        Subcommand::Component(args::Component::List { verbosity }) => {
            command::component::list(
                db_file,
                match verbosity {
                    0 => None,
                    1 => Some(FunctionMetadataVerbosity::FfqnOnly),
                    _ => Some(FunctionMetadataVerbosity::WithTypes),
                },
            )
            .await
            .unwrap();
        }
        Subcommand::Component(args::Component::Get { id, verbosity }) => {
            command::component::get(
                db_file,
                id,
                match verbosity {
                    0 => None,
                    1 => Some(FunctionMetadataVerbosity::FfqnOnly),
                    _ => Some(FunctionMetadataVerbosity::WithTypes),
                },
            )
            .await
            .unwrap();
        }
        Subcommand::Execution(args::Execution::Schedule {
            ffqn,
            params,
            force,
        }) => {
            // TODO interactive search for ffqn showing param types and result, file name
            // enter parameters one by one
            let params = format!("[{}]", params.join(","));
            let params = serde_json::from_str(&params)
                .expect("parameters should be passed as an json array");
            let params = Params::from_json_array(params).expect("cannot parse parameters");
            // TODO: typecheck the params
            command::execution::schedule(ffqn, params, db_file, force)
                .await
                .unwrap();
        }
        Subcommand::Execution(args::Execution::Get {
            execution_id,
            verbosity,
        }) => {
            command::execution::get(
                db_file,
                execution_id,
                match verbosity {
                    0 => None,
                    1 => Some(ExecutionVerbosity::EventHistory),
                    _ => Some(ExecutionVerbosity::Full),
                },
            )
            .await
            .unwrap();
        }
        other => {
            eprintln!("TODO {other:?}");
            std::process::exit(1);
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum FunctionMetadataVerbosity {
    FfqnOnly,
    WithTypes,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WasmWorkflowConfig {
    wasm_path: String,
    exec_config: ExecConfig,
    workflow_config: WorkflowConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WasmActivityConfig {
    wasm_path: String,
    exec_config: ExecConfig,
    activity_config: ActivityConfig,
}
