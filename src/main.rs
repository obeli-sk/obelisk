mod args;
mod command;
mod init;

use args::{Args, Server, Subcommand};
use clap::Parser;
use concepts::storage::DbConnection;
use concepts::storage::{CreateRequest, DbPool};
use concepts::StrVariant;
use concepts::{ExecutionId, FunctionMetadata, Params, SupportedFunctionResult};
use executor::executor::ExecConfig;
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;
use utils::time::now;
use val_json::wast_val::{WastVal, WastValWithType};
use wasm_workers::activity_worker::ActivityConfig;
use wasm_workers::auto_worker::DetectedComponent;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowConfig;
use wasm_workers::{
    activity_worker::get_activity_engine, workflow_worker::get_workflow_engine, EngineConfig,
};

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let _guard = init::init();

    let db_file =
        // TODO: XDG specs or ~/.obelisk/obelisk.sqlite
        PathBuf::from("obelisk.sqlite");
    match Args::parse().command {
        Subcommand::Server(Server::Run { clean }) => {
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
            .await
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
        Subcommand::Exe(args::Exe::Schedule { ffqn, params, verbose }) => {
            // TODO interactive search for ffqn showing param types and result, file name
            // enter parameters one by one
            let params = if let Some(params) = params {
                let params = serde_json::from_str(&params)
                    .expect("parameters should be passed as an json array");
                Params::from_json_array(params).expect("cannot parse parameters")
            } else {
                Params::default()
            };
            // TODO: typecheck the params
            command::exe::schedule(ffqn, params, verbose, db_file).await.unwrap();
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
