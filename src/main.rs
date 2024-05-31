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
        other => {
            eprintln!("{other:?}");
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

#[allow(clippy::too_many_lines)]
async fn run<DB: DbConnection + 'static>(db_pool: impl DbPool<DB> + 'static) {
    let activity_engine = get_activity_engine(EngineConfig::default());
    let workflow_engine = get_workflow_engine(EngineConfig::default());

    let _epoch_ticker = EpochTicker::spawn_new(
        vec![activity_engine.weak(), workflow_engine.weak()],
        Duration::from_millis(10),
    );

    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );

    // let wasm_tasks = vec![
    //     exec(
    //         StrVariant::Static(
    //             test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
    //         ),
    //         db_pool.clone(),
    //         workflow_engine.clone(),
    //         activity_engine.clone(),
    //     ),
    //     exec(
    //         StrVariant::Static(
    //             test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
    //         ),
    //         db_pool.clone(),
    //         workflow_engine.clone(),
    //         activity_engine.clone(),
    //     ),
    //     exec(
    //         StrVariant::Static(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY),
    //         db_pool.clone(),
    //         workflow_engine.clone(),
    //         activity_engine.clone(),
    //     ),
    //     exec(
    //         StrVariant::Static(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW),
    //         db_pool.clone(),
    //         workflow_engine,
    //         activity_engine,
    //     ),
    // ];

    // parse input
    let mut args = std::env::args();
    args.next();
    let args = args.collect::<Vec<_>>();
    if args.len() == 2 {
        let ffqn = args.first().unwrap();
        let ffqn = ffqn.parse().unwrap();
        let params = args.get(1).unwrap();
        let params = serde_json::from_str(params).unwrap();
        let params = Params::from_json_array(params).unwrap();

        let db_connection = db_pool.connection();
        let execution_id = ExecutionId::generate();
        let created_at = now();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 5,
            })
            .await
            .unwrap();

        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap();

        let duration = (now() - created_at).to_std().unwrap();
        match res {
            Ok(
                SupportedFunctionResult::None
                | SupportedFunctionResult::Infallible(_)
                | SupportedFunctionResult::Fallible(WastValWithType {
                    value: WastVal::Result(Ok(_)),
                    ..
                }),
            ) => {
                error!("Finished OK in {duration:?}");
            }
            _ => {
                error!("Finished with an error in {duration:?} - {res:?}");
            }
        }

        // for (task, _) in wasm_tasks {
        //     task.close().await;
        // }
        timers_watcher.close().await;
        db_pool.close().await.unwrap();
    } else if args.is_empty() {
        loop {
            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        }
    } else {
        eprintln!("Two arguments expected: `function's fully qualified name` `parameters`");
        eprintln!("Available functions:");
        // for (_, ffqns) in &wasm_tasks {
        //     for ffqn in ffqns {
        //         eprintln!("{ffqn}");
        //     }
        // }
    }
}
