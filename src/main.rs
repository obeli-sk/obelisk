mod args;
mod init;

use args::{Args, Server, Subcommand, Worker};
use clap::Parser;
use concepts::storage::DbConnection;
use concepts::storage::{CreateRequest, DbPool};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use std::sync::Arc;
use std::time::Duration;
use tracing::error;
use utils::time::now;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::NonBlockingEventBatching;
use wasm_workers::{
    activity_worker::{activity_engine, RecycleInstancesSetting},
    auto_worker::{AutoConfig, AutoWorker},
    workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
    EngineConfig,
};
use wasmtime::Engine;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let _guard = init::init();

    let db_file = {
        // TODO: XDG specs or ~/.obelisk/obelisk.sqlite
        "obelisk.sqlite"
    };
    let db_pool = SqlitePool::new(db_file).await.unwrap();
    match Args::parse().command {
        Subcommand::Server(Server::Run { clean }) => {
            if clean {
                tokio::fs::remove_file(db_file)
                    .await
                    .expect("cannot delete database file");
            }
            let activity_engine = activity_engine(EngineConfig::default());
            let workflow_engine = workflow_engine(EngineConfig::default());
            let _epoch_ticker = EpochTicker::spawn_new(
                vec![activity_engine.weak(), workflow_engine.weak()],
                Duration::from_millis(10),
            );
            let db_pool = SqlitePool::new(db_file).await.unwrap();
            let _timers_watcher = TimersWatcherTask::spawn_new(
                db_pool.connection(),
                TimersWatcherConfig {
                    tick_sleep: Duration::from_millis(100),
                    clock_fn: now,
                },
            );
            // TODO: start worker watcher
            loop {
                tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
            }
        }
        Subcommand::Worker(Worker::Inspect { wasm_path, verbose }) => {
            let activity_engine = activity_engine(EngineConfig::default());
            let workflow_engine = workflow_engine(EngineConfig::default());
            let auto_worker = AutoWorker::new_with_config(
                AutoConfig {
                    wasm_path: StrVariant::Arc(Arc::from(wasm_path)),
                    config_id: ConfigId::generate(),
                    activity_recycled_instances: RecycleInstancesSetting::Enable,
                    workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::Await,
                    workflow_child_retry_exp_backoff: Duration::from_millis(10),
                    workflow_child_max_retries: 5,
                    non_blocking_event_batching: NonBlockingEventBatching::Enabled,
                },
                workflow_engine,
                activity_engine,
                db_pool.clone(),
                now,
            )
            .unwrap();
            println!("Kind:");
            println!("\t{}", auto_worker.kind());

            println!("Exports:");
            inspect(
                executor::worker::Worker::exported_functions(&auto_worker),
                verbose,
            );

            println!("Imports:");
            inspect(
                executor::worker::Worker::imported_functions(&auto_worker),
                verbose,
            );
        }
        other => {
            eprintln!("{other:?}");
            std::process::exit(1);
        }
    }
}

fn inspect<'a>(
    iter: impl Iterator<Item = (FunctionFqn, &'a [TypeWrapper], &'a Option<TypeWrapper>)>,
    verbose: bool,
) {
    for (ffqn, params, result) in iter {
        print!("\t{ffqn}");
        if verbose {
            let mut params = serde_json::to_string(params).unwrap().replacen('[', "", 1);
            params.replace_range((params.len() - 1).., "");
            print!(" ({params})");
            if let Some(result) = result {
                let result = serde_json::to_string(result).unwrap();
                print!(" -> {result}");
            }
        }
        println!();
    }
}

#[allow(clippy::too_many_lines)]
async fn run<DB: DbConnection + 'static>(db_pool: impl DbPool<DB> + 'static) {
    let activity_engine = activity_engine(EngineConfig::default());
    let workflow_engine = workflow_engine(EngineConfig::default());

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

fn exec<DB: DbConnection + 'static>(
    wasm_path: StrVariant,
    db_pool: impl DbPool<DB> + 'static,
    workflow_engine: Arc<Engine>,
    activity_engine: Arc<Engine>,
) -> (ExecutorTaskHandle, Vec<FunctionFqn>) {
    let worker = Arc::new(
        AutoWorker::new_with_config(
            AutoConfig {
                wasm_path,
                config_id: ConfigId::generate(),
                activity_recycled_instances: RecycleInstancesSetting::Enable,
                workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::Await,
                workflow_child_retry_exp_backoff: Duration::from_millis(10),
                workflow_child_max_retries: 5,
                non_blocking_event_batching: NonBlockingEventBatching::Enabled,
            },
            workflow_engine,
            activity_engine,
            db_pool.clone(),
            now,
        )
        .unwrap(),
    );
    let ffqns = executor::worker::Worker::exported_functions(worker.as_ref())
        .map(|(ffqn, _, _)| ffqn)
        .collect::<Vec<_>>();
    let exec_config = ExecConfig {
        batch_size: 10,
        lock_expiry: Duration::from_secs(10),
        tick_sleep: Duration::from_millis(200),
        clock_fn: now,
    };
    (
        ExecTask::spawn_new(worker, exec_config, db_pool, None),
        ffqns,
    )
}
