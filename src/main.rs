mod args;
mod init;

use args::{Args, Server, Subcommand};
use clap::Parser;
use concepts::storage::{Component, ComponentWithMetadata, DbConnection};
use concepts::storage::{CreateRequest, DbPool};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{
    ComponentId, ComponentType, ExecutionId, FunctionFqn, FunctionMetadata, Params,
    SupportedFunctionResult,
};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use utils::time::now;
use val_json::wast_val::{WastVal, WastValWithType};
use wasm_workers::activity_worker::{ActivityConfig, ActivityWorker};
use wasm_workers::auto_worker::DetectedComponent;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::{NonBlockingEventBatching, WorkflowConfig, WorkflowWorker};
use wasm_workers::{
    activity_worker::{activity_engine, RecycleInstancesSetting},
    workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
    EngineConfig,
};

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    let _guard = init::init();

    let db_file = {
        // TODO: XDG specs or ~/.obelisk/obelisk.sqlite
        "obelisk.sqlite"
    };
    let db_pool = SqlitePool::new(db_file).await.unwrap();
    let activity_engine = activity_engine(EngineConfig::default());
    let workflow_engine = workflow_engine(EngineConfig::default());

    match Args::parse().command {
        Subcommand::Server(Server::Run { clean }) => {
            if clean {
                tokio::fs::remove_file(db_file)
                    .await
                    .expect("cannot delete database file");
            }
            let _epoch_ticker = EpochTicker::spawn_new(
                vec![activity_engine.weak(), workflow_engine.weak()],
                Duration::from_millis(10),
            );
            let db_pool = SqlitePool::new(db_file).await.unwrap();
            let timers_watcher = TimersWatcherTask::spawn_new(
                db_pool.connection(),
                TimersWatcherConfig {
                    tick_sleep: Duration::from_millis(100),
                    clock_fn: now,
                },
            );
            // Attempt to start executors for every active component
            let components = db_pool.connection().list_active_components().await.unwrap();
            let mut exec_join_handles = Vec::with_capacity(components.len());
            for component in components {
                info!(
                    "Starting {component_type} executor `{file_name}`",
                    component_type = component.component_type,
                    file_name = component.file_name
                );
                match component.component_type {
                    ComponentType::WasmActivity => {
                        let wasm_activity_config: WasmActivityConfig =
                            serde_json::from_value(component.config).unwrap();
                        let worker = Arc::new(
                            ActivityWorker::new_with_config(
                                StrVariant::Arc(Arc::from(wasm_activity_config.wasm_path)),
                                wasm_activity_config.activity_config,
                                activity_engine.clone(),
                                now,
                            )
                            .unwrap(),
                        );
                        exec_join_handles.push(ExecTask::spawn_new(
                            worker,
                            wasm_activity_config.exec_config,
                            now,
                            db_pool.clone(),
                            None,
                        ));
                    }
                    ComponentType::WasmWorkflow => {
                        let wasm_workflow_config: WasmWorkflowConfig =
                            serde_json::from_value(component.config).unwrap();
                        let worker = Arc::new(
                            WorkflowWorker::new_with_config(
                                StrVariant::Arc(Arc::from(wasm_workflow_config.wasm_path)),
                                wasm_workflow_config.workflow_config,
                                workflow_engine.clone(),
                                db_pool.clone(),
                                now,
                            )
                            .unwrap(),
                        );
                        exec_join_handles.push(ExecTask::spawn_new(
                            worker,
                            wasm_workflow_config.exec_config,
                            now,
                            db_pool.clone(),
                            None,
                        ));
                    }
                }
            }
            // TODO: start worker watcher
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    println!("Gracefully shutting down");
                    timers_watcher.close().await;
                    for exec_join_handle in exec_join_handles {
                        exec_join_handle.close().await;
                    }
                    db_pool.close().await.unwrap();
                    println!("Done");
                }
                Err(err) => {
                    eprintln!("Unable to listen for shutdown signal: {}", err);
                }
            }
        }
        Subcommand::Component(args::Component::Inspect { wasm_path, verbose }) => {
            let detected =
                DetectedComponent::new(StrVariant::Arc(Arc::from(wasm_path)), workflow_engine)
                    .unwrap();
            println!("Component type:");
            println!("\t{}", detected.component_type);

            println!("Exports:");
            inspect(&detected.exports, verbose);

            println!("Imports:");
            inspect(&detected.imports, verbose);
        }
        Subcommand::Component(args::Component::Add { replace, wasm_path }) => {
            let wasm_path = wasm_path.canonicalize().unwrap();
            let file_name = wasm_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let component_id = hash(&wasm_path).unwrap();
            let config_id = ConfigId::generate();

            let exec_config = ExecConfig {
                batch_size: 10,
                lock_expiry: Duration::from_secs(10),
                tick_sleep: Duration::from_millis(200),
                config_id,
            };
            let detected = DetectedComponent::new(
                StrVariant::Arc(Arc::from(wasm_path.to_string_lossy())),
                workflow_engine,
            )
            .unwrap();
            let config = match detected.component_type {
                ComponentType::WasmActivity => serde_json::to_value(&WasmActivityConfig {
                    wasm_path: wasm_path.to_string_lossy().to_string(),
                    exec_config,
                    activity_config: ActivityConfig {
                        config_id,
                        recycled_instances: RecycleInstancesSetting::Enable,
                    },
                })
                .unwrap(),
                ComponentType::WasmWorkflow => serde_json::to_value(&WasmWorkflowConfig {
                    wasm_path: wasm_path.to_string_lossy().to_string(),
                    exec_config,
                    workflow_config: WorkflowConfig {
                        config_id,
                        join_next_blocking_strategy: JoinNextBlockingStrategy::Await,
                        child_retry_exp_backoff: Duration::from_millis(10),
                        child_max_retries: 5,
                        non_blocking_event_batching: NonBlockingEventBatching::Enabled,
                    },
                })
                .unwrap(),
            };
            let component = ComponentWithMetadata {
                component: Component {
                    component_id,
                    component_type: detected.component_type,
                    config,
                    file_name,
                },
                exports: detected.exports,
                imports: detected.imports,
            };
            let replaced = db_pool
                .connection()
                .append_component(now(), component, replace)
                .await
                .unwrap();
            if !replaced.is_empty() {
                println!("Replaced components:");
                for replaced in replaced {
                    println!("\t{replaced}");
                }
            }
        }
        other => {
            eprintln!("{other:?}");
            std::process::exit(1);
        }
    }
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

fn hash<P: AsRef<Path>>(path: P) -> anyhow::Result<ComponentId> {
    use sha2::{Digest, Sha256};
    use std::{fs, io};
    let mut file = fs::File::open(&path)?;
    let mut hasher = Sha256::new();
    io::copy(&mut file, &mut hasher)?;
    let hash = hasher.finalize();
    let hash_base64 = base16ct::lower::encode_string(&hash);
    Ok(ComponentId::new(concepts::HashType::Sha256, hash_base64))
}

fn inspect<'a>(iter: &[FunctionMetadata], verbose: bool) {
    for (ffqn, parameter_types, result) in iter {
        print!("\t{ffqn}");
        if verbose {
            print!(" {parameter_types}");
            if let Some(result) = result {
                let result = serde_json::to_string(&result).unwrap();
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
