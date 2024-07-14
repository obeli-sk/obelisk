use crate::config::{ConfigHolder, ObeliskConfig};
use crate::{WasmActivityConfig, WasmWorkflowConfig};
use anyhow::Context;
use concepts::storage::Component;
use concepts::storage::ComponentToggle;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::ComponentId;
use concepts::ComponentType;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecTask;
use executor::executor::ExecutorTaskHandle;
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use std::fmt::Debug;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowWorker;

pub(crate) async fn run(config_holder: ConfigHolder, clean: bool) -> anyhow::Result<()> {
    let (config, config_watcher) = config_holder.load_config_watch_changes().await?;
    let db_file = &config.sqlite_file;
    if clean {
        tokio::fs::remove_file(db_file)
            .await
            .with_context(|| format!("cannot delete database file `{db_file}`"))?;
    }
    let engines = Engines::auto_detect(&get_opts_from_env())?;
    let _epoch_ticker = EpochTicker::spawn_new(
        vec![
            engines.activity_engine.weak(),
            engines.workflow_engine.weak(),
        ],
        Duration::from_millis(10),
    );
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;

    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );
    match config_watcher {
        None => {
            println!("Loading components: {config:#?}");
            let mut component_to_exec_join_handle: hashbrown::HashMap<
                ComponentId,
                ExecutorTaskHandle,
            > = hashbrown::HashMap::new();

            if tokio::signal::ctrl_c().await.is_err() {
                warn!("Cannot listen to ctrl-c event");
                loop {
                    tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                }
            }
            timers_watcher.close().await;

            for exec_join_handle in component_to_exec_join_handle.values() {
                exec_join_handle.close().await;
            }
            db_pool.close().await.context("cannot close database")?;
            Ok::<_, anyhow::Error>(())
        }
        Some(mut config_watcher) => {
            tokio::task::spawn(async move {
                let mut component_to_exec_join_handle: hashbrown::HashMap<
                    ComponentId,
                    ExecutorTaskHandle,
                > = hashbrown::HashMap::new();
                // Attempt to start executors for every enabled component
                // let mut config = Some(config);
                let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());
                let mut config = config;
                loop {
                    // if let Some(config) = config.take() {
                    println!("Updating components: {config:#?}");
                    // if let Err(err) = update_components(
                    //     db_pool.clone(),
                    //     &mut component_to_exec_join_handle,
                    //     &engines,
                    //     config,
                    // )
                    // .await
                    // {
                    //     eprintln!("Error while updating components - {err:?}");
                    //     break;
                    // }
                    // }
                    tokio::select! { // future's liveness: Assuming that dropping `ctrl_c` is OK and the signal will be received in the next loop.
                        signal_res = &mut ctrl_c => {
                            if signal_res.is_err() {
                                warn!("Cannot listen to ctrl-c event");
                                loop {
                                    tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                                }
                            }
                            break
                        },
                        config_res = config_watcher.rx.recv() => {
                            match config_res {
                                Some(conf) => {
                                    config = conf;
                                },
                                None => {
                                    warn!("Not watching config changes anymore");
                                    break
                                }
                            }
                        }
                    }
                }
                println!("Gracefully shutting down");
                timers_watcher.close().await;
                for exec_join_handle in component_to_exec_join_handle.values() {
                    exec_join_handle.close().await;
                }
                db_pool.close().await.context("cannot close database")?;
                config_watcher.abort_handle.abort();
                Ok::<_, anyhow::Error>(())
            })
            .await
            .unwrap()
        }
    }
}

fn get_opts_from_env() -> wasm_workers::engines::PoolingOptions {
    fn get_env<T: FromStr + Display>(key: &str, into: &mut Option<T>)
    where
        <T as FromStr>::Err: Debug,
    {
        if let Ok(val) = std::env::var(key) {
            let val = val.parse().unwrap();
            info!("Setting {key}={val}");
            *into = Some(val);
        }
    }
    let mut opts = wasm_workers::engines::PoolingOptions::default();
    get_env(
        "WASMTIME_POOLING_MEMORY_KEEP_RESIDENT",
        &mut opts.pooling_memory_keep_resident,
    );
    get_env(
        "WASMTIME_POOLING_TABLE_KEEP_RESIDENT",
        &mut opts.pooling_table_keep_resident,
    );
    get_env(
        "WASMTIME_MEMORY_PROTECTION_KEYS",
        &mut opts.memory_protection_keys,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_CORE_INSTANCES",
        &mut opts.pooling_total_core_instances,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_COMPONENT_INSTANCES",
        &mut opts.pooling_total_component_instances,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_MEMORIES",
        &mut opts.pooling_total_memories,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_TABLES",
        &mut opts.pooling_total_tables,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_STACKS",
        &mut opts.pooling_total_stacks,
    );
    get_env(
        "WASMTIME_POOLING_MAX_MEMORY_SIZE",
        &mut opts.pooling_max_memory_size,
    );
    opts
}

// async fn update_components<DB: DbConnection + 'static>(
//     db_pool: impl DbPool<DB> + 'static,
//     component_to_exec_join_handle: &mut hashbrown::HashMap<ComponentId, ExecutorTaskHandle>,
//     engines: &Engines,
//     config: Config,
// ) -> Result<(), anyhow::Error> {
//     let enabled_components = db_pool
//         .connection()
//         .component_list(ComponentToggle::Enabled)
//         .await
//         .context("cannot list enabled components")?;

//     // Shut down disabled components
//     let mut disabling = component_to_exec_join_handle
//         .keys()
//         .cloned()
//         .collect::<hashbrown::HashSet<_>>();
//     // Retain components that are no longer enabled
//     for component_id in enabled_components.iter().map(|c| &c.component_id) {
//         disabling.remove(component_id);
//     }
//     for component_id in disabling {
//         println!("Shutting down executor of component {component_id}");
//         component_to_exec_join_handle
//             .remove(&component_id)
//             .expect("comonent id was taken from `component_to_exec_join_handle` so it must exist")
//             .close()
//             .await;
//     }

//     // Spawn new component executors
//     for component in enabled_components {
//         if component_to_exec_join_handle.contains_key(&component.component_id) {
//             continue;
//         }
//         let component_id = component.component_id.clone();
//         println!(
//             "Starting new executor of component `{name}` {component_id}",
//             name = component.name
//         );
//         let stopwatch = std::time::Instant::now();
//         match instantiate_component(component, db_pool.clone(), engines) {
//             Ok(exec) => {
//                 println!("Instantiated in {:?}", stopwatch.elapsed());
//                 component_to_exec_join_handle.insert(component_id, exec);
//             }
//             Err(err) => {
//                 eprintln!("Error activating component {component_id} - {err}");
//             }
//         }
//     }
//     Ok(())
// }

// fn instantiate_component<DB: DbConnection + 'static>(
//     component: Component,
//     db_pool: impl DbPool<DB> + 'static,
//     engines: &Engines,
// ) -> Result<ExecutorTaskHandle, anyhow::Error> {
//     match component.component_type {
//         ComponentType::WasmActivity => {
//             let wasm_activity_config: WasmActivityConfig = serde_json::from_value(component.config)
//                 .context("could not deserialize `WasmActivityConfig`, try cleaning the database")?;
//             let worker = Arc::new(
//                 ActivityWorker::new_with_config(
//                     wasm_activity_config.wasm_path,
//                     wasm_activity_config.activity_config,
//                     engines.activity_engine.clone(),
//                     now,
//                 )
//                 .context("cannot start activity worker")?,
//             );
//             Ok(ExecTask::spawn_new(
//                 worker,
//                 wasm_activity_config.exec_config,
//                 now,
//                 db_pool,
//                 None,
//             ))
//         }
//         ComponentType::WasmWorkflow => {
//             let wasm_workflow_config: WasmWorkflowConfig = serde_json::from_value(component.config)
//                 .context("could not deserialize `WasmWorkflowConfig`, try cleaning the database")?;
//             let worker = Arc::new(
//                 WorkflowWorker::new_with_config(
//                     wasm_workflow_config.wasm_path,
//                     wasm_workflow_config.workflow_config,
//                     engines.workflow_engine.clone(),
//                     db_pool.clone(),
//                     now,
//                 )
//                 .context("cannot start workflow worker")?,
//             );
//             Ok(ExecTask::spawn_new(
//                 worker,
//                 wasm_workflow_config.exec_config,
//                 now,
//                 db_pool,
//                 None,
//             ))
//         }
//     }
// }
