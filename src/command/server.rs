use crate::config::store::ConfigStore;
use crate::config::toml::ConfigHolder;
use crate::config::toml::VerifiedActivityConfig;
use crate::config::toml::VerifiedWorkflowConfig;
use anyhow::Context;
use concepts::storage::Component;
use concepts::storage::ComponentToggle;
use concepts::storage::ComponentWithMetadata;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::FunctionRegistry;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::fmt::Debug;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
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
            debug!("Loading components: {config:?}");
            let mut exec_join_handles = Vec::new();

            for activity in config.activity.into_iter().filter(|it| it.common.enabled) {
                let activity = activity.verify_content_digest().await?;

                if activity.enabled.into() {
                    let exec_task_handle =
                        instantiate_activity(activity, db_pool.clone(), &engines).await?;
                    exec_join_handles.push(exec_task_handle);
                }
            }
            for workflow in config.workflow.into_iter().filter(|it| it.common.enabled) {
                let workflow = workflow.verify_content_digest().await?;
                if workflow.enabled.into() {
                    let exec_task_handle =
                        instantiate_workflow(workflow, db_pool.clone(), &engines).await?;
                    exec_join_handles.push(exec_task_handle);
                }
            }
            if tokio::signal::ctrl_c().await.is_err() {
                warn!("Cannot listen to ctrl-c event");
                loop {
                    tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                }
            }
            timers_watcher.close().await;

            for exec_join_handle in exec_join_handles {
                exec_join_handle.close().await;
            }
            db_pool.close().await.context("cannot close database")?;
            Ok::<_, anyhow::Error>(())
        }
        Some(mut config_watcher) => {
            /*
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
                    info!("Updating components: {config:#?}");
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
                info!("Gracefully shutting down");
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
            */
            todo!()
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

async fn instantiate_activity<DB: DbConnection + 'static>(
    activity: VerifiedActivityConfig,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!(
        "Instantiating activity {name} with id {config_id} from {wasm_path:?}",
        name = activity.config_store.name(),
        config_id = activity.exec_config.config_id,
        wasm_path = activity.wasm_path,
    );
    debug!("Full configuration: {activity:?}");
    let worker = Arc::new(ActivityWorker::new_with_config(
        activity.wasm_path,
        activity.activity_config,
        engines.activity_engine.clone(),
        now,
    )?);
    register_and_spawn(
        worker,
        &activity.config_store,
        activity.exec_config,
        db_pool,
    )
    .await
}

async fn instantiate_workflow<DB: DbConnection + 'static>(
    workflow: VerifiedWorkflowConfig,
    db_pool: impl DbPool<DB> + FunctionRegistry + 'static,
    engines: &Engines,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!(
        "Instantiating workflow {name} with id {config_id} from {wasm_path:?}",
        name = workflow.config_store.name(),
        config_id = workflow.exec_config.config_id,
        wasm_path = workflow.wasm_path,
    );
    debug!("Full configuration: {workflow:?}");
    let fn_registry = Arc::from(db_pool.clone());
    let worker = Arc::new(WorkflowWorker::new_with_config(
        workflow.wasm_path,
        workflow.workflow_config,
        engines.workflow_engine.clone(),
        db_pool.clone(),
        now,
        fn_registry,
    )?);
    register_and_spawn(
        worker,
        &workflow.config_store,
        workflow.exec_config,
        db_pool,
    )
    .await
}

async fn register_and_spawn<W: Worker, DB: DbConnection + 'static>(
    worker: Arc<W>,
    config: &ConfigStore,
    exec_config: ExecConfig,
    db_pool: impl DbPool<DB> + 'static,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let component = ComponentWithMetadata {
        component: Component {
            config_id: exec_config.config_id.clone(),
            config: serde_json::to_value(&config)
                .expect("ConfigStore must be serializable to JSON"),
            enabled: ComponentToggle::Enabled,
        },
        exports: worker.exported_functions().collect(),
        imports: worker.imported_functions().collect(),
    };
    // FIXME: Disable old components first
    db_pool
        .connection()
        .component_add(now(), component, ComponentToggle::Enabled)
        .await?;
    Ok(ExecTask::spawn_new(worker, exec_config, now, db_pool, None))
}
