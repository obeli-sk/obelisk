use crate::config::ConfigHolder;
use anyhow::Context;
use async_trait::async_trait;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::{ComponentId, FunctionRegistry};
use concepts::{ComponentType, FunctionFqn, FunctionMetadata};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::fmt::Debug;
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use utils::time::now;
use wasm_workers::activity_worker::{ActivityConfig, ActivityWorker, RecycleInstancesSetting};
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::{WorkflowConfig, WorkflowWorker};

#[derive(Debug, thiserror::Error)]
enum RegistrationError {
    #[error(
        "name conflict - component with name `{name}` already exists for type {component_type}"
    )]
    ConflictingName {
        name: String,
        component_type: ComponentType,
    },
    #[error("function conflict - {ffqn}  already exists")]
    ConflictingFfqn { ffqn: FunctionFqn },
    #[error("component id conflict - {component_id} already exists")]
    ConflictingComponentId { component_id: ComponentId },
}

#[derive(Debug, Default)]
struct SharedFunctionRegistryInner {
    ffqn_map: hashbrown::HashMap<FunctionFqn, (FunctionMetadata, ComponentId)>,
    component_map: hashbrown::HashMap<ComponentId, FunctionFqn>,
    activity_names: hashbrown::HashSet<String>,
    workflow_names: hashbrown::HashSet<String>,
}

#[derive(Debug, Default)]
struct SharedFunctionRegistry(std::sync::Mutex<SharedFunctionRegistryInner>);

impl SharedFunctionRegistry {
    fn register(
        &self,
        component_id: &ComponentId,
        functions: Vec<FunctionMetadata>,
        component_type: ComponentType,
        name: String,
    ) -> Result<(), RegistrationError> {
        let mut guard = self.0.lock().unwrap();
        // check for conflicts - name
        if match component_type {
            ComponentType::WasmActivity => &guard.activity_names,
            ComponentType::WasmWorkflow => &guard.workflow_names,
        }
        .contains(&name)
        {
            return Err(RegistrationError::ConflictingName {
                name,
                component_type,
            });
        }
        // check for conflicts - ffqn
        for (ffqn, _, _) in &functions {
            if guard.ffqn_map.contains_key(ffqn) {
                return Err(RegistrationError::ConflictingFfqn { ffqn: ffqn.clone() });
            }
        }
        // check for conflicts - component id
        if guard.component_map.contains_key(component_id) {
            return Err(RegistrationError::ConflictingComponentId {
                component_id: component_id.clone(),
            });
        }
        // populate the maps
        match component_type {
            ComponentType::WasmActivity => &mut guard.activity_names,
            ComponentType::WasmWorkflow => &mut guard.workflow_names,
        }
        .insert(name);
        for (ffqn, params, ret) in functions {
            let metadata = (ffqn.clone(), params, ret);
            guard
                .ffqn_map
                .insert(ffqn.clone(), (metadata, component_id.clone()));
            guard.component_map.insert(component_id.clone(), ffqn);
        }
        Ok(())
    }
}

#[async_trait]
impl FunctionRegistry for SharedFunctionRegistry {
    async fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId)> {
        self.0.lock().unwrap().ffqn_map.get(ffqn).cloned()
    }
}

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
    let fn_registry = Arc::new(SharedFunctionRegistry::default());
    match config_watcher {
        None => {
            debug!("Loading components: {config:?}");
            let mut component_to_exec_join_handle: hashbrown::HashMap<
                ComponentId,
                ExecutorTaskHandle,
            > = hashbrown::HashMap::new();

            for activity in config.activity.into_iter().filter(|it| it.common.enabled) {
                let (component_id, wasm_path) = activity.verify_content_digest().await?;
                let config_id = activity.common.config_id;
                info!(
                    "Instantiating activity {name} with configuration {config_id} and component {component_id} from {wasm_path:?}",
                    name = activity.common.name
                );
                debug!("Full configuration: {activity:?}");
                let exec_config = activity.common.exec.into_exec_exec_config(config_id);
                let activity_config = ActivityConfig {
                    config_id,
                    recycled_instances: RecycleInstancesSetting::from(activity.recycle_instances),
                };
                let exec_task_handle = instantiate_activity(
                    &component_id,
                    activity.common.name,
                    wasm_path,
                    exec_config,
                    activity_config,
                    db_pool.clone(),
                    &engines,
                    fn_registry.clone(),
                )?;
                component_to_exec_join_handle.insert(component_id, exec_task_handle);
            }
            for workflow in config.workflow.into_iter().filter(|it| it.common.enabled) {
                let (component_id, wasm_path) = workflow.verify_content_digest().await?;
                let config_id = workflow.common.config_id;
                info!(
                    "Instantiating workflow {name} with configuration {config_id} and component {component_id} from {wasm_path:?}",
                    name = workflow.common.name
                );
                debug!("Full configuration: {workflow:?}");
                let exec_config = workflow.common.exec.into_exec_exec_config(config_id);
                let workflow_config = WorkflowConfig {
                    config_id,
                    join_next_blocking_strategy: workflow.join_next_blocking_strategy,
                    child_retry_exp_backoff: workflow.child_retry_exp_backoff.into(),
                    child_max_retries: workflow.child_max_retries,
                    non_blocking_event_batching: workflow.non_blocking_event_batching.into(),
                };
                let exec_task_handle = instantiate_workflow(
                    &component_id,
                    workflow.common.name,
                    wasm_path,
                    exec_config,
                    workflow_config,
                    db_pool.clone(),
                    &engines,
                    fn_registry.clone(),
                )?;
                component_to_exec_join_handle.insert(component_id, exec_task_handle);
            }
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

fn instantiate_activity<DB: DbConnection + 'static>(
    component_id: &ComponentId,
    name: String,
    wasm_path: impl AsRef<Path>,
    exec_config: ExecConfig,
    activity_config: ActivityConfig,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
    fn_registry: Arc<SharedFunctionRegistry>,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let worker = Arc::new(ActivityWorker::new_with_config(
        wasm_path,
        activity_config,
        engines.activity_engine.clone(),
        now,
    )?);
    register_and_spawn(
        worker,
        component_id,
        ComponentType::WasmActivity,
        name,
        exec_config,
        db_pool,
        fn_registry,
    )
}

fn instantiate_workflow<DB: DbConnection + 'static>(
    component_id: &ComponentId,
    name: String,
    wasm_path: impl AsRef<Path>,
    exec_config: ExecConfig,
    workflow_config: WorkflowConfig,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
    fn_registry: Arc<SharedFunctionRegistry>,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let worker = Arc::new(WorkflowWorker::new_with_config(
        wasm_path,
        workflow_config,
        engines.workflow_engine.clone(),
        db_pool.clone(),
        now,
        fn_registry.clone(),
    )?);
    register_and_spawn(
        worker,
        component_id,
        ComponentType::WasmWorkflow,
        name,
        exec_config,
        db_pool,
        fn_registry,
    )
}

fn register_and_spawn<W: Worker, DB: DbConnection + 'static>(
    worker: Arc<W>,
    component_id: &ComponentId,
    component_type: ComponentType,
    name: String,
    exec_config: ExecConfig,
    db_pool: impl DbPool<DB> + 'static,
    fn_registry: Arc<SharedFunctionRegistry>,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let exported_functions = worker.exported_functions().collect();
    fn_registry.register(component_id, exported_functions, component_type, name)?;
    Ok(ExecTask::spawn_new(worker, exec_config, now, db_pool, None))
}
