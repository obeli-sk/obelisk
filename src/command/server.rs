use crate::{WasmActivityConfig, WasmWorkflowConfig};
use anyhow::Context;
use concepts::storage::Component;
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
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowWorker;

pub(crate) async fn run<P: AsRef<Path>>(db_file: P, clean: bool) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    if clean {
        tokio::fs::remove_file(db_file)
            .await
            .with_context(|| format!("cannot delete database file `{db_file:?}`"))?;
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
    tokio::task::spawn(async move {
        let timers_watcher = TimersWatcherTask::spawn_new(
            db_pool.connection(),
            TimersWatcherConfig {
                tick_sleep: Duration::from_millis(100),
                clock_fn: now,
            },
        );
        let mut component_to_exec_join_handle = hashbrown::HashMap::new();
        // Attempt to start executors for every active component
        loop {
            let res = update_components(
                db_pool.clone(),
                &mut component_to_exec_join_handle,
                &engines,
            )
            .await;
            if let Err(err) = res {
                eprintln!("Error while updating components - {err:?}");
                break;
            }
            tokio::select! { // future's liveness: Dropping the loser immediately.
                signal_res = tokio::signal::ctrl_c() => {
                    if signal_res.is_err() {
                        eprintln!("Cannot listen to ctrl-c event");
                    }
                    break
                },
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
        println!("Gracefully shutting down");
        timers_watcher.close().await;
        for exec_join_handle in component_to_exec_join_handle.values() {
            exec_join_handle.close().await;
        }
        db_pool.close().await.context("cannot close database")?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .unwrap()
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

async fn update_components<DB: DbConnection + 'static>(
    db_pool: impl DbPool<DB> + 'static,
    component_to_exec_join_handle: &mut hashbrown::HashMap<ComponentId, ExecutorTaskHandle>,
    engines: &Engines,
) -> Result<(), anyhow::Error> {
    let active_components = db_pool
        .connection()
        .component_list(true)
        .await
        .context("cannot list active components")?;

    // Shut down deactivated components
    let mut deactivating = component_to_exec_join_handle
        .keys()
        .cloned()
        .collect::<hashbrown::HashSet<_>>();
    // Retain components that are no longer active
    for component_id in active_components.iter().map(|c| &c.component_id) {
        deactivating.remove(component_id);
    }
    for component_id in deactivating {
        println!("Shutting down executor of component {component_id}");
        component_to_exec_join_handle
            .remove(&component_id)
            .expect("comonent id was taken from `component_to_exec_join_handle` so it must exist")
            .close()
            .await;
    }

    // Spawn new component executors
    for component in active_components {
        if component_to_exec_join_handle.contains_key(&component.component_id) {
            continue;
        }
        let component_id = component.component_id.clone();
        println!(
            "Starting new executor of component `{file_name}` {component_id}",
            file_name = component.file_name
        );
        let stopwatch = std::time::Instant::now();
        match instantiate_component(component, db_pool.clone(), engines) {
            Ok(exec) => {
                println!("Instantiated in {:?}", stopwatch.elapsed());
                component_to_exec_join_handle.insert(component_id, exec);
            }
            Err(err) => {
                eprintln!("Error activating component {component_id} - {err}");
            }
        }
    }
    Ok(())
}

fn instantiate_component<DB: DbConnection + 'static>(
    component: Component,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    match component.component_type {
        ComponentType::WasmActivity => {
            let wasm_activity_config: WasmActivityConfig = serde_json::from_value(component.config)
                .context("could not deserialize `WasmActivityConfig`, try cleaning the database")?;
            let worker = Arc::new(
                ActivityWorker::new_with_config(
                    wasm_activity_config.wasm_path,
                    wasm_activity_config.activity_config,
                    engines.activity_engine.clone(),
                    now,
                )
                .context("cannot start activity worker")?,
            );
            Ok(ExecTask::spawn_new(
                worker,
                wasm_activity_config.exec_config,
                now,
                db_pool,
                None,
            ))
        }
        ComponentType::WasmWorkflow => {
            let wasm_workflow_config: WasmWorkflowConfig = serde_json::from_value(component.config)
                .context("could not deserialize `WasmWorkflowConfig`, try cleaning the database")?;
            let worker = Arc::new(
                WorkflowWorker::new_with_config(
                    wasm_workflow_config.wasm_path,
                    wasm_workflow_config.workflow_config,
                    engines.workflow_engine.clone(),
                    db_pool.clone(),
                    now,
                )
                .context("cannot start workflow worker")?,
            );
            Ok(ExecTask::spawn_new(
                worker,
                wasm_workflow_config.exec_config,
                now,
                db_pool,
                None,
            ))
        }
    }
}
