use crate::{WasmActivityConfig, WasmWorkflowConfig};
use anyhow::Context;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::ComponentId;
use concepts::ComponentType;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecTask;
use executor::executor::ExecutorTaskHandle;
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowWorker;
use wasm_workers::EngineConfig;
use wasm_workers::Engines;

pub(crate) async fn run<P: AsRef<Path>>(db_file: P, clean: bool) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    if clean {
        tokio::fs::remove_file(db_file)
            .await
            .with_context(|| format!("cannot delete database file `{db_file:?}`"))?;
    }
    let engines = Engines::new(EngineConfig::default());
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
            tokio::select! {
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
        println!("Shutting down {component_id}");
        component_to_exec_join_handle
            .remove(&component_id)
            .unwrap()
            .close()
            .await;
    }

    // Spawn new component executors
    for component in active_components {
        if component_to_exec_join_handle.contains_key(&component.component_id) {
            continue;
        }
        info!(
            "Starting {component_type} executor `{file_name}`",
            component_type = component.component_type,
            file_name = component.file_name
        );
        match component.component_type {
            ComponentType::WasmActivity => {
                let wasm_activity_config: WasmActivityConfig = serde_json::from_value(
                    component.config,
                )
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
                component_to_exec_join_handle.insert(
                    component.component_id,
                    ExecTask::spawn_new(
                        worker,
                        wasm_activity_config.exec_config,
                        now,
                        db_pool.clone(),
                        None,
                    ),
                );
            }
            ComponentType::WasmWorkflow => {
                let wasm_workflow_config: WasmWorkflowConfig = serde_json::from_value(
                    component.config,
                )
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
                component_to_exec_join_handle.insert(
                    component.component_id,
                    ExecTask::spawn_new(
                        worker,
                        wasm_workflow_config.exec_config,
                        now,
                        db_pool.clone(),
                        None,
                    ),
                );
            }
        }
    }
    Ok(())
}
