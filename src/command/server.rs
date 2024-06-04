use anyhow::Context;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::ComponentType;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecTask;
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

use crate::{WasmActivityConfig, WasmWorkflowConfig};

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
    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );
    // Attempt to start executors for every active component
    let components = db_pool
        .connection()
        .list_components(true)
        .await
        .context("cannot list active components")?;
    let mut exec_join_handles = Vec::with_capacity(components.len());
    for component in components {
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
                exec_join_handles.push(ExecTask::spawn_new(
                    worker,
                    wasm_activity_config.exec_config,
                    now,
                    db_pool.clone(),
                    None,
                ));
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
    tokio::signal::ctrl_c()
        .await
        .context("Unable to listen for shutdown signal")?;

    println!("Gracefully shutting down");
    timers_watcher.close().await;
    for exec_join_handle in exec_join_handles {
        exec_join_handle.close().await;
    }
    db_pool.close().await.context("cannot close sqlite")?;
    println!("Done");
    Ok(())
}
