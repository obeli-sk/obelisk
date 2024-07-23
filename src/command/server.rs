use crate::config::store::ConfigStore;
use crate::config::toml::ConfigHolder;
use crate::config::toml::ObeliskConfig;
use crate::config::toml::VerifiedActivityConfig;
use crate::config::toml::VerifiedWorkflowConfig;
use anyhow::Context;
use concepts::storage::Component;
use concepts::storage::ComponentToggle;
use concepts::storage::ComponentWithMetadata;
use concepts::storage::DbConnection;
use concepts::storage::DbError;
use concepts::storage::DbPool;
use concepts::FunctionRegistry;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tracing::trace;
use tracing::{debug, info, warn};
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowWorker;

pub(crate) async fn run(
    config: ObeliskConfig,
    db_file: &PathBuf,
    clean: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
    let wasm_cache_dir = config
        .oci
        .get_wasm_directory(config_holder.project_dirs.as_ref())
        .await?;
    let codegen_cache = config
        .codegen_cache
        .get_directory_if_enabled(config_holder.project_dirs.as_ref())
        .await?;
    debug!("Using codegen cache? {codegen_cache:?}");
    if clean {
        let ignore_not_found = |err: std::io::Error| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        };
        tokio::fs::remove_file(db_file)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database file `{db_file:?}`"))?;
        tokio::fs::remove_dir_all(&wasm_cache_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete wasm cache directory {wasm_cache_dir:?}"))?;
        if let Some(codegen_cache) = &codegen_cache {
            tokio::fs::remove_dir_all(codegen_cache)
                .await
                .or_else(ignore_not_found)
                .with_context(|| {
                    format!("cannot delete codegen cache directory {wasm_cache_dir:?}")
                })?;
            tokio::fs::create_dir_all(codegen_cache)
                .await
                .with_context(|| {
                    format!("cannot create codegen cache directory {codegen_cache:?}")
                })?;
        }
    }
    tokio::fs::create_dir_all(&wasm_cache_dir)
        .await
        .with_context(|| format!("cannot create wasm cache directory {wasm_cache_dir:?}"))?;

    // Set up codegen cache
    let codegen_cache_config_file_holder =
        write_codegen_config(codegen_cache).context("error configuring codegen cache")?;
    let engines = Engines::auto_detect_allocator(
        &get_opts_from_env(),
        codegen_cache_config_file_holder
            .as_ref()
            .map(tempfile::NamedTempFile::path),
    )?;

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
    disable_all_components(db_pool.connection()).await?;

    debug!("Loading components: {config:?}");
    let mut exec_join_handles = Vec::new();

    for activity in config.activity.into_iter().filter(|it| it.common.enabled) {
        let activity = activity.verify_content_digest(&wasm_cache_dir).await?;

        if activity.enabled.into() {
            let exec_task_handle =
                instantiate_activity(activity, db_pool.clone(), &engines).await?;
            exec_join_handles.push(exec_task_handle);
        }
    }
    for workflow in config.workflow.into_iter().filter(|it| it.common.enabled) {
        let workflow = workflow.verify_content_digest(&wasm_cache_dir).await?;
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

fn write_codegen_config(
    codegen_cache: Option<PathBuf>,
) -> Result<Option<NamedTempFile>, anyhow::Error> {
    Ok(if let Some(codegen_cache) = codegen_cache {
        use std::io::Write;
        let mut codegen_cache_config_file = tempfile::NamedTempFile::new()?;
        writeln!(
            codegen_cache_config_file,
            r#"[cache]
enabled = true
directory = {codegen_cache:?}
"#
        )?;
        trace!(
            "Wrote temporary cache config to {:?}",
            codegen_cache_config_file.path()
        );
        Some(codegen_cache_config_file)
    } else {
        None
    })
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

async fn disable_all_components(conn: impl DbConnection) -> Result<(), anyhow::Error> {
    // TODO: should be in a tx together with enabling the current components
    for component in conn.component_list(ComponentToggle::Enabled).await? {
        conn.component_toggle(&component.config_id, ComponentToggle::Disabled, now())
            .await?;
    }
    Ok(())
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
    let config_id = exec_config.config_id.clone();
    let connection = db_pool.connection();
    // If the component exists, just enable it
    let found = match connection
        .component_toggle(&config_id, ComponentToggle::Enabled, now())
        .await
    {
        Ok(()) => {
            debug!("Enabled component {config_id}");
            true
        }
        Err(DbError::Specific(concepts::storage::SpecificError::NotFound)) => false,
        Err(other) => Err(other)?,
    };
    if !found {
        let component = ComponentWithMetadata {
            component: Component {
                config_id,
                config: serde_json::to_value(config)
                    .expect("ConfigStore must be serializable to JSON"),
                enabled: ComponentToggle::Enabled,
            },
            exports: worker.exported_functions().collect(),
            imports: worker.imported_functions().collect(),
        };
        connection
            .component_add(now(), component, ComponentToggle::Enabled)
            .await?;
    }
    Ok(ExecTask::spawn_new(worker, exec_config, now, db_pool, None))
}
