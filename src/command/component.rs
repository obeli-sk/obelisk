use crate::{FunctionMetadataVerbosity, WasmActivityConfig, WasmWorkflowConfig};
use anyhow::Context;
use concepts::prefixed_ulid::ConfigId;
use concepts::storage::{Component, ComponentToggle, ComponentWithMetadata, DbConnection};
use concepts::storage::{ComponentAddError, DbPool};
use concepts::{ComponentId, ComponentType, FunctionMetadata};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecConfig;
use std::path::Path;
use std::time::Duration;
use utils::time::now;
use wasm_workers::activity_worker::ActivityConfig;
use wasm_workers::component_detector::ComponentDetector;
use wasm_workers::workflow_worker::{NonBlockingEventBatching, WorkflowConfig};
use wasm_workers::{
    activity_worker::RecycleInstancesSetting, workflow_worker::JoinNextBlockingStrategy,
};

pub(crate) async fn add<P: AsRef<Path>>(
    toggle: ComponentToggle,
    wasm_path: P,
    db_file: P,
) -> anyhow::Result<()> {
    let wasm_path = wasm_path.as_ref();
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let wasm_path = wasm_path
        .canonicalize()
        .with_context(|| format!("cannot canonicalize file `{wasm_path:?}`"))?;
    let name = wasm_path
        .file_name()
        .with_context(|| format!("cannot file name of `{wasm_path:?}`"))?
        .to_string_lossy()
        .into_owned();
    let component_id = wasm_workers::component_detector::file_hash(&wasm_path)
        .await
        .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
    let config_id = todo!();
    let exec_config = ExecConfig {
        batch_size: 10,
        lock_expiry: Duration::from_secs(1),
        tick_sleep: Duration::from_millis(200),
        config_id,
    };
    let engine = ComponentDetector::get_engine();
    let detected = ComponentDetector::new(&wasm_path, &engine).context("parsing error")?;
    let config = match detected.component_type {
        ComponentType::WasmActivity => serde_json::to_value(WasmActivityConfig {
            wasm_path: wasm_path.to_string_lossy().to_string(),
            exec_config,
            activity_config: ActivityConfig {
                config_id,
                recycled_instances: RecycleInstancesSetting::Enable,
            },
        })
        .expect("serializing of `WasmActivityConfig` must not fail"),
        ComponentType::WasmWorkflow => serde_json::to_value(WasmWorkflowConfig {
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
        .expect("serializing of `WasmWorkflowConfig` must not fail"),
    };
    let component = ComponentWithMetadata {
        component: Component {
            component_id: component_id.clone(),
            component_type: detected.component_type,
            config,
            name,
        },
        exports: detected.exports,
        imports: detected.imports,
    };
    // TODO: Add "Already exists" error
    match db_pool
        .connection()
        .component_add(now(), component, toggle)
        .await
    {
        Ok(()) => {
            println!("{component_id} added");
            Ok(())
        }
        Err(ComponentAddError::Conflict(conflicts)) => {
            println!("Cannot append component with state set to active. Following components have conflicting exports:");
            for conflict in conflicts {
                println!("{conflict}");
            }
            Err(anyhow::anyhow!("cannot add componnent"))
        }
        Err(db_error) => Err(db_error).context("database error"),
    }
}

pub(crate) async fn inspect<P: AsRef<Path>>(
    wasm_path: P,
    verbosity: FunctionMetadataVerbosity,
) -> anyhow::Result<()> {
    let wasm_path = wasm_path.as_ref();
    let component_id = wasm_workers::component_detector::file_hash(wasm_path).await?;
    println!("Id:\n\t{component_id}");
    let engine = ComponentDetector::get_engine();
    let detected = ComponentDetector::new(wasm_path, &engine).context("parsing error")?;
    println!("Component type:\n\t{}", detected.component_type);
    println!("Exports:");
    inspect_fns(&detected.exports, FunctionMetadataVerbosity::WithTypes);
    println!("Imports:");
    inspect_fns(&detected.imports, verbosity);
    Ok(())
}

fn inspect_fns(functions: &[FunctionMetadata], verbosity: FunctionMetadataVerbosity) {
    for (ffqn, parameter_types, result) in functions {
        print!("\t{ffqn}");
        if verbosity == FunctionMetadataVerbosity::WithTypes {
            print!(" func{parameter_types:?}");
            if let Some(result) = result {
                print!(" -> {result:?}");
            }
        }
        println!();
    }
}

pub(crate) async fn list<P: AsRef<Path>>(
    db_file: P,
    toggle: ComponentToggle,
    verbosity: Option<FunctionMetadataVerbosity>,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    let components = db_connection
        .component_list(toggle)
        .await
        .context("database error")?;
    for component in components {
        println!(
            "{component_type}\t{name}\tid: {hash}",
            component_type = component.component_type,
            hash = component.component_id,
            name = component.name,
        );
        let (component, _) = db_connection
            .component_get_metadata(component.component_id)
            .await
            .context("database error")?;
        println!("Exports:");
        inspect_fns(&component.exports, FunctionMetadataVerbosity::WithTypes);
        if let Some(verbosity) = verbosity {
            println!("Imports:");
            inspect_fns(&component.imports, verbosity);
        }
        println!();
    }
    Ok(())
}

pub(crate) async fn get<P: AsRef<Path>>(
    db_file: P,
    component_id: ComponentId,
    verbosity: Option<FunctionMetadataVerbosity>,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    let (component, toggle) = db_connection
        .component_get_metadata(component_id)
        .await
        .context("database error")?;

    println!(
        "{component_type}\t{name}\tid: {hash}\tToggle: {toggle}",
        component_type = component.component.component_type,
        hash = component.component.component_id,
        name = component.component.name,
    );
    println!("Exports:");
    inspect_fns(&component.exports, FunctionMetadataVerbosity::WithTypes);
    if let Some(verbosity) = verbosity {
        println!("Imports:");
        inspect_fns(&component.imports, verbosity);
    }
    Ok(())
}

pub(crate) async fn disable<P: AsRef<Path>>(
    db_file: P,
    component_id: ComponentId,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    db_connection
        .component_toggle(component_id, ComponentToggle::Disabled, now())
        .await?;
    Ok(())
}

pub(crate) async fn enable<P: AsRef<Path>>(
    db_file: P,
    component_id: ComponentId,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    db_connection
        .component_toggle(component_id, ComponentToggle::Enabled, now())
        .await?;
    Ok(())
}
