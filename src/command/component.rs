use crate::{FunctionMetadataVerbosity, WasmActivityConfig, WasmWorkflowConfig};
use anyhow::Context;
use concepts::storage::DbPool;
use concepts::storage::{Component, ComponentWithMetadata, DbConnection};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{ComponentId, ComponentType, FunctionMetadata};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use utils::time::now;
use wasm_workers::activity_worker::ActivityConfig;
use wasm_workers::auto_worker::DetectedComponent;
use wasm_workers::workflow_worker::{NonBlockingEventBatching, WorkflowConfig};
use wasm_workers::{
    activity_worker::RecycleInstancesSetting, workflow_worker::JoinNextBlockingStrategy,
};

pub(crate) async fn add<P: AsRef<Path>>(
    replace: bool,
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
    let file_name = wasm_path
        .file_name()
        .with_context(|| format!("cannot file name of `{wasm_path:?}`"))?
        .to_string_lossy()
        .into_owned();
    let component_id =
        hash(&wasm_path).with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
    let config_id = ConfigId::generate();
    let exec_config = ExecConfig {
        batch_size: 10,
        lock_expiry: Duration::from_secs(10),
        tick_sleep: Duration::from_millis(200),
        config_id,
    };
    let engine = DetectedComponent::get_engine();
    let detected = DetectedComponent::new(
        &StrVariant::Arc(Arc::from(wasm_path.to_string_lossy())),
        &engine,
    )
    .context("parsing error")?;
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
        .context("database error")?;
    if !replaced.is_empty() {
        println!("Replaced components:");
        for replaced in replaced {
            println!("\t{replaced}");
        }
    }
    Ok(())
}

pub(crate) fn inspect<P: AsRef<Path>>(
    wasm_path: P,
    verbosity: FunctionMetadataVerbosity,
) -> anyhow::Result<()> {
    let wasm_path = wasm_path.as_ref();
    let engine = DetectedComponent::get_engine();
    let detected = DetectedComponent::new(
        &StrVariant::Arc(Arc::from(wasm_path.to_string_lossy().into_owned())),
        &engine,
    )
    .context("parsing error")?;
    println!("Component type:");
    println!("\t{}", detected.component_type);

    println!("Exports:");
    inspect_fns(&detected.exports, verbosity);

    println!("Imports:");
    inspect_fns(&detected.imports, verbosity);
    Ok(())
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

fn inspect_fns(functions: &[FunctionMetadata], verbosity: FunctionMetadataVerbosity) {
    for (ffqn, parameter_types, result) in functions {
        print!("\t{ffqn}");
        if verbosity == FunctionMetadataVerbosity::WithTypes {
            print!(" {parameter_types}");
            if let Some(result) = result {
                let result = serde_json::to_string(&result).unwrap();
                print!(" -> {result}");
            }
        }
        println!();
    }
}

pub(crate) async fn list<P: AsRef<Path>>(
    db_file: P,
    verbosity: Option<FunctionMetadataVerbosity>,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    let components = db_connection
        .list_active_components()
        .await
        .context("database error")?;
    for component in components {
        println!(
            "{component_type}\t{hash}-{file_name}",
            component_type = component.component_type,
            hash = component.component_id,
            file_name = component.file_name,
        );
        let component = db_connection
            .get_component_metadata(component.component_id)
            .await
            .context("database error")?;
        println!("Exports");
        inspect_fns(&component.exports, FunctionMetadataVerbosity::WithTypes);
        if let Some(verbosity) = verbosity {
            println!("Imports");
            inspect_fns(&component.imports, verbosity);
        }
        println!();
    }
    Ok(())
}
