use crate::config::store::ConfigStore;
use crate::FunctionMetadataVerbosity;
use anyhow::Context;
use concepts::storage::DbPool;
use concepts::storage::{ComponentToggle, DbConnection};
use concepts::{ComponentConfigHash, FunctionMetadata};
use db_sqlite::sqlite_dao::SqlitePool;
use std::path::Path;
use wasm_workers::component_detector::ComponentDetector;

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
        let config_store: ConfigStore = serde_json::from_value(component.config)
            .expect("deserialization of config store failed");
        println!(
            "{component_type}\t{name}\tid: {hash}",
            component_type = component.config_id.component_type,
            name = config_store.name(),
            hash = component.config_id,
        );
        let component = db_connection
            .component_get_metadata(component.config_id)
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
    config_id: ComponentConfigHash,
    verbosity: Option<FunctionMetadataVerbosity>,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    let component = db_connection
        .component_get_metadata(config_id)
        .await
        .context("database error")?;
    let config_store: ConfigStore = serde_json::from_value(component.component.config)
        .expect("deserialization of config store failed");
    println!(
        "{component_type}\t{name}\tid: {hash}\tToggle: {toggle}",
        component_type = component.component.config_id.component_type,
        name = config_store.name(),
        hash = component.component.config_id,
        toggle = component.component.enabled,
    );
    println!("Exports:");
    inspect_fns(&component.exports, FunctionMetadataVerbosity::WithTypes);
    if let Some(verbosity) = verbosity {
        println!("Imports:");
        inspect_fns(&component.imports, verbosity);
    }
    Ok(())
}
