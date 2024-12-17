use super::grpc;
use crate::FunctionMetadataVerbosity;
use crate::{grpc_util::grpc_mapping::TonicClientResultExt, FunctionRepositoryClient};
use anyhow::Context;
use concepts::{FunctionFqn, FunctionMetadata};
use std::path::PathBuf;
use utils::wasm_tools::WasmComponent;
use wasmtime::Engine;

pub(crate) async fn inspect(
    wasm_path: PathBuf,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
    convert_core_module: bool,
) -> anyhow::Result<()> {
    if std::env::var("OBELISK_LOG").is_ok() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("OBELISK_LOG"))
            .init();
    }
    let wasm_path = if convert_core_module {
        let output_parent = wasm_path
            .parent()
            .expect("direct parent of a file is never None");
        WasmComponent::convert_core_module_to_component(&wasm_path, output_parent)
            .await?
            .unwrap_or(wasm_path)
    } else {
        wasm_path
    };
    let engine = {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        Engine::new(&wasmtime_config).unwrap()
    };
    let wasm_component = WasmComponent::new(wasm_path, &engine, None)?;

    println!("Exports:");
    inspect_fns(wasm_component.exported_functions(extensions));
    if verbosity > FunctionMetadataVerbosity::ExportsOnly {
        println!("Imports:");
        inspect_fns(wasm_component.imported_functions());
    }
    Ok(())
}

fn inspect_fns(functions: &[FunctionMetadata]) {
    for fn_metadata in functions {
        println!("\t{fn_metadata}");
    }
}

pub(crate) async fn list_components(
    mut client: FunctionRepositoryClient,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
) -> anyhow::Result<()> {
    let components = client
        .list_components(tonic::Request::new(super::grpc::ListComponentsRequest {
            function_name: None,
            component_id: None,
            extensions,
        }))
        .await
        .to_anyhow()?
        .into_inner()
        .components;
    for component in components {
        println!(
            "{name}\t{id}",
            name = component.name,
            id = component.component_id.map(|id| id.id).unwrap_or_default()
        );
        println!("Exports:");
        print_fn_details(component.exports)?;
        if verbosity > FunctionMetadataVerbosity::ExportsOnly {
            println!("Imports:");
            print_fn_details(component.imports)?;
        }
        println!();
    }
    Ok(())
}

fn print_fn_details(vec: Vec<grpc::FunctionDetail>) -> Result<(), anyhow::Error> {
    for fn_detail in vec {
        let func = FunctionFqn::try_from(fn_detail.function_name.context("function must exist")?)
            .expect("ffqn sent by the server must be valid");
        print!("\t{func} : func(");
        let mut params = fn_detail.params.into_iter().peekable();
        while let Some(param) = params.next() {
            print!("{}: ", param.name.as_deref().unwrap_or("(unknown)"));
            print_wit_type(param.r#type.context("field `params.type` must exist")?);
            if params.peek().is_some() {
                print!(", ");
            }
        }
        print!(")");
        if let Some(return_type) = fn_detail.return_type {
            print!(" -> ");
            print_wit_type(return_type);
        }
        println!();
    }
    Ok(())
}

fn print_wit_type(wit_type: grpc::WitType) {
    if let Some(wit_type) = wit_type.wit_type {
        print!("{wit_type}");
    } else {
        print!("<unknown type>");
    }
}
