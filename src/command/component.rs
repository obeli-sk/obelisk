use super::grpc;
use crate::FunctionMetadataVerbosity;
use crate::{grpc_util::grpc_mapping::TonicClientResultExt, FunctionRepositoryClient};
use anyhow::Context;
use concepts::{ConfigId, FunctionFqn, FunctionMetadata};
use std::path::Path;
use utils::wasm_tools::WasmComponent;
use wasmtime::Engine;

pub(crate) fn inspect<P: AsRef<Path>>(
    wasm_path: P,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
) -> anyhow::Result<()> {
    let wasm_path = wasm_path.as_ref();
    let engine = {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        Engine::new(&wasmtime_config).unwrap()
    };
    let wasm_component = WasmComponent::new(wasm_path, &engine)?;
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
    config_id: Option<&ConfigId>,
    ffqn: Option<&FunctionFqn>,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
) -> anyhow::Result<()> {
    let components = client
        .list_components(tonic::Request::new(super::grpc::ListComponentsRequest {
            function: ffqn.map(grpc::FunctionName::from),
            config_id: config_id.map(|config_id| grpc::ConfigId {
                id: config_id.to_string(),
            }),
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
            id = component.config_id.map(|id| id.id).unwrap_or_default()
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

fn print_fn_details(vec: Vec<grpc::FunctionDetails>) -> Result<(), anyhow::Error> {
    for fn_detail in vec {
        let func = FunctionFqn::from(fn_detail.function.context("function must exist")?);
        print!("\t{func} : func(");
        let mut params = fn_detail.params.into_iter().peekable();
        while let Some(param) = params.next() {
            print!("{}: ", param.name.as_deref().unwrap_or("unknown"));
            print_wit_type(param.r#type.context("field `params.type` must exist")?)?;
            if params.peek().is_some() {
                print!(", ");
            }
        }
        print!(")");
        if let Some(return_type) = fn_detail.return_type {
            print!(" -> ");
            print_wit_type(return_type)?;
        }
        println!();
    }
    Ok(())
}

fn print_wit_type(wit_type: grpc::WitType) -> Result<(), anyhow::Error> {
    if let Some(wit_type) = wit_type.wit_type {
        print!("{wit_type}");
    } else if let Some(internal) = wit_type.internal {
        let str = String::from_utf8(internal.value)
            .with_context(|| format!("cannot convert to UTF-8 - {}", internal.type_url))?;
        print!("{str}");
    } else {
        print!("<unknown type>");
    }
    Ok(())
}
