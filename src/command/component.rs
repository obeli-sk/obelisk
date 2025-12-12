use crate::FunctionMetadataVerbosity;
use crate::FunctionRepositoryClient;
use crate::args;
use crate::get_fn_repository_client;
use crate::oci;
use anyhow::Context;
use concepts::ComponentType;
use concepts::{FunctionFqn, FunctionMetadata};
use grpc::grpc_gen;
use grpc::to_channel;
use std::path::PathBuf;
use utils::wasm_tools::WasmComponent;

impl args::Component {
    pub(crate) async fn run(self, api_url: &str) -> Result<(), anyhow::Error> {
        match self {
            args::Component::Inspect {
                path,
                component_type,
                imports,
                extensions,
                convert_core_module,
            } => {
                inspect(
                    path,
                    component_type,
                    if imports {
                        FunctionMetadataVerbosity::ExportsAndImports
                    } else {
                        FunctionMetadataVerbosity::ExportsOnly
                    },
                    extensions,
                    convert_core_module,
                )
                .await
            }
            args::Component::List {
                imports,
                extensions,
            } => {
                let channel = to_channel(api_url).await?;
                let client = get_fn_repository_client(channel).await?;
                list_components(
                    client,
                    if imports {
                        FunctionMetadataVerbosity::ExportsAndImports
                    } else {
                        FunctionMetadataVerbosity::ExportsOnly
                    },
                    extensions,
                )
                .await
            }
            args::Component::Push { path, image_name } => oci::push(path, &image_name).await,
        }
    }
}

pub(crate) async fn inspect(
    wasm_path: PathBuf,
    component_type: ComponentType,
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
            .map(|(wasm_path, _content_digest)| wasm_path)
            .unwrap_or(wasm_path)
    } else {
        wasm_path
    };

    let wasm_component = WasmComponent::new(wasm_path, component_type)?;

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
        .list_components(tonic::Request::new(grpc_gen::ListComponentsRequest {
            function_name: None,
            component_id: None,
            extensions,
        }))
        .await?
        .into_inner()
        .components;
    for component in components {
        let component_id = component.component_id.expect("`component_id` is sent");
        println!(
            "{name}\t{ty}",
            name = component_id.name,
            ty = grpc_gen::ComponentType::try_from(component_id.component_type).map_or_else(
                |_| "unknown type".to_string(),
                |ty| ComponentType::from(ty).to_string()
            )
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

fn print_fn_details(vec: Vec<grpc_gen::FunctionDetail>) -> Result<(), anyhow::Error> {
    for fn_detail in vec {
        let func = fn_detail.function_name.context("function must exist")?;
        let func = if let Ok(func) = FunctionFqn::try_from(func.clone())
            .with_context(|| format!("ffqn sent by the server must be valid - {func:?}"))
        {
            func.to_string()
        } else {
            // FIXME: here because of functions like: interface_name: "wasi:io/poll@0.2.3", function_name: "[method]pollable.block"
            format!("{} . {}", func.interface_name, func.function_name)
        };
        print!("\t{func} : func(");
        let mut params = fn_detail.params.into_iter().peekable();
        while let Some(param) = params.next() {
            print!("{}: ", param.name);
            print_wit_type(&param.r#type.context("field `params.type` must exist")?);
            if params.peek().is_some() {
                print!(", ");
            }
        }
        print!(")");
        if let Some(return_type) = fn_detail.return_type {
            print!(" -> ");
            print_wit_type(&return_type);
        }
        println!();
    }
    Ok(())
}

fn print_wit_type(wit_type: &grpc_gen::WitType) {
    print!("{}", wit_type.wit_type);
}
