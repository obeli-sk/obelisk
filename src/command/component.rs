use crate::FunctionMetadataVerbosity;
use crate::FunctionRepositoryClient;
use crate::args;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::OBELISK_HELP_TOML;
use crate::config::toml::ComponentLocationToml;
use crate::config::toml::OCI_SCHEMA_PREFIX;
use crate::get_fn_repository_client;
use crate::init;
use crate::init::Guard;
use crate::oci;
use crate::project_dirs;
use anyhow::Context;
use concepts::ComponentType;
use concepts::{FunctionFqn, FunctionMetadata};
use directories::BaseDirs;
use grpc::grpc_gen;
use grpc::to_channel;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use utils::sha256sum::calculate_sha256_file;
use utils::wasm_tools::WasmComponent;

impl args::Component {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            args::Component::Inspect {
                location,
                component_type,
                imports,
                extensions,
                config,
            } => {
                inspect(
                    config,
                    location,
                    component_type,
                    if imports {
                        FunctionMetadataVerbosity::ExportsAndImports
                    } else {
                        FunctionMetadataVerbosity::ExportsOnly
                    },
                    extensions,
                )
                .await
            }
            args::Component::List {
                api_url,
                imports,
                extensions,
            } => {
                let channel = to_channel(&api_url).await?;
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
            args::Component::Add {
                component_type,
                name,
                location,
                config,
            } => add(component_type, name, location, config).await,
        }
    }
}

pub(crate) async fn add(
    component_type: ComponentType,
    name: Option<String>,
    location: ComponentLocationToml,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    fn sanitize_name(input: &str) -> String {
        input
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    let toml_path = config.unwrap_or_else(|| PathBuf::from("obelisk.toml"));
    // If file exists => append to it
    // otherwise generate from default
    let (mut file, contents) = if toml_path.try_exists().unwrap_or_default() {
        (
            OpenOptions::new()
                .create(false)
                .append(true)
                .open(&toml_path)
                .await
                .with_context(|| format!("configuration file {toml_path:?} must exist "))?,
            "",
        )
    } else {
        (
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .append(false)
                .open(&toml_path)
                .await
                .with_context(|| format!("cannot create configuration file {toml_path:?}"))?,
            OBELISK_HELP_TOML,
        )
    };

    let (name, location_raw) = match location {
        ComponentLocationToml::Path(path) => {
            let name = {
                let path = Path::new(&path);
                if let Some(stem) = path.file_stem()
                    && let Some(name) = stem.to_str()
                {
                    name
                } else {
                    "unknown"
                }
                .to_string()
            };
            (name, path)
        }
        ComponentLocationToml::Oci(reference) => {
            let name = name.unwrap_or_else(|| {
                reference
                    .repository()
                    .rsplit_once('/')
                    .map(|(_, name)| name.to_string())
                    .unwrap_or_else(|| reference.repository().to_string())
            });
            (name, format!("{OCI_SCHEMA_PREFIX}{}", reference.whole()))
        }
    };
    let name = sanitize_name(&name);

    let appended = format!(
        r#"
{contents}
[[{component_type}]]
name = "{name}"
location = "{location_raw}"
"#
    );

    file.write_all(appended.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

pub(crate) async fn inspect(
    config: Option<PathBuf>,
    location: ComponentLocationToml,
    component_type: ComponentType,
    verbosity: FunctionMetadataVerbosity,
    extensions: bool,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), config, true)?;
    let mut config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&mut config)?;
    let path_prefixes = &config_holder.path_prefixes;

    let wasm_cache_dir = config
        .wasm_global_config
        .get_wasm_cache_directory(path_prefixes)
        .await?;

    let metadata_dir = wasm_cache_dir.join("metadata");
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .with_context(|| format!("cannot create wasm metadata directory {metadata_dir:?}"))?;

    let (_content_digest, wasm_path) = location
        .fetch(&wasm_cache_dir, &metadata_dir, path_prefixes)
        .await?;

    let wasm_path = {
        let output_parent = wasm_path
            .parent()
            .expect("direct parent of a file is never None");
        let input_digest = calculate_sha256_file(&wasm_path).await?;
        let transformed = WasmComponent::convert_core_module_to_component(
            &wasm_path,
            &input_digest,
            output_parent,
        )
        .await?;
        if let Some(transformed) = transformed {
            println!("Transformed Core WASM {wasm_path:?} to Component {transformed:?}");
            transformed
        } else {
            wasm_path
        }
    };

    let content_digest = calculate_sha256_file(&wasm_path).await?;
    println!("Content digest: {content_digest}");

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
            component_digest: None,
            extensions,
        }))
        .await?
        .into_inner()
        .components;
    for component in components {
        let component_id = component.component_id.expect("`component_id` is sent");
        println!(
            "{name}\t{ty}\t{sha}",
            name = component_id.name,
            ty = grpc_gen::ComponentType::try_from(component_id.component_type)
                .map_err(|_| ())
                .and_then(|ty| ComponentType::try_from(ty).map_err(|_| ()))
                .map(|ct| ct.to_string())
                .unwrap_or_else(|()| "unknown type".to_string()),
            sha = component_id.digest.map(|d| d.digest).unwrap_or_default()
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
