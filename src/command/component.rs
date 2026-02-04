use crate::FunctionMetadataVerbosity;
use crate::FunctionRepositoryClient;
use crate::args;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::ConfigSource;
use crate::config::config_holder::OBELISK_HELP_TOML;
use crate::config::toml::ComponentLocationToml;
use crate::config::toml::ConfigName;
use crate::config::toml::OCI_SCHEMA_PREFIX;
use crate::get_fn_repository_client;
use crate::github;
use crate::init;
use crate::init::Guard;
use crate::oci;
use crate::project_dirs;
use anyhow::Context;
use concepts::ComponentType;
use concepts::{ContentDigest, FunctionFqn, FunctionMetadata};
use directories::BaseDirs;
use grpc::grpc_gen;
use grpc::to_channel;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tracing::info;
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
                locked,
            } => add(component_type, name, location, config, locked).await,
        }
    }
}

pub(crate) async fn add(
    component_type: ComponentType,
    name: String,
    location: ComponentLocationToml,
    toml_path: Option<PathBuf>,
    locked: bool,
) -> anyhow::Result<()> {
    // Check name
    ConfigName::new(name.clone().into()).context("name is invalid")?;
    let toml_path = toml_path.unwrap_or_else(|| PathBuf::from("obelisk.toml"));
    // Generate from default if file does not exist.
    let (mut file, contents, prefix) = if toml_path.try_exists().unwrap_or_default() {
        let contents = tokio::fs::read_to_string(&toml_path)
            .await
            .with_context(|| format!("cannot read {toml_path:?}"))?;
        let file = OpenOptions::new()
            .create(false)
            .truncate(true)
            .write(true)
            .open(&toml_path)
            .await
            .with_context(|| format!("cannot open {toml_path:?}"))?;
        (file, contents, "")
    } else {
        (
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .append(false)
                .open(&toml_path)
                .await
                .with_context(|| format!("cannot create {toml_path:?}"))?,
            String::new(),
            OBELISK_HELP_TOML,
        )
    };

    // Fetch, store to local Compute content_digest if `locked`
    let content_digest: Option<ContentDigest> = if locked {
        let config_holder = ConfigHolder::new(project_dirs(), BaseDirs::new(), None, true)?;
        let config = config_holder.load_config().await?;
        let wasm_cache_dir = config
            .wasm_global_config
            .get_wasm_cache_directory(&config_holder.path_prefixes)
            .await?;
        Some(compute_content_digest(&location, &wasm_cache_dir).await?)
    } else {
        None
    };

    let location_raw = match &location {
        ComponentLocationToml::Path(path) => path.clone(),
        ComponentLocationToml::Oci(reference) => {
            format!("{OCI_SCHEMA_PREFIX}{}", reference.whole())
        }
        ComponentLocationToml::GitHub(github_ref) => github_ref.to_string(),
    };

    let contents = {
        use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};

        let mut doc = contents.parse::<DocumentMut>()?;

        let key = component_type.to_string();

        // Ensure the entry exists in the document.
        // If missing, `insert` appends it to the end of the key list (End of File).
        if !doc.contains_key(&key) {
            doc.insert(&key, Item::ArrayOfTables(ArrayOfTables::new()));
        }

        // Get or create the array-of-tables (e.g. [[workflow]] items)

        // Get mutable reference to the array of tables
        let components = doc[&key]
            .as_array_of_tables_mut()
            .with_context(|| format!("expected {component_type} to be an array of tables"))?;

        // Find existing table by name
        if let Some(table) = components.iter_mut().find(|t| {
            t.get("name")
                .and_then(|item| item.as_str())
                .is_some_and(|s| s == name)
        }) {
            // Update existing
            table["location"] = value(location_raw);
            if let Some(ref digest) = content_digest {
                table["content_digest"] = value(digest.to_string());
            } else {
                table.remove("content_digest");
            }
        } else {
            // Insert new standard table
            let mut new_table = Table::new();
            new_table["name"] = value(name);
            new_table["location"] = value(location_raw);
            if let Some(ref digest) = content_digest {
                new_table["content_digest"] = value(digest.to_string());
            }
            components.push(new_table);
        }
        format!("{prefix}{doc}")
    };
    file.write_all(contents.as_bytes()).await?;
    file.flush().await?;
    Ok(())
}

/// Compute content digest for the given location and copy to cache.
/// For remote locations (OCI, GitHub), fetches to the cache directory.
/// For local files, computes hash and copies to cache.
async fn compute_content_digest(
    location: &ComponentLocationToml,
    wasm_cache_dir: &std::path::Path,
) -> anyhow::Result<ContentDigest> {
    match location {
        ComponentLocationToml::Path(path) => {
            let path = PathBuf::from(path);
            if !path.exists() {
                anyhow::bail!("file not found: {path:?}");
            }
            let digest = calculate_sha256_file(&path)
                .await
                .with_context(|| format!("cannot compute hash of file `{path:?}`"))?;

            // Copy to cache directory
            let cache_path = github::content_digest_to_wasm_file(wasm_cache_dir, &digest);
            if !cache_path.exists() {
                tokio::fs::create_dir_all(wasm_cache_dir)
                    .await
                    .with_context(|| format!("cannot create cache directory {wasm_cache_dir:?}"))?;
                tokio::fs::copy(&path, &cache_path)
                    .await
                    .with_context(|| format!("cannot copy {path:?} to cache {cache_path:?}"))?;
                info!("Copied to cache: {cache_path:?}");
            }
            info!("Computed content_digest: {digest}");
            Ok(digest)
        }
        ComponentLocationToml::Oci(reference) => {
            let metadata_dir = wasm_cache_dir.join("metadata");
            tokio::fs::create_dir_all(&metadata_dir)
                .await
                .with_context(|| format!("cannot create metadata directory {metadata_dir:?}"))?;
            let (digest, _path) = oci::pull_to_cache_dir(reference, wasm_cache_dir, &metadata_dir)
                .await
                .context("failed to pull OCI image")?;
            info!("Fetched OCI image, content_digest: {digest}");
            Ok(digest)
        }
        ComponentLocationToml::GitHub(github_ref) => {
            tokio::fs::create_dir_all(wasm_cache_dir)
                .await
                .with_context(|| format!("cannot create cache directory {wasm_cache_dir:?}"))?;
            let (digest, _path) = github::pull_to_cache_dir(github_ref, wasm_cache_dir)
                .await
                .context("failed to pull GitHub release asset")?;
            info!("Fetched GitHub release asset, content_digest: {digest}");
            Ok(digest)
        }
    }
}

pub(crate) async fn inspect(
    config: Option<ConfigSource>,
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
        .fetch(&wasm_cache_dir, &metadata_dir, path_prefixes, None)
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
