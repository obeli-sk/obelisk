use crate::args::Generate;
use crate::args::shadow::PKG_VERSION;
use crate::command::server::{
    PrepareDirsParams, VerifyParams, create_engines, prepare_dirs, verify_config_compile_link,
};
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::{ConfigHolder, load_deployment_toml};
use crate::init::{self};
use crate::project_dirs;
use crate::wit_printer::{OutputToFile, process_pkg_with_deps};
use anyhow::Context;
use concepts::{ComponentType, ExecutionId, prefixed_ulid::DeploymentId};
use directories::{BaseDirs, ProjectDirs};
use hashbrown::HashSet;
use std::sync::Arc;
use std::{borrow::Cow, path::PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;
use utils::{wasm_tools::WasmComponent, wit};

impl Generate {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            #[cfg(debug_assertions)]
            Generate::ServerConfigSchema { output } => generate_server_config_schema(output),
            #[cfg(debug_assertions)]
            Generate::DeploymentSchema { output } => generate_deployment_schema(output),
            #[cfg(debug_assertions)]
            Generate::DeploymentCanonicalSchema { output } => {
                generate_deployment_canonical_schema(output)
            }
            #[cfg(debug_assertions)]
            Generate::DbSchema { output } => generate_db_schema(output),
            #[cfg(debug_assertions)]
            Generate::OpenApiSchema { output } => generate_openapi_schema(output),
            Generate::ServerConfig { output, overwrite } => {
                let config_file =
                    ConfigHolder::generate_default_server_config(output, overwrite).await?;
                println!("Generated {config_file:?}");
                Ok(())
            }
            Generate::Deployment { output, overwrite } => {
                let config_file =
                    ConfigHolder::generate_default_deployment_config(output, overwrite).await?;
                println!("Generated {config_file:?}");
                Ok(())
            }

            Generate::WitExtensions {
                component_type,
                input_wit_directory,
                output_directory,
                force,
            } => {
                generate_exported_extension_wits(
                    input_wit_directory,
                    output_directory,
                    component_type,
                    force,
                )
                .await
            }
            Generate::WitSupport {
                component_type,
                output_directory,
                overwrite,
            } => generate_support_wits(component_type, output_directory, overwrite).await,
            Generate::WitDeps {
                deployment,
                output_directory,
                overwrite,
            } => {
                generate_wit_deps(
                    project_dirs(),
                    BaseDirs::new(),
                    deployment,
                    output_directory,
                    overwrite,
                )
                .await
            }
            Generate::ExecutionId => {
                println!("{}", ExecutionId::generate());
                Ok(())
            }
        }
    }
}

#[cfg(debug_assertions)]
fn write_schema<T: schemars::JsonSchema>(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    use std::{
        fs::File,
        io::{BufWriter, Write as _, stdout},
    };
    let schema = schemars::schema_for!(T);
    if let Some(output) = output {
        let mut writer = BufWriter::new(File::create(&output)?);
        serde_json::to_writer_pretty(&mut writer, &schema)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    } else {
        serde_json::to_writer_pretty(stdout().lock(), &schema)?;
    }
    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn generate_server_config_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    write_schema::<crate::config::toml::ServerConfigToml>(output)
}

#[cfg(debug_assertions)]
pub(crate) fn generate_deployment_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    write_schema::<crate::config::toml::DeploymentToml>(output)
}

#[cfg(debug_assertions)]
pub(crate) fn generate_deployment_canonical_schema(
    output: Option<PathBuf>,
) -> Result<(), anyhow::Error> {
    write_schema::<crate::config::toml::DeploymentCanonical>(output)
}

#[cfg(debug_assertions)]
pub(crate) fn generate_db_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    use std::{
        fs::File,
        io::{BufWriter, Write as _, stdout},
    };
    let schema = schemars::schema_for!(concepts::storage::DbStorageSchema);
    if let Some(output) = output {
        let mut writer = BufWriter::new(File::create(&output)?);
        serde_json::to_writer_pretty(&mut writer, &schema)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    } else {
        serde_json::to_writer_pretty(stdout().lock(), &schema)?;
    }
    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn generate_openapi_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    use std::{
        fs::File,
        io::{BufWriter, Write as _, stdout},
    };
    use utoipa::OpenApi as _;
    let schema = crate::server::web_api_server::ApiDoc::openapi();
    if let Some(output) = output {
        let mut writer = BufWriter::new(File::create(&output)?);
        serde_json::to_writer_pretty(&mut writer, &schema)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    } else {
        serde_json::to_writer_pretty(stdout().lock(), &schema)?;
        println!();
    }
    Ok(())
}

pub(crate) const OBELISK_WIT_HEADER: &str = "// Generated by Obelisk";

pub(crate) async fn generate_exported_extension_wits(
    input_wit_directory: PathBuf,
    output_directory: PathBuf,
    component_type: ComponentType,
    force: bool,
) -> Result<(), anyhow::Error> {
    let wasm_component = WasmComponent::new_from_wit_folder(&input_wit_directory, component_type)?;
    let pkgs_to_wits = wasm_component.exported_extension_wits()?;
    for (pkg_fqn, new_content) in pkgs_to_wits {
        let pkg_file_name = pkg_fqn.as_file_name();
        let pkg_folder = output_directory.join(&pkg_file_name);
        let wit_file = pkg_folder.join(format!("{pkg_file_name}.wit"));

        let old_content = tokio::fs::read_to_string(&wit_file)
            .await
            .unwrap_or_default();

        let old_content = if force {
            None
        } else {
            Some(strip_header(&old_content))
        };
        if old_content.as_ref() != Some(&new_content) {
            let new_content = format!("{OBELISK_WIT_HEADER} {PKG_VERSION}\n{new_content}");
            tokio::fs::create_dir_all(&pkg_folder)
                .await
                .with_context(|| format!("cannot write {pkg_folder:?}"))?;
            tokio::fs::write(&wit_file, new_content.as_bytes())
                .await
                .with_context(|| format!("cannot write {wit_file:?}"))?;
            println!("{wit_file:?} created or updated");
        } else {
            println!("{wit_file:?} is up to date");
        }
    }
    Ok(())
}

fn strip_header(old_content: &str) -> String {
    let old_content = match old_content.strip_prefix(OBELISK_WIT_HEADER) {
        Some(wit) => {
            if let Some((_, wit)) = wit.split_once('\n') {
                Cow::Borrowed(wit)
            } else {
                Cow::Borrowed(wit)
            }
        }
        None => Cow::Borrowed(old_content),
    };
    let old_content = match old_content.strip_prefix(&format!("/{OBELISK_WIT_HEADER}")) {
        // Bug in wasm_tools is turning // into ///
        Some(wit) => {
            if let Some((_, wit)) = wit.split_once('\n') {
                Cow::Borrowed(wit)
            } else {
                Cow::Borrowed(wit)
            }
        }
        None => old_content,
    };
    old_content.into_owned()
}

pub(crate) async fn generate_support_wits(
    component_type: ComponentType,
    output_directory: PathBuf,
    overwrite: bool,
) -> Result<(), anyhow::Error> {
    let files = match component_type {
        ComponentType::Activity => {
            vec![
                wit::WIT_OBELISK_ACTIVITY_PACKAGE_PROCESS,
                wit::WIT_OBELISK_LOG_PACKAGE,
            ]
        }
        ComponentType::ActivityStub => vec![],
        ComponentType::Workflow => vec![
            wit::WIT_OBELISK_TYPES_PACKAGE,
            wit::WIT_OBELISK_WORKFLOW_PACKAGE,
            wit::WIT_OBELISK_LOG_PACKAGE,
        ],
        ComponentType::WebhookEndpoint => {
            vec![
                wit::WIT_OBELISK_TYPES_PACKAGE, // Needed for -schedule ext functions.
                wit::WIT_OBELISK_LOG_PACKAGE,
            ]
        }
    };
    for [folder, filename, contents] in files {
        let output_directory = output_directory.join(folder);
        let target_wit = output_directory.join(filename);
        if let Ok(actual) = tokio::fs::read_to_string(&target_wit).await
            && actual == contents
        {
            println!("{target_wit:?} is up to date");
        } else {
            tokio::fs::create_dir_all(&output_directory)
                .await
                .with_context(|| format!("cannot write {output_directory:?}"))?;
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .create_new(!overwrite)
                .open(&target_wit)
                .await
                .with_context(|| {
                    format!(
                        "cannot open {target_wit:?} for writing{}",
                        if !overwrite {
                            ", try using `--overwrite`"
                        } else {
                            ""
                        }
                    )
                })?;
            file.write_all(contents.as_bytes())
                .await
                .with_context(|| format!("cannot write to {target_wit:?}"))?;

            println!("{target_wit:?} created or updated");
        }
    }
    Ok(())
}

pub(crate) async fn generate_wit_deps(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    deployment_path: PathBuf,
    output_directory: PathBuf,
    overwrite: bool,
) -> Result<(), anyhow::Error> {
    let (deployment_toml, deployment_dir) = load_deployment_toml(deployment_path).await?;
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, None)?;
    let config = config_holder.load_config().await?;
    let _guard = init::init(&config)?;
    let mut path_prefixes = config_holder.path_prefixes;
    path_prefixes.deployment_dir = Some(deployment_dir);
    let path_prefixes = Arc::new(path_prefixes);
    let deployment =
        crate::config::toml::resolve_local_refs_to_canonical(&deployment_toml, &path_prefixes)
            .await?;
    let (termination_sender, mut termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    let verify_params = VerifyParams {
        dir_params: PrepareDirsParams {
            clean_cache: false,
            clean_codegen_cache: false,
        },
        ignore_missing_env_vars: true,
        suppress_type_checking_errors: true, // Just extracting WITs, not running components
    };
    let prepared_dirs = prepare_dirs(&config, &verify_params.dir_params, &path_prefixes).await?;
    let engines = create_engines(&config, &prepared_dirs)?;
    let compiled_and_linked = Box::pin(verify_config_compile_link(
        config,
        engines,
        &prepared_dirs,
        deployment,
        path_prefixes,
        DeploymentId::generate(),
        verify_params,
        &mut termination_watcher,
    ))
    .await?;
    tokio::fs::create_dir_all(&output_directory)
        .await
        .with_context(|| format!("cannot create the output directory {output_directory:?}"))?;
    let include_exports = true;
    let mut output = OutputToFile::default();
    for component in &compiled_and_linked
        .component_registry_ro
        .list(include_exports)
    {
        if let Some(importable) = &component.workflow_or_activity_config {
            let wit = &component.wit;
            assert!(
                component.component_id.component_type.is_activity()
                    || component.component_id.component_type == ComponentType::Workflow
            );
            let mut already_processed_packages = HashSet::new();

            let packages: HashSet<_> = importable
                .exports_hierarchy_ext
                .iter()
                .map(|ifc_fqns| ifc_fqns.ifc_fqn.pkg_fqn_name())
                .collect();

            for pkg in packages {
                output = process_pkg_with_deps(wit, &pkg, &mut already_processed_packages, output)
                    .await?;
            }
        }
    }
    output.write(&output_directory, overwrite).await?;
    Ok(())
}
