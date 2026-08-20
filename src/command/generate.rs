use crate::args::Generate;
use crate::args::shadow::PKG_VERSION;
use crate::command::server::{
    PrepareDirsParams, RuntimeConfigAvailability, VerifyParams, create_engines,
    deployment_compile_link, deployment_verify_config, prepare_dirs, server_verify,
};
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::{
    ConfigHolder, OBELISK_HELP_DEPLOYMENT_TOML, server_config_template,
};
use crate::config::deployment::{prepare_deployment_manifest, resolve_manifest};
use crate::config::secret_registry::SecretRegistry;
use crate::config::deployment::OCI_SCHEMA_PREFIX;
use crate::config::server::ServerConfigToml;
use crate::init::{self};
use crate::project_dirs;
use anyhow::{Context, ensure};
use concepts::cas::{Cas, InMemoryCas};
use concepts::{ComponentType, ExecutionId, PackageIfcFns, PkgFqn, prefixed_ulid::DeploymentId};
use directories::{BaseDirs, ProjectDirs};
use hashbrown::{HashMap, HashSet};
use serde::Serialize;
use std::{borrow::Cow, path::PathBuf, sync::Arc};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;
use toml_edit::{DocumentMut, Item};
use utils::{wasm_tools::WasmComponent, wit};
use wasm_workers::registry::WitOrigin;

impl Generate {
    pub(crate) async fn run(
        self,
        secret_registry: Arc<SecretRegistry>,
    ) -> Result<(), anyhow::Error> {
        match self {
            Generate::ServerConfig {
                json,
                trusted,
                output,
                force,
            } => {
                if let Some(output) = output {
                    let config_file =
                        ConfigHolder::generate_server_config(output, trusted, force).await?;
                    let result = GeneratedPathStatus {
                        path: config_file,
                        status: "generated",
                    };
                    print_generated_path_statuses(&[result], json)?;
                } else {
                    print!("{}", server_config_template(trusted));
                }
                Ok(())
            }
            Generate::Deployment {
                json,
                output,
                force,
            } => {
                if let Some(output) = output {
                    let config_file =
                        ConfigHolder::generate_default_deployment_config(output, force).await?;
                    let result = GeneratedPathStatus {
                        path: config_file,
                        status: "generated",
                    };
                    print_generated_path_statuses(&[result], json)?;
                } else {
                    print!("{OBELISK_HELP_DEPLOYMENT_TOML}");
                }
                Ok(())
            }

            Generate::WitExtensions {
                json,
                component_type,
                input_wit_directory,
                output_directory,
                force,
            } => {
                let results = generate_exported_extension_wits(
                    input_wit_directory,
                    output_directory,
                    component_type,
                    force,
                )
                .await?;
                print_generated_path_statuses(&results, json)?;
                Ok(())
            }
            Generate::WitSupport {
                json,
                component_type,
                output_directory,
                force,
            } => {
                let results =
                    generate_support_wits(component_type, output_directory, force).await?;
                print_generated_path_statuses(&results, json)?;
                Ok(())
            }
            Generate::WitDeps {
                json,
                deployment,
                output_directory,
                force,
                skip_local,
                prune,
            } => {
                let results = generate_wit_deps(
                    project_dirs(),
                    BaseDirs::new(),
                    deployment,
                    output_directory,
                    GenerateWitDepsOptions {
                        force,
                        skip_local,
                        prune,
                    },
                    secret_registry,
                )
                .await?;
                print_generated_path_statuses(&results, json)?;
                Ok(())
            }
            Generate::ExecutionId { json } => {
                let execution_id = ExecutionId::generate();
                if json {
                    println!("{}", serde_json::to_string_pretty(&execution_id)?);
                } else {
                    println!("{execution_id}");
                }
                Ok(())
            }
            Generate::DeploymentId { json } => {
                let deployment_id = DeploymentId::generate();
                if json {
                    println!("{}", serde_json::to_string_pretty(&deployment_id)?);
                } else {
                    println!("{deployment_id}");
                }
                Ok(())
            }
            Generate::Token {
                json,
                server_config,
            } => {
                let token = crate::api::generate_token();
                let hash = crate::api::token_hash(&token);
                if let Some(server_config) = &server_config {
                    let contents = tokio::fs::read_to_string(server_config)
                        .await
                        .with_context(|| format!("cannot read {server_config:?}"))?;
                    let contents = add_token_hash(&contents, &hash)
                        .with_context(|| format!("cannot update {server_config:?}"))?;
                    tokio::fs::write(server_config, contents)
                        .await
                        .with_context(|| format!("cannot write {server_config:?}"))?;
                    eprintln!("Added {hash} to `api.token_hashes` in {server_config:?}");
                }
                if json {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&serde_json::json!({
                            "token": token,
                            "hash": hash.to_string(),
                        }))?
                    );
                } else {
                    println!("{token}");
                    // Quiet when stdout is captured ($(...), direnv, pipes).
                    if server_config.is_none()
                        && std::io::IsTerminal::is_terminal(&std::io::stdout())
                    {
                        eprintln!(
                            "Add to server.toml (or rerun with --server-config <server.toml>):"
                        );
                        eprintln!("api.token_hashes = [\"{hash}\"]");
                    }
                }
                Ok(())
            }
            Generate::Prompt { description } => {
                let version = format!("v{PKG_VERSION}");
                let description = description.join(" ");
                println!(
                    "Fetch https://obeli.sk/docs/{version}/llms.txt for the full Obelisk reference. Task:\n{description}"
                );
                Ok(())
            }
        }
    }
}

fn add_token_hash(
    server_config_contents: &str,
    hash: &concepts::component_id::Digest,
) -> Result<String, anyhow::Error> {
    use toml_edit::{DocumentMut, Item, Table};
    let mut doc = server_config_contents
        .parse::<DocumentMut>()
        .context("cannot parse server config as TOML")?;
    let api = doc
        .entry("api")
        .or_insert(Item::Table(Table::new()))
        .as_table_like_mut()
        .context("`api` must be a table")?;
    let hashes = api
        .entry("token_hashes")
        .or_insert(Item::Value(
            toml_edit::Value::Array(toml_edit::Array::new()),
        ))
        .as_array_mut()
        .context("`api.token_hashes` must be an array")?;
    hashes.push(hash.to_string());
    Ok(doc.to_string())
}

#[derive(Debug, Serialize)]
struct GeneratedPathStatus {
    path: PathBuf,
    status: &'static str,
}

fn print_generated_path_statuses(
    results: &[GeneratedPathStatus],
    json: bool,
) -> Result<(), anyhow::Error> {
    if json {
        println!("{}", serde_json::to_string_pretty(results)?);
    } else {
        for result in results {
            match result.status {
                "generated" => println!("Generated {:?}", result.path),
                "created_or_updated" => println!("{:?} created or updated", result.path),
                "up_to_date" => println!("{:?} is up to date", result.path),
                "written" => println!("{:?} written", result.path),
                status => println!("{:?} {status}", result.path),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
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

#[cfg(test)]
fn generate_server_config_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    write_schema::<crate::config::server::ServerConfigToml>(output)
}

#[cfg(test)]
fn generate_authored_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    write_schema::<crate::config::deployment::DeploymentToml>(output)
}

#[cfg(test)]
fn generate_db_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
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

#[cfg(test)]
fn generate_openapi_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
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

#[cfg(test)]
fn generate_component_metadata_annotation_schema(
    output: Option<PathBuf>,
) -> Result<(), anyhow::Error> {
    write_schema::<crate::oci::ComponentMetadataAnnotation>(output)
}

#[cfg(test)]
#[derive(Debug, Serialize)]
struct CliCommandSchema {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    about: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<CliArgSchema>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    positionals: Vec<CliArgSchema>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    subcommands: Vec<CliCommandSchema>,
}

#[cfg(test)]
#[derive(Debug, Serialize)]
struct CliArgSchema {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    short: Option<char>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    accepts: Option<CliArgAcceptsSchema>,
}

#[cfg(test)]
#[derive(Debug, Serialize)]
struct CliArgAcceptsSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    many: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<bool>,
}

#[cfg(test)]
fn generate_cli_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    let command = <crate::args::Args as clap::CommandFactory>::command();
    let schema = command_to_schema(&command);
    write_json(output, &schema)
}

#[cfg(test)]
fn command_to_schema(command: &clap::Command) -> CliCommandSchema {
    let mut options = Vec::new();
    let mut positionals = Vec::new();
    for arg in command.get_arguments() {
        if skip_arg(arg) {
            continue;
        }
        let schema = arg_to_schema(arg);
        if arg.is_positional() {
            positionals.push(schema);
        } else {
            options.push(schema);
        }
    }

    CliCommandSchema {
        name: command.get_name().to_string(),
        about: command_about(command),
        options,
        positionals,
        subcommands: command.get_subcommands().map(command_to_schema).collect(),
    }
}

#[cfg(test)]
fn arg_to_schema(arg: &clap::Arg) -> CliArgSchema {
    CliArgSchema {
        name: arg_name(arg),
        short: arg.get_short(),
        value_name: arg_value_name(arg),
        help: arg_help(arg),
        required: arg.is_required_set(),
        accepts: arg_accepts(arg),
    }
}

#[cfg(test)]
fn command_about(command: &clap::Command) -> Option<String> {
    command
        .get_about()
        .map(ToString::to_string)
        .filter(|about| !about.trim().is_empty())
}

#[cfg(test)]
fn arg_help(arg: &clap::Arg) -> Option<String> {
    arg.get_help()
        .map(ToString::to_string)
        .filter(|help| !help.trim().is_empty())
}

#[cfg(test)]
fn arg_name(arg: &clap::Arg) -> String {
    if let Some(long) = arg.get_long() {
        format!("--{long}")
    } else {
        arg.get_id().to_string()
    }
}

#[cfg(test)]
fn arg_value_name(arg: &clap::Arg) -> Option<String> {
    arg.get_num_args()
        .filter(clap::builder::ValueRange::takes_values)
        .and_then(|_| arg.get_value_names())
        .and_then(|names| names.first())
        .map(ToString::to_string)
}

#[cfg(test)]
fn arg_accepts(arg: &clap::Arg) -> Option<CliArgAcceptsSchema> {
    let choices: Vec<String> = arg
        .get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_string())
        .filter(|value| value != "true" && value != "false")
        .collect();
    let one_of = (!choices.is_empty()).then_some(choices);

    let many = arg.get_num_args().and_then(|range| {
        let max = range.max_values();
        ((range.min_values() > 1) || max > 1 || max == usize::MAX).then_some(true)
    });

    let path = matches!(
        arg.get_value_hint(),
        clap::ValueHint::AnyPath
            | clap::ValueHint::FilePath
            | clap::ValueHint::DirPath
            | clap::ValueHint::ExecutablePath
    )
    .then_some(true);

    if one_of.is_none() && many.is_none() && path.is_none() {
        None
    } else {
        Some(CliArgAcceptsSchema { one_of, many, path })
    }
}

#[cfg(test)]
fn skip_arg(arg: &clap::Arg) -> bool {
    matches!(
        arg.get_action(),
        clap::ArgAction::Help | clap::ArgAction::HelpShort | clap::ArgAction::HelpLong
    )
}

#[cfg(test)]
fn write_json<T: serde::Serialize>(
    output: Option<PathBuf>,
    value: &T,
) -> Result<(), anyhow::Error> {
    use std::{
        fs::File,
        io::{BufWriter, Write as _, stdout},
    };
    if let Some(output) = output {
        let mut writer = BufWriter::new(File::create(&output)?);
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    } else {
        serde_json::to_writer_pretty(stdout().lock(), value)?;
        println!();
    }
    Ok(())
}

pub(crate) const OBELISK_WIT_HEADER: &str = "// Generated by Obelisk";

async fn generate_exported_extension_wits(
    input_wit_directory: PathBuf,
    output_directory: PathBuf,
    component_type: ComponentType,
    force: bool,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let wasm_component = WasmComponent::new_from_wit_folder(&input_wit_directory, component_type)?;
    let pkgs_to_wits = wasm_component.exported_extension_wits()?;
    let mut results = Vec::new();
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
            results.push(GeneratedPathStatus {
                path: wit_file,
                status: "created_or_updated",
            });
        } else {
            results.push(GeneratedPathStatus {
                path: wit_file,
                status: "up_to_date",
            });
        }
    }
    Ok(results)
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

async fn generate_support_wits(
    component_type: ComponentType,
    output_directory: PathBuf,
    force: bool,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let mut results = Vec::new();
    let files = match component_type {
        ComponentType::Activity => {
            vec![wit::WIT_OBELISK_LOG_PACKAGE]
        }
        ComponentType::Workflow => vec![
            wit::WIT_OBELISK_TYPES_PACKAGE,
            wit::WIT_OBELISK_WORKFLOW_PACKAGE,
            wit::WIT_OBELISK_LOG_PACKAGE,
        ],
        ComponentType::WebhookEndpoint => {
            vec![
                wit::WIT_OBELISK_TYPES_PACKAGE, // Needed for -schedule ext functions.
                wit::WIT_OBELISK_WEBHOOK_PACKAGE,
                wit::WIT_OBELISK_LOG_PACKAGE,
            ]
        }
        ComponentType::ActivityStub | ComponentType::Cron => vec![],
    };
    for [folder, filename, contents] in files {
        let output_directory = output_directory.join(folder);
        let target_wit = output_directory.join(filename);
        if let Ok(actual) = tokio::fs::read_to_string(&target_wit).await
            && actual == contents
        {
            results.push(GeneratedPathStatus {
                path: target_wit,
                status: "up_to_date",
            });
        } else {
            tokio::fs::create_dir_all(&output_directory)
                .await
                .with_context(|| format!("cannot write {output_directory:?}"))?;
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .create_new(!force)
                .open(&target_wit)
                .await
                .with_context(|| {
                    format!(
                        "cannot open {target_wit:?} for writing{}",
                        if !force { ", try using `--force`" } else { "" }
                    )
                })?;
            file.write_all(contents.as_bytes())
                .await
                .with_context(|| format!("cannot write to {target_wit:?}"))?;

            results.push(GeneratedPathStatus {
                path: target_wit,
                status: "created_or_updated",
            });
        }
    }
    Ok(results)
}

struct GenerateWitDepsOptions {
    force: bool,
    skip_local: bool,
    prune: bool,
}

async fn generate_wit_deps(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    deployment_toml: PathBuf,
    output_directory: PathBuf,
    options: GenerateWitDepsOptions,
    secret_registry: Arc<SecretRegistry>,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let raw = tokio::fs::read_to_string(&deployment_toml)
        .await
        .with_context(|| format!("cannot read deployment manifest {deployment_toml:?}"))?;
    let filtered = filter_wit_deps_toml(&raw, options.skip_local)
        .with_context(|| format!("cannot filter {deployment_toml:?}"))?;
    let deployment_dir = deployment_toml
        .canonicalize()
        .with_context(|| format!("cannot canonicalize {deployment_toml:?}"))?
        .parent()
        .with_context(|| format!("cannot resolve parent of {deployment_toml:?}"))?
        .to_path_buf();
    let mut server_config = ServerConfigToml::default();
    server_config.webui.enabled = false;
    let _guard = init::init(&server_config)?; // Configure logging

    // Route through the shared CAS-based pipeline: prepare the filtered manifest into an
    // ephemeral in-memory CAS, then resolve it by digest exactly like a server would.
    let prepared = prepare_deployment_manifest(&filtered, &deployment_dir)
        .await
        .with_context(|| format!("cannot prepare {deployment_toml:?}"))?;
    let cas = Arc::new(InMemoryCas::default());
    for file in &prepared.files {
        let stored = cas
            .write_blob(&file.bytes)
            .await
            .with_context(|| format!("cannot store deployment file `{}`", file.path))?;
        ensure!(
            stored == file.digest,
            "prepared blob digest mismatch for `{}`",
            file.path
        );
    }
    let cas: Arc<dyn Cas> = cas;
    let deployment = resolve_manifest(&prepared.deployment_toml, cas.as_ref())
        .await
        .with_context(|| format!("cannot resolve {deployment_toml:?}"))?;
    let (termination_sender, mut termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    let verify_params = VerifyParams {
        dir_params: PrepareDirsParams {
            clean_cache: false,
            clean_codegen_cache: false,
        },
        runtime_config_availability: RuntimeConfigAvailability::AllowUnavailable, // Just extracting WITs, not running components
        suppress_type_checking_errors: true, // Just extracting WITs, not running components
        suppress_linking_errors: true,       // Just extracting WITs, not running components
    };

    let config_holder = ConfigHolder::new(project_dirs, base_dirs, None)?;
    let prepared_dirs = prepare_dirs(
        &server_config,
        &verify_params.dir_params,
        &config_holder.path_prefixes,
        &secret_registry,
    )
    .await?;
    let engines = create_engines(&server_config, &prepared_dirs)?;

    // WIT extraction resolves no secrets; the caller passes a no-secrets registry.
    let server_verified = Box::pin(server_verify(server_config, engines, secret_registry)).await?;
    let deployment_verified = deployment_verify_config(
        &server_verified,
        &prepared_dirs,
        deployment,
        cas,
        verify_params.clone(),
        &mut termination_watcher,
    )
    .await?;
    let compiled_and_linked = deployment_compile_link(
        server_verified,
        deployment_verified,
        DeploymentId::generate(),
        verify_params,
        &mut termination_watcher,
    )
    .await?;

    tokio::fs::create_dir_all(&output_directory)
        .await
        .with_context(|| format!("cannot create the output directory {output_directory:?}"))?;

    // Build per-package WITs from each component:
    //
    // * WASM components — parse their per-component `wit` text and
    //   walk the package graph via `wit_printer::process_pkg_with_deps`.
    // * Synthesized-WIT components (JS, inline stubs, exec activities) — collect their `PackageIfcFns` and feed
    //   them through `wit::build_wit_deps_map`, which rebuilds a `Resolve` from `TypeWrapper`s.
    //
    // Sharing of `ifc_fqn` between WASM and synthesized-WIT components is rejected at registry
    // insertion time, so the two outputs can never collide on the same interface.
    let mut pkg_to_wit: HashMap<PkgFqn, String> = HashMap::new();
    let mut synthesized_exports: Vec<PackageIfcFns> = Vec::new();
    for component in compiled_and_linked.component_registry_ro.list(true) {
        let Some(importable) = &component.workflow_or_activity_config else {
            unreachable!("webhooks and crons are filtered out, found {component:?}");
        };
        match component.wit_origin {
            WitOrigin::Synthesized => {
                synthesized_exports.extend(importable.exports_hierarchy_ext.iter().cloned());
            }
            WitOrigin::Authored => {
                let requested_pkgs: Vec<PkgFqn> = importable
                    .exports_hierarchy_ext
                    .iter()
                    .map(|ifc_fns| ifc_fns.ifc_fqn.pkg_fqn_name())
                    .collect::<hashbrown::HashSet<_>>()
                    .into_iter()
                    .collect();
                crate::wit_printer::process_pkg_with_deps(
                    &component.wit,
                    &requested_pkgs,
                    &mut pkg_to_wit,
                )
                .with_context(|| {
                    format!(
                        "cannot extract authored WIT packages from {}",
                        component.component_id
                    )
                })?;
            }
            WitOrigin::Wasm => {
                let requested_pkgs: Vec<PkgFqn> = importable
                    .exports_hierarchy_ext
                    .iter()
                    .map(|ifc_fns| ifc_fns.ifc_fqn.pkg_fqn_name())
                    .collect::<hashbrown::HashSet<_>>()
                    .into_iter()
                    .collect();
                crate::wit_printer::process_pkg_with_deps(
                    &component.wit,
                    &requested_pkgs,
                    &mut pkg_to_wit,
                )
                .with_context(|| {
                    format!(
                        "cannot extract WIT packages from {}",
                        component.component_id
                    )
                })?;
            }
        }
    }
    if !synthesized_exports.is_empty() {
        let synthesized_map = wit::build_wit_deps_map(&synthesized_exports)?;
        for (pkg_fqn, content) in synthesized_map {
            pkg_to_wit.entry(pkg_fqn).or_insert(content);
        }
    }
    write_wit_deps(&pkg_to_wit, &output_directory, options.force, options.prune).await
}

/// Prune a deployment manifest for WIT-deps extraction: webhooks and crons never contribute WITs,
/// and `--skip-local` drops every deployment-owned (non-OCI) component so only OCI dependencies
/// remain. Operating on the manifest text keeps a single resolution path: the pruned manifest is
/// prepared and resolved through the CAS like any other.
fn filter_wit_deps_toml(deployment_toml: &str, skip_local: bool) -> anyhow::Result<String> {
    /// Whether a component table's `location` is an OCI reference.
    fn table_is_oci_location(table: &toml_edit::Table) -> bool {
        table
            .get("location")
            .and_then(Item::as_str)
            .is_some_and(|location| location.starts_with(OCI_SCHEMA_PREFIX))
    }

    fn retain_tables(
        doc: &mut DocumentMut,
        section: &str,
        keep: impl Fn(&toml_edit::Table) -> bool,
    ) {
        if let Some(tables) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) {
            tables.retain(keep);
        }
    }

    let mut doc = deployment_toml
        .parse::<DocumentMut>()
        .context("cannot parse deployment manifest as TOML")?;

    for section in ["webhook_endpoint_wasm", "webhook_endpoint_js", "cron"] {
        doc.remove(section);
    }

    if skip_local {
        for section in ["activity_wasm", "workflow_wasm"] {
            retain_tables(&mut doc, section, table_is_oci_location);
        }
        // Stub/external File entries carry a `location`; Inline entries (no `location`) are kept.
        for section in ["activity_stub", "activity_external"] {
            retain_tables(&mut doc, section, |table| {
                table.get("location").is_none() || table_is_oci_location(table)
            });
        }
        // Script components are local when they carry inline `content` or a non-OCI `location`.
        for section in ["activity_js", "workflow_js", "activity_exec"] {
            retain_tables(&mut doc, section, |table| {
                table.get("content").is_none() && table_is_oci_location(table)
            });
        }
    }

    Ok(doc.to_string())
}

async fn write_wit_deps(
    pkg_to_wit: &HashMap<PkgFqn, String>,
    output_directory: &std::path::Path,
    force: bool,
    prune: bool,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let mut results = Vec::new();
    for (pkg_fqn, content) in pkg_to_wit {
        let pkg_file_name = pkg_fqn.as_file_name();
        let directory = output_directory.join(&pkg_file_name);
        tokio::fs::create_dir_all(&directory)
            .await
            .with_context(|| format!("cannot create directory {directory:?}"))?;
        let target_wit = directory.join(format!("{pkg_file_name}.wit"));
        // Do not overwrite the file if it only differs in the header (version)
        let old_content = tokio::fs::read_to_string(&target_wit)
            .await
            .unwrap_or_default();
        let old_content = old_content
            .split_once('\n')
            .map(|(_, rest)| rest)
            .unwrap_or("");
        if content != old_content {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .create_new(!force)
                .open(&target_wit)
                .await
                .with_context(|| {
                    format!(
                        "cannot open {target_wit:?} for writing{}",
                        if !force { ", try using `--force`" } else { "" }
                    )
                })?;

            let content = format!("{OBELISK_WIT_HEADER} {PKG_VERSION}\n{content}");
            file.write_all(content.as_bytes())
                .await
                .with_context(|| format!("cannot write to {target_wit:?}"))?;
            results.push(GeneratedPathStatus {
                path: target_wit,
                status: "written",
            });
        }
    }
    if prune {
        results.extend(prune_wit_deps(pkg_to_wit, output_directory).await?);
    }
    Ok(results)
}

async fn prune_wit_deps(
    pkg_to_wit: &HashMap<PkgFqn, String>,
    output_directory: &std::path::Path,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let desired: HashSet<_> = pkg_to_wit.keys().map(PkgFqn::as_file_name).collect();
    let mut entries = tokio::fs::read_dir(output_directory)
        .await
        .with_context(|| format!("cannot read output directory {output_directory:?}"))?;
    let mut results = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(directory_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if desired.contains(&directory_name) {
            continue;
        }
        let wit_file = entry.path().join(format!("{directory_name}.wit"));
        let Ok(content) = tokio::fs::read_to_string(&wit_file).await else {
            continue;
        };
        if !has_obelisk_wit_header(&content) {
            continue;
        }
        tokio::fs::remove_file(&wit_file)
            .await
            .with_context(|| format!("cannot remove obsolete WIT dependency {wit_file:?}"))?;
        if let Err(err) = tokio::fs::remove_dir(entry.path()).await
            && err.kind() != std::io::ErrorKind::DirectoryNotEmpty
        {
            return Err(err).with_context(|| format!("cannot remove directory {:?}", entry.path()));
        }
        results.push(GeneratedPathStatus {
            path: wit_file,
            status: "removed",
        });
    }
    Ok(results)
}

fn has_obelisk_wit_header(content: &str) -> bool {
    content.lines().next().is_some_and(|line| {
        let matches = |candidate: &str| {
            candidate
                .strip_prefix(OBELISK_WIT_HEADER)
                .is_some_and(|suffix| suffix.starts_with(' '))
        };
        matches(line) || line.strip_prefix('/').is_some_and(matches)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        OBELISK_WIT_HEADER, add_token_hash, generate_authored_schema, generate_cli_schema,
        generate_component_metadata_annotation_schema, generate_db_schema, generate_openapi_schema,
        generate_server_config_schema, write_wit_deps,
    };
    use concepts::PkgFqn;
    use hashbrown::HashMap;
    use std::path::PathBuf;

    #[tokio::test]
    async fn write_wit_deps_prunes_only_obsolete_generated_wits() {
        let output_directory = tempfile::tempdir().unwrap();
        let pkg_fqn = PkgFqn {
            namespace: "test".to_string(),
            package_name: "required".to_string(),
            version: Some("1.0.0".to_string()),
        };
        let required_name = pkg_fqn.as_file_name();
        let required_directory = output_directory.path().join(&required_name);
        tokio::fs::create_dir(&required_directory).await.unwrap();
        let required_wit = required_directory.join(format!("{required_name}.wit"));
        let wit = "package test:required@1.0.0;\n";
        let old_content = format!("{OBELISK_WIT_HEADER} 0.1.0\n{wit}");
        tokio::fs::write(&required_wit, &old_content).await.unwrap();

        let obsolete_directory = output_directory.path().join("test_obsolete@1.0.0");
        tokio::fs::create_dir(&obsolete_directory).await.unwrap();
        let obsolete_wit = obsolete_directory.join("test_obsolete@1.0.0.wit");
        tokio::fs::write(
            &obsolete_wit,
            format!("{OBELISK_WIT_HEADER} 0.1.0\npackage test:obsolete@1.0.0;\n"),
        )
        .await
        .unwrap();

        let authored_directory = output_directory.path().join("test_authored@1.0.0");
        tokio::fs::create_dir(&authored_directory).await.unwrap();
        let authored_wit = authored_directory.join("test_authored@1.0.0.wit");
        tokio::fs::write(&authored_wit, "package test:authored@1.0.0;\n")
            .await
            .unwrap();

        let results = write_wit_deps(
            &HashMap::from([(pkg_fqn, wit.to_string())]),
            output_directory.path(),
            true,
            true,
        )
        .await
        .unwrap();

        assert_eq!(1, results.len());
        assert_eq!("removed", results[0].status);
        assert_eq!(
            old_content,
            tokio::fs::read_to_string(required_wit).await.unwrap()
        );
        assert!(!obsolete_wit.exists());
        assert!(authored_wit.exists());
    }

    #[test]
    #[ignore = "updates committed schema assets"]
    fn update_toml_schemas() {
        generate_server_config_schema(Some(PathBuf::from("assets/schemas/toml/server.json")))
            .unwrap();
        generate_authored_schema(Some(PathBuf::from("assets/schemas/toml/authored.json"))).unwrap();
    }

    #[test]
    #[ignore = "updates committed schema assets"]
    fn update_openapi_schema() {
        generate_openapi_schema(Some(PathBuf::from("assets/schemas/openapi.json"))).unwrap();
    }

    #[test]
    #[ignore = "updates committed schema assets"]
    fn update_db_schema() {
        generate_db_schema(Some(PathBuf::from("assets/schemas/db.json"))).unwrap();
    }

    #[test]
    #[ignore = "updates committed schema assets"]
    fn update_cli_schema() {
        generate_cli_schema(Some(PathBuf::from("assets/schemas/cli.json"))).unwrap();
    }

    #[test]
    #[ignore = "updates committed schema assets"]
    fn update_component_metadata_annotation_schema() {
        generate_component_metadata_annotation_schema(Some(PathBuf::from(
            "assets/schemas/oci-metadata-annotation.json",
        )))
        .unwrap();
    }

    #[test]
    fn add_token_hash_should_create_and_append() {
        let hash = crate::api::token_hash("some-token");
        // Missing `api` table is created.
        let contents = add_token_hash("[webui]\nenabled = false\n", &hash).unwrap();
        insta::assert_snapshot!("created", contents);
        // Existing entries, including dotted-key style, are kept.
        let contents = add_token_hash(
            &format!("api.token_hashes = [\"{hash}\"]\n"),
            &crate::api::token_hash("other-token"),
        )
        .unwrap();
        insta::assert_snapshot!("appended", contents);
        // A scalar in place of the array is rejected.
        add_token_hash("[api]\ntoken_hashes = true\n", &hash).unwrap_err();
    }
}
