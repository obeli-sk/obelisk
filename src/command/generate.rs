use crate::args::shadow::PKG_VERSION;
use crate::args::{Args, Generate};
use crate::command::server::{
    PrepareDirsParams, VerifyParams, create_engines, deployment_compile_link,
    deployment_verify_config, prepare_dirs, server_verify,
};
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::{ConfigHolder, load_deployment_validated};
use crate::config::toml::{
    ActivityExternalComponentConfigToml, ActivityStubComponentConfigToml, ComponentLocationToml,
    JsLocationToml,
};
use crate::init::{self};
use crate::project_dirs;
use anyhow::Context;
use clap::{Arg, ArgAction, Command, CommandFactory, ValueHint};
use concepts::{ComponentType, ExecutionId, PackageIfcFns, PkgFqn, prefixed_ulid::DeploymentId};
use directories::{BaseDirs, ProjectDirs};
use hashbrown::{HashMap, HashSet};
use serde::Serialize;
use std::{borrow::Cow, path::PathBuf};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::watch;
use utils::{wasm_tools::WasmComponent, wit};
use wasm_workers::registry::WitOrigin;

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
            #[cfg(debug_assertions)]
            Generate::CliSchema { output } => generate_cli_schema(output),
            Generate::ServerConfig {
                json,
                output,
                force,
            } => {
                let config_file =
                    ConfigHolder::generate_default_server_config(output, force).await?;
                let result = GeneratedPathStatus {
                    path: config_file,
                    status: "generated",
                };
                print_generated_path_statuses(&[result], json)?;
                Ok(())
            }
            Generate::Deployment {
                json,
                output,
                force,
            } => {
                let config_file =
                    ConfigHolder::generate_default_deployment_config(output, force).await?;
                let result = GeneratedPathStatus {
                    path: config_file,
                    status: "generated",
                };
                print_generated_path_statuses(&[result], json)?;
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
            } => {
                let results = generate_wit_deps(
                    project_dirs(),
                    BaseDirs::new(),
                    deployment,
                    output_directory,
                    force,
                    skip_local,
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
            Generate::Prompt { description } => {
                print!("{}", build_ai_prompt(&description));
                Ok(())
            }
        }
    }
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

#[cfg(debug_assertions)]
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

#[cfg(debug_assertions)]
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

#[cfg(debug_assertions)]
#[derive(Debug, Serialize)]
struct CliArgAcceptsSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    many: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<bool>,
}

#[cfg(debug_assertions)]
fn generate_cli_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    let command = Args::command();
    let schema = command_to_schema(&command);
    write_json(output, &schema)
}

#[cfg(debug_assertions)]
fn command_to_schema(command: &Command) -> CliCommandSchema {
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

#[cfg(debug_assertions)]
fn arg_to_schema(arg: &Arg) -> CliArgSchema {
    CliArgSchema {
        name: arg_name(arg),
        short: arg.get_short(),
        value_name: arg_value_name(arg),
        help: arg_help(arg),
        required: arg.is_required_set(),
        accepts: arg_accepts(arg),
    }
}

#[cfg(debug_assertions)]
fn command_about(command: &Command) -> Option<String> {
    command
        .get_about()
        .map(ToString::to_string)
        .filter(|about| !about.trim().is_empty())
}

#[cfg(debug_assertions)]
fn arg_help(arg: &Arg) -> Option<String> {
    arg.get_help()
        .map(ToString::to_string)
        .filter(|help| !help.trim().is_empty())
}

#[cfg(debug_assertions)]
fn arg_name(arg: &Arg) -> String {
    if let Some(long) = arg.get_long() {
        format!("--{long}")
    } else {
        arg.get_id().to_string()
    }
}

#[cfg(debug_assertions)]
fn arg_value_name(arg: &Arg) -> Option<String> {
    arg.get_num_args()
        .filter(clap::builder::ValueRange::takes_values)
        .and_then(|_| arg.get_value_names())
        .and_then(|names| names.first())
        .map(ToString::to_string)
}

#[cfg(debug_assertions)]
fn arg_accepts(arg: &Arg) -> Option<CliArgAcceptsSchema> {
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
        ValueHint::AnyPath | ValueHint::FilePath | ValueHint::DirPath | ValueHint::ExecutablePath
    )
    .then_some(true);

    if one_of.is_none() && many.is_none() && path.is_none() {
        None
    } else {
        Some(CliArgAcceptsSchema { one_of, many, path })
    }
}

#[cfg(debug_assertions)]
fn skip_arg(arg: &Arg) -> bool {
    matches!(
        arg.get_action(),
        ArgAction::Help | ArgAction::HelpShort | ArgAction::HelpLong
    )
}

#[cfg(debug_assertions)]
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

async fn generate_wit_deps(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    deployment_toml: PathBuf,
    output_directory: PathBuf,
    force: bool,
    skip_local: bool,
) -> Result<Vec<GeneratedPathStatus>, anyhow::Error> {
    let deployment = load_deployment_validated(&deployment_toml).await?;

    let skipped_oci_component_names: HashSet<String> = if skip_local {
        // When `--external-only` is set, build the set of component names that have an OCI location.
        // Local components will be skipped during WIT extraction.
        let mut skipped_names: HashSet<String> = HashSet::new();
        for c in &deployment.inner.activities_wasm {
            if matches!(c.common.location, ComponentLocationToml::Path(_)) {
                skipped_names.insert(c.common.name.to_string());
            }
        }
        for (c, name) in &deployment.activities_stub {
            if let ActivityStubComponentConfigToml::File(f) = c
                && matches!(f.common.location, ComponentLocationToml::Path(_))
            {
                skipped_names.insert(name.to_string());
            }
        }
        for (c, name) in &deployment.activities_external {
            if let ActivityExternalComponentConfigToml::File(f) = c
                && matches!(f.common.location, ComponentLocationToml::Path(_))
            {
                skipped_names.insert(name.to_string());
            }
        }
        for (c, name) in &deployment.activities_js {
            if matches!(c.location, JsLocationToml::Path(_)) {
                skipped_names.insert(name.to_string());
            }
        }
        // Exec activities are always local — skip them from WIT generation.
        for (_, name) in &deployment.activities_exec {
            skipped_names.insert(name.to_string());
        }
        for c in &deployment.inner.workflows {
            if matches!(c.common.location, ComponentLocationToml::Path(_)) {
                skipped_names.insert(c.common.name.to_string());
            }
        }
        for (c, name) in &deployment.workflows_js {
            if matches!(c.location, JsLocationToml::Path(_)) {
                skipped_names.insert(name.to_string());
            }
        }
        // webhooks are skipped in any case
        skipped_names
    } else {
        HashSet::new()
    };

    let config_holder = ConfigHolder::new(project_dirs, base_dirs, None)?;
    let config = config_holder.load_config().await?;
    let _guard = init::init(&config)?;
    let deployment = deployment
        .canonicalize()
        .await
        .with_context(|| format!("cannot canonicalize {deployment_toml:?}"))?;
    let (termination_sender, mut termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    let verify_params = VerifyParams {
        dir_params: PrepareDirsParams {
            clean_cache: false,
            clean_codegen_cache: false,
        },
        ignore_missing_env_vars: true,
        suppress_type_checking_errors: true, // Just extracting WITs, not running components
        suppress_linking_errors: true,       // Just extracting WITs, not running components
    };
    let prepared_dirs = prepare_dirs(
        &config,
        &verify_params.dir_params,
        &config_holder.path_prefixes,
    )
    .await?;
    let engines = create_engines(&config, &prepared_dirs)?;

    let server_verified = Box::pin(server_verify(config, engines)).await?;
    let deployment_verified = deployment_verify_config(
        &server_verified,
        &prepared_dirs,
        deployment,
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
    // * Synthesized-WIT components (JS, inline stubs) — collect their `PackageIfcFns` and feed
    //   them through `wit::build_wit_deps_map`, which rebuilds a `Resolve` from `TypeWrapper`s.
    //
    // Sharing of `ifc_fqn` between WASM and synthesized-WIT components is rejected at registry
    // insertion time, so the two outputs can never collide on the same interface.
    let mut pkg_to_wit: HashMap<PkgFqn, String> = HashMap::new();
    let mut synthesized_exports: Vec<PackageIfcFns> = Vec::new();
    for component in compiled_and_linked
        .component_registry_ro
        .list(true)
        .into_iter()
        .filter(|component| {
            !skipped_oci_component_names.contains(component.component_id.name.as_ref())
        })
    {
        if let Some(importable) = &component.workflow_or_activity_config {
            match component.wit_origin {
                WitOrigin::Synthesized => {
                    synthesized_exports.extend(importable.exports_hierarchy_ext.iter().cloned());
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
        } // webhooks are ignored, nothing depends on them
    }
    if !synthesized_exports.is_empty() {
        let synthesized_map = wit::build_wit_deps_map(&synthesized_exports)?;
        for (pkg_fqn, content) in synthesized_map {
            pkg_to_wit.entry(pkg_fqn).or_insert(content);
        }
    }
    write_wit_deps(&pkg_to_wit, &output_directory, force).await
}

async fn write_wit_deps(
    pkg_to_wit: &HashMap<PkgFqn, String>,
    output_directory: &std::path::Path,
    force: bool,
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
    Ok(results)
}

const PREAMBLE: &str = "\
You are helping write an Obelisk application. Obelisk is a durable workflow engine that runs \
JS or Rust/WASM components as activities, workflows, and webhook endpoints, all wired together \
in a `deployment.toml` file.

Key concepts:
- **Activities**: idempotent, side-effectful functions (HTTP calls, DB writes, …). \
Auto-retried on failure or timeout.
- **Workflows**: deterministic orchestration — call activities, sleep durably, fan-out with \
join sets. Survive server crashes by replaying the execution log.
- **Webhook endpoints**: HTTP handlers that can call activities and workflows synchronously \
or schedule them fire-and-forget.
- All functions are identified by an FFQN (`namespace:package/interface.function`) and typed \
with WIT. JS components declare their types inline in `deployment.toml` — no WIT files needed.

When generating an Obelisk application, produce:
1. A `deployment.toml` with every component wired up.
2. One `.js` file per component placed under conventional subdirectories \
(`activity/`, `workflow/`, `webhook/`).
3. A README.md showing how to run the server and interact with the application \
(via CLI or webhook). Follow the conventions in the \
\"Running an Obelisk application\" and \"Common pitfalls\" sections of the \
js-patterns reference below.

Key CLI commands:
- `obelisk server verify -d deployment.toml --ignore-missing-env-vars` — validate config and compile components without \
starting the server.
- `obelisk server run -d deployment.toml` — start the server (omit `-d` on \
subsequent restarts).
- `obelisk execution submit <ffqn> '<json-args>'` — submit an execution; prints its ID.
- `obelisk execution submit --follow <ffqn> '<json-args>'` — submit and block until finished.
- `obelisk execution status [--follow] <id>` — inspect the current execution state.
- `obelisk execution result [--follow] <id>` — fetch the final execution result.
- `obelisk execution list [--ffqn <ffqn>] [-e <id> --show-derived]` — list executions; \
filter by FFQN prefix or by execution ID (use `--show-derived` to include child executions).
- `obelisk execution logs <id> --show-derived [--follow]` — fetch structured logs including child executions; \
useful for debugging workflow trees.
- `obelisk execution events <id>` — show the raw execution event log (history); useful for \
diagnosing non-determinism.
- `obelisk execution responses <id>` — show join-set responses (child results, delay completions) \
for a workflow execution.

The sections below are the authoritative reference documentation:
";

const WIT_REFERENCE: &str = include_str!("../../assets/ai-prompt/wit-reference.md");
const JS_COMPONENTS: &str = include_str!("../../assets/ai-prompt/js-components.md");
const JS_ACTIVITIES: &str = include_str!("../../assets/ai-prompt/js-activities.md");
const JS_WORKFLOWS: &str = include_str!("../../assets/ai-prompt/js-workflows.md");
const JS_WEBHOOKS: &str = include_str!("../../assets/ai-prompt/js-webhooks.md");
const JS_PATTERNS: &str = include_str!("../../assets/ai-prompt/js-patterns.md");
const COMPONENTS_REPO: &str = include_str!("../../assets/ai-prompt/components-repo.md");

fn build_ai_prompt(description: &str) -> String {
    use crate::config::config_holder::OBELISK_HELP_DEPLOYMENT_TOML;
    let mut parts: Vec<&str> = vec![
        PREAMBLE,
        "<wit_reference>\n",
        WIT_REFERENCE,
        "\n</wit_reference>\n\n",
        "<js_components>\n",
        JS_COMPONENTS,
        "\n</js_components>\n\n",
        "<js_activities>\n",
        JS_ACTIVITIES,
        "\n</js_activities>\n\n",
        "<js_workflows>\n",
        JS_WORKFLOWS,
        "\n</js_workflows>\n\n",
        "<js_webhooks>\n",
        JS_WEBHOOKS,
        "\n</js_webhooks>\n\n",
        "<js_patterns>\n",
        JS_PATTERNS,
        "\n</js_patterns>\n\n",
        "<deployment_toml_reference>\n```toml\n",
        OBELISK_HELP_DEPLOYMENT_TOML,
        "```\n</deployment_toml_reference>\n\n",
        "<github.com/obeli-sk/components>\n",
        COMPONENTS_REPO,
        "\n</github.com/obeli-sk/components>\n\n",
    ];
    parts.extend_from_slice(&["<task>\n", description, "\n</task>\n"]);
    parts.concat()
}
