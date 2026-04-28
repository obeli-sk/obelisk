use crate::args::Generate;
use crate::args::shadow::PKG_VERSION;
use crate::command::server::{
    PrepareDirsParams, VerifyParams, create_engines, deployment_verify_config_compile_link,
    prepare_dirs, server_verify,
};
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::{ConfigHolder, load_deployment_toml};
use crate::config::toml::{
    ActivityExternalComponentConfigToml, ActivityStubComponentConfigToml, ComponentLocationToml,
    JsLocationToml,
};
use crate::init::{self};
use crate::project_dirs;
use anyhow::Context;
use concepts::{ComponentType, ExecutionId, PackageIfcFns, PkgFqn, prefixed_ulid::DeploymentId};
use directories::{BaseDirs, ProjectDirs};
use hashbrown::{HashMap, HashSet};
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
                skip_local,
            } => {
                generate_wit_deps(
                    project_dirs(),
                    BaseDirs::new(),
                    deployment,
                    output_directory,
                    overwrite,
                    skip_local,
                )
                .await
            }
            Generate::ExecutionId => {
                println!("{}", ExecutionId::generate());
                Ok(())
            }
            Generate::Prompt { description } => {
                print!("{}", build_ai_prompt(&description));
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
    skip_local: bool,
) -> Result<(), anyhow::Error> {
    let deployment = load_deployment_toml(deployment_path).await?;

    let skipped_oci_component_names: HashSet<String> = if skip_local {
        // When `--external-only` is set, build the set of component names that have an OCI location.
        // Local components will be skipped during WIT extraction.
        let mut skipped_names: HashSet<String> = HashSet::new();
        for c in &deployment.inner.activities_wasm {
            if matches!(c.common.location, ComponentLocationToml::Path(_)) {
                skipped_names.insert(c.common.name.to_string());
            }
        }
        for c in &deployment.inner.activities_stub {
            if let ActivityStubComponentConfigToml::File(f) = c
                && matches!(f.common.location, ComponentLocationToml::Path(_))
            {
                skipped_names.insert(f.common.name.to_string());
            }
        }
        for c in &deployment.inner.activities_external {
            if let ActivityExternalComponentConfigToml::File(f) = c
                && matches!(f.common.location, ComponentLocationToml::Path(_))
            {
                skipped_names.insert(f.common.name.to_string());
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
    let deployment = crate::config::toml::resolve_local_refs_to_canonical(&deployment).await?;
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
    let compiled_and_linked = deployment_verify_config_compile_link(
        server_verified,
        &prepared_dirs,
        deployment,
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
    write_wit_deps(&pkg_to_wit, &output_directory, overwrite).await?;
    Ok(())
}

async fn write_wit_deps(
    pkg_to_wit: &HashMap<PkgFqn, String>,
    output_directory: &std::path::Path,
    overwrite: bool,
) -> Result<(), anyhow::Error> {
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

            let content = format!("{OBELISK_WIT_HEADER} {PKG_VERSION}\n{content}");
            file.write_all(content.as_bytes())
                .await
                .with_context(|| format!("cannot write to {target_wit:?}"))?;
            println!("{target_wit:?} written");
        }
    }
    Ok(())
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
- `obelisk execution get --follow <id>` — follow a previously submitted execution.
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
