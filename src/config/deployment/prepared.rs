use crate::command::server::cas_deployment_dir;
use crate::config::deployment::{
    DeploymentResolved, DeploymentToml, OCI_SCHEMA_PREFIX, sanitize_deployment_relative_path,
    strip_deployment_dir_prefix,
};
use crate::config::file_provider::parse_wit_dir;
use anyhow::{Context, bail, ensure};
use concepts::ContentDigest;
use concepts::cas::Cas;
use concepts::component_id::Digest;
use concepts::storage::{ComponentFileRole, DeploymentComponentFileRecord, DeploymentFileRecord};
use hashbrown::{HashMap, HashSet};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct DeploymentManifestFile {
    pub(crate) path: String,
    pub(crate) digest: ContentDigest,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedDeploymentManifest {
    pub(crate) deployment_toml: String,
    #[allow(dead_code)] // digest is recomputed server-side from the stored manifest
    pub(crate) digest: ContentDigest,
    pub(crate) files: Vec<DeploymentManifestFile>,
}

impl PreparedDeploymentManifest {
    /// The empty deployment: an empty manifest with no referenced files.
    pub(crate) fn empty() -> Self {
        Self {
            deployment_toml: String::new(),
            digest: compute_manifest_digest(""),
            files: Vec::new(),
        }
    }
}

/// Resolve a processed deployment manifest against `cas`, reading every deployment-owned
/// reference from the content-addressed store by digest.
///
/// This is the single resolution entry point shared by RPC submit, activation,
/// `server run (-d)`, and offline `deployment verify`. The manifest is addressed by content:
/// deployment-relative paths stay logical filenames (for imports, diagnostics, and exported
/// sources) and are never storage addresses, so resolution never touches the submitter's disk.
pub(crate) async fn resolve_manifest(
    deployment_toml: &str,
    cas: &dyn Cas,
) -> anyhow::Result<DeploymentResolved> {
    // A processed manifest carries no submitter host, so relative WASM/script paths stay
    // relative (addressed by digest in the CAS) after validation against an empty root.
    //
    // Validate the processed shape (deployment-owned locations carry a `content_digest`,
    // JS/WIT/backtrace sources are present in `component_files`) up front, so resolution can
    // treat those invariants as guaranteed rather than re-checking them lazily.
    DeploymentManifest::try_from_toml(deployment_toml, &cas_deployment_dir())
        .context("cannot validate processed deployment manifest")?;
    parse_manifest(deployment_toml, &cas_deployment_dir())?
        .resolve(cas)
        .await
}

pub(crate) async fn prepare_deployment_manifest_from_disk(
    deployment_toml_path: &Path,
) -> anyhow::Result<PreparedDeploymentManifest> {
    let deployment_toml = tokio::fs::read_to_string(deployment_toml_path)
        .await
        .with_context(|| format!("cannot read deployment manifest {deployment_toml_path:?}"))?;
    let deployment_dir = canonicalize_parent(deployment_toml_path)
        .with_context(|| format!("cannot resolve parent of {deployment_toml_path:?}"))?;
    prepare_deployment_manifest(&deployment_toml, &deployment_dir).await
}

pub(crate) async fn prepare_deployment_manifest(
    deployment_toml: &str,
    deployment_dir: &Path,
) -> anyhow::Result<PreparedDeploymentManifest> {
    let mut doc = deployment_toml
        .parse::<DocumentMut>()
        .context("cannot parse deployment manifest as TOML")?;

    // Validate through the typed config before collecting files so malformed component
    // entries fail with the same errors as today's resolution path.
    parse_manifest(deployment_toml, deployment_dir)?;

    let mut files = Vec::new();
    collect_wasm_refs(&mut doc, "activity_wasm", deployment_dir, &mut files).await?;
    collect_wasm_refs(&mut doc, "activity_stub", deployment_dir, &mut files).await?;
    collect_wasm_refs(&mut doc, "activity_external", deployment_dir, &mut files).await?;
    collect_wasm_refs(&mut doc, "workflow_wasm", deployment_dir, &mut files).await?;
    collect_backtrace_refs(&mut doc, "workflow_wasm", deployment_dir, &mut files).await?;
    collect_wasm_refs(
        &mut doc,
        "webhook_endpoint_wasm",
        deployment_dir,
        &mut files,
    )
    .await?;
    collect_backtrace_refs(
        &mut doc,
        "webhook_endpoint_wasm",
        deployment_dir,
        &mut files,
    )
    .await?;
    collect_js_refs(&mut doc, "activity_js", deployment_dir, &mut files).await?;
    collect_js_refs(&mut doc, "workflow_js", deployment_dir, &mut files).await?;
    collect_js_refs(&mut doc, "webhook_endpoint_js", deployment_dir, &mut files).await?;
    collect_script_refs(&mut doc, "activity_exec", deployment_dir, &mut files).await?;
    for section in WIT_SECTIONS {
        collect_wit_refs(&mut doc, section, deployment_dir, &mut files).await?;
    }

    let mut paths = HashMap::new();
    for file in &files {
        if let Some(previous) = paths.insert(file.path.clone(), file.digest.clone()) {
            ensure!(
                previous == file.digest,
                "deployment path `{}` has conflicting content digests",
                file.path
            );
        }
    }
    let mut seen = HashSet::new();
    files.retain(|file| seen.insert(file.path.clone()));

    let deployment_toml = doc.to_string();
    // Self-check: the injected `content_digest`/`component_files` must satisfy the
    // processed-shape invariants, so a bug in the collectors surfaces here at preparation
    // rather than lazily during resolution.
    DeploymentManifest::try_from_toml(&deployment_toml, deployment_dir)
        .context("prepared deployment manifest failed processed-shape validation")?;
    let digest = compute_manifest_digest(&deployment_toml);
    Ok(PreparedDeploymentManifest {
        deployment_toml,
        digest,
        files,
    })
}

pub(crate) fn compute_manifest_digest(deployment_toml: &str) -> ContentDigest {
    let hash: [u8; 32] = Sha256::digest(deployment_toml.as_bytes()).into();
    ContentDigest(Digest(hash))
}

pub(crate) fn strip_generated_deployment_metadata(deployment_toml: &str) -> anyhow::Result<String> {
    let mut doc = deployment_toml
        .parse::<DocumentMut>()
        .context("cannot parse stored deployment manifest as TOML")?;

    strip_generated_deployment_metadata_from_doc(&mut doc)?;

    Ok(doc.to_string())
}

fn strip_generated_deployment_metadata_from_doc(doc: &mut DocumentMut) -> anyhow::Result<()> {
    for section in WASM_SECTIONS.iter().chain(SCRIPT_SECTIONS) {
        let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
            continue;
        };
        for table in components.iter_mut() {
            let is_deployment_owned = table
                .get("location")
                .and_then(Item::as_str)
                .map(deployment_owned_path)
                .transpose()?
                .flatten()
                .is_some();
            if is_deployment_owned {
                table.remove("content_digest");
            }
        }
    }

    // Every section a `collect_*_refs` writer can attach a generated `component_files` map to.
    for section in SCRIPT_SECTIONS
        .iter()
        .chain(WIT_SECTIONS)
        .chain(BACKTRACE_SECTIONS)
    {
        let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
            continue;
        };
        for table in components.iter_mut() {
            table.remove("component_files");
        }
    }

    for section in BACKTRACE_SECTIONS {
        let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
            continue;
        };
        for table in components.iter_mut() {
            let Some(sources) = table
                .get_mut("backtrace")
                .and_then(Item::as_table_like_mut)
                .and_then(|backtrace| backtrace.get_mut("sources"))
                .and_then(Item::as_table_like_mut)
            else {
                continue;
            };
            for (_, source) in sources.iter_mut() {
                let Some(path) = backtrace_source_path(source) else {
                    continue;
                };
                if deployment_owned_path(&path)?.is_some() {
                    *source = value(path);
                }
            }
        }
    }

    Ok(())
}

/// A validated, digest-bearing projection of a stored deployment manifest.
///
/// This stage carries no file bytes. It proves the manifest is structurally valid
/// and classifies every component location, so the requested TOML is storeable
/// as-is. The deployment-owned file references it collects are the CAS objects the
/// stored TOML depends on. See `meta/designs/deployment-submit-package-state-pipeline.md`.
#[derive(Debug, Clone)]
pub(crate) struct DeploymentManifest {
    #[allow(dead_code)] // consumed by submit/storage paths in later phases
    pub(crate) deployment_toml: String,
    pub(crate) files: Vec<DeploymentFileRef>,
    pub(crate) component_files: Vec<DeploymentComponentFileRef>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeploymentComponentFileRef {
    pub(crate) component_name: String,
    pub(crate) path: String,
    pub(crate) role: ComponentFileRole,
}

/// A deployment-owned file reference: a deployment-relative path and its required
/// digest, plus field context for contextual submit errors.
#[derive(Debug, Clone)]
pub(crate) struct DeploymentFileRef {
    pub(crate) path: String,
    pub(crate) digest: ContentDigest,
    pub(crate) field: ManifestFieldRef,
}

/// Locates a file reference within the manifest for contextual error reporting.
#[derive(Debug, Clone)]
pub(crate) struct ManifestFieldRef {
    /// TOML section, e.g. `activity_wasm` or `workflow_wasm.backtrace.sources`.
    pub(crate) section: String,
    /// Component `name`, when present.
    pub(crate) component_name: Option<String>,
    /// Stable field path, e.g. `activity_wasm[name=a].location` or
    /// `workflow_wasm[name=w].backtrace.sources[.../src/lib.rs]`.
    pub(crate) field_path: String,
}

/// Classification of a script (JS/exec) component `location` / `content`.
enum ManifestScriptLocation {
    /// Inline `content`, no deployment-owned file.
    Inline,
    DeploymentFile {
        path: String,
        digest: ContentDigest,
    },
    Oci,
}

/// Classification of a WASM component `location`. WASM has no inline form, so a
/// deployment-owned file cannot be disguised as a generic path.
enum ManifestWasmLocation {
    DeploymentFile { path: String, digest: ContentDigest },
    Oci,
}

const WASM_SECTIONS: &[&str] = &[
    "activity_wasm",
    "activity_stub",
    "activity_external",
    "workflow_wasm",
    "webhook_endpoint_wasm",
];
const BACKTRACE_SECTIONS: &[&str] = &["workflow_wasm", "webhook_endpoint_wasm"];
const SCRIPT_SECTIONS: &[&str] = &[
    "activity_js",
    "workflow_js",
    "webhook_endpoint_js",
    "activity_exec",
];
const WIT_SECTIONS: &[&str] = &[
    "activity_js",
    "activity_exec",
    "workflow_js",
    "activity_stub",
    "activity_external",
];

impl DeploymentManifest {
    /// Parse and structurally validate `deployment_toml`, then classify every
    /// component location into a digest-bearing, deployment-relative file set.
    /// No file I/O happens here. Absolute local paths are rejected.
    pub(crate) fn try_from_toml(
        deployment_toml: &str,
        deployment_dir: &Path,
    ) -> anyhow::Result<Self> {
        let validated = parse_manifest(deployment_toml, deployment_dir)?;
        let names = resolved_component_names(&validated);

        let doc = deployment_toml
            .parse::<DocumentMut>()
            .context("cannot parse deployment manifest as TOML")?;

        // Section order matches the historical record collection so digest
        // deduplication keeps the same first-seen path for colliding contents.
        let mut files = Vec::new();
        let mut component_files = Vec::new();
        collect_wasm_section(
            &doc,
            "activity_wasm",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_wasm_section(
            &doc,
            "activity_stub",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_wasm_section(
            &doc,
            "activity_external",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_wasm_section(
            &doc,
            "workflow_wasm",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_backtrace_section(
            &doc,
            "workflow_wasm",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_wasm_section(
            &doc,
            "webhook_endpoint_wasm",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_backtrace_section(
            &doc,
            "webhook_endpoint_wasm",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_script_section(
            &doc,
            "activity_js",
            &names,
            &mut files,
            &mut component_files,
        )?;

        for section in WIT_SECTIONS {
            collect_wit_section(&doc, section, &names, &mut files, &mut component_files)?;
        }
        collect_script_section(
            &doc,
            "workflow_js",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_script_section(
            &doc,
            "webhook_endpoint_js",
            &names,
            &mut files,
            &mut component_files,
        )?;
        collect_script_section(
            &doc,
            "activity_exec",
            &names,
            &mut files,
            &mut component_files,
        )?;

        debug_assert!(
            WASM_SECTIONS.len() + SCRIPT_SECTIONS.len() + BACKTRACE_SECTIONS.len() == 11,
            "section lists drifted from collection order"
        );

        deduplicate_files_by_path(&mut files)?;
        deduplicate_component_files(&mut component_files)?;
        Ok(Self {
            deployment_toml: deployment_toml.to_string(),
            files,
            component_files,
        })
    }

    /// Indexed projection of deployment-owned files for `t_deployment_file`.
    pub(crate) fn file_records(&self) -> Vec<DeploymentFileRecord> {
        self.files
            .iter()
            .map(|file| DeploymentFileRecord {
                path: file.path.clone(),
                digest: file.digest.clone(),
                // Size is not carried at this stage; it is read back from t_file on listing.
                size: 0,
            })
            .collect()
    }

    pub(crate) fn component_file_records(&self) -> Vec<DeploymentComponentFileRecord> {
        self.component_files
            .iter()
            .map(|file| DeploymentComponentFileRecord {
                component_name: file.component_name.clone().into(),
                path: file.path.clone(),
                role: file.role,
            })
            .collect()
    }
}

fn resolved_component_names(
    deployment: &crate::config::deployment::DeploymentTomlValidated,
) -> HashMap<&'static str, Vec<String>> {
    HashMap::from([
        (
            "activity_wasm",
            deployment
                .activities_wasm
                .iter()
                .map(|c| c.common.name.to_string())
                .collect(),
        ),
        (
            "activity_stub",
            deployment
                .activities_stub
                .iter()
                .map(|(_, n)| n.to_string())
                .collect(),
        ),
        (
            "activity_external",
            deployment
                .activities_external
                .iter()
                .map(|(_, n)| n.to_string())
                .collect(),
        ),
        (
            "activity_js",
            deployment
                .activities_js
                .iter()
                .map(|(_, n)| n.to_string())
                .collect(),
        ),
        (
            "activity_exec",
            deployment
                .activities_exec
                .iter()
                .map(|(_, n)| n.to_string())
                .collect(),
        ),
        (
            "workflow_wasm",
            deployment
                .workflows_wasm
                .iter()
                .map(|c| c.common.name.to_string())
                .collect(),
        ),
        (
            "workflow_js",
            deployment
                .workflows_js
                .iter()
                .map(|(_, n)| n.to_string())
                .collect(),
        ),
        (
            "webhook_endpoint_wasm",
            deployment
                .webhooks_wasm
                .iter()
                .map(|c| c.common.name.to_string())
                .collect(),
        ),
        (
            "webhook_endpoint_js",
            deployment
                .webhooks_js
                .iter()
                .map(|c| c.name.to_string())
                .collect(),
        ),
    ])
}

fn deduplicate_files_by_path(files: &mut Vec<DeploymentFileRef>) -> anyhow::Result<()> {
    let mut paths = HashMap::new();
    for file in files.iter() {
        if let Some(previous) = paths.insert(file.path.clone(), file.digest.clone()) {
            ensure!(
                previous == file.digest,
                "deployment path `{}` has conflicting content digests",
                file.path
            );
        }
    }
    let mut seen = HashSet::new();
    files.retain(|file| seen.insert(file.path.clone()));
    Ok(())
}

fn deduplicate_component_files(files: &mut Vec<DeploymentComponentFileRef>) -> anyhow::Result<()> {
    let mut roles = HashMap::new();
    for file in files.iter() {
        let key = (file.component_name.clone(), file.path.clone());
        if let Some(previous) = roles.insert(key, file.role) {
            ensure!(
                previous == file.role,
                "component `{}` uses `{}` with conflicting roles",
                file.component_name,
                file.path
            );
        }
    }
    let mut seen = HashSet::new();
    files.retain(|file| seen.insert((file.component_name.clone(), file.path.clone())));
    Ok(())
}

fn parse_manifest(
    deployment_toml: &str,
    deployment_dir: &Path,
) -> anyhow::Result<crate::config::deployment::DeploymentTomlValidated> {
    let deployment: DeploymentToml =
        toml::from_str(deployment_toml).context("cannot parse deployment manifest")?;
    deployment
        .validate(deployment_dir)
        .context("cannot validate deployment manifest")
}

fn component_name(table: &toml_edit::Table) -> Option<String> {
    table.get("name").and_then(Item::as_str).map(str::to_string)
}

fn collect_script_section(
    doc: &DocumentMut,
    section: &str,
    names: &HashMap<&str, Vec<String>>,
    files: &mut Vec<DeploymentFileRef>,
    component_files: &mut Vec<DeploymentComponentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for (index, table) in components.iter().enumerate() {
        let name = names[section][index].clone();
        match classify_script_location(table)? {
            ManifestScriptLocation::DeploymentFile { path, digest } => {
                let field_path = format!("{section}[name={name}].location");
                if let Some(file_digests) =
                    table.get("component_files").and_then(Item::as_table_like)
                {
                    let wit_prefix = table
                        .get("wit")
                        .and_then(Item::as_str)
                        .map(sanitize_deployment_relative_path)
                        .transpose()?
                        .map(|root| format!("{root}/"));
                    let mut script_file_count = 0;
                    let mut found_entry = false;
                    for (module_path, module_digest) in file_digests.iter() {
                        let module_path = sanitize_deployment_relative_path(module_path)?;
                        if wit_prefix
                            .as_ref()
                            .is_some_and(|prefix| module_path.starts_with(prefix))
                        {
                            continue;
                        }
                        script_file_count += 1;
                        let module_digest = module_digest
                            .as_str()
                            .with_context(|| {
                                format!(
                                    "{field_path}.component_files[{module_path}] must be a digest"
                                )
                            })?
                            .parse()
                            .with_context(|| {
                                format!("invalid digest for module `{module_path}`")
                            })?;
                        if module_path == path {
                            ensure!(
                                module_digest == digest,
                                "{field_path}: component_files entry digest does not match content_digest"
                            );
                            found_entry = true;
                        }
                        files.push(DeploymentFileRef {
                            path: module_path.clone(),
                            digest: module_digest,
                            field: ManifestFieldRef {
                                section: section.to_string(),
                                component_name: Some(name.clone()),
                                field_path: format!("{field_path}.component_files"),
                            },
                        });
                        component_files.push(DeploymentComponentFileRef {
                            component_name: name.clone(),
                            role: if section == "activity_exec" {
                                ComponentFileRole::ExecProgram
                            } else if module_path == path {
                                ComponentFileRole::JsEntrypoint
                            } else {
                                ComponentFileRole::JsModule
                            },
                            path: module_path,
                        });
                    }
                    if script_file_count > 0 {
                        ensure!(
                            found_entry,
                            "{field_path}: component_files must contain the entry path with its content_digest"
                        );
                    } else {
                        files.push(DeploymentFileRef {
                            path: path.clone(),
                            digest,
                            field: ManifestFieldRef {
                                section: section.to_string(),
                                component_name: Some(name.clone()),
                                field_path,
                            },
                        });
                        component_files.push(DeploymentComponentFileRef {
                            component_name: name,
                            path,
                            role: if section == "activity_exec" {
                                ComponentFileRole::ExecProgram
                            } else {
                                ComponentFileRole::JsEntrypoint
                            },
                        });
                    }
                } else {
                    files.push(DeploymentFileRef {
                        path: path.clone(),
                        digest,
                        field: ManifestFieldRef {
                            section: section.to_string(),
                            component_name: Some(name.clone()),
                            field_path,
                        },
                    });
                    component_files.push(DeploymentComponentFileRef {
                        component_name: name,
                        path,
                        role: if section == "activity_exec" {
                            ComponentFileRole::ExecProgram
                        } else {
                            ComponentFileRole::JsEntrypoint
                        },
                    });
                }
            }
            ManifestScriptLocation::Inline | ManifestScriptLocation::Oci => {}
        }
    }

    Ok(())
}

fn collect_wit_section(
    doc: &DocumentMut,
    section: &str,
    names: &HashMap<&str, Vec<String>>,
    files: &mut Vec<DeploymentFileRef>,
    component_files: &mut Vec<DeploymentComponentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };
    for (index, table) in components.iter().enumerate() {
        let Some(raw_root) = table.get("wit").and_then(Item::as_str) else {
            continue;
        };
        let root = sanitize_deployment_relative_path(raw_root)?;
        let prefix = format!("{root}/");
        let name = names[section][index].clone();
        let field_path = format!("{section}[name={name}].wit");
        let file_digests = table
            .get("component_files")
            .and_then(Item::as_table_like)
            .with_context(|| format!("{field_path}: missing generated component_files"))?;
        let mut found = false;
        for (path, digest) in file_digests.iter() {
            let path = sanitize_deployment_relative_path(path)?;
            if !path.starts_with(&prefix) {
                continue;
            }
            ensure!(
                Path::new(&path).extension().and_then(|ext| ext.to_str()) == Some("wit"),
                "{field_path}: non-WIT file in WIT source set: `{path}`"
            );
            let digest = digest
                .as_str()
                .with_context(|| format!("{field_path}.component_files[{path}] must be a digest"))?
                .parse()
                .with_context(|| format!("invalid digest for WIT source `{path}`"))?;
            found = true;
            files.push(DeploymentFileRef {
                path: path.clone(),
                digest,
                field: ManifestFieldRef {
                    section: section.to_string(),
                    component_name: Some(name.clone()),
                    field_path: field_path.clone(),
                },
            });
            component_files.push(DeploymentComponentFileRef {
                component_name: name.clone(),
                path,
                role: ComponentFileRole::WitSource,
            });
        }
        ensure!(found, "{field_path}: no parser-selected WIT files found");
    }
    Ok(())
}

fn collect_wasm_section(
    doc: &DocumentMut,
    section: &str,
    names: &HashMap<&str, Vec<String>>,
    files: &mut Vec<DeploymentFileRef>,
    component_files: &mut Vec<DeploymentComponentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for (index, table) in components.iter().enumerate() {
        let name = names[section][index].clone();
        let Some(raw_location) = table.get("location").and_then(Item::as_str) else {
            continue;
        };
        match classify_wasm_location(raw_location, table.get("content_digest"))? {
            ManifestWasmLocation::DeploymentFile { path, digest } => {
                let field_path = format!("{section}[name={name}].location");
                files.push(DeploymentFileRef {
                    path: path.clone(),
                    digest,
                    field: ManifestFieldRef {
                        section: section.to_string(),
                        component_name: Some(name.clone()),
                        field_path,
                    },
                });
                component_files.push(DeploymentComponentFileRef {
                    component_name: name,
                    path,
                    role: ComponentFileRole::WasmComponent,
                });
            }
            ManifestWasmLocation::Oci => {}
        }
    }

    Ok(())
}

fn collect_backtrace_section(
    doc: &DocumentMut,
    section: &str,
    names: &HashMap<&str, Vec<String>>,
    files: &mut Vec<DeploymentFileRef>,
    component_files: &mut Vec<DeploymentComponentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for (index, table) in components.iter().enumerate() {
        let name = names[section][index].clone();
        let Some(sources) = table
            .get("backtrace")
            .and_then(Item::as_table_like)
            .and_then(|backtrace| backtrace.get("sources"))
            .and_then(Item::as_table_like)
        else {
            continue;
        };

        for (key, source) in sources.iter() {
            let Some(raw_path) = backtrace_source_path(source) else {
                continue;
            };
            let Some(path) = deployment_owned_path(&raw_path)? else {
                continue;
            };
            let generated_digest = table
                .get("component_files")
                .and_then(Item::as_table_like)
                .and_then(|files| files.get(&path));
            let digest = required_content_digest(generated_digest, &path)
                .with_context(|| {
                    format!(
                        "{section}[name={name}].backtrace.sources[{key}]: component_files must contain `{path}`"
                    )
                })?;
            files.push(DeploymentFileRef {
                path: path.clone(),
                digest,
                field: ManifestFieldRef {
                    section: format!("{section}.backtrace.sources"),
                    component_name: Some(name.clone()),
                    field_path: format!("{section}[name={name}].backtrace.sources[{key}]"),
                },
            });
            component_files.push(DeploymentComponentFileRef {
                component_name: name.clone(),
                path,
                role: ComponentFileRole::BacktraceSource,
            });
        }
    }

    Ok(())
}

fn classify_script_location(table: &toml_edit::Table) -> anyhow::Result<ManifestScriptLocation> {
    let has_inline_content = table.get("content").and_then(Item::as_str).is_some();
    let Some(raw_location) = table.get("location").and_then(Item::as_str) else {
        // No `location`: inline content (or neither, already rejected by validation).
        return Ok(ManifestScriptLocation::Inline);
    };
    ensure!(
        !has_inline_content,
        "exactly one of `location` or `content` must be set for script components"
    );
    let Some(path) = deployment_owned_path(raw_location)? else {
        return Ok(ManifestScriptLocation::Oci);
    };
    let digest = required_content_digest(table.get("content_digest"), &path)?;
    Ok(ManifestScriptLocation::DeploymentFile { path, digest })
}

fn classify_wasm_location(
    raw_location: &str,
    content_digest: Option<&Item>,
) -> anyhow::Result<ManifestWasmLocation> {
    let Some(path) = deployment_owned_path(raw_location)? else {
        return Ok(ManifestWasmLocation::Oci);
    };
    let digest = required_content_digest(content_digest, &path)?;
    Ok(ManifestWasmLocation::DeploymentFile { path, digest })
}

fn required_content_digest(item: Option<&Item>, path: &str) -> anyhow::Result<ContentDigest> {
    item.and_then(Item::as_str)
        .with_context(|| format!("deployment-owned file `{path}` must set `content_digest`"))?
        .parse()
        .with_context(|| format!("invalid content_digest for deployment-owned file `{path}`"))
}

async fn collect_script_refs(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let has_inline_content = table.get("content").and_then(Item::as_str).is_some();
        let Some(raw_location) = table.get("location").and_then(Item::as_str) else {
            continue;
        };
        ensure!(
            !has_inline_content,
            "exactly one of `location` or `content` must be set for script components"
        );
        let Some(path) = deployment_owned_path(raw_location)? else {
            continue;
        };
        let (digest, bytes) = read_deployment_file(deployment_dir, &path).await?;
        table["content_digest"] = value(digest.to_string());
        files.push(DeploymentManifestFile {
            path,
            digest,
            bytes,
        });
    }

    Ok(())
}

async fn collect_js_refs(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let has_inline_content = table.get("content").and_then(Item::as_str).is_some();
        let Some(raw_location) = table.get("location").and_then(Item::as_str) else {
            continue;
        };
        ensure!(
            !has_inline_content,
            "exactly one of `location` or `content` must be set for script components"
        );
        let Some(entry_path) = deployment_owned_path(raw_location)? else {
            continue;
        };
        let graph = crate::javascript::graph::collect_graph(deployment_dir, &entry_path)
            .await
            .with_context(|| format!("cannot collect JS module graph from `{entry_path}`"))?;
        let entry_source = graph
            .files
            .get(&graph.entry_path)
            .expect("collected graph contains its entry");
        let entry_digest = content_digest(entry_source.as_bytes());
        table["content_digest"] = value(entry_digest.to_string());

        if graph.files.len() > 1 {
            let mut refs = InlineTable::new();
            for (path, source) in &graph.files {
                refs.insert(
                    path,
                    Value::from(content_digest(source.as_bytes()).to_string()),
                );
            }
            table["component_files"] = Item::Value(Value::InlineTable(refs));
        } else {
            table.remove("component_files");
        }

        for (path, source) in graph.files {
            files.push(DeploymentManifestFile {
                path,
                digest: content_digest(source.as_bytes()),
                bytes: source.into_bytes(),
            });
        }
    }

    Ok(())
}

async fn collect_wit_refs(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };
    for table in components.iter_mut() {
        let Some(raw_root) = table.get("wit").and_then(Item::as_str) else {
            continue;
        };
        let root = sanitize_deployment_relative_path(raw_root)?;
        let parsed = parse_wit_dir(deployment_dir, &root).await?;

        let mut refs = InlineTable::new();
        if let Some(existing) = table.get("component_files").and_then(Item::as_table_like) {
            for (path, digest) in existing.iter() {
                let normalized_path = sanitize_deployment_relative_path(path)?;
                refs.insert(
                    &normalized_path,
                    digest
                        .as_value()
                        .with_context(|| format!("component_files[{path}] must be a digest"))?
                        .clone(),
                );
            }
        }
        for (path, source) in parsed.files {
            let bytes = source.into_bytes();
            let digest = content_digest(&bytes);
            if let Some(previous) = refs.get(&path).and_then(Value::as_str) {
                ensure!(
                    previous == digest.to_string(),
                    "deployment path `{path}` has conflicting content digests"
                );
            }
            refs.insert(&path, Value::from(digest.to_string()));
            files.push(DeploymentManifestFile {
                path,
                digest,
                bytes,
            });
        }
        table["wit"] = value(root);
        table["component_files"] = Item::Value(Value::InlineTable(refs));
    }
    Ok(())
}

async fn collect_wasm_refs(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let Some(raw_location) = table
            .get("location")
            .and_then(Item::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        collect_location_ref(table, &raw_location, deployment_dir, files).await?;
    }

    Ok(())
}

async fn collect_location_ref(
    table: &mut toml_edit::Table,
    raw_location: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(path) = deployment_owned_path(raw_location)? else {
        return Ok(());
    };
    let (digest, bytes) = read_deployment_file(deployment_dir, &path).await?;
    table["content_digest"] = value(digest.to_string());
    files.push(DeploymentManifestFile {
        path,
        digest,
        bytes,
    });
    Ok(())
}

async fn collect_backtrace_refs(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    files: &mut Vec<DeploymentManifestFile>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let mut refs = table
            .get("component_files")
            .and_then(Item::as_table_like)
            .map(|files| {
                let mut refs = InlineTable::new();
                for (path, digest) in files.iter() {
                    if let Some(digest) = digest.as_value() {
                        refs.insert(path, digest.clone());
                    }
                }
                refs
            })
            .unwrap_or_default();
        let Some(sources) = table
            .get_mut("backtrace")
            .and_then(Item::as_table_like_mut)
            .and_then(|backtrace| backtrace.get_mut("sources"))
            .and_then(Item::as_table_like_mut)
        else {
            continue;
        };
        let mut unreadable = Vec::new();
        for (key, source) in sources.iter_mut() {
            if let Some(raw_path) = backtrace_source_path(source)
                && let Some(path) = deployment_owned_path(&raw_path)?
            {
                match read_deployment_file(deployment_dir, &path).await {
                    Ok((digest, bytes)) => {
                        refs.insert(&path, Value::from(digest.to_string()));
                        *source = value(raw_path);
                        files.push(DeploymentManifestFile {
                            path,
                            digest,
                            bytes,
                        });
                    }
                    Err(err) => {
                        warn!("Cannot read backtrace source {path:?} - {err:?}");
                        unreadable.push(key.get().to_string());
                    }
                }
            }
        }
        // Removed after the loop: `iter_mut()` above borrows `sources`.
        for key in unreadable {
            sources.remove(&key);
        }
        if !refs.is_empty() {
            table["component_files"] = Item::Value(Value::InlineTable(refs));
        }
    }

    Ok(())
}

fn backtrace_source_path(source: &Item) -> Option<String> {
    source.as_str().map(str::to_string)
}

fn deployment_owned_path(raw: &str) -> anyhow::Result<Option<String>> {
    if raw.starts_with(OCI_SCHEMA_PREFIX) {
        return Ok(None);
    }
    if Path::new(raw).is_absolute() {
        bail!("absolute local paths are not allowed in deployment manifests: `{raw}`");
    }
    let path = strip_deployment_dir_prefix(raw).unwrap_or(raw);
    sanitize_deployment_relative_path(path).map(Some)
}

fn content_digest(bytes: &[u8]) -> ContentDigest {
    let hash: [u8; 32] = Sha256::digest(bytes).into();
    ContentDigest(Digest(hash))
}

fn canonicalize_parent(path: &Path) -> Result<PathBuf, anyhow::Error> {
    Ok(path
        .canonicalize()
        .with_context(|| format!("error calling canonicalize on {path:?}"))?
        .parent()
        .with_context(|| format!("error getting parent path of {path:?}"))?
        .to_path_buf())
}

async fn read_deployment_file(
    deployment_dir: &Path,
    path: &str,
) -> anyhow::Result<(ContentDigest, Vec<u8>)> {
    let full_path = deployment_dir.join(path);
    let bytes = tokio::fs::read(&full_path)
        .await
        .with_context(|| format!("cannot read deployment file {full_path:?}"))?;
    let digest = content_digest(&bytes);
    Ok((digest, bytes))
}

#[derive(Debug, Clone)]
pub(crate) struct BrokenDigest {
    pub(crate) field_path: String,
    pub(crate) path: String,
    pub(crate) stored: String,
    pub(crate) actual: ContentDigest,
}

pub(crate) async fn reconcile_deployment_digests(
    deployment_toml_path: &Path,
    fix: bool,
) -> anyhow::Result<Vec<BrokenDigest>> {
    let deployment_toml = tokio::fs::read_to_string(deployment_toml_path)
        .await
        .with_context(|| format!("cannot read deployment manifest {deployment_toml_path:?}"))?;
    let deployment_dir = canonicalize_parent(deployment_toml_path)
        .with_context(|| format!("cannot resolve parent of {deployment_toml_path:?}"))?;
    let mut doc = deployment_toml
        .parse::<DocumentMut>()
        .context("cannot parse deployment manifest as TOML")?;

    let mut broken = Vec::new();
    for section in WASM_SECTIONS.iter().chain(SCRIPT_SECTIONS) {
        reconcile_location_section(&mut doc, section, &deployment_dir, fix, &mut broken).await?;
    }
    for section in BACKTRACE_SECTIONS {
        reconcile_backtrace_section(&mut doc, section, &deployment_dir, fix, &mut broken).await?;
    }

    if fix {
        strip_generated_deployment_metadata_from_doc(&mut doc)?;
        let fixed_toml = doc.to_string();
        if fixed_toml == deployment_toml {
            return Ok(broken);
        }
        tokio::fs::write(deployment_toml_path, fixed_toml)
            .await
            .with_context(|| {
                format!("cannot write fixed deployment manifest {deployment_toml_path:?}")
            })?;
    }
    Ok(broken)
}

async fn reconcile_location_section(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    fix: bool,
    broken: &mut Vec<BrokenDigest>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let name = component_name(table);
        let Some(stored) = table
            .get("content_digest")
            .and_then(Item::as_str)
            .map(str::to_string)
        else {
            continue;
        };
        let (path, actual) =
            if let Some(raw_location) = table.get("location").and_then(Item::as_str) {
                let Some(path) = deployment_owned_path(raw_location)? else {
                    continue;
                };
                let (actual, _) = read_deployment_file(deployment_dir, &path).await?;
                (path, actual)
            } else if let Some(content) = table.get("content").and_then(Item::as_str) {
                ("<inline>".to_string(), content_digest(content.as_bytes()))
            } else {
                continue;
            };

        if stored != actual.to_string() {
            if fix {
                table["content_digest"] = value(actual.to_string());
            }
            broken.push(BrokenDigest {
                field_path: format!(
                    "{section}[name={}].content_digest",
                    name.as_deref().unwrap_or("")
                ),
                path,
                stored,
                actual,
            });
        }
    }

    Ok(())
}

async fn reconcile_backtrace_section(
    doc: &mut DocumentMut,
    section: &str,
    deployment_dir: &Path,
    fix: bool,
    broken: &mut Vec<BrokenDigest>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get_mut(section).and_then(Item::as_array_of_tables_mut) else {
        return Ok(());
    };

    for table in components.iter_mut() {
        let name = component_name(table);
        // Collect deployment-owned backtrace source paths before touching `component_files`,
        // which now holds every source's digest (the source map is a plain path map).
        let Some(sources) = table
            .get("backtrace")
            .and_then(Item::as_table_like)
            .and_then(|backtrace| backtrace.get("sources"))
            .and_then(Item::as_table_like)
        else {
            continue;
        };
        let mut owned_paths = Vec::new();
        for (_, source) in sources.iter() {
            let Some(raw_path) = backtrace_source_path(source) else {
                continue;
            };
            if let Some(path) = deployment_owned_path(&raw_path)? {
                owned_paths.push(path);
            }
        }

        for path in owned_paths {
            let Some(stored) = table
                .get("component_files")
                .and_then(Item::as_table_like)
                .and_then(|files| files.get(&path))
                .and_then(Item::as_str)
                .map(str::to_string)
            else {
                continue;
            };
            let (actual, _) = read_deployment_file(deployment_dir, &path).await?;
            if stored != actual.to_string() {
                if fix
                    && let Some(files) = table
                        .get_mut("component_files")
                        .and_then(Item::as_table_like_mut)
                {
                    files.insert(&path, value(actual.to_string()));
                }
                broken.push(BrokenDigest {
                    field_path: format!(
                        "{section}[name={}].component_files[{path}]",
                        name.as_deref().unwrap_or("")
                    ),
                    path,
                    stored,
                    actual,
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::cas::InMemoryCas;

    /// Upload a prepared manifest's blobs to an in-memory CAS (checking each digest), then
    /// resolve the processed TOML through the shared CAS-based path.
    async fn resolve_prepared(prepared: &PreparedDeploymentManifest) -> DeploymentResolved {
        let cas = InMemoryCas::default();
        for file in &prepared.files {
            let stored = cas.write_blob(&file.bytes).await.unwrap();
            assert_eq!(stored, file.digest, "prepared blob digest mismatch");
        }
        resolve_manifest(&prepared.deployment_toml, &cas)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn prepare_fills_relative_script_digest_and_collects_blob() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("scripts"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("scripts/a.js"), "export const x = 1;")
            .await
            .unwrap();
        let manifest = r#"
[[activity_js]]
name = "a"
location = "scripts/a.js"
ffqn = "ns:pkg/ifc.fn"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        assert_eq!(prepared.files.len(), 1);
        assert_eq!(prepared.files[0].path, "scripts/a.js");
        assert_eq!(prepared.files[0].bytes, b"export const x = 1;");
        assert!(
            prepared
                .deployment_toml
                .contains("content_digest = \"sha256:")
        );
        assert_eq!(
            prepared.digest,
            compute_manifest_digest(&prepared.deployment_toml)
        );
    }

    #[tokio::test]
    async fn prepare_collects_only_parser_selected_wit_files() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("wit/ignored"))
            .await
            .unwrap();
        tokio::fs::write(
            dir.path().join("wit/main.wit"),
            r"
                package example:agent;
                interface api {
                    record request { prompt: string }
                    run: func(request: request) -> result<request, string>;
                }
                world agent { export api; }
            ",
        )
        .await
        .unwrap();
        tokio::fs::write(
            dir.path().join("wit/ignored/not-part-of-package.wit"),
            "package ignored:file; interface unused {}",
        )
        .await
        .unwrap();
        let manifest = r#"
[[activity_js]]
name = "agent"
content = "export default () => null;"
ffqn = "example:agent/api.run"
wit = "wit"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();
        assert_eq!(
            prepared
                .files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            ["wit/main.wit"]
        );
        assert!(!prepared.deployment_toml.contains("not-part-of-package"));

        let classified =
            DeploymentManifest::try_from_toml(&prepared.deployment_toml, Path::new("")).unwrap();
        assert_eq!(classified.component_files.len(), 1);
        assert_eq!(
            classified.component_files[0].role,
            ComponentFileRole::WitSource
        );
    }

    #[tokio::test]
    async fn prepare_rejects_wit_symlink_escape() {
        #[cfg(unix)]
        {
            let deployment = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            tokio::fs::write(
                outside.path().join("main.wit"),
                "package example:escape; interface api {} world app { export api; }",
            )
            .await
            .unwrap();
            std::os::unix::fs::symlink(outside.path(), deployment.path().join("wit")).unwrap();
            let manifest = r#"
[[activity_js]]
content = "export default () => null;"
ffqn = "example:escape/api.run"
wit = "wit"
"#;
            let error = prepare_deployment_manifest(manifest, deployment.path())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("outside the deployment directory"),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn prepare_rejects_absolute_wit_and_mixed_interface_modes() {
        let directory = tempfile::tempdir().unwrap();
        let absolute = format!(
            r##"
[[activity_exec]]
content = "#!/bin/sh"
ffqn = "example:agent/api.run"
wit = {:?}
"##,
            directory.path()
        );
        assert!(
            prepare_deployment_manifest(&absolute, directory.path())
                .await
                .is_err()
        );

        let mixed = r#"
[[activity_js]]
content = "export default () => null;"
ffqn = "example:agent/api.run"
wit = "wit"
params = [{ name = "request", type = "string" }]
"#;
        assert!(
            prepare_deployment_manifest(mixed, directory.path())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn prepare_preserves_distinct_paths_with_identical_content() {
        let dir = tempfile::tempdir().unwrap();
        for component in ["a", "b"] {
            let component_dir = dir.path().join(component);
            tokio::fs::create_dir_all(&component_dir).await.unwrap();
            tokio::fs::write(component_dir.join("lib.js"), "export default 1;")
                .await
                .unwrap();
        }
        let manifest = r#"
[[activity_js]]
name = "a"
location = "a/lib.js"
ffqn = "ns:pkg/ifc.a"

[[activity_js]]
name = "b"
location = "b/lib.js"
ffqn = "ns:pkg/ifc.b"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();
        assert_eq!(prepared.files.len(), 2);
        assert_eq!(prepared.files[0].digest, prepared.files[1].digest);
        assert_ne!(prepared.files[0].path, prepared.files[1].path);

        let classified =
            DeploymentManifest::try_from_toml(&prepared.deployment_toml, Path::new("")).unwrap();
        assert_eq!(classified.files.len(), 2);
        assert_eq!(classified.component_files.len(), 2);
    }

    #[tokio::test]
    async fn prepare_collects_js_module_graph() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(
            dir.path().join("src/index.js"),
            "import { value } from './lib.js'; export default () => value;",
        )
        .await
        .unwrap();
        tokio::fs::write(dir.path().join("src/lib.js"), "export const value = 42;")
            .await
            .unwrap();
        let manifest = r#"
[[activity_js]]
name = "a"
location = "src/index.js"
ffqn = "ns:pkg/ifc.fn"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();
        assert_eq!(prepared.files.len(), 2);
        assert!(prepared.deployment_toml.contains("component_files"));

        let classified =
            DeploymentManifest::try_from_toml(&prepared.deployment_toml, Path::new("")).unwrap();
        assert_eq!(classified.files.len(), 2);
        assert_eq!(classified.component_files.len(), 2);
        assert_eq!(
            classified.component_files[0].role,
            ComponentFileRole::JsEntrypoint
        );
        assert_eq!(
            classified.component_files[1].role,
            ComponentFileRole::JsModule
        );

        let resolved = resolve_prepared(&prepared).await;
        assert_matches::assert_matches!(
            &resolved.activities_js[0].location,
            crate::config::deployment::ScriptLocationResolved::Graph { entry_path, files }
                if entry_path == "src/index.js" && files.len() == 2
        );
    }

    #[tokio::test]
    async fn export_strips_generated_metadata_and_preserves_formatting() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(
            dir.path().join("src/index.js"),
            "import { value } from './lib.js'; export default () => value;",
        )
        .await
        .unwrap();
        tokio::fs::write(dir.path().join("src/lib.js"), "export const value = 42;")
            .await
            .unwrap();
        let manifest = r#"# Keep this comment and layout.
[[activity_js]]
name = "a"
location = "src/index.js"
ffqn = "ns:pkg/ifc.fn"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();
        assert!(prepared.deployment_toml.contains("component_files"));
        assert!(prepared.deployment_toml.contains("content_digest"));

        let exported = strip_generated_deployment_metadata(&prepared.deployment_toml).unwrap();
        assert_eq!(exported, manifest);
    }

    #[tokio::test]
    async fn prepare_fills_relative_wasm_digest_and_collects_blob() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("components"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("components/a.wasm"), b"\0asm")
            .await
            .unwrap();
        let manifest = r#"
[[activity_wasm]]
name = "a"
location = "components/a.wasm"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        assert_eq!(prepared.files.len(), 1);
        assert_eq!(prepared.files[0].path, "components/a.wasm");
        let classified =
            DeploymentManifest::try_from_toml(&prepared.deployment_toml, Path::new("")).unwrap();
        assert_eq!(classified.component_files.len(), 1);
        assert_eq!(classified.component_files[0].component_name, "a");
        assert_eq!(
            classified.component_files[0].role,
            ComponentFileRole::WasmComponent
        );
        assert_eq!(prepared.files[0].bytes, b"\0asm");
        assert!(
            prepared
                .deployment_toml
                .contains("content_digest = \"sha256:")
        );
    }

    #[tokio::test]
    async fn prepare_puts_relative_backtrace_digest_in_component_files() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("components"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("components/w.wasm"), b"\0asm")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("src/lib.rs"), "fn workflow() {}")
            .await
            .unwrap();
        let manifest = r#"
[[workflow_wasm]]
name = "w"
location = "components/w.wasm"

[workflow_wasm.backtrace.sources]
".../src/lib.rs" = "src/lib.rs"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        assert_eq!(prepared.files.len(), 2);
        assert!(
            prepared
                .files
                .iter()
                .any(|file| file.path == "components/w.wasm")
        );
        assert!(prepared.files.iter().any(|file| file.path == "src/lib.rs"));
        let doc = prepared.deployment_toml.parse::<DocumentMut>().unwrap();
        let workflow = doc["workflow_wasm"]
            .as_array_of_tables()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(
            workflow["backtrace"]["sources"][".../src/lib.rs"].as_str(),
            Some("src/lib.rs")
        );
        assert!(
            workflow["component_files"]["src/lib.rs"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
        let classified =
            DeploymentManifest::try_from_toml(&prepared.deployment_toml, Path::new("")).unwrap();
        assert_eq!(classified.files.len(), 2);
        assert_eq!(classified.component_files.len(), 2);
        assert_eq!(
            classified.component_files[1].role,
            ComponentFileRole::BacktraceSource
        );

        let resolved = resolve_prepared(&prepared).await;
        assert_eq!(
            resolved.workflows_wasm[0].backtrace.frame_files_to_sources[".../src/lib.rs"]
                .content_digest,
            content_digest(b"fn workflow() {}")
        );
    }

    #[tokio::test]
    async fn export_strips_local_backtrace_digest_but_keeps_oci_digest() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("components"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("components/w.wasm"), b"\0asm")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("src/lib.rs"), "fn workflow() {}")
            .await
            .unwrap();
        let oci_digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let manifest = format!(
            r#"
[[workflow_wasm]]
name = "local"
location = "components/w.wasm"

[workflow_wasm.backtrace.sources]
".../src/lib.rs" = "src/lib.rs"

[[workflow_js]]
name = "oci"
location = "oci://docker.io/library/example:latest"
content_digest = "{oci_digest}"
ffqn = "ns:pkg/ifc.oci"
"#
        );

        let prepared = prepare_deployment_manifest(&manifest, dir.path())
            .await
            .unwrap();
        let exported = strip_generated_deployment_metadata(&prepared.deployment_toml).unwrap();
        let doc = exported.parse::<DocumentMut>().unwrap();
        let local = doc["workflow_wasm"]
            .as_array_of_tables()
            .unwrap()
            .get(0)
            .unwrap();
        assert!(!local.contains_key("content_digest"));
        assert_eq!(
            local["backtrace"]["sources"][".../src/lib.rs"].as_str(),
            Some("src/lib.rs")
        );
        let oci = doc["workflow_js"]
            .as_array_of_tables()
            .unwrap()
            .get(0)
            .unwrap();
        assert_eq!(oci["content_digest"].as_str(), Some(oci_digest));
    }

    #[tokio::test]
    async fn prepare_skips_oci_script_locations() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = r#"
[[workflow_js]]
name = "oci"
location = "oci://docker.io/library/example:latest"
ffqn = "ns:pkg/ifc.oci"
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        assert!(prepared.files.is_empty());
        assert!(!prepared.deployment_toml.contains("content_digest"));
    }

    #[tokio::test]
    async fn prepare_rejects_absolute_script_locations() {
        let dir = tempfile::tempdir().unwrap();
        let abs = dir.path().join("external.js");
        tokio::fs::write(&abs, "external").await.unwrap();
        let manifest = format!(
            r#"
[[activity_js]]
name = "external"
location = "{}"
ffqn = "ns:pkg/ifc.external"
"#,
            abs.display()
        );

        let err = prepare_deployment_manifest(&manifest, dir.path())
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("absolute local paths are not allowed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn manifest_classifies_files_with_field_context() {
        let manifest = r#"
[[activity_wasm]]
name = "act"
location = "components/a.wasm"
content_digest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

[[workflow_wasm]]
name = "wf"
location = "components/w.wasm"
content_digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
component_files = { "src/lib.rs" = "sha256:3333333333333333333333333333333333333333333333333333333333333333" }

[workflow_wasm.backtrace.sources]
".../src/lib.rs" = "src/lib.rs"
"#;

        let deployment_dir = Path::new("/does-not-matter");
        let manifest = DeploymentManifest::try_from_toml(manifest, deployment_dir).unwrap();

        assert_eq!(manifest.files.len(), 3);
        let act = &manifest.files[0];
        assert_eq!(act.path, "components/a.wasm");
        assert_eq!(act.field.section, "activity_wasm");
        assert_eq!(act.field.component_name.as_deref(), Some("act"));
        assert_eq!(act.field.field_path, "activity_wasm[name=act].location");

        let backtrace = &manifest.files[2];
        assert_eq!(backtrace.path, "src/lib.rs");
        assert_eq!(backtrace.field.section, "workflow_wasm.backtrace.sources");
        assert_eq!(
            backtrace.field.field_path,
            "workflow_wasm[name=wf].backtrace.sources[.../src/lib.rs]"
        );
    }

    #[test]
    fn manifest_rejects_deployment_owned_wasm_without_digest() {
        let manifest = r#"
[[activity_wasm]]
name = "act"
location = "components/a.wasm"
"#;
        let err = DeploymentManifest::try_from_toml(manifest, Path::new("/x"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("must set `content_digest`"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn resolve_manifest_reads_blobs_from_cas() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("a.js"), "export const x = 1;")
            .await
            .unwrap();
        let manifest = r#"
[[activity_js]]
name = "a"
location = "a.js"
ffqn = "ns:pkg/ifc.fn"
"#;
        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        let resolved = resolve_prepared(&prepared).await;

        assert_eq!(resolved.activities_js.len(), 1);
    }

    #[tokio::test]
    async fn reconcile_reports_all_mismatches_and_fixes_them() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::create_dir_all(dir.path().join("components"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("scripts"))
            .await
            .unwrap();
        tokio::fs::create_dir_all(dir.path().join("src"))
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("components/w.wasm"), b"workflow")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("scripts/a.js"), b"activity")
            .await
            .unwrap();
        tokio::fs::write(dir.path().join("src/lib.rs"), b"source")
            .await
            .unwrap();

        let wrong = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let deployment_path = dir.path().join("deployment.toml");
        let manifest = format!(
            r#"# formatting is preserved
[[workflow_wasm]]
name = "wf"
location = "components/w.wasm"
content_digest = "{wrong}"
component_files = {{ "src/lib.rs" = "{wrong}" }}

[workflow_wasm.backtrace.sources]
".../src/lib.rs" = "src/lib.rs"

[[activity_js]]
name = "file"
location = "scripts/a.js"
content_digest = "{wrong}"

[[activity_exec]]
name = "inline"
content = "echo hello"
content_digest = "{wrong}"
"#
        );
        tokio::fs::write(&deployment_path, &manifest).await.unwrap();

        let broken = reconcile_deployment_digests(&deployment_path, false)
            .await
            .unwrap();
        assert_eq!(broken.len(), 4);
        assert_eq!(
            broken
                .iter()
                .map(|digest| digest.field_path.as_str())
                .collect::<Vec<_>>(),
            vec![
                "workflow_wasm[name=wf].content_digest",
                "activity_js[name=file].content_digest",
                "activity_exec[name=inline].content_digest",
                "workflow_wasm[name=wf].component_files[src/lib.rs]",
            ]
        );
        assert_eq!(
            tokio::fs::read_to_string(&deployment_path).await.unwrap(),
            manifest
        );

        let fixed = reconcile_deployment_digests(&deployment_path, true)
            .await
            .unwrap();
        assert_eq!(fixed.len(), 4);
        assert!(
            tokio::fs::read_to_string(&deployment_path)
                .await
                .unwrap()
                .starts_with("# formatting is preserved")
        );
        assert!(
            reconcile_deployment_digests(&deployment_path, false)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
