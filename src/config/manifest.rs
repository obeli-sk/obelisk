use crate::config::file_provider::{DiskProvider, FileProvider};
use crate::config::toml::{
    DeploymentResolved, DeploymentToml, OCI_SCHEMA_PREFIX, sanitize_deployment_relative_path,
    strip_deployment_dir_prefix,
};
use anyhow::{Context, bail, ensure};
use concepts::ContentDigest;
use concepts::component_id::Digest;
use concepts::storage::DeploymentFileRecord;
use hashbrown::HashSet;
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, InlineTable, Item, Value, value};

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

/// Parse a verbatim manifest and canonicalize it using the supplied file provider.
///
/// `deployment_dir` remains explicit until later slices move all `${DEPLOYMENT_DIR}` and
/// WASM path resolution to the runtime environment.
pub(crate) async fn manifest_to_canonical(
    deployment_toml: &str,
    deployment_dir: &Path,
    provider: &dyn FileProvider,
) -> anyhow::Result<DeploymentResolved> {
    parse_manifest(deployment_toml, deployment_dir)?
        .canonicalize_with_provider(provider)
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
    // entries fail with the same errors as today's canonicalization path.
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
    collect_script_refs(&mut doc, "activity_js", deployment_dir, &mut files).await?;
    collect_script_refs(&mut doc, "workflow_js", deployment_dir, &mut files).await?;
    collect_script_refs(&mut doc, "webhook_endpoint_js", deployment_dir, &mut files).await?;
    collect_script_refs(&mut doc, "activity_exec", deployment_dir, &mut files).await?;

    let mut seen = HashSet::new();
    files.retain(|file| seen.insert(file.digest.clone()));

    let deployment_toml = doc.to_string();
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

impl DeploymentManifest {
    /// Parse and structurally validate `deployment_toml`, then classify every
    /// component location into a digest-bearing, deployment-relative file set.
    /// No file I/O happens here. Absolute local paths are rejected.
    pub(crate) fn try_from_toml(
        deployment_toml: &str,
        deployment_dir: &Path,
    ) -> anyhow::Result<Self> {
        parse_manifest(deployment_toml, deployment_dir)?;

        let doc = deployment_toml
            .parse::<DocumentMut>()
            .context("cannot parse deployment manifest as TOML")?;

        // Section order matches the historical record collection so digest
        // deduplication keeps the same first-seen path for colliding contents.
        let mut files = Vec::new();
        collect_wasm_section(&doc, "activity_wasm", &mut files)?;
        collect_wasm_section(&doc, "activity_stub", &mut files)?;
        collect_wasm_section(&doc, "activity_external", &mut files)?;
        collect_wasm_section(&doc, "workflow_wasm", &mut files)?;
        collect_backtrace_section(&doc, "workflow_wasm", &mut files)?;
        collect_wasm_section(&doc, "webhook_endpoint_wasm", &mut files)?;
        collect_backtrace_section(&doc, "webhook_endpoint_wasm", &mut files)?;
        collect_script_section(&doc, "activity_js", &mut files)?;
        collect_script_section(&doc, "workflow_js", &mut files)?;
        collect_script_section(&doc, "webhook_endpoint_js", &mut files)?;
        collect_script_section(&doc, "activity_exec", &mut files)?;

        debug_assert!(
            WASM_SECTIONS.len() + SCRIPT_SECTIONS.len() + BACKTRACE_SECTIONS.len() == 11,
            "section lists drifted from collection order"
        );

        let mut seen = HashSet::new();
        files.retain(|file| seen.insert(file.digest.clone()));
        Ok(Self {
            deployment_toml: deployment_toml.to_string(),
            files,
        })
    }

    /// Indexed projection of deployment-owned files for `t_deployment_file`.
    pub(crate) fn file_records(&self) -> Vec<DeploymentFileRecord> {
        self.files
            .iter()
            .map(|file| DeploymentFileRecord {
                path: file.path.clone(),
                digest: file.digest.clone(),
            })
            .collect()
    }
}

fn parse_manifest(
    deployment_toml: &str,
    deployment_dir: &Path,
) -> anyhow::Result<crate::config::toml::DeploymentTomlValidated> {
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
    files: &mut Vec<DeploymentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for table in components {
        let name = component_name(table);
        match classify_script_location(table)? {
            ManifestScriptLocation::DeploymentFile { path, digest } => {
                let field_path =
                    format!("{section}[name={}].location", name.as_deref().unwrap_or(""));
                files.push(DeploymentFileRef {
                    path,
                    digest,
                    field: ManifestFieldRef {
                        section: section.to_string(),
                        component_name: name,
                        field_path,
                    },
                });
            }
            ManifestScriptLocation::Inline | ManifestScriptLocation::Oci => {}
        }
    }

    Ok(())
}

fn collect_wasm_section(
    doc: &DocumentMut,
    section: &str,
    files: &mut Vec<DeploymentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for table in components {
        let name = component_name(table);
        let Some(raw_location) = table.get("location").and_then(Item::as_str) else {
            continue;
        };
        match classify_wasm_location(raw_location, table.get("content_digest"))? {
            ManifestWasmLocation::DeploymentFile { path, digest } => {
                let field_path =
                    format!("{section}[name={}].location", name.as_deref().unwrap_or(""));
                files.push(DeploymentFileRef {
                    path,
                    digest,
                    field: ManifestFieldRef {
                        section: section.to_string(),
                        component_name: name,
                        field_path,
                    },
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
    files: &mut Vec<DeploymentFileRef>,
) -> anyhow::Result<()> {
    let Some(components) = doc.get(section).and_then(Item::as_array_of_tables) else {
        return Ok(());
    };

    for table in components {
        let name = component_name(table);
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
            let digest = required_content_digest(
                source
                    .as_table_like()
                    .and_then(|table| table.get("content_digest")),
                &path,
            )?;
            files.push(DeploymentFileRef {
                path,
                digest,
                field: ManifestFieldRef {
                    section: format!("{section}.backtrace.sources"),
                    component_name: name.clone(),
                    field_path: format!(
                        "{section}[name={}].backtrace.sources[{key}]",
                        name.as_deref().unwrap_or("")
                    ),
                },
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
        let Some(sources) = table
            .get_mut("backtrace")
            .and_then(Item::as_table_like_mut)
            .and_then(|backtrace| backtrace.get_mut("sources"))
            .and_then(Item::as_table_like_mut)
        else {
            continue;
        };

        for (_, source) in sources.iter_mut() {
            let Some(raw_path) = backtrace_source_path(source) else {
                continue;
            };
            let Some(path) = deployment_owned_path(&raw_path)? else {
                continue;
            };
            let (digest, bytes) = read_deployment_file(deployment_dir, &path).await?;
            write_backtrace_source_digest(source, raw_path, &digest);
            files.push(DeploymentManifestFile {
                path,
                digest,
                bytes,
            });
        }
    }

    Ok(())
}

fn backtrace_source_path(source: &Item) -> Option<String> {
    source.as_str().map(str::to_string).or_else(|| {
        source
            .as_table_like()
            .and_then(|table| table.get("path"))
            .and_then(Item::as_str)
            .map(str::to_string)
    })
}

fn write_backtrace_source_digest(source: &mut Item, path: String, digest: &ContentDigest) {
    if let Some(table) = source.as_table_like_mut() {
        table.insert("content_digest", value(digest.to_string()));
        return;
    }

    let mut inline = InlineTable::new();
    inline.insert("path", Value::from(path));
    inline.insert("content_digest", Value::from(digest.to_string()));
    *source = Item::Value(Value::InlineTable(inline));
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

#[expect(
    dead_code,
    reason = "used by CLI manifest canonicalization in later slices"
)]
pub(crate) async fn manifest_file_to_canonical(
    deployment_toml_path: &Path,
) -> anyhow::Result<DeploymentResolved> {
    let deployment_toml = tokio::fs::read_to_string(deployment_toml_path)
        .await
        .with_context(|| format!("cannot read deployment manifest {deployment_toml_path:?}"))?;
    let deployment_dir = canonicalize_parent(deployment_toml_path)
        .with_context(|| format!("cannot resolve parent of {deployment_toml_path:?}"))?;
    let provider = DiskProvider {
        deployment_dir: deployment_dir.clone(),
    };
    manifest_to_canonical(&deployment_toml, &deployment_dir, &provider).await
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(prepared.files[0].bytes, b"\0asm");
        assert!(
            prepared
                .deployment_toml
                .contains("content_digest = \"sha256:")
        );
    }

    #[tokio::test]
    async fn prepare_fills_relative_backtrace_digest_and_collects_blob() {
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
        assert!(
            prepared.deployment_toml.contains(
                "\".../src/lib.rs\" = { path = \"src/lib.rs\", content_digest = \"sha256:"
            )
        );
    }

    #[tokio::test]
    async fn prepare_preserves_detailed_backtrace_source_shape() {
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
".../src/lib.rs" = { path = "src/lib.rs" }
"#;

        let prepared = prepare_deployment_manifest(manifest, dir.path())
            .await
            .unwrap();

        assert!(prepared.files.iter().any(|file| file.path == "src/lib.rs"));
        assert!(prepared.deployment_toml.contains("path = \"src/lib.rs\""));
        assert!(
            prepared
                .deployment_toml
                .contains("content_digest = \"sha256:")
        );
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

[workflow_wasm.backtrace.sources]
".../src/lib.rs" = { path = "src/lib.rs", content_digest = "sha256:3333333333333333333333333333333333333333333333333333333333333333" }
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
    async fn manifest_to_canonical_uses_supplied_provider() {
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
        let provider = DiskProvider {
            deployment_dir: dir.path().to_path_buf(),
        };

        let canonical = manifest_to_canonical(manifest, dir.path(), &provider)
            .await
            .unwrap();

        assert_eq!(canonical.activities_js.len(), 1);
    }
}
