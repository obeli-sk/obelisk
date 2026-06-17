use crate::config::file_provider::{DiskProvider, FileProvider};
use crate::config::toml::{
    DeploymentCanonical, DeploymentToml, OCI_SCHEMA_PREFIX, sanitize_deployment_relative_path,
    strip_deployment_dir_prefix,
};
use anyhow::{Context, ensure};
use concepts::ContentDigest;
use concepts::component_id::Digest;
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};
use toml_edit::{DocumentMut, Item, value};

#[derive(Debug, Clone)]
#[expect(
    dead_code,
    reason = "used by manifest submit/upload flow in later slices"
)]
pub(crate) struct DeploymentManifestFile {
    pub(crate) path: String,
    pub(crate) digest: ContentDigest,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct PreparedDeploymentManifest {
    pub(crate) deployment_toml: String,
    pub(crate) digest: ContentDigest,
    pub(crate) files: Vec<DeploymentManifestFile>,
}

/// Parse a verbatim manifest and canonicalize it using the supplied file provider.
///
/// `deployment_dir` remains explicit until later slices move all `${DEPLOYMENT_DIR}` and
/// WASM path resolution to the runtime environment.
pub(crate) async fn manifest_to_canonical(
    deployment_toml: &str,
    deployment_dir: &Path,
    provider: &dyn FileProvider,
) -> anyhow::Result<DeploymentCanonical> {
    parse_manifest(deployment_toml, deployment_dir)?
        .canonicalize_with_provider(provider)
        .await
}

#[expect(
    dead_code,
    reason = "used by manifest submit/upload flow in later slices"
)]
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
    collect_wasm_refs(
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
        let full_path = deployment_dir.join(&path);
        let bytes = tokio::fs::read(&full_path)
            .await
            .with_context(|| format!("cannot read deployment file {full_path:?}"))?;
        let digest = content_digest(&bytes);
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
    let full_path = deployment_dir.join(&path);
    let bytes = tokio::fs::read(&full_path)
        .await
        .with_context(|| format!("cannot read deployment file {full_path:?}"))?;
    let digest = content_digest(&bytes);
    table["content_digest"] = value(digest.to_string());
    files.push(DeploymentManifestFile {
        path,
        digest,
        bytes,
    });
    Ok(())
}

fn deployment_owned_path(raw: &str) -> anyhow::Result<Option<String>> {
    if raw.starts_with(OCI_SCHEMA_PREFIX) || Path::new(raw).is_absolute() {
        return Ok(None);
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

#[expect(
    dead_code,
    reason = "used by CLI manifest canonicalization in later slices"
)]
pub(crate) async fn manifest_file_to_canonical(
    deployment_toml_path: &Path,
) -> anyhow::Result<DeploymentCanonical> {
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
    async fn prepare_skips_absolute_and_oci_script_locations() {
        let dir = tempfile::tempdir().unwrap();
        let abs = dir.path().join("external.js");
        tokio::fs::write(&abs, "external").await.unwrap();
        let manifest = format!(
            r#"
[[activity_js]]
name = "external"
location = "{}"
ffqn = "ns:pkg/ifc.external"

[[workflow_js]]
name = "oci"
location = "oci://docker.io/library/example:latest"
ffqn = "ns:pkg/ifc.oci"
"#,
            abs.display()
        );

        let prepared = prepare_deployment_manifest(&manifest, dir.path())
            .await
            .unwrap();

        assert!(prepared.files.is_empty());
        assert!(!prepared.deployment_toml.contains("content_digest"));
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
