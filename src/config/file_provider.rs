use anyhow::{Context, ensure};
use concepts::ContentDigest;
use concepts::cas::Cas;
use concepts::component_id::Digest;
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::BTreeMap, path::Path};

#[derive(Debug)]
pub(crate) struct ParsedWitFiles {
    pub(crate) files: Vec<(String, String)>,
    pub(crate) resolve: wit_parser::Resolve,
    pub(crate) main_pkg_id: wit_parser::PackageId,
}

/// Source of deployment-owned file bytes during deployment resolution.
///
/// Resolution inlines every deployment-owned script/source into the
/// `DeploymentResolved`; where the bytes come from depends on context.
/// OCI refs are not deployment-owned and are not read through a provider.
#[async_trait::async_trait]
pub(crate) trait FileProvider: Send + Sync {
    /// Read the bytes of a deployment-owned file.
    ///
    /// `path` is its deployment-relative path; `digest`, when present, is the
    /// expected content hash. Implementations must ensure returned bytes hash
    /// to `digest` when one is supplied.
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>>;

    /// Parse the JS module at `entry_path` and return its closed import graph: the
    /// entry plus every transitively-imported relative module, as `(deployment-relative
    /// path, source)`. Every relative import must resolve to a module the provider can
    /// supply, so a manifest that under-declares its graph is rejected here rather than
    /// failing at runtime. `known_files` is the manifest's declared `path -> digest` map;
    /// CAS-backed resolution reads each module through it, the disk provider walks the
    /// filesystem and ignores it. Non-JS scripts never call this.
    async fn parse_js_graph(
        &self,
        entry_path: &str,
        known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<Vec<(String, String)>>;

    /// Parse the WIT package rooted at `root`, retaining the resolve, main package ID, and
    /// exactly the `.wit` sources selected as `(deployment-relative path, source)`. The parser
    /// decides the file set, so malformed WIT is rejected and stray declarations cannot smuggle
    /// in unused sources. `known_files` is the manifest's declared `path -> digest` map;
    /// CAS-backed resolution materializes those blobs, while the disk provider ignores it.
    async fn parse_wit_files(
        &self,
        root: &str,
        known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<ParsedWitFiles>;
}

/// Parse the WIT package under `deployment_dir/root` and retain its resolve and selected sources.
async fn parse_wit_dir(deployment_dir: &Path, root: &str) -> anyhow::Result<ParsedWitFiles> {
    let root = crate::config::toml::sanitize_deployment_relative_path(root)?;
    let deployment_dir = deployment_dir
        .canonicalize()
        .with_context(|| format!("cannot canonicalize deployment directory {deployment_dir:?}"))?;
    let wit_root = deployment_dir.join(&root).canonicalize().with_context(|| {
        format!(
            "cannot canonicalize WIT directory {:?}",
            deployment_dir.join(&root)
        )
    })?;
    ensure!(
        wit_root.starts_with(&deployment_dir),
        "WIT directory `{root}` resolves outside the deployment directory"
    );
    ensure!(wit_root.is_dir(), "WIT path `{root}` is not a directory");

    let mut resolve = wit_parser::Resolve::default();
    let (main_pkg_id, parsed_sources) = resolve
        .push_dir(&wit_root)
        .with_context(|| format!("cannot parse WIT directory `{root}`"))?;
    let parsed_paths: Vec<_> = parsed_sources.paths().map(Path::to_path_buf).collect();
    ensure!(
        !parsed_paths.is_empty(),
        "WIT directory `{root}` contains no parsed files"
    );

    let mut files = Vec::with_capacity(parsed_paths.len());
    for parsed_path in parsed_paths {
        let canonical = parsed_path
            .canonicalize()
            .with_context(|| format!("cannot canonicalize parsed WIT file {parsed_path:?}"))?;
        ensure!(
            canonical.starts_with(&deployment_dir),
            "parsed WIT file {parsed_path:?} resolves outside the deployment directory"
        );
        ensure!(
            canonical.extension().and_then(|ext| ext.to_str()) == Some("wit"),
            "parsed WIT source is not a .wit file: {parsed_path:?}"
        );
        let relative = canonical
            .strip_prefix(&deployment_dir)
            .expect("checked prefix");
        let relative = path_to_deployment_string(relative)?;
        let bytes = tokio::fs::read(&canonical)
            .await
            .with_context(|| format!("cannot read parsed WIT file {canonical:?}"))?;
        let source = String::from_utf8(bytes)
            .with_context(|| format!("WIT file {canonical:?} is not valid UTF-8"))?;
        files.push((relative, source));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files.dedup_by(|a, b| a.0 == b.0);
    Ok(ParsedWitFiles {
        files,
        resolve,
        main_pkg_id,
    })
}

/// Reads from the submitter's disk, under the deployment directory.
pub(crate) struct DiskProvider {
    pub(crate) deployment_dir: PathBuf,
}

#[async_trait::async_trait]
impl FileProvider for DiskProvider {
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>> {
        let full = self.deployment_dir.join(path);
        let bytes = tokio::fs::read(&full)
            .await
            .with_context(|| format!("cannot read file {full:?}"))?;
        verify_content_digest(&bytes, digest, path)?;
        Ok(bytes)
    }

    async fn parse_js_graph(
        &self,
        entry_path: &str,
        _known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let graph =
            crate::javascript::graph::collect_graph(&self.deployment_dir, entry_path).await?;
        Ok(graph.files.into_iter().collect())
    }

    async fn parse_wit_files(
        &self,
        root: &str,
        _known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<ParsedWitFiles> {
        parse_wit_dir(&self.deployment_dir, root).await
    }
}

/// Reads blobs from the content-addressed store by digest.
///
/// A digest is required; later manifest work makes digests mandatory on every
/// relative ref before this provider is used for resolution.
pub(crate) struct CasFileProvider {
    pub(crate) cas: Arc<dyn Cas>,
}

/// Reads JS module sources from the CAS, resolving each deployment-relative path
/// against the manifest's declared `component_files`. An import with no declared
/// digest is rejected: it was never uploaded, so the graph is open.
struct CasModuleReader<'a> {
    cas: &'a Arc<dyn Cas>,
    known_files: &'a BTreeMap<String, ContentDigest>,
}

#[async_trait::async_trait]
impl crate::javascript::graph::ModuleSourceReader for CasModuleReader<'_> {
    async fn read_source(&self, dep_path: &str) -> anyhow::Result<String> {
        let digest = self.known_files.get(dep_path).with_context(|| {
            format!(
                "JS module `{dep_path}` is imported but not part of the deployment package \
                 (no matching `component_files` entry)"
            )
        })?;
        let bytes =
            self.cas.read_blob(digest).await?.with_context(|| {
                format!("blob {digest} for JS module `{dep_path}` not in the CAS")
            })?;
        String::from_utf8(bytes)
            .with_context(|| format!("JS module `{dep_path}` is not valid UTF-8"))
    }
}

#[async_trait::async_trait]
impl FileProvider for CasFileProvider {
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>> {
        let digest = digest.with_context(|| {
            format!("CAS-backed resolution requires a content digest for `{path}`")
        })?;
        self.cas
            .read_blob(digest)
            .await?
            .with_context(|| format!("blob {digest} for `{path}` not present in the CAS"))
    }

    async fn parse_js_graph(
        &self,
        entry_path: &str,
        known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let reader = CasModuleReader {
            cas: &self.cas,
            known_files,
        };
        let graph = crate::javascript::graph::collect_graph_with(&reader, entry_path).await?;
        Ok(graph.files.into_iter().collect())
    }

    async fn parse_wit_files(
        &self,
        root: &str,
        known_files: &BTreeMap<String, ContentDigest>,
    ) -> anyhow::Result<ParsedWitFiles> {
        let root = crate::config::toml::sanitize_deployment_relative_path(root)?;
        let prefix = format!("{root}/");
        // Materialize every declared blob under `root` into a temp dir, then let
        // `wit-parser` decide the file set. Trusting the declared prefix instead would
        // let a broken client smuggle garbage or unused `.wit` files past submit.
        let declared: Vec<(String, &ContentDigest)> = known_files
            .iter()
            .filter(|(path, _)| path.starts_with(&prefix))
            .map(|(path, digest)| (path.clone(), digest))
            .collect();
        ensure!(
            !declared.is_empty(),
            "CAS-backed resolution has no declared WIT files for `{root}`"
        );
        let temp_dir = tempfile::tempdir().context("cannot create temp dir to parse WIT")?;
        for (path, digest) in &declared {
            let path = crate::config::toml::sanitize_deployment_relative_path(path)?;
            ensure!(
                Path::new(&path).extension().and_then(|ext| ext.to_str()) == Some("wit"),
                "declared WIT source is not a .wit file: `{path}`"
            );
            let full = temp_dir.path().join(&path);
            if let Some(parent) = full.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("cannot stage WIT file `{path}`"))?;
            }
            let bytes = self.read(&path, Some(digest)).await?;
            tokio::fs::write(&full, &bytes)
                .await
                .with_context(|| format!("cannot stage WIT file `{path}`"))?;
        }

        let parsed = parse_wit_dir(temp_dir.path(), &root).await?;

        // Reject stray declarations: a declared `.wit` file `wit-parser` did not select is
        // dead weight the deployment -> digest mapping would pin, so refuse it here.
        let selected: std::collections::BTreeSet<&str> =
            parsed.files.iter().map(|(path, _)| path.as_str()).collect();
        let stray: Vec<&str> = declared
            .iter()
            .map(|(path, _)| path.as_str())
            .filter(|path| !selected.contains(path))
            .collect();
        ensure!(
            stray.is_empty(),
            "WIT directory `{root}` declares {stray:?} that `wit-parser` does not select"
        );
        Ok(parsed)
    }
}

fn path_to_deployment_string(path: &Path) -> anyhow::Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(part) = component else {
            anyhow::bail!("invalid deployment-relative path {path:?}")
        };
        parts.push(
            part.to_str()
                .with_context(|| format!("non-UTF8 deployment path {path:?}"))?,
        );
    }
    ensure!(!parts.is_empty(), "empty deployment-relative path");
    Ok(parts.join("/"))
}

/// Verify `bytes` against `expected` content digest, if one is set. No-op when unset.
pub(crate) fn verify_content_digest(
    bytes: &[u8],
    expected: Option<&ContentDigest>,
    what: &str,
) -> anyhow::Result<()> {
    if let Some(expected) = expected {
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        let actual = ContentDigest(Digest(hash));
        ensure!(
            *expected == actual,
            "content digest mismatch for {what}: expected {expected}, got {actual}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::cas::CasError;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn digest_of(bytes: &[u8]) -> ContentDigest {
        ContentDigest(Digest(Sha256::digest(bytes).into()))
    }

    #[derive(Default)]
    struct MemCas {
        blobs: Mutex<HashMap<ContentDigest, Vec<u8>>>,
    }
    impl MemCas {
        fn insert(&self, bytes: &[u8]) -> ContentDigest {
            let digest = digest_of(bytes);
            self.blobs
                .lock()
                .unwrap()
                .insert(digest.clone(), bytes.to_vec());
            digest
        }
    }
    #[async_trait::async_trait]
    impl Cas for MemCas {
        async fn read_blob(&self, digest: &ContentDigest) -> Result<Option<Vec<u8>>, CasError> {
            Ok(self.blobs.lock().unwrap().get(digest).cloned())
        }
        async fn write_blob(&self, content: &[u8]) -> Result<ContentDigest, CasError> {
            Ok(self.insert(content))
        }
        async fn contains_blob(&self, digest: &ContentDigest) -> Result<bool, CasError> {
            Ok(self.blobs.lock().unwrap().contains_key(digest))
        }
    }

    #[tokio::test]
    async fn cas_parse_wit_files_parses_a_valid_package() {
        let cas = Arc::new(MemCas::default());
        let src = "package any:any;\n\nworld any {}\n";
        let digest = cas.insert(src.as_bytes());
        let provider = CasFileProvider { cas };
        let known = BTreeMap::from([("a/wit/impl.wit".to_string(), digest)]);
        let parsed = provider.parse_wit_files("a/wit", &known).await.unwrap();
        assert_eq!(
            parsed.files,
            vec![("a/wit/impl.wit".to_string(), src.to_string())]
        );
    }

    #[tokio::test]
    async fn cas_parse_wit_files_rejects_malformed_wit() {
        // Before parsing on the CAS path, a client could push arbitrary bytes as `.wit`.
        let cas = Arc::new(MemCas::default());
        let digest = cas.insert(b"this is not valid wit @@@");
        let provider = CasFileProvider { cas };
        let known = BTreeMap::from([("a/wit/impl.wit".to_string(), digest)]);
        let err = provider
            .parse_wit_files("a/wit", &known)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot parse WIT directory"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn cas_parse_wit_files_rejects_stray_declared_wit() {
        // A declared `.wit` `wit-parser` does not select (here, one buried in a non-`deps`
        // subdirectory) must be refused rather than silently persisted.
        let cas = Arc::new(MemCas::default());
        let impl_src = "package any:any;\n\nworld any {}\n";
        let impl_digest = cas.insert(impl_src.as_bytes());
        let stray_digest = cas.insert(b"package other:other;\n");
        let provider = CasFileProvider { cas };
        let known = BTreeMap::from([
            ("a/wit/impl.wit".to_string(), impl_digest),
            ("a/wit/notes/scratch.wit".to_string(), stray_digest),
        ]);
        let err = provider
            .parse_wit_files("a/wit", &known)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("scratch.wit") && err.contains("does not select"),
            "unexpected error: {err}"
        );
    }
}
