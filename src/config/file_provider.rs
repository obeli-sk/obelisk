use anyhow::{Context, ensure};
use concepts::ContentDigest;
use concepts::cas::Cas;
use concepts::component_id::Digest;
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;
use std::sync::Arc;

/// Source of deployment-owned file bytes during canonicalization.
///
/// Canonicalization inlines every deployment-owned script/source into the
/// `DeploymentCanonical`; where the bytes come from depends on context.
/// OCI refs are not deployment-owned and are not read through a provider.
#[async_trait::async_trait]
pub(crate) trait FileProvider: Send + Sync {
    /// Read the bytes of a deployment-owned file.
    ///
    /// `path` is its deployment-relative path; `digest`, when present, is the
    /// expected content hash. Implementations must ensure returned bytes hash
    /// to `digest` when one is supplied.
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>>;
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
}

/// Reads blobs from the content-addressed store by digest.
///
/// A digest is required; later manifest work makes digests mandatory on every
/// relative ref before this provider is used for canonicalization.
pub(crate) struct CasFileProvider {
    pub(crate) cas: Arc<dyn Cas>,
}

#[async_trait::async_trait]
impl FileProvider for CasFileProvider {
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>> {
        let digest = digest.with_context(|| {
            format!("CAS-backed canonicalization requires a content digest for `{path}`")
        })?;
        self.cas
            .read_blob(digest)
            .await?
            .with_context(|| format!("blob {digest} for `{path}` not present in the CAS"))
    }
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
