use crate::ContentDigest;
use async_trait::async_trait;

/// Content-addressed blob store for deployment files (scripts, WASM, backtrace
/// sources), addressed purely by content digest.
///
/// Deliberately separate from `DbConnection`: bytes may live in `SQLite`,
/// Postgres, or a future object store such as S3, while the metadata that
/// references blobs stays in the database.
#[async_trait]
pub trait Cas: Send + Sync {
    /// Fetch a blob by digest. `Ok(None)` if absent.
    async fn read_blob(&self, digest: &ContentDigest) -> Result<Option<Vec<u8>>, CasError>;

    /// Store a blob. Content-addressed and idempotent: storing bytes already
    /// present is a no-op. Returns the digest computed over `content`.
    async fn write_blob(&self, content: &[u8]) -> Result<ContentDigest, CasError>;

    /// Whether a blob is present, without fetching its bytes.
    async fn contains_blob(&self, digest: &ContentDigest) -> Result<bool, CasError>;
}

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    #[error("content-addressed store error: {0}")]
    Uncategorized(String),
}
