use crate::ContentDigest;
use crate::component_id::Digest;
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::sync::Mutex;

/// Content digest of `content`, the address every `Cas` keys blobs by.
#[must_use]
pub fn content_digest(content: &[u8]) -> ContentDigest {
    ContentDigest(Digest(Sha256::digest(content).into()))
}

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

/// A `Cas` that keeps blobs in a `HashMap`, for offline deployment verification
/// and tests. Follows the same content-addressed contract as the persistent
/// implementations: `write_blob` computes and returns the digest of the content.
#[derive(Default)]
pub struct InMemoryCas {
    blobs: Mutex<HashMap<ContentDigest, Vec<u8>>>,
}

#[async_trait]
impl Cas for InMemoryCas {
    async fn read_blob(&self, digest: &ContentDigest) -> Result<Option<Vec<u8>>, CasError> {
        Ok(self.blobs.lock().unwrap().get(digest).cloned())
    }

    async fn write_blob(&self, content: &[u8]) -> Result<ContentDigest, CasError> {
        let digest = content_digest(content);
        self.blobs
            .lock()
            .unwrap()
            .insert(digest.clone(), content.to_vec());
        Ok(digest)
    }

    async fn contains_blob(&self, digest: &ContentDigest) -> Result<bool, CasError> {
        Ok(self.blobs.lock().unwrap().contains_key(digest))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CasError {
    #[error("content-addressed store error: {0}")]
    Uncategorized(String),
}
