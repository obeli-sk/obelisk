use concepts::component_id::Digest;
use rand::Rng as _;
use sha2::{Digest as _, Sha256};
use std::fmt::Write as _;

/// Maximum encoded gRPC message size for deployment repository requests and responses.
pub(crate) const MAX_GRPC_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

pub(crate) fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Digest of a token as accepted in `api.token_hashes`; displays as `sha256:<hex>`.
pub(crate) fn token_hash(token: &str) -> Digest {
    Digest(token_digest(token))
}

/// Generate a high-entropy random token (64 hex chars).
pub(crate) fn generate_token() -> String {
    let bytes = rand::rng().random::<[u8; 32]>();
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut out, "{b:02x}").expect("writing to string");
    }
    out
}
