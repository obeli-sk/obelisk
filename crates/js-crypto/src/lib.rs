//! Web Crypto primitives shared by the Boa and V8 JS runtimes; JS glue lives in each runtime.

use hmac::{Hmac, Mac as _};
use sha2::{Sha256, Sha384, Sha512};

/// `getRandomValues` throws `QuotaExceededError` above this many bytes.
pub const RANDOM_VALUES_MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HmacHash {
    Sha256,
    Sha384,
    Sha512,
}

impl HmacHash {
    /// Parse a Web Crypto hash name; matching is case-insensitive as the spec requires.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "SHA-256" | "SHA256" => Ok(Self::Sha256),
            "SHA-384" | "SHA384" => Ok(Self::Sha384),
            "SHA-512" | "SHA512" => Ok(Self::Sha512),
            _ => Err(format!(
                "NotSupportedError: unsupported hash algorithm '{name}'"
            )),
        }
    }

    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SHA-256",
            Self::Sha384 => "SHA-384",
            Self::Sha512 => "SHA-512",
        }
    }

    #[must_use]
    pub fn sign(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        macro_rules! sign {
            ($digest:ty) => {{
                let mut mac =
                    Hmac::<$digest>::new_from_slice(key).expect("HMAC accepts any key length");
                mac.update(data);
                mac.finalize().into_bytes().to_vec()
            }};
        }
        match self {
            Self::Sha256 => sign!(Sha256),
            Self::Sha384 => sign!(Sha384),
            Self::Sha512 => sign!(Sha512),
        }
    }

    /// Constant-time comparison against the expected tag.
    #[must_use]
    pub fn verify(self, key: &[u8], data: &[u8], signature: &[u8]) -> bool {
        macro_rules! verify {
            ($digest:ty) => {{
                let mut mac =
                    Hmac::<$digest>::new_from_slice(key).expect("HMAC accepts any key length");
                mac.update(data);
                mac.verify_slice(signature).is_ok()
            }};
        }
        match self {
            Self::Sha256 => verify!(Sha256),
            Self::Sha384 => verify!(Sha384),
            Self::Sha512 => verify!(Sha512),
        }
    }
}

/// Produce `byte_length` random bytes for `getRandomValues`, filled by the runtime's CSPRNG.
pub fn random_values(byte_length: usize, fill: impl FnOnce(&mut [u8])) -> Result<Vec<u8>, String> {
    if byte_length > RANDOM_VALUES_MAX_BYTES {
        return Err(format!(
            "QuotaExceededError: getRandomValues: byteLength {byte_length} exceeds {RANDOM_VALUES_MAX_BYTES}"
        ));
    }
    let mut bytes = vec![0; byte_length];
    fill(&mut bytes);
    Ok(bytes)
}
