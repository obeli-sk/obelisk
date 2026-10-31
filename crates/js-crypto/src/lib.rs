//! Web Crypto primitives shared by the Boa and V8 JS runtimes; JS glue lives in each runtime.

use hmac::{Hmac, KeyInit as _, Mac as _};
use sha2::{Digest as _, Sha256, Sha384, Sha512};

/// `getRandomValues` throws `QuotaExceededError` above this many bytes.
pub const RANDOM_VALUES_MAX_BYTES: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    Sha256,
    Sha384,
    Sha512,
}

impl HashAlgorithm {
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
    pub fn digest(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::Sha256 => Sha256::digest(data).to_vec(),
            Self::Sha384 => Sha384::digest(data).to_vec(),
            Self::Sha512 => Sha512::digest(data).to_vec(),
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

#[derive(Debug, serde::Deserialize)]
struct RsaPublicJwk {
    kty: String,
    n: String,
    e: String,
    alg: Option<String>,
    #[serde(rename = "use")]
    key_use: Option<String>,
    key_ops: Option<Vec<String>>,
    ext: Option<bool>,
}

/// An imported public RSA key for RSASSA-PKCS1-v1_5 with SHA-256.
#[derive(Debug)]
pub struct RsaVerificationKey(rsa::RsaPublicKey);

impl RsaVerificationKey {
    pub fn import(jwk: &str, extractable: bool, usages: &[String]) -> Result<Self, String> {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let value: serde_json::Value = serde_json::from_str(jwk)
            .map_err(|err| format!("DataError: invalid RSA JWK: {err}"))?;
        if value.get("d").is_some() {
            return Err("DataError: expected a public RSA JWK".into());
        }
        let jwk: RsaPublicJwk = serde_json::from_value(value)
            .map_err(|err| format!("DataError: invalid RSA JWK: {err}"))?;
        if usages.iter().any(|usage| usage != "verify") {
            return Err("SyntaxError: RSA public keys only support verify".into());
        }
        if jwk.kty != "RSA" {
            return Err("DataError: expected a public RSA JWK".into());
        }
        if jwk.alg.as_deref().is_some_and(|alg| alg != "RS256")
            || (!usages.is_empty() && jwk.key_use.as_deref().is_some_and(|usage| usage != "sig"))
            || jwk.key_ops.as_ref().is_some_and(|ops| {
                ops.iter().enumerate().any(|(i, op)| ops[..i].contains(op))
                    || usages.iter().any(|usage| !ops.contains(usage))
            })
            || (extractable && jwk.ext == Some(false))
        {
            return Err("DataError: JWK restrictions do not match the requested key".into());
        }
        let decode = |value: &str| {
            let bytes = URL_SAFE_NO_PAD
                .decode(value)
                .map_err(|_| "DataError: invalid RSA base64url integer".to_owned())?;
            if bytes.is_empty() || bytes[0] == 0 {
                return Err("DataError: invalid RSA unsigned integer".to_owned());
            }
            Ok(rsa::BigUint::from_bytes_be(&bytes))
        };
        let key = rsa::RsaPublicKey::new(decode(&jwk.n)?, decode(&jwk.e)?)
            .map_err(|err| format!("DataError: invalid RSA public key: {err}"))?;
        Ok(Self(key))
    }

    #[must_use]
    pub fn modulus_length(&self) -> usize {
        use rsa::traits::PublicKeyParts as _;
        self.0.n().bits()
    }

    #[must_use]
    pub fn public_exponent(&self) -> Vec<u8> {
        use rsa::traits::PublicKeyParts as _;
        self.0.e().to_bytes_be()
    }

    #[must_use]
    pub fn verify(&self, data: &[u8], signature: &[u8]) -> bool {
        use rsa::sha2::{Digest as _, Sha256};
        self.0
            .verify(
                rsa::Pkcs1v15Sign::new::<Sha256>(),
                &Sha256::digest(data),
                signature,
            )
            .is_ok()
    }
}
