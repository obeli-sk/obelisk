use deno_error::JsErrorBox;
use hmac::{Hmac, Mac as _};
use sha2::{Sha256, Sha384, Sha512};

pub(crate) fn hmac(hash: &str, key: &[u8], message: &[u8]) -> Result<Vec<u8>, JsErrorBox> {
    macro_rules! sign {
        ($digest:ty) => {{
            let mut mac = Hmac::<$digest>::new_from_slice(key)
                .map_err(|err| JsErrorBox::type_error(err.to_string()))?;
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }};
    }
    match hash.to_ascii_uppercase().as_str() {
        "SHA-256" | "SHA256" => sign!(Sha256),
        "SHA-384" | "SHA384" => sign!(Sha384),
        "SHA-512" | "SHA512" => sign!(Sha512),
        _ => Err(JsErrorBox::type_error(format!(
            "unsupported HMAC hash algorithm: {hash}"
        ))),
    }
}
