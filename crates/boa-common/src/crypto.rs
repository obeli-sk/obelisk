//! Web Crypto API (`crypto.subtle`) for Boa-based Obelisk runtimes.
//!
//! Implements the subset of the [Web Crypto API][spec] needed for HMAC-SHA-256/384/512
//! signature generation and verification — the minimum required for GitHub webhook
//! signature verification.
//!
//! Supported operations:
//! - `crypto.subtle.importKey("raw", keyData, {name:"HMAC", hash:"SHA-256"}, extractable, usages)`
//! - `crypto.subtle.sign({name:"HMAC"}, key, data)`
//! - `crypto.subtle.verify({name:"HMAC"}, key, signature, data)`
//!
//! All `subtle.*` methods return immediately-resolved `Promise`s (HMAC is synchronous).
//! The Promise return is required by the spec and allows callers to use `await`.
//!
//! [spec]: https://www.w3.org/TR/WebCryptoAPI/

use boa_engine::{
    Context, JsArgs, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, js_string,
    object::builtins::{AlignedVec, JsArrayBuffer, JsPromise, JsTypedArray},
    property::Attribute,
};
use boa_gc::{Finalize, Trace};
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha384, Sha512};

// ── Key material ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum HmacHash {
    Sha256,
    Sha384,
    Sha512,
}

impl HmacHash {
    fn name(&self) -> &'static str {
        match self {
            HmacHash::Sha256 => "SHA-256",
            HmacHash::Sha384 => "SHA-384",
            HmacHash::Sha512 => "SHA-512",
        }
    }
}

/// Rust data stored inside a `CryptoKey` JS object.
#[derive(Debug, Trace, Finalize, boa_engine::JsData)]
pub struct CryptoKeyData {
    #[unsafe_ignore_trace]
    key_bytes: Vec<u8>,
    #[unsafe_ignore_trace]
    hash: HmacHash,
    extractable: bool,
    #[unsafe_ignore_trace]
    usages: Vec<String>,
}

// ── Buffer extraction ─────────────────────────────────────────────────────────

/// Extract raw bytes from a `BufferSource` (`ArrayBuffer` or `TypedArray`).
fn buffer_source_to_bytes(val: &JsValue, context: &mut Context) -> JsResult<Vec<u8>> {
    let obj = val.as_object().ok_or_else(|| {
        JsNativeError::typ().with_message("expected BufferSource (ArrayBuffer or TypedArray)")
    })?;

    // Try ArrayBuffer first.
    if let Ok(ab) = JsArrayBuffer::from_object(obj.clone()) {
        return Ok(ab
            .to_vec()
            .ok_or_else(|| JsNativeError::error().with_message("ArrayBuffer is detached"))?);
    }

    // Try TypedArray (e.g. Uint8Array from TextEncoder).
    if let Ok(ta) = JsTypedArray::from_object(obj.clone()) {
        let buf_val = ta.buffer(context)?;
        let buf_obj = buf_val
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("TypedArray has no backing buffer"))?;
        let ab = JsArrayBuffer::from_object(buf_obj.clone())?;
        let offset = ta.byte_offset(context)?;
        let len = ta.byte_length(context)?;
        let all_bytes = ab
            .to_vec()
            .ok_or_else(|| JsNativeError::error().with_message("ArrayBuffer is detached"))?;
        return Ok(all_bytes[offset..offset + len].to_vec());
    }

    Err(JsNativeError::typ()
        .with_message("expected BufferSource (ArrayBuffer or TypedArray)")
        .into())
}

/// Wrap a `Vec<u8>` in a JS `ArrayBuffer`.
fn bytes_to_array_buffer(bytes: Vec<u8>, context: &mut Context) -> JsResult<JsValue> {
    let mut aligned = AlignedVec::new(64);
    aligned.extend_from_slice(&bytes);
    Ok(JsArrayBuffer::from_byte_block(aligned, context)?.into())
}

// ── Algorithm parsing ─────────────────────────────────────────────────────────

/// Parse a hash algorithm from a string `"SHA-256"` or object `{name: "SHA-256"}`.
fn parse_hash(val: &JsValue, context: &mut Context) -> JsResult<HmacHash> {
    let name = if let Some(s) = val.as_string() {
        s.to_std_string_escaped()
    } else {
        let obj = val
            .as_object()
            .ok_or_else(|| JsNativeError::typ().with_message("hash must be a string or object"))?;
        let name_val = obj.get(js_string!("name"), context)?;
        name_val
            .as_string()
            .ok_or_else(|| JsNativeError::typ().with_message("hash.name must be a string"))?
            .to_std_string_escaped()
    };
    match name.as_str() {
        "SHA-256" => Ok(HmacHash::Sha256),
        "SHA-384" => Ok(HmacHash::Sha384),
        "SHA-512" => Ok(HmacHash::Sha512),
        _ => Err(JsNativeError::error()
            .with_message(format!(
                "NotSupportedError: unsupported hash algorithm '{name}'"
            ))
            .into()),
    }
}

/// Parse an algorithm argument for `importKey` / `sign` / `verify`.
///
/// Accepts:
/// - String `"HMAC"`
/// - Object `{ name: "HMAC", hash: "SHA-256" | { name: "SHA-256" } }`
///
/// Returns `(algorithm_name, optional_hash)`.
fn parse_algorithm(val: &JsValue, context: &mut Context) -> JsResult<(String, Option<HmacHash>)> {
    if let Some(s) = val.as_string() {
        return Ok((s.to_std_string_escaped(), None));
    }
    let obj = val
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("algorithm must be a string or object"))?;
    let name = obj
        .get(js_string!("name"), context)?
        .as_string()
        .ok_or_else(|| JsNativeError::typ().with_message("algorithm.name must be a string"))?
        .to_std_string_escaped();
    let hash = if obj.has_own_property(js_string!("hash"), context)? {
        let hash_val = obj.get(js_string!("hash"), context)?;
        Some(parse_hash(&hash_val, context)?)
    } else {
        None
    };
    Ok((name, hash))
}

// ── HMAC ──────────────────────────────────────────────────────────────────────

/// Compute an HMAC tag. Constant-time by construction (RustCrypto `hmac` crate).
fn hmac_sign(hash: &HmacHash, key: &[u8], data: &[u8]) -> Vec<u8> {
    macro_rules! sign {
        ($D:ty) => {{
            let mut mac = Hmac::<$D>::new_from_slice(key).expect("HMAC accepts any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }};
    }
    match hash {
        HmacHash::Sha256 => sign!(Sha256),
        HmacHash::Sha384 => sign!(Sha384),
        HmacHash::Sha512 => sign!(Sha512),
    }
}

/// Verify an HMAC tag in constant time.
fn hmac_verify(hash: &HmacHash, key: &[u8], data: &[u8], signature: &[u8]) -> bool {
    macro_rules! verify {
        ($D:ty) => {{
            let mut mac = Hmac::<$D>::new_from_slice(key).expect("HMAC accepts any key length");
            mac.update(data);
            mac.verify_slice(signature).is_ok()
        }};
    }
    match hash {
        HmacHash::Sha256 => verify!(Sha256),
        HmacHash::Sha384 => verify!(Sha384),
        HmacHash::Sha512 => verify!(Sha512),
    }
}

// ── CryptoKey construction ────────────────────────────────────────────────────

fn make_crypto_key(
    key_bytes: Vec<u8>,
    hash: HmacHash,
    extractable: bool,
    usages: Vec<String>,
    context: &mut Context,
) -> JsResult<JsObject> {
    let hash_name = hash.name();
    let data = CryptoKeyData {
        key_bytes,
        hash,
        extractable,
        usages: usages.clone(),
    };

    // Object backed by CryptoKeyData; prototype is plain Object.prototype.
    let obj = JsObject::from_proto_and_data(
        context.intrinsics().constructors().object().prototype(),
        data,
    );

    // Spec-required read-only enumerable properties.
    obj.set(js_string!("type"), js_string!("secret"), false, context)?;
    obj.set(
        js_string!("extractable"),
        JsValue::from(extractable),
        false,
        context,
    )?;

    // algorithm: { name: "HMAC", hash: { name: "SHA-256" } }
    let hash_obj = JsObject::default(context.intrinsics());
    hash_obj.set(js_string!("name"), js_string!(hash_name), false, context)?;
    let alg_obj = JsObject::default(context.intrinsics());
    alg_obj.set(js_string!("name"), js_string!("HMAC"), false, context)?;
    alg_obj.set(js_string!("hash"), hash_obj, false, context)?;
    obj.set(js_string!("algorithm"), alg_obj, false, context)?;

    // usages: string[]
    use boa_engine::object::builtins::JsArray;
    let usages_arr = JsArray::new(context)?;
    for (i, u) in usages.iter().enumerate() {
        usages_arr.set(i, js_string!(u.as_str()), false, context)?;
    }
    obj.set(js_string!("usages"), usages_arr, false, context)?;

    Ok(obj)
}

// ── SubtleCrypto methods ──────────────────────────────────────────────────────

/// `subtle.importKey(format, keyData, algorithm, extractable, keyUsages) → Promise<CryptoKey>`
///
/// Spec: <https://www.w3.org/TR/WebCryptoAPI/#SubtleCrypto-method-importKey>
///
/// Only `"raw"` format and `"HMAC"` algorithm are supported.
fn import_key(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let format = args
        .get_or_undefined(0)
        .as_string()
        .ok_or_else(|| JsNativeError::typ().with_message("importKey: format must be a string"))?
        .to_std_string_escaped();

    if format != "raw" {
        return Err(JsNativeError::error()
            .with_message(format!(
                "NotSupportedError: key format '{format}' is not supported"
            ))
            .into());
    }

    let key_data = buffer_source_to_bytes(args.get_or_undefined(1), context)?;
    let algorithm_val = args.get_or_undefined(2);
    let extractable = args.get_or_undefined(3).to_boolean();

    // Parse key usages array.
    let usages_val = args.get_or_undefined(4);
    let usages_obj = usages_val.as_object().ok_or_else(|| {
        JsNativeError::typ().with_message("importKey: keyUsages must be an array")
    })?;
    let usages_len = usages_obj
        .get(js_string!("length"), context)?
        .to_length(context)?;
    let mut usages = Vec::with_capacity(usages_len as usize);
    for i in 0..usages_len {
        let usage = usages_obj
            .get(i, context)?
            .as_string()
            .ok_or_else(|| {
                JsNativeError::typ().with_message("importKey: keyUsage must be a string")
            })?
            .to_std_string_escaped();
        usages.push(usage);
    }

    // Only HMAC is supported.
    let (alg_name, hash_opt) = parse_algorithm(algorithm_val, context)?;
    if alg_name.to_uppercase() != "HMAC" {
        return Err(JsNativeError::error()
            .with_message(format!(
                "NotSupportedError: algorithm '{alg_name}' is not supported"
            ))
            .into());
    }
    let hash = hash_opt.ok_or_else(|| {
        JsNativeError::error().with_message("DataError: HMAC importKey requires a hash algorithm")
    })?;

    let key_obj = make_crypto_key(key_data, hash, extractable, usages, context)?;
    let promise = JsPromise::resolve(key_obj, context)?;
    Ok(promise.into())
}

/// `subtle.sign(algorithm, key, data) → Promise<ArrayBuffer>`
///
/// Spec: <https://www.w3.org/TR/WebCryptoAPI/#SubtleCrypto-method-sign>
fn sign(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let algorithm_val = args.get_or_undefined(0);
    let (alg_name, _) = parse_algorithm(algorithm_val, context)?;
    if alg_name.to_uppercase() != "HMAC" {
        return Err(JsNativeError::error()
            .with_message(format!(
                "NotSupportedError: algorithm '{alg_name}' is not supported"
            ))
            .into());
    }

    let key_obj = args
        .get_or_undefined(1)
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("sign: key must be a CryptoKey"))?;
    let key_data = key_obj
        .downcast_ref::<CryptoKeyData>()
        .ok_or_else(|| JsNativeError::typ().with_message("sign: key is not a valid CryptoKey"))?;
    if !key_data.usages.iter().any(|u| u == "sign") {
        return Err(JsNativeError::error()
            .with_message("InvalidAccessError: key does not have the 'sign' usage")
            .into());
    }

    let data = buffer_source_to_bytes(args.get_or_undefined(2), context)?;
    let sig = hmac_sign(&key_data.hash, &key_data.key_bytes, &data);

    let ab = bytes_to_array_buffer(sig, context)?;
    let promise = JsPromise::resolve(ab, context)?;
    Ok(promise.into())
}

/// `subtle.verify(algorithm, key, signature, data) → Promise<boolean>`
///
/// Spec: <https://www.w3.org/TR/WebCryptoAPI/#SubtleCrypto-method-verify>
fn verify(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let algorithm_val = args.get_or_undefined(0);
    let (alg_name, _) = parse_algorithm(algorithm_val, context)?;
    if alg_name.to_uppercase() != "HMAC" {
        return Err(JsNativeError::error()
            .with_message(format!(
                "NotSupportedError: algorithm '{alg_name}' is not supported"
            ))
            .into());
    }

    let key_obj = args
        .get_or_undefined(1)
        .as_object()
        .ok_or_else(|| JsNativeError::typ().with_message("verify: key must be a CryptoKey"))?;
    let key_data = key_obj
        .downcast_ref::<CryptoKeyData>()
        .ok_or_else(|| JsNativeError::typ().with_message("verify: key is not a valid CryptoKey"))?;
    if !key_data.usages.iter().any(|u| u == "verify") {
        return Err(JsNativeError::error()
            .with_message("InvalidAccessError: key does not have the 'verify' usage")
            .into());
    }

    let signature = buffer_source_to_bytes(args.get_or_undefined(2), context)?;
    let data = buffer_source_to_bytes(args.get_or_undefined(3), context)?;
    let result = hmac_verify(&key_data.hash, &key_data.key_bytes, &data, &signature);

    let promise = JsPromise::resolve(JsValue::from(result), context)?;
    Ok(promise.into())
}

// ── Registration ──────────────────────────────────────────────────────────────

/// Register the `crypto` global object on `context`.
///
/// After this call, JS code can use:
/// ```js
/// const key = await crypto.subtle.importKey("raw", keyBytes,
///     { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"]);
/// const sig = await crypto.subtle.sign({ name: "HMAC" }, key, data);
/// const ok  = await crypto.subtle.verify({ name: "HMAC" }, key, sig, data);
/// ```
pub fn setup_crypto(context: &mut Context) -> JsResult<()> {
    use crate::helpers::new_object;

    let subtle = new_object(context);
    subtle.set(
        js_string!("importKey"),
        NativeFunction::from_fn_ptr(import_key).to_js_function(context.realm()),
        false,
        context,
    )?;
    subtle.set(
        js_string!("sign"),
        NativeFunction::from_fn_ptr(sign).to_js_function(context.realm()),
        false,
        context,
    )?;
    subtle.set(
        js_string!("verify"),
        NativeFunction::from_fn_ptr(verify).to_js_function(context.realm()),
        false,
        context,
    )?;

    let crypto = new_object(context);
    crypto.set(js_string!("subtle"), subtle, false, context)?;

    context.register_global_property(js_string!("crypto"), crypto, Attribute::all())?;
    Ok(())
}
