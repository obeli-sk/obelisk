use crate::v8_panic::V8PanicState;
use deno_core::{OpState, op2};
use deno_error::JsErrorBox;
use js_crypto::HmacHash;
use rand::RngCore as _;

fn catch<T>(state: &mut OpState, f: impl FnOnce() -> Result<T, String>) -> Result<T, JsErrorBox> {
    let panic = state.borrow::<V8PanicState>().clone();
    panic.catch(|| f().map_err(JsErrorBox::generic))
}

#[op2]
fn op_crypto_random(state: &mut OpState, #[anybuffer] buf: &mut [u8]) -> Result<(), JsErrorBox> {
    catch(state, || {
        let bytes = js_crypto::random_values(buf.len(), |bytes| rand::rng().fill_bytes(bytes))?;
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

#[op2]
#[string]
fn op_crypto_hmac_hash(state: &mut OpState, #[string] hash: &str) -> Result<String, JsErrorBox> {
    catch(state, || {
        HmacHash::parse(hash).map(|hash| hash.name().to_owned())
    })
}

#[op2]
#[buffer]
fn op_crypto_hmac_sign(
    state: &mut OpState,
    #[string] hash: &str,
    #[anybuffer] key: &[u8],
    #[anybuffer] data: &[u8],
) -> Result<Vec<u8>, JsErrorBox> {
    catch(state, || Ok(HmacHash::parse(hash)?.sign(key, data)))
}

#[op2]
fn op_crypto_hmac_verify(
    state: &mut OpState,
    #[string] hash: &str,
    #[anybuffer] key: &[u8],
    #[anybuffer] signature: &[u8],
    #[anybuffer] data: &[u8],
) -> Result<bool, JsErrorBox> {
    catch(state, || {
        Ok(HmacHash::parse(hash)?.verify(key, data, signature))
    })
}

// Ops expect a `V8PanicState` in the `OpState`.
deno_core::extension!(
    obelisk_crypto_v8,
    ops = [
        op_crypto_random,
        op_crypto_hmac_hash,
        op_crypto_hmac_sign,
        op_crypto_hmac_verify
    ]
);

pub(crate) const CRYPTO_BOOTSTRAP: &str = r"(() => {
const ops = Deno.core.ops;
const keyBytes = new WeakMap();
const toBytes = data => ArrayBuffer.isView(data) ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength) : new Uint8Array(data);
const hmacKey = (algorithm, key, usage) => {
  if (String(typeof algorithm === 'string' ? algorithm : algorithm?.name).toUpperCase() !== 'HMAC') throw new Error('NotSupportedError: only HMAC is supported');
  const bytes = keyBytes.get(key);
  if (!bytes) throw new TypeError(`${usage}: key is not a valid CryptoKey`);
  if (!key.usages.includes(usage)) throw new Error(`InvalidAccessError: key does not have the '${usage}' usage`);
  return bytes;
};
const integerArrays = [Int8Array, Uint8Array, Uint8ClampedArray, Int16Array, Uint16Array, Int32Array, Uint32Array, BigInt64Array, BigUint64Array];
globalThis.crypto = {
  getRandomValues(array) {
    if (!ArrayBuffer.isView(array) || array instanceof DataView) throw new TypeError('getRandomValues: argument must be a TypedArray');
    if (!integerArrays.some(type => array instanceof type)) throw new Error('TypeMismatchError: getRandomValues requires an integer TypedArray');
    ops.op_crypto_random(array);
    return array;
  },
  subtle: {
    async importKey(format, keyData, algorithm, extractable, usages) {
      if (format !== 'raw') throw new Error(`NotSupportedError: key format '${format}' is not supported`);
      if (String(algorithm?.name).toUpperCase() !== 'HMAC') throw new Error(`NotSupportedError: algorithm '${algorithm?.name}' is not supported`);
      if (algorithm.hash === undefined) throw new Error('DataError: HMAC importKey requires a hash algorithm');
      const hash = ops.op_crypto_hmac_hash(typeof algorithm.hash === 'string' ? algorithm.hash : algorithm.hash.name);
      const key = Object.freeze({ type: 'secret', extractable: Boolean(extractable), algorithm: Object.freeze({ name: 'HMAC', hash: Object.freeze({ name: hash }) }), usages: Object.freeze([...usages]) });
      keyBytes.set(key, toBytes(keyData).slice());
      return key;
    },
    async sign(algorithm, key, data) {
      const bytes = hmacKey(algorithm, key, 'sign');
      return ops.op_crypto_hmac_sign(key.algorithm.hash.name, bytes, toBytes(data)).buffer;
    },
    async verify(algorithm, key, signature, data) {
      const bytes = hmacKey(algorithm, key, 'verify');
      return ops.op_crypto_hmac_verify(key.algorithm.hash.name, bytes, toBytes(signature), toBytes(data));
    }
  }
};
})();";
