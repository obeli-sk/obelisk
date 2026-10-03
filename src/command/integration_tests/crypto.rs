use super::*;

const DEPLOYMENT: &str = r#"[[activity_js]]
name = "test_hmac_sign_verify_activity"
ffqn = "testing:integration/activity-hmac.hmac-sign-verify"
content = '''
// Test fixture: HMAC sign and verify using crypto.subtle.
// Returns the SHA-256 and SHA-512 signatures as hex, separated by ':'.
export default async function hmac_sign_verify(keyStr, message) {
    const enc = new TextEncoder();
    const signatures = [];
    for (const hash of ['SHA-256', 'SHA-512']) {
        const key = await crypto.subtle.importKey(
            'raw',
            enc.encode(keyStr),
            { name: 'HMAC', hash },
            false,
            ['sign', 'verify'],
        );
        const sig = await crypto.subtle.sign('HMAC', key, enc.encode(message));
        if (!await crypto.subtle.verify('HMAC', key, sig, enc.encode(message))) throw new Error(`${hash}: valid signature rejected`);
        const tampered = new Uint8Array(sig).slice();
        tampered[0] ^= 1;
        if (await crypto.subtle.verify({ name: 'HMAC' }, key, tampered, enc.encode(message))) throw new Error(`${hash}: tampered signature accepted`);
        signatures.push([...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join(''));
    }
    return signatures.join(':');
}
'''
params = [
  { name = "key", type = "string" },
  { name = "message", type = "string" },
]
return_type = "result<string, string>"

[[activity_js]]
name = "test_get_random_values_activity"
ffqn = "testing:integration/activity-random.get-random-values"
content = '''
export default function get_random_values() {
    const errorName = fn => { try { fn(); return 'none'; } catch (e) { return e.message.split(':')[0]; } };
    const bytes = new Uint8Array(64);
    if (crypto.getRandomValues(bytes) !== bytes) throw new Error('must return the same array');
    if (bytes.every(b => b === 0)) throw new Error('bytes were not filled');
    const view = new Uint8Array(new ArrayBuffer(8), 2, 4);
    crypto.getRandomValues(view);
    if (new Uint8Array(view.buffer, 0, 2).some(b => b !== 0) || new Uint8Array(view.buffer, 6).some(b => b !== 0)) throw new Error('wrote outside the view');
    crypto.getRandomValues(new Uint32Array(16384));
    return [
        errorName(() => crypto.getRandomValues(new Float64Array(1))),
        errorName(() => crypto.getRandomValues(new Uint8Array(65537))),
    ].join(',');
}
'''
params = []
return_type = "result<string, string>"
"#;

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn hmac_sign_verify(#[case] runtime: JsRuntime) {
    const KEY: &str = "super-secret-key";
    const MSG: &str = "hello world";

    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(34),
        JsRuntime::V8 => test_addr!(123),
    };
    let server = TestServer::start_inline_deployment_with_js_runtime(
        ip,
        "",
        DEPLOYMENT,
        &[],
        runtime.mode(),
    )
    .await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-hmac.hmac-sign-verify",
            vec![json!(KEY), json!(MSG)],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();

    let js_hex = body["ok"].as_str().expect("expected ok string");

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut acc, b| {
            write!(acc, "{b:02x}").unwrap();
            acc
        })
    }
    let mut sha256 = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    sha256.update(MSG.as_bytes());
    let mut sha512 = Hmac::<Sha512>::new_from_slice(KEY.as_bytes()).unwrap();
    sha512.update(MSG.as_bytes());
    let expected = format!(
        "{}:{}",
        hex(&sha256.finalize().into_bytes()),
        hex(&sha512.finalize().into_bytes())
    );

    assert_eq!(
        js_hex,
        expected,
        "JS HMAC signatures must match Rust using {}",
        runtime.name()
    );
    server.shutdown().await;
}

#[rstest::rstest]
#[case::boa_wasm(JsRuntime::BoaWasm)]
#[case::v8(JsRuntime::V8)]
#[tokio::test]
async fn get_random_values(#[case] runtime: JsRuntime) {
    let ip = match runtime {
        JsRuntime::BoaWasm => test_addr!(189),
        JsRuntime::V8 => test_addr!(190),
    };
    let server = TestServer::start_inline_deployment_with_js_runtime(
        ip,
        "",
        DEPLOYMENT,
        &[],
        runtime.mode(),
    )
    .await;
    let resp = server
        .submit_follow(
            "testing:integration/activity-random.get-random-values",
            vec![],
        )
        .await;
    assert_eq!(resp.status().as_u16(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["ok"].as_str(),
        Some("TypeMismatchError,QuotaExceededError"),
        "{body} using {}",
        runtime.name()
    );
    server.shutdown().await;
}
