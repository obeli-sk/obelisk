use super::*;

const DEPLOYMENT: &str = r#"[[activity_js]]
name = "test_hmac_sign_verify_activity"
ffqn = "testing:integration/activity-hmac.hmac-sign-verify"
content = '''
// Test fixture: HMAC-SHA256 sign using crypto.subtle.
// Accepts a key string and a message string, signs the message, and returns
// the signature as a hex string for the caller to verify.
export default async function hmac_sign_verify(keyStr, message) {
    const enc = new TextEncoder();
    const key = await crypto.subtle.importKey(
        'raw',
        enc.encode(keyStr),
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign'],
    );
    const sig = await crypto.subtle.sign('HMAC', key, enc.encode(message));
    return [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
}
'''
params = [
  { name = "key", type = "string" },
  { name = "message", type = "string" },
]
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

    // The JS activity returns the HMAC-SHA256 signature as a hex string.
    let js_hex = body["ok"].as_str().expect("expected ok string");

    // Compute the expected HMAC-SHA256 on the Rust side and compare.
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    mac.update(MSG.as_bytes());
    let mut expected = String::with_capacity(64);
    for b in mac.finalize().into_bytes() {
        write!(expected, "{b:02x}").unwrap();
    }

    assert_eq!(
        js_hex,
        expected,
        "JS HMAC-SHA256 signature must match Rust using {}",
        runtime.name()
    );
    server.shutdown().await;
}
