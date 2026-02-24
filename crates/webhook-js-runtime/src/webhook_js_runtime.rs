//! JavaScript webhook runtime using Boa JS engine.
//!
//! This is a WASI HTTP handler that runs JavaScript code to process HTTP requests.
//! The JS code is provided via the `JS_SOURCE` environment variable.
//!
//! The JS handler receives a request object and should return a response object:
//! ```js
//! function handle(request) {
//!     return {
//!         status: 200,
//!         headers: [["content-type", "text/plain"]],
//!         body: "Hello from JS!"
//!     };
//! }
//! ```

use crate::console;
use crate::generated::obelisk::log::log;
use boa_engine::{Context, Source};
use wstd::http::body::Body;
use wstd::http::{Request, Response, StatusCode};

const FN_NAME: &str = "handle";

#[wstd::http_server]
async fn main(request: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    // Get JS source from environment
    let js_source = match std::env::var("__OBELISK_JS_SOURCE__") {
        Ok(source) => source,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(
                    "__OBELISK_JS_SOURCE__ environment variable not set",
                ))?);
        }
    };

    // Convert request to JSON for JS
    let request_json = request_to_json(&request);

    // Run JS and get response
    match run_js_handler(&js_source, FN_NAME, &request_json) {
        Ok(response_json) => json_to_response(&response_json),
        Err(err) => {
            log::error(&format!("JS error: {err}"));
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("JS error: {err}")))?)
        }
    }
}

/// Convert HTTP request to JSON string for JS consumption.
fn request_to_json(request: &Request<Body>) -> String {
    let method = request.method().as_str();
    let uri = request.uri().to_string();

    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    // For now, we don't read the body to keep it simple
    // TODO: Add body reading support
    let body = "";

    serde_json::json!({
        "method": method,
        "url": uri,
        "headers": headers,
        "body": body,
    })
    .to_string()
}

/// Run the JS handler function and return the response JSON.
fn run_js_handler(js_source: &str, fn_name: &str, request_json: &str) -> Result<String, String> {
    let mut context = Context::default();

    // Set up console.log -> obelisk:log
    console::setup_console(&mut context).expect("console setup must work");

    // Parse the request JSON and make it available
    let request_js = format!("const __request__ = {request_json};");
    context
        .eval(Source::from_bytes(&request_js))
        .map_err(|e| format!("Failed to set request: {e}"))?;

    // Execute the user's JS code
    context
        .eval(Source::from_bytes(js_source))
        .map_err(|e| format!("Failed to execute JS: {e}"))?;

    // Call the handler function
    let call_code = format!("JSON.stringify({fn_name}(__request__))");
    let result = context
        .eval(Source::from_bytes(&call_code))
        .map_err(|e| format!("Failed to call {fn_name}: {e}"))?;

    // Extract the JSON string result
    result
        .as_string()
        .map(|s| s.to_std_string_escaped())
        .ok_or_else(|| "Handler did not return a JSON-serializable object".to_string())
}

/// Convert JSON response from JS to HTTP Response.
fn json_to_response(json: &str) -> Result<Response<Body>, wstd::http::Error> {
    #[derive(serde::Deserialize)]
    struct ResponseJson {
        status: u16,
        #[serde(default)]
        headers: Vec<(String, String)>,
        #[serde(default)]
        body: String,
    }

    match serde_json::from_str::<ResponseJson>(json) {
        Ok(r) => {
            let mut builder = Response::builder()
                .status(StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK));
            for (k, v) in r.headers {
                builder = builder.header(k, v);
            }
            builder
                .body(Body::from(r.body))
                .map_err(wstd::http::Error::new)
        }
        Err(err) => {
            log::error(&format!("Failed to parse response JSON: {err}"));
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("Invalid response format: {err}")))
                .map_err(wstd::http::Error::new)
        }
    }
}
