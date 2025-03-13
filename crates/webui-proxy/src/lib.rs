use futures_concurrency::prelude::*;
use wstd::http::body::{BodyForthcoming, IncomingBody};
use wstd::http::server::{Finished, Responder};
use wstd::http::{Client, HeaderValue, Request, Response, Uri};
use wstd::io::{copy, AsyncWrite};

#[wstd::http_server]
async fn main(mut server_req: Request<IncomingBody>, responder: Responder) -> Finished {
    match server_req.uri().path_and_query().unwrap().as_str() {
        "/webui_bg.wasm" => {
            let content = get_webui_bg_wasm();
            let content_type = "application/wasm";
            write_static_response(content, content_type, responder).await
        }
        "/webui.js" => {
            let content = get_webui_js();
            let content_type = "text/javascript";
            write_static_response(content, content_type, responder).await
        }
        "/blueprint.css" => {
            let content = get_blueprint_css();
            let content_type = "text/css";
            write_static_response(content, content_type, responder).await
        }
        "/styles.css" => {
            let content = get_styles_css();
            let content_type = "text/css";
            write_static_response(content, content_type, responder).await
        }
        api_prefixed_path if api_prefixed_path.starts_with("/api") => {
            // Remove /api prefix
            let target_url =
                std::env::var("TARGET_URL").expect("missing environment variable TARGET_URL");
            let target_url: Uri = format!(
                "{target_url}{}",
                api_prefixed_path
                    .strip_prefix("/api")
                    .expect("checked above")
            )
            .parse()
            .expect("final target url should be parseable");

            let client = Client::new();
            let mut client_req = Request::builder();
            client_req = client_req.uri(target_url).method(server_req.method());

            // copy headers from incoming request to the client_request
            for (key, value) in server_req.headers() {
                client_req = client_req.header(key, value);
            }

            // Send the request.
            let client_req = client_req
                .body(BodyForthcoming)
                .expect("client_req.body failed");
            let (mut client_request_body, client_resp) = client
                .start_request(client_req)
                .await
                .expect("client.start_request failed");

            // Copy the outgoing request body to client_outgoing_body
            let server_req_to_client_req = async {
                let res = copy(server_req.body_mut(), &mut client_request_body).await;
                Client::finish(client_request_body, None)
                    .map_err(|_http_err| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Failed write the HTTP request body",
                        )
                    })
                    .and(res)
            };

            // Copy the client_response headers to incoming response
            let client_resp_to_server_resp = async {
                let client_resp = client_resp.await.unwrap();
                let mut server_resp = Response::builder();
                for (key, value) in client_resp.headers() {
                    server_resp
                        .headers_mut()
                        .unwrap()
                        .append(key, value.clone());
                }
                // Start sending the incoming_response.
                let server_resp = server_resp.body(BodyForthcoming).unwrap();
                let mut server_resp = responder.start_response(server_resp);

                (
                    copy(client_resp.into_body(), &mut server_resp).await,
                    server_resp,
                )
            };

            let (server_req_to_client_req, (client_resp_to_server_resp, server_resp)) =
                (server_req_to_client_req, client_resp_to_server_resp).join().await;
            let is_success = server_req_to_client_req.and(client_resp_to_server_resp);

            Finished::finish(server_resp, is_success, None)
        }
        _ => {
            let content = get_index();
            let content_type = "text/html";
            write_static_response(content, content_type, responder).await
        }
    }
}

async fn write_static_response(
    body: &[u8],
    content_type: &'static str,
    responder: Responder,
) -> Finished {
    let mut resp = Response::builder();
    resp.headers_mut()
        .unwrap()
        .append("content-type", HeaderValue::from_static(content_type));
    let resp = resp.body(BodyForthcoming).unwrap();
    let mut out_body = responder.start_response(resp);
    let result = out_body.write_all(body).await;
    Finished::finish(out_body, result, None)
}

// release: Include real files
#[cfg(not(debug_assertions))]
fn get_index() -> &'static [u8] {
    include_bytes!("../../webui/dist/index.html")
}

#[cfg(not(debug_assertions))]
fn get_webui_bg_wasm() -> &'static [u8] {
    include_bytes!("../../webui/dist/webui_bg.wasm")
}

#[cfg(not(debug_assertions))]
fn get_webui_js() -> &'static [u8] {
    include_bytes!("../../webui/dist/webui.js")
}

#[cfg(not(debug_assertions))]
fn get_blueprint_css() -> &'static [u8] {
    include_bytes!("../../webui/dist/blueprint.css")
}

#[cfg(not(debug_assertions))]
fn get_styles_css() -> &'static [u8] {
    include_bytes!("../../webui/dist/styles.css")
}

// debug: Include dummy file content
#[cfg(debug_assertions)]
fn get_index() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}

#[cfg(debug_assertions)]
fn get_webui_bg_wasm() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}

#[cfg(debug_assertions)]
fn get_webui_js() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}

#[cfg(debug_assertions)]
fn get_blueprint_css() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}

#[cfg(debug_assertions)]
fn get_styles_css() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}
