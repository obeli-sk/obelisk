use wstd::http::body::{BodyForthcoming, IncomingBody};
use wstd::http::server::{Finished, Responder};
use wstd::http::{Client, HeaderValue, Request, Response, Uri};
use wstd::io::{AsyncWrite, copy};

#[wstd::http_server]
async fn main(mut incoming_req: Request<IncomingBody>, incoming_responder: Responder) -> Finished {
    match incoming_req.uri().path_and_query().unwrap().as_str() {
        "/webui_bg.wasm" => {
            let content = get_webui_bg_wasm();
            let content_type = "application/wasm";
            write_static_response(content, content_type, incoming_responder).await
        }
        "/webui.js" => {
            let content = get_webui_js();
            let content_type = "text/javascript";
            write_static_response(content, content_type, incoming_responder).await
        }
        "/blueprint.css" => {
            let content = get_blueprint_css();
            let content_type = "text/css";
            write_static_response(content, content_type, incoming_responder).await
        }
        "/styles.css" => {
            let content = get_styles_css();
            let content_type = "text/css";
            write_static_response(content, content_type, incoming_responder).await
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
            let mut client_request = Request::builder();
            client_request = client_request.uri(target_url).method(incoming_req.method());

            // copy headers from incoming request to the client_request
            for (key, value) in incoming_req.headers() {
                client_request = client_request.header(key, value);
            }

            // Send the request.
            let client_request = client_request
                .body(BodyForthcoming)
                .expect("client_request.body failed");
            let (mut client_outgoing_body, client_response) = client
                .start_request(client_request)
                .await
                .expect("client.start_request failed");

            // Copy the incoming request body to client_outgoing_body
            let req_to_req = async {
                let res = copy(incoming_req.body_mut(), &mut client_outgoing_body).await;
                // TODO: Convert to io error if necessary
                let _ = Client::finish(client_outgoing_body, None);
                res
            };

            // Copy the client_response headers to incoming response
            let resp_to_resp = async {
                let client_response = client_response.await.unwrap();
                let mut incoming_response = Response::builder();
                for (key, value) in client_response.headers() {
                    incoming_response
                        .headers_mut()
                        .unwrap()
                        .append(key, value.clone());
                }
                // Start sending the incoming_response.
                let incoming_response = incoming_response.body(BodyForthcoming).unwrap();
                let mut incoming_response = incoming_responder.start_response(incoming_response);

                (
                    copy(client_response.into_body(), &mut incoming_response).await,
                    incoming_response,
                )
            };
            let (req_to_req, (resp_to_resp, incoming_response)) =
                futures::join!(req_to_req, resp_to_resp);
            let result = req_to_req.and(resp_to_resp);

            // join:
            Finished::finish(incoming_response, result, None)
        }
        _ => {
            let content = get_index();
            let content_type = "text/html";
            write_static_response(content, content_type, incoming_responder).await
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
