use wstd::http::{Body, Client, Error, HeaderValue, Request, Response, Uri};

#[wstd::http_server]
async fn main(server_req: Request<Body>) -> Result<Response<Body>, Error> {
    match server_req.uri().path_and_query().unwrap().as_str() {
        "/webui_bg.wasm" => {
            let content = get_webui_bg_wasm();
            let content_type = "application/wasm";
            write_static_response(content, content_type)
        }
        "/webui.js" => {
            let content = get_webui_js();
            let content_type = "text/javascript";
            write_static_response(content, content_type)
        }
        "/blueprint.css" => {
            let content = get_blueprint_css();
            let content_type = "text/css";
            write_static_response(content, content_type)
        }
        "/styles.css" => {
            let content = get_styles_css();
            let content_type = "text/css";
            write_static_response(content, content_type)
        }
        "/syntect.css" => {
            let content = get_syntect_css();
            let content_type = "text/css";
            write_static_response(content, content_type)
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
            proxy(server_req, target_url).await
        }
        _ => {
            let content = get_index();
            let content_type = "text/html";
            write_static_response(content, content_type)
        }
    }
}

async fn proxy(server_req: Request<Body>, target_url: Uri) -> Result<Response<Body>, Error> {
    let client = Client::new();
    let mut client_req = Request::builder();
    client_req = client_req.uri(target_url).method(server_req.method());

    // Copy headers from `server_req` to the `client_req`.
    for (key, value) in server_req.headers() {
        client_req = client_req.header(key, value);
    }

    // Stream the request body.
    let client_body = Body::from_http_body(server_req.into_body().into_boxed_body());
    let client_req = client_req.body(client_body)?;
    // Send the request.
    let client_resp = client.send(client_req).await?;
    // Copy headers from `client_resp` to `server_resp`.
    let mut server_resp = Response::builder();
    for (key, value) in client_resp.headers() {
        server_resp
            .headers_mut()
            .expect("no errors could be in ResponseBuilder")
            .append(key, value.clone());
    }
    let resp_body = Body::from_http_body(client_resp.into_body().into_boxed_body());
    Ok(server_resp.body(resp_body)?)
}

fn write_static_response(body: &[u8], content_type: &'static str) -> Result<Response<Body>, Error> {
    let mut resp = Response::builder();
    resp.headers_mut()
        .unwrap()
        .append("content-type", HeaderValue::from_static(content_type));
    resp.body(body.into()).map_err(Error::from)
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

#[cfg(not(debug_assertions))]
fn get_syntect_css() -> &'static [u8] {
    include_bytes!("../../webui/dist/syntect.css")
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

#[cfg(debug_assertions)]
fn get_syntect_css() -> &'static [u8] {
    unreachable!("embedding is skipped in debug mode")
}
