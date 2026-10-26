use wasip3::http::types::{ErrorCode, Fields, Request, Response};
use wasip3::wit_bindgen::StreamResult;
use wasip3::{wit_bindgen, wit_future, wit_stream};

struct Component;

impl wasip3::exports::http::handler::Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let echo = request.get_path_with_query().as_deref() == Some("/echo");
        let (mut body_tx, body_rx) = wit_stream::new();
        let (body_result_tx, body_result_rx) = wit_future::new(|| Ok(None));
        let (response, _response_result) =
            Response::new(Fields::new(), Some(body_rx), body_result_rx);
        drop(body_result_tx);
        if echo {
            // Echo each request chunk as soon as it arrives.
            let (request_result_tx, request_result_rx) = wit_future::new(|| Ok(()));
            let (mut request_body, _trailers) = Request::consume_body(request, request_result_rx);
            wit_bindgen::spawn_local(async move {
                let mut buf = Vec::with_capacity(1024);
                loop {
                    let (result, chunk) = request_body.read(buf).await;
                    if !matches!(result, StreamResult::Complete(_)) {
                        break;
                    }
                    let remaining = body_tx.write_all(chunk.clone()).await;
                    assert!(remaining.is_empty());
                    buf = chunk;
                    buf.clear();
                }
                drop(request_result_tx);
            });
        } else {
            wit_bindgen::spawn_local(async move {
                let remaining = body_tx.write_all(b"Hello from WASIp3".to_vec()).await;
                assert!(remaining.is_empty());
            });
        }
        Ok(response)
    }
}

wasip3::http::service::export!(Component);
