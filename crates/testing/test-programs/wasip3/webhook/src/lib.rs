use wasip3::http::types::{ErrorCode, Fields, Request, Response};
use wasip3::{wit_bindgen, wit_future, wit_stream};

struct Component;

impl wasip3::exports::http::handler::Guest for Component {
    async fn handle(_request: Request) -> Result<Response, ErrorCode> {
        let (mut body_tx, body_rx) = wit_stream::new();
        let (body_result_tx, body_result_rx) = wit_future::new(|| Ok(None));
        let (response, _response_result) =
            Response::new(Fields::new(), Some(body_rx), body_result_rx);
        drop(body_result_tx);
        wit_bindgen::spawn_local(async move {
            let remaining = body_tx.write_all(b"Hello from WASIp3".to_vec()).await;
            assert!(remaining.is_empty());
        });
        Ok(response)
    }
}

wasip3::http::service::export!(Component);
