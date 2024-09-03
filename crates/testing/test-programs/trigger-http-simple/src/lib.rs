#[allow(warnings)]
mod bindings;

use bindings::wasi::http::types::{
    Fields, IncomingRequest, OutgoingBody, OutgoingResponse, ResponseOutparam,
};

use crate::bindings::exports::wasi::http::incoming_handler::Guest;

struct Component;

bindings::export!(Component with_types_in bindings);

impl Guest for Component {
    fn handle(request: IncomingRequest, outparam: ResponseOutparam) {
        let req_path = request.path_with_query().unwrap();
        let n: u8 = req_path
            .strip_prefix("/")
            .unwrap()
            .parse()
            .expect("should be a number");
        let resp_headers = Fields::new();
        let resp = OutgoingResponse::new(resp_headers);
        let body = resp.body().expect("outgoing response");
        ResponseOutparam::set(outparam, Ok(resp));
        let out = body.write().expect("outgoing stream");
        let fibo_res = bindings::testing::fibo_workflow::workflow::fiboa(n, 1);
        out.blocking_write_and_flush(format!("fibonacci({n}) = {fibo_res}").as_bytes())
            .expect("writing response");
        drop(out);
        OutgoingBody::finish(body, None).unwrap();
    }
}
