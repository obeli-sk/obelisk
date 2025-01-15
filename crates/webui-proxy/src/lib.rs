use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

use anyhow::anyhow;
use exports::wasi::http::incoming_handler::Guest;
use futures::{SinkExt as _, StreamExt as _};
use std::future::Future;
use url::Url;
use wasi::http::types::{
    ErrorCode, Fields, IncomingRequest, IncomingResponse, OutgoingBody, OutgoingRequest,
    OutgoingResponse, ResponseOutparam, Scheme,
};

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

impl Guest for Component {
    fn handle(incoming_request: IncomingRequest, response_outparam: ResponseOutparam) {
        match incoming_request.path_with_query().as_deref() {
            Some("/webui_bg.wasm") => {
                let content = get_webui_bg_wasm();
                let content_type = "application/wasm";
                write_static_response(content, content_type, response_outparam);
            }
            Some("/webui.js") => {
                let content = get_webui_js();
                let content_type = "text/javascript";
                write_static_response(content, content_type, response_outparam);
            }
            Some("/blueprint.css") => {
                let content = get_blueprint_css();
                let content_type = "text/css";
                write_static_response(content, content_type, response_outparam);
            }
            Some("/styles.css") => {
                let content = get_styles_css();
                let content_type = "text/css";
                write_static_response(content, content_type, response_outparam);
            }
            Some(api_prefixed_path) if api_prefixed_path.starts_with("/api") => {
                // Remove /api prefix
                let target_url =
                    std::env::var("TARGET_URL").expect("missing environment variable TARGET_URL");
                let target_url = format!(
                    "{target_url}{}",
                    api_prefixed_path
                        .strip_prefix("/api")
                        .expect("checked above")
                );
                let target_url = Url::parse(&target_url).unwrap(); // gRPC server
                executor::run(async move {
                    match start_outgoing_request(&incoming_request, &target_url) {
                        Ok((request_copy, incoming_response)) => {
                            let response_copy = async move {
                                let incoming_response = incoming_response.await.unwrap(); // Blocks if more outgoing request body chunks are needed.

                                // In order to create `outgoing_response`, we need the `incoming_response` headers.
                                let outgoing_response = OutgoingResponse::new(
                                    Fields::from_list(&incoming_response.headers().entries())
                                        .unwrap(),
                                );

                                let mut incoming_response_stream = executor::incoming_body(
                                    incoming_response
                                        .consume()
                                        .expect("response should be consumable"),
                                );

                                let mut outgoing_body = executor::outgoing_body(
                                    outgoing_response
                                        .body()
                                        .expect("response should be writable"),
                                );

                                ResponseOutparam::set(response_outparam, Ok(outgoing_response));

                                while let Some(chunk) = incoming_response_stream.next().await {
                                    outgoing_body.send(chunk?).await?;
                                }
                                Ok::<_, anyhow::Error>(())
                            };

                            let (request_copy, response_copy) =
                                futures::future::join(request_copy, response_copy).await;
                            if let Err(e) = request_copy.and(response_copy) {
                                eprintln!("error piping to and from {target_url}: {e}");
                            }
                        }

                        Err(e) => {
                            eprintln!("Error sending outgoing request to {target_url}: {e}");
                            server_error(response_outparam);
                        }
                    }
                });
            }
            _ => {
                let content = get_index();
                let content_type = "text/html";
                write_static_response(content, content_type, response_outparam);
            }
        }
    }
}

fn server_error(response_out: ResponseOutparam) {
    respond(500, response_out);
}

fn respond(status: u16, response_out: ResponseOutparam) {
    let response = OutgoingResponse::new(Fields::new());
    response
        .set_status_code(status)
        .expect("setting status code");
    let body = response.body().expect("response should be writable");
    ResponseOutparam::set(response_out, Ok(response));
    OutgoingBody::finish(body, None).expect("outgoing-body.finish");
}

fn start_outgoing_request(
    incoming_request: &IncomingRequest,
    url: &Url,
) -> anyhow::Result<(
    impl Future<Output = anyhow::Result<()>>,
    impl Future<Output = Result<IncomingResponse, ErrorCode>>,
)> {
    let headers = incoming_request.headers().entries();
    let outgoing_request = OutgoingRequest::new(Fields::from_list(&headers).unwrap());
    outgoing_request
        .set_method(&incoming_request.method())
        .map_err(|()| anyhow!("failed to set method"))?;

    outgoing_request
        .set_path_with_query(Some(url.path()))
        .map_err(|()| anyhow!("failed to set path_with_query"))?;

    outgoing_request
        .set_scheme(Some(&match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            scheme => Scheme::Other(scheme.into()),
        }))
        .map_err(|()| anyhow!("failed to set scheme"))?;

    outgoing_request
        .set_authority(Some(&format!(
            "{}{}",
            url.host_str().unwrap_or(""),
            if let Some(port) = url.port() {
                format!(":{port}")
            } else {
                String::new()
            }
        )))
        .map_err(|()| anyhow!("failed to set authority"))?;

    let mut body = executor::outgoing_body(
        outgoing_request
            .body()
            .expect("request body should be writable"),
    );

    let incoming_response_fut = executor::outgoing_request_send(outgoing_request);

    let mut stream = executor::incoming_body(
        incoming_request
            .consume()
            .expect("request should be consumable"),
    );

    let copy = async move {
        while let Some(chunk) = stream.next().await {
            body.send(chunk?).await?;
        }
        body.flush().await.unwrap();
        Ok::<_, anyhow::Error>(())
    };

    Ok((copy, incoming_response_fut))
}

fn write_static_response(mut buf: &[u8], content_type: &str, outparam: ResponseOutparam) {
    let content_type = content_type.as_bytes().to_vec();
    let resp_headers = Fields::new();
    resp_headers.set("content-type", &[content_type]).unwrap();
    let resp = OutgoingResponse::new(resp_headers);
    let body = resp.body().expect("outgoing response");
    // Start sending the 200 OK response
    ResponseOutparam::set(outparam, Ok(resp));
    let out = body.write().expect("outgoing stream");
    let pollable = out.subscribe();
    while !buf.is_empty() {
        pollable.block();
        let permit = out.check_write().unwrap();
        let len = buf.len().min(usize::try_from(permit).unwrap());
        let (chunk, remainding) = buf.split_at(len);
        buf = remainding;
        out.write(chunk).unwrap();
    }
    out.flush().unwrap();
    drop(pollable);
    drop(out);
    OutgoingBody::finish(body, None).unwrap();
}

mod executor {
    use anyhow::{anyhow, Error, Result};
    use futures::{future, sink, stream, Sink, Stream};
    use std::{
        cell::RefCell,
        future::Future,
        mem,
        rc::Rc,
        sync::{Arc, Mutex},
        task::{Context, Poll, Wake, Waker},
    };

    use crate::wasi::{
        http::{
            outgoing_handler,
            types::{
                self, IncomingBody, IncomingResponse, InputStream, OutgoingBody, OutgoingRequest,
                OutputStream,
            },
        },
        io::{self, streams::StreamError},
    };

    const READ_SIZE: u64 = 16 * 1024;

    static WAKERS: Mutex<Vec<(io::poll::Pollable, Waker)>> = Mutex::new(Vec::new());

    pub fn run<T>(future: impl Future<Output = T>) -> T {
        struct DummyWaker;
        impl Wake for DummyWaker {
            fn wake(self: Arc<Self>) {}
        }

        futures::pin_mut!(future);

        let waker = Arc::new(DummyWaker).into();

        loop {
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => {
                    let mut new_wakers = Vec::new();

                    let wakers = mem::take::<Vec<_>>(&mut WAKERS.lock().unwrap());

                    assert!(!wakers.is_empty());

                    let pollables = wakers
                        .iter()
                        .map(|(pollable, _)| pollable)
                        .collect::<Vec<_>>();

                    let mut ready = vec![false; wakers.len()];

                    for index in io::poll::poll(&pollables) {
                        ready[usize::try_from(index).unwrap()] = true;
                    }

                    for (ready, (pollable, waker)) in ready.into_iter().zip(wakers) {
                        if ready {
                            waker.wake();
                        } else {
                            new_wakers.push((pollable, waker));
                        }
                    }

                    *WAKERS.lock().unwrap() = new_wakers;
                }
                Poll::Ready(result) => break result,
            }
        }
    }

    pub fn outgoing_body(body: OutgoingBody) -> impl Sink<Vec<u8>, Error = Error> {
        struct Outgoing(Option<(OutputStream, OutgoingBody)>);

        impl Drop for Outgoing {
            fn drop(&mut self) {
                if let Some((stream, body)) = self.0.take() {
                    drop(stream);
                    OutgoingBody::finish(body, None).expect("outgoing-body.finish");
                }
            }
        }

        let stream = body.write().expect("response body should be writable");
        let pair = Rc::new(RefCell::new(Outgoing(Some((stream, body)))));

        sink::unfold((), {
            move |(), chunk: Vec<u8>| {
                future::poll_fn({
                    let mut offset = 0;
                    let mut flushing = false;
                    let pair = pair.clone();

                    move |context| {
                        let pair = pair.borrow();
                        let (stream, _) = &pair.0.as_ref().unwrap();

                        loop {
                            match stream.check_write() {
                                Ok(0) => {
                                    WAKERS
                                        .lock()
                                        .unwrap()
                                        .push((stream.subscribe(), context.waker().clone()));

                                    break Poll::Pending;
                                }
                                Ok(count) => {
                                    if offset == chunk.len() {
                                        if flushing {
                                            break Poll::Ready(Ok(()));
                                        }
                                        stream.flush().expect("stream should be flushable");
                                        flushing = true;
                                    } else {
                                        let count = usize::try_from(count)
                                            .unwrap()
                                            .min(chunk.len() - offset);

                                        match stream.write(&chunk[offset..][..count]) {
                                            Ok(()) => {
                                                offset += count;
                                            }
                                            Err(_) => break Poll::Ready(Err(anyhow!("I/O error"))),
                                        }
                                    }
                                }
                                Err(_) => break Poll::Ready(Err(anyhow!("I/O error"))),
                            }
                        }
                    }
                })
            }
        })
    }

    pub fn outgoing_request_send(
        request: OutgoingRequest,
    ) -> impl Future<Output = Result<IncomingResponse, types::ErrorCode>> {
        future::poll_fn({
            let response = outgoing_handler::handle(request, None);

            move |context| match &response {
                Ok(response) => {
                    if let Some(response) = response.get() {
                        Poll::Ready(response.unwrap())
                    } else {
                        WAKERS
                            .lock()
                            .unwrap()
                            .push((response.subscribe(), context.waker().clone()));
                        Poll::Pending
                    }
                }
                Err(error) => Poll::Ready(Err(error.clone())),
            }
        })
    }

    pub fn incoming_body(body: IncomingBody) -> impl Stream<Item = Result<Vec<u8>>> {
        struct Incoming(Option<(InputStream, IncomingBody)>);

        impl Drop for Incoming {
            fn drop(&mut self) {
                if let Some((stream, body)) = self.0.take() {
                    drop(stream);
                    IncomingBody::finish(body);
                }
            }
        }

        stream::poll_fn({
            let stream = body.stream().expect("response body should be readable");
            let pair = Incoming(Some((stream, body)));

            move |context| {
                if let Some((stream, _)) = &pair.0 {
                    match stream.read(READ_SIZE) {
                        Ok(buffer) => {
                            if buffer.is_empty() {
                                WAKERS
                                    .lock()
                                    .unwrap()
                                    .push((stream.subscribe(), context.waker().clone()));
                                Poll::Pending
                            } else {
                                Poll::Ready(Some(Ok(buffer)))
                            }
                        }
                        Err(StreamError::Closed) => Poll::Ready(None),
                        Err(StreamError::LastOperationFailed(error)) => {
                            Poll::Ready(Some(Err(anyhow!("{}", error.to_debug_string()))))
                        }
                    }
                } else {
                    Poll::Ready(None)
                }
            }
        })
    }
}
