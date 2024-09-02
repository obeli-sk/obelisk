use concepts::ComponentConfigHash;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::{fmt::Debug, sync::Arc};
use tokio::net::TcpListener;
use tracing::{error, info};
use utils::wasm_tools::WasmComponent;
use wasmtime::component::Linker;
use wasmtime::Engine;
use wasmtime::PoolingAllocationConfig;
use wasmtime_wasi_http::bindings::http::types::Scheme;
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::WasiHttpView;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HttpTriggerConfig {
    pub config_id: ComponentConfigHash,
}

type StdError = Box<dyn std::error::Error + Send + Sync>;

type Request = hyper::Request<hyper::body::Incoming>;

pub async fn server() -> Result<(), StdError> {
    let wasm_path = "target/wasm32-wasip1/release/trigger_http_simple.wasm";
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;

    let engine = {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
            PoolingAllocationConfig::default(),
        ));
        wasmtime_config.wasm_component_model(true);
        // TODO epoch_interruption
        wasmtime_config.async_support(true);
        Arc::new(Engine::new(&wasmtime_config).unwrap())
    };
    let wasm_component = WasmComponent::new(wasm_path, &engine).unwrap();
    let mut linker = Linker::new(&engine);
    wasmtime_wasi_http::add_to_linker_async(&mut linker)?;
    let instance = linker.instantiate_pre(&wasm_component.component)?;
    let instance = ProxyPre::new(instance)?;

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        // Spawn a tokio task for each connection
        tokio::task::spawn({
            let instance = instance.clone();
            let engine = engine.clone();
            async move {
                let res = http1::Builder::new()
                    .serve_connection(
                        io,
                        hyper::service::service_fn(move |req| {
                            handle_request(req, engine.clone(), instance.clone())
                        }),
                    )
                    .await;
                if let Err(err) = res {
                    error!("Error serving connection: {err:?}");
                }
            }
        });
    }
}

async fn handle_request(
    req: Request,
    engine: Arc<Engine>,
    instance: ProxyPre<crate::wasi_http::Ctx>,
) -> Result<hyper::Response<HyperOutgoingBody>, StdError> {
    let (sender, receiver) = tokio::sync::oneshot::channel();

    info!("Request {} to {}", req.method(), req.uri());

    let mut store = crate::wasi_http::store(&engine);

    // if self.run.common.wasm.timeout.is_some() {
    //     store.set_epoch_deadline(u64::from(EPOCH_PRECISION) + 1);
    // }

    let req = store.data_mut().new_incoming_request(Scheme::Http, req)?;
    let out = store.data_mut().new_response_outparam(sender)?;
    let proxy = instance.instantiate_async(&mut store).await?;

    let task = tokio::task::spawn(async move {
        if let Err(e) = proxy
            .wasi_http_incoming_handler()
            .call_handle(store, req, out)
            .await
        {
            error!("{e:?}");
            return Err(e);
        }

        Ok(())
    });
    match receiver.await {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => {
            // An error in the receiver (`RecvError`) only indicates that the
            // task exited before a response was sent (i.e., the sender was
            // dropped); it does not describe the underlying cause of failure.
            // Instead we retrieve and propagate the error from inside the task
            // which should more clearly tell the user what went wrong. Note
            // that we assume the task has already exited at this point so the
            // `await` should resolve immediately.
            let e = match task.await {
                Ok(r) => r.expect_err("if the receiver has an error, the task must have failed"),
                Err(e) => e.into(),
            };
            // FIXME: bail!
            panic!("guest never invoked `response-outparam::set` method: {e:?}")
        }
    }
}
