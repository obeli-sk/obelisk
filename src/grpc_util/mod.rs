use anyhow::Context;
use tonic::transport::{Channel, ClientTlsConfig};

pub(crate) mod grpc_mapping;

pub(crate) type TonicResult<T> = Result<T, tonic::Status>;

pub(crate) type TonicRespResult<T> = TonicResult<tonic::Response<T>>;

pub(crate) async fn to_channel(url: String) -> Result<Channel, anyhow::Error> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let url = url.parse().context("cannot parse uri")?;
    Channel::builder(url)
        .tls_config(tls)?
        .connect()
        .await
        .context("connect error")
}

// Source: https://github.com/hseeberger/hello-tracing-rs/blob/b411f8b192b7d585c42b5928ea635b2bd8bde29c/hello-tracing-common/src/otel/grpc.rs
pub mod injector {
    use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
    use tracing::{warn, Span};

    struct MetadataInjector<'a>(&'a mut MetadataMap);

    impl opentelemetry::propagation::Injector for MetadataInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            match MetadataKey::from_bytes(key.as_bytes()) {
                Ok(key) => match MetadataValue::try_from(&value) {
                    Ok(value) => {
                        self.0.insert(key, value);
                    }

                    Err(error) => warn!(
                        value,
                        error = format!("{error:?}"),
                        "cannot parse metadata value"
                    ),
                },

                Err(error) => warn!(
                    key,
                    error = format!("{error:?}"),
                    "cannot parse metadata key"
                ),
            }
        }
    }

    /// Client interceptor that injects current span IDs to the request's metadata.
    pub struct TracingInjector;

    impl tonic::service::Interceptor for TracingInjector {
        fn call(
            &mut self,
            mut request: tonic::Request<()>,
        ) -> Result<tonic::Request<()>, tonic::Status> {
            use tracing_opentelemetry::OpenTelemetrySpanExt as _;
            opentelemetry::global::get_text_map_propagator(|propagator| {
                let context = Span::current().context();
                propagator.inject_context(&context, &mut MetadataInjector(request.metadata_mut()))
            });
            Ok(request)
        }
    }
}

// TODO: replace with opentelemetry-http
// Source: https://github.com/hseeberger/hello-tracing-rs/blob/b411f8b192b7d585c42b5928ea635b2bd8bde29c/hello-tracing-common/src/otel/http.rs
pub mod extractor {
    use opentelemetry::propagation::Extractor;

    use tracing::{warn, Span};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    struct HttpHeaderExtractor<'a>(&'a http::HeaderMap);

    impl<'a> Extractor for HttpHeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| {
                let s = v.to_str();
                if let Err(ref error) = s {
                    warn!(%error, ?v, "cannot convert header value to ASCII")
                };
                s.ok()
            })
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    /// Trace context propagation: associate the current span with the OTel trace of the given request,
    /// if any and valid.
    pub fn accept_trace<B>(request: http::Request<B>) -> http::Request<B> {
        // Current context, if no or invalid data is received.
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HttpHeaderExtractor(request.headers()))
        });
        Span::current().set_parent(parent_context);
        request
    }
}
