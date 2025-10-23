pub mod grpc_gen;
pub mod grpc_mapping;

use http::{Uri, uri::Scheme};
use tonic::transport::{Channel, ClientTlsConfig};

pub type TonicResult<T> = Result<T, tonic::Status>;

pub type TonicRespResult<T> = TonicResult<tonic::Response<T>>;

#[derive(Debug, thiserror::Error)]
pub enum ToChannelError {
    #[error(transparent)]
    InvalidUri(http::uri::InvalidUri),
    #[error(transparent)]
    Transport(tonic::transport::Error),
    #[error("unknown schame {0}")]
    UnknownScheme(String),
}

pub async fn to_channel(url: &str) -> Result<Channel, ToChannelError> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let url: Uri = url.parse().map_err(ToChannelError::InvalidUri)?;
    if url.scheme() == Some(&Scheme::HTTP) {
        Channel::builder(url)
            .connect()
            .await
            .map_err(ToChannelError::Transport)
    } else if url.scheme() == Some(&Scheme::HTTPS) {
        Channel::builder(url)
            .tls_config(tls)
            .map_err(ToChannelError::Transport)?
            .connect()
            .await
            .map_err(ToChannelError::Transport)
    } else {
        Err(ToChannelError::UnknownScheme(format!("{:?}", url.scheme())))
    }
}

#[cfg(feature = "otlp")]
// Source: https://github.com/hseeberger/hello-tracing-rs/blob/b411f8b192b7d585c42b5928ea635b2bd8bde29c/hello-tracing-common/src/otel/grpc.rs
pub mod injector {
    use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
    use tracing::{Span, warn};

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
                propagator.inject_context(&context, &mut MetadataInjector(request.metadata_mut()));
            });
            Ok(request)
        }
    }
}

#[cfg(feature = "otlp")]
pub mod extractor {
    use opentelemetry_http::HeaderExtractor;
    use tracing::Span;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    /// Uses [`HeaderExtractor`] to fetch trace and span IDs from the request headers,
    /// and sets the extracted context as the current span's parent, if valid.
    pub fn accept_trace<B>(request: http::Request<B>) -> http::Request<B> {
        // Current context, if no or invalid data is received.
        let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(request.headers()))
        });
        let _ = Span::current().set_parent(parent_context);
        request
    }
}
