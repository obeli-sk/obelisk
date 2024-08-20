use std::borrow::Cow;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;

#[cfg(feature = "tokio-console")]
fn tokio_console_layer<S>() -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if crate::env_vars::is_env_true(&crate::env_vars::SupportedEnvVar::TOKIO_CONSOLE) {
        // Run with
        // TOKIO_CONSOLE=true RUSTFLAGS="--cfg tokio_unstable" DB=mem RUST_LOG=error,runtime=trace,tokio=trace cargo run --target-dir=target/debug/tokio-console --features parallel-compilation,tokio-console > /dev/null
        use tracing_subscriber::Layer;
        Some(
            console_subscriber::spawn().with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("tokio", tracing::Level::TRACE)
                    .with_target("runtime", tracing::Level::TRACE),
            ),
        )
    } else {
        None
    }
}
#[cfg(not(feature = "tokio-console"))]
fn tokio_console_layer() -> Option<tracing::level_filters::LevelFilter> {
    None
}

#[cfg(feature = "otlp")]
fn tokio_tracing_otlp<S>(
    name: impl Into<Cow<'static, str>>,
) -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::BatchConfig;
    use opentelemetry_sdk::trace::Config;
    use opentelemetry_sdk::Resource;
    // Endpoints per protocol https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
    const OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DEFAULT: &str = "http://localhost:4317";

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let name = name.into();
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_batch_config(BatchConfig::default())
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(
            crate::env_vars::get_env_or_default(
                &crate::env_vars::SupportedEnvVar::OTLP_ENDPOIONT,
                OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DEFAULT,
            ),
        ))
        .with_trace_config(Config::default().with_resource(Resource::new(vec![
            opentelemetry::KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                name.clone(),
            ),
        ])))
        .install_batch(runtime::Tokio)
        .expect("cannot setup otlp");

    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    Some(telemetry_layer)
}
#[cfg(not(feature = "otlp"))]
#[allow(clippy::needless_pass_by_value)]
fn tokio_tracing_otlp(
    _name: impl Into<Cow<'static, str>>,
) -> Option<tracing::level_filters::LevelFilter> {
    None
}

pub(crate) fn init(name: impl Into<Cow<'static, str>>, machine_readable: bool) -> Guard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let chrome_guard;
    tracing_subscriber::registry()
        .with(tokio_console_layer())
        .with(tokio_tracing_otlp(name))
        .with(if machine_readable {
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .boxed()
        } else {
            tracing_subscriber::fmt::layer()
                .compact()
                .with_target(false)
                .boxed()
        })
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with({
            let (layer, guard) = chrome_layer();
            chrome_guard = guard;
            layer
        })
        .init();

    std::panic::set_hook(Box::new(utils::tracing_panic_hook));
    Guard {
        _chrome_guard: chrome_guard,
    }
}

#[cfg(not(feature = "tracing-chrome"))]
fn chrome_layer() -> (Option<tracing::level_filters::LevelFilter>, Option<()>) {
    (None, None)
}
#[cfg(feature = "tracing-chrome")]
fn chrome_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span> + Send + Sync,
>() -> (
    Option<tracing_chrome::ChromeLayer<S>>,
    Option<tracing_chrome::FlushGuard>,
) {
    if crate::env_vars::is_env_true(&crate::env_vars::SupportedEnvVar::CHROME_TRACE) {
        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        (Some(chrome_layer), Some(guard))
    } else {
        (None, None)
    }
}

pub(crate) struct Guard {
    #[cfg(feature = "tracing-chrome")]
    _chrome_guard: Option<tracing_chrome::FlushGuard>,
    #[cfg(not(feature = "tracing-chrome"))]
    _chrome_guard: Option<()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        cfg_if::cfg_if! {
            if #[cfg(feature = "otlp")] {
                opentelemetry::global::shutdown_tracer_provider();
            }
        }
    }
}
