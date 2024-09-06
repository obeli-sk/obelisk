use crate::config::toml::{log::LoggingStyle, ObeliskConfig};
use tracing_subscriber::Layer;

#[cfg(feature = "tokio-console")]
fn tokio_console_layer<S>() -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if crate::env_vars::is_env_true(&crate::env_vars::SupportedEnvVar::TOKIO_CONSOLE) {
        // Run with
        // TOKIO_CONSOLE=true RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console
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
fn tokio_tracing_otlp<S>(config: &mut ObeliskConfig) -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::runtime;
    use opentelemetry_sdk::trace::BatchConfig;
    use opentelemetry_sdk::trace::Config;
    use opentelemetry_sdk::Resource;

    if let Some(otlp) = &mut config.otlp {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_batch_config(BatchConfig::default())
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(otlp.otlp_endpoint.to_string()),
            )
            .with_trace_config(Config::default().with_resource(Resource::new(vec![
                opentelemetry::KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    otlp.service_name.clone(),
                ),
            ])))
            .install_batch(runtime::Tokio)
            .expect("cannot setup otlp");
        // EnvFilter missing Clone
        let env_filter = std::mem::take(&mut otlp.level).0;
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(env_filter);
        Some(telemetry_layer)
    } else {
        None
    }
}
#[cfg(not(feature = "otlp"))]
#[expect(clippy::needless_pass_by_value)]
fn tokio_tracing_otlp(_config: &mut ObeliskConfig) -> Option<tracing::level_filters::LevelFilter> {
    None
}

pub(crate) fn init(config: &mut ObeliskConfig) -> Guard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let mut guard = Guard::default();

    let out_layer = if let Some(stdout) = &mut config.log.stdout {
        // EnvFilter missing Clone
        let env_filter = std::mem::take(&mut stdout.common.level).0;

        // Code repetition because of https://github.com/tokio-rs/tracing/issues/575
        Some(match stdout.style {
            LoggingStyle::Plain => tracing_subscriber::fmt::layer()
                .with_target(stdout.common.target)
                .with_span_events(stdout.common.span.into())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::PlainCompact => tracing_subscriber::fmt::layer()
                .compact()
                .with_target(stdout.common.target)
                .with_span_events(stdout.common.span.into())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::Json => tracing_subscriber::fmt::layer()
                .json()
                .with_target(stdout.common.target)
                .with_span_events(stdout.common.span.into())
                .with_filter(env_filter)
                .boxed(),
        })
    } else {
        None
    };
    let rolling_file_layer = if let Some(rolling) = &mut config.log.file {
        // EnvFilter missing Clone
        let env_filter = std::mem::take(&mut rolling.common.level).0;
        let file_appender = tracing_appender::rolling::RollingFileAppender::new(
            rolling.rotation.into(),
            &rolling.directory,
            &rolling.prefix,
        );
        let (non_blocking, file_guard) = tracing_appender::non_blocking(file_appender);
        guard.file_guard = Some(file_guard);
        Some(match rolling.style {
            LoggingStyle::Plain => tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .with_target(rolling.common.target)
                .with_span_events(rolling.common.span.into())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::PlainCompact => tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .compact()
                .with_target(rolling.common.target)
                .with_span_events(rolling.common.span.into())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::Json => tracing_subscriber::fmt::layer()
                .with_writer(non_blocking)
                .json()
                .with_target(rolling.common.target)
                .with_span_events(rolling.common.span.into())
                .with_filter(env_filter)
                .boxed(),
        })
    } else {
        None
    };
    tracing_subscriber::registry()
        .with(tokio_console_layer())
        .with(tokio_tracing_otlp(config))
        .with(out_layer)
        .with(rolling_file_layer)
        .with({
            let (layer, chrome_guard) = chrome_layer();
            guard.chrome_guard = chrome_guard;
            layer
        })
        .init();

    std::panic::set_hook(Box::new(utils::tracing_panic_hook));
    guard
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

#[derive(Default)]
pub(crate) struct Guard {
    #[cfg(feature = "tracing-chrome")]
    chrome_guard: Option<tracing_chrome::FlushGuard>,
    #[cfg(not(feature = "tracing-chrome"))]
    chrome_guard: Option<()>,
    file_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
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
