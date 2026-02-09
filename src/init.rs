use std::io::IsTerminal as _;

use crate::config::toml::{
    ConfigToml,
    log::{AppenderConsoleWriter, LoggingStyle},
};
use tracing::warn;
use tracing_error::ErrorLayer;
use tracing_subscriber::Layer;
use utils::panic_hook::tracing_panic_hook;

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
fn tokio_tracing_otlp<S>(
    config: &mut ConfigToml,
) -> Result<Option<impl tracing_subscriber::Layer<S>>, anyhow::Error>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use anyhow::Context;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig as _;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    Ok(match &mut config.otlp {
        Some(otlp) if otlp.enabled => {
            opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&otlp.otlp_endpoint)
                .build()
                .context("otlp endpoint setup failure")?;
            let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(
                    Resource::builder()
                        .with_service_name(otlp.service_name.clone())
                        .build(),
                )
                .build();
            // EnvFilter missing Clone
            let env_filter = std::mem::take(&mut otlp.level).0;
            let telemetry_layer = tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer(""))
                .with_filter(env_filter);
            Some(telemetry_layer)
        }
        _ => None,
    })
}
#[cfg(not(feature = "otlp"))]
#[expect(clippy::needless_pass_by_value)]
fn tokio_tracing_otlp(
    _config: &mut ConfigToml,
) -> Result<Option<tracing::level_filters::LevelFilter>, anyhow::Error> {
    None
}

pub(crate) fn init(config: &mut ConfigToml) -> Result<Guard, anyhow::Error> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let mut guard = Guard::default();

    let console_layer = if config.log.console.enabled {
        let env_filter = config.log.console.common.level.0.clone();
        let writer = config.log.console.writer;
        // Code repetition because of https://github.com/tokio-rs/tracing/issues/575
        let console_layer = match config.log.console.style {
            LoggingStyle::Plain => tracing_subscriber::fmt::layer()
                .with_writer(move || -> Box<dyn std::io::Write> {
                    if writer == AppenderConsoleWriter::Stderr {
                        Box::new(std::io::stderr())
                    } else {
                        Box::new(std::io::stdout())
                    }
                })
                .with_file(true)
                .with_line_number(true)
                .with_target(config.log.console.common.target)
                .with_span_events(config.log.console.common.span.into())
                .with_ansi(std::io::stdout().is_terminal())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::PlainCompact => tracing_subscriber::fmt::layer()
                .with_writer(move || -> Box<dyn std::io::Write> {
                    if writer == AppenderConsoleWriter::Stderr {
                        Box::new(std::io::stderr())
                    } else {
                        Box::new(std::io::stdout())
                    }
                })
                .compact()
                .with_target(config.log.console.common.target)
                .with_span_events(config.log.console.common.span.into())
                .with_ansi(std::io::stdout().is_terminal())
                .with_filter(env_filter)
                .boxed(),
            LoggingStyle::Json => tracing_subscriber::fmt::layer()
                .with_writer(move || -> Box<dyn std::io::Write> {
                    if writer == AppenderConsoleWriter::Stderr {
                        Box::new(std::io::stderr())
                    } else {
                        Box::new(std::io::stdout())
                    }
                })
                .json()
                .with_file(true)
                .with_line_number(true)
                .with_target(config.log.console.common.target)
                .with_span_events(config.log.console.common.span.into())
                .with_filter(env_filter)
                .boxed(),
        };

        Some(console_layer)
    } else {
        None
    };
    let rolling_file_layer = match &mut config.log.file {
        Some(rolling) if rolling.enabled => {
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
                    .with_file(true)
                    .with_line_number(true)
                    .with_ansi(false)
                    .with_target(rolling.common.target)
                    .with_span_events(rolling.common.span.into())
                    .with_filter(env_filter)
                    .boxed(),
                LoggingStyle::PlainCompact => tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false)
                    .compact()
                    .with_target(rolling.common.target)
                    .with_span_events(rolling.common.span.into())
                    .with_filter(env_filter)
                    .boxed(),
                LoggingStyle::Json => tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_file(true)
                    .with_line_number(true)
                    .json()
                    .with_target(rolling.common.target)
                    .with_span_events(rolling.common.span.into())
                    .with_filter(env_filter)
                    .boxed(),
            })
        }
        _ => None,
    };
    tracing_subscriber::registry()
        .with(ErrorLayer::default())
        .with(tokio_console_layer())
        .with(tokio_tracing_otlp(config)?)
        .with(rolling_file_layer) // Must be before `out_layer`: https://github.com/tokio-rs/tracing/issues/3116
        .with(console_layer)
        .init();

    std::panic::set_hook(Box::new(tracing_panic_hook));
    Ok(guard)
}

#[derive(Default)]
pub(crate) struct Guard {
    file_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
    #[cfg(feature = "otlp")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        cfg_if::cfg_if! {
            if #[cfg(feature = "otlp")] {
                if let Some(tracer_provider) = self.tracer_provider.take()
                    && let Err(err) = tracer_provider.shutdown() {
                        warn!("Error shutting down the tracing provider - {err:?}");
                    }
            }
        }
    }
}
