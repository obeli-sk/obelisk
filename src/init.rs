use tracing::level_filters::LevelFilter;

fn is_env_true(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default()
}

#[cfg(feature = "tokio-console")]
fn tokio_console_layer<S>() -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if is_env_true("TOKIO_CONSOLE") {
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

#[allow(clippy::too_many_lines)]
pub(crate) fn init() -> Guard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let mut chrome_guard = None;
    tracing_subscriber::registry()
        .with(tokio_console_layer())
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .without_time()
                .with_target(false),
        )
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(if is_env_true("CHROME_TRACE") {
            let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .trace_style(tracing_chrome::TraceStyle::Async)
                .build();
            chrome_guard = Some(guard);
            Some(chrome_layer)
        } else {
            None
        })
        .init();

    std::panic::set_hook(Box::new(utils::tracing_panic_hook));
    Guard {
        _chrome_guard: chrome_guard,
    }
}

pub(crate) struct Guard {
    _chrome_guard: Option<tracing_chrome::FlushGuard>,
}
