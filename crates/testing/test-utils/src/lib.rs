use std::str::FromStr;
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};

pub mod sim_clock;

static mut CHRMOE_TRACE_FILE_GUARD: Option<tracing_chrome::FlushGuard> = None;

static INIT: std::sync::Once = std::sync::Once::new();

pub fn set_up() -> Option<FlushGuard> {
    INIT.call_once(|| {
        std::panic::set_hook(Box::new(|panic_info| utils::tracing_panic_hook(panic_info)));
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        let enable_chrome_layer = std::env::var("CHROME_TRACE")
            .ok()
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or_default();
        let builder = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true)
                    .json(),
            )
            .with(tracing_subscriber::EnvFilter::from_default_env());

        if enable_chrome_layer {
            let (chrome_layer, guard) = ChromeLayerBuilder::new()
                .trace_style(tracing_chrome::TraceStyle::Threaded)
                .build();
            unsafe {
                CHRMOE_TRACE_FILE_GUARD = Some(guard);
            }

            builder.with(chrome_layer).init();
        } else {
            builder.init();
        }
    });
    unsafe { CHRMOE_TRACE_FILE_GUARD.take() }
}

pub fn env_or_default<T: FromStr>(env_var: &str, default: T) -> T {
    std::env::var(env_var)
        .ok()
        .and_then(|val| str::parse(&val).ok())
        .unwrap_or(default)
}
