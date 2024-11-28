use std::str::FromStr;

pub mod arbitrary;
pub mod sim_clock;

static INIT: std::sync::Once = std::sync::Once::new();

pub fn set_up() {
    INIT.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        std::panic::set_hook(Box::new(utils::tracing_panic_hook));
        let builder = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_thread_ids(true)
                    .with_file(true)
                    .with_line_number(true)
                    .with_target(true),
            )
            .with(tracing_subscriber::EnvFilter::from_default_env());

        builder.init();
    });
}

pub fn env_or_default<T: FromStr>(env_var: &str, default: T) -> T {
    std::env::var(env_var)
        .ok()
        .and_then(|val| str::parse(&val).ok())
        .unwrap_or(default)
}
