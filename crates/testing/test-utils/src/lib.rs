use std::str::FromStr;

static INIT: std::sync::Once = std::sync::Once::new();
pub fn set_up() {
    INIT.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .without_time()
                    .with_target(false)
                    .with_thread_ids(true),
            )
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    });
}

pub fn env_or_default<T: FromStr>(env_var: &str, default: T) -> T {
    std::env::var(env_var)
        .ok()
        .and_then(|val| str::parse(&val).ok())
        .unwrap_or(default)
}
