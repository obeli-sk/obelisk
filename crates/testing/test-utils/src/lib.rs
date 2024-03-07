static INIT: std::sync::Once = std::sync::Once::new();
pub fn set_up() {
    INIT.call_once(|| {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().without_time())
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    });
}
