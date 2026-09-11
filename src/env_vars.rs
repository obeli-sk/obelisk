#[cfg(feature = "tokio-console")]
#[derive(strum::Display, Clone, Copy)]
#[expect(non_camel_case_types)]
pub(crate) enum SupportedEnvVar {
    TOKIO_CONSOLE,
}

#[cfg(feature = "tokio-console")]
static TOKIO_CONSOLE_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub(crate) fn capture() {
    #[cfg(feature = "tokio-console")]
    TOKIO_CONSOLE_ENABLED.get_or_init(|| {
        std::env::var(SupportedEnvVar::TOKIO_CONSOLE.to_string())
            .ok()
            .and_then(|val| val.parse::<bool>().ok())
            .unwrap_or_default()
    });
}

#[cfg(feature = "tokio-console")]
pub(crate) fn is_env_true(key: &SupportedEnvVar) -> bool {
    match key {
        SupportedEnvVar::TOKIO_CONSOLE => *TOKIO_CONSOLE_ENABLED
            .get()
            .expect("environment variables must be captured during startup"),
    }
}
