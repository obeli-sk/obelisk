#[cfg(feature = "tokio-console")]
#[derive(strum::Display, Clone, Copy)]
#[expect(non_camel_case_types)]
pub(crate) enum SupportedEnvVar {
    TOKIO_CONSOLE,
}

#[cfg(feature = "tokio-console")]
pub(crate) fn is_env_true(key: &SupportedEnvVar) -> bool {
    std::env::var(key.to_string())
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default()
}
