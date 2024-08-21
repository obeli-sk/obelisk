#[derive(strum::Display, Clone, Copy)]
#[allow(non_camel_case_types, dead_code)]
pub(crate) enum SupportedEnvVar {
    TOKIO_CONSOLE,
    CHROME_TRACE,
}

#[allow(dead_code)]
pub(crate) fn is_env_true(key: &SupportedEnvVar) -> bool {
    std::env::var(key.to_string())
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default()
}
