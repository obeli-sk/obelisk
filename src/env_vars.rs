use std::borrow::Cow;

#[derive(strum::Display, Clone, Copy)]
#[allow(non_camel_case_types, dead_code)]
pub(crate) enum SupportedEnvVar {
    TOKIO_CONSOLE,
    CHROME_TRACE,
    OTLP_ENDPOIONT,
}

#[allow(dead_code)]
pub(crate) fn is_env_true(key: &SupportedEnvVar) -> bool {
    std::env::var(key.to_string())
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default()
}

#[allow(dead_code)]
pub(crate) fn get_env_or_default<'a>(
    key: &SupportedEnvVar,
    default: impl Into<Cow<'a, str>>,
) -> Cow<'a, str> {
    std::env::var(key.to_string())
        .map(Cow::from)
        .unwrap_or_else(|_| default.into())
}
