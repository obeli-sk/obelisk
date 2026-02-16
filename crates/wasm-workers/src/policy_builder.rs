use crate::http_request_policy::{
    AllowedHostConfig, AllowedHostPolicy, HttpRequestPolicy, PlaceholderSecret,
    generate_placeholder,
};
use wasmtime_wasi::WasiCtxBuilder;

/// Build an [`HttpRequestPolicy`] from resolved allowed-host configs,
/// generating fresh random placeholders for each secret env mapping.
pub(crate) fn build_http_policy(
    allowed_hosts: &[AllowedHostConfig],
    wasi_ctx: &mut WasiCtxBuilder,
) -> HttpRequestPolicy {
    let hosts = allowed_hosts
        .iter()
        .map(|host_config| {
            let secrets = host_config
                .secret_env_mappings
                .iter()
                .map(|(env_key, real_value)| {
                    let placeholder = generate_placeholder();
                    wasi_ctx.env(env_key, &placeholder);
                    PlaceholderSecret {
                        placeholder,
                        real_value: real_value.clone(),
                        replace_in: host_config.replace_in.clone(),
                    }
                })
                .collect();
            AllowedHostPolicy {
                pattern: host_config.pattern.clone(),
                secrets,
            }
        })
        .collect();
    HttpRequestPolicy { hosts }
}
