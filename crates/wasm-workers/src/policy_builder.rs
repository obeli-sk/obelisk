use crate::http_request_policy::{
    AllowedHostConfig, AllowedHostPolicy, HttpRequestPolicy, PlaceholderSecret,
    generate_placeholder,
};
use wasmtime_wasi::WasiCtxBuilder;

/// Build an [`HttpRequestPolicy`] from resolved allowed-host configs, generating
/// one random placeholder per unique secret env var name and binding each env
/// var into `wasi_ctx` exactly once.
pub(crate) fn build_http_policy(
    allowed_hosts: &[AllowedHostConfig],
    wasi_ctx: &mut WasiCtxBuilder,
) -> HttpRequestPolicy {
    let (policy, placeholders) = build_http_policy_inner(allowed_hosts);
    for (env_key, placeholder) in &placeholders {
        wasi_ctx.env(env_key, placeholder);
    }
    policy
}

/// Pure core of [`build_http_policy`]: returns the policy plus the
/// `env_key -> placeholder` map that must be set on the guest env.
///
/// The same secret env var may be declared on multiple `allowed_host` entries
/// (e.g. one per method/path regex). Since the real value is identical across
/// entries, a single shared placeholder is generated per env var name and bound
/// once.
fn build_http_policy_inner(
    allowed_hosts: &[AllowedHostConfig],
) -> (
    HttpRequestPolicy,
    hashbrown::HashMap<
        &str,   // env key
        String, // placeholder
    >,
) {
    let mut placeholders: hashbrown::HashMap<&str, String> = hashbrown::HashMap::new();
    let mut hosts = Vec::with_capacity(allowed_hosts.len());
    for host_config in allowed_hosts {
        let mut secrets = Vec::with_capacity(host_config.secret_env_mappings.len());
        for (env_key, real_value) in &host_config.secret_env_mappings {
            let placeholder = placeholders
                .entry(env_key.as_str())
                .or_insert_with(generate_placeholder)
                .clone();
            secrets.push(PlaceholderSecret {
                placeholder,
                real_value: real_value.clone(),
                replace_in: host_config.replace_in.clone(),
            });
        }
        hosts.push(AllowedHostPolicy {
            pattern: host_config.pattern.clone(),
            request_url_regex: host_config.request_url_regex.clone(),
            secrets,
        });
    }
    (HttpRequestPolicy { hosts }, placeholders)
}

#[cfg(test)]
mod tests {
    use super::build_http_policy_inner;
    use crate::http_request_policy::{
        AllowedHostConfig, HostPattern, MethodsPattern, ReplacementLocation,
    };
    use hyper::http::Method;
    use secrecy::SecretString;
    use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

    fn empty_body() -> HyperOutgoingBody {
        http_body_util::combinators::UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
            http_body_util::Empty::<hyper::body::Bytes>::new(),
            |_| unreachable!(),
        ))
    }

    fn host_config(env_key: &str, real_value: &str, methods: Vec<Method>) -> AllowedHostConfig {
        AllowedHostConfig {
            pattern: HostPattern::parse_with_methods(
                "api.example.com",
                MethodsPattern::Specific(methods),
            )
            .unwrap(),
            request_url_regex: None,
            secret_env_mappings: vec![(env_key.to_string(), SecretString::from(real_value))],
            replace_in: hashbrown::HashSet::from([ReplacementLocation::Headers]),
        }
    }

    /// The same secret env var declared on two host entries (scoped to different
    /// methods) must bind a single shared placeholder, and `apply()` must
    /// substitute the real value for a request matching EITHER entry.
    #[test]
    fn shared_secret_across_two_allowed_hosts_substitutes_for_either_entry() {
        const ENV_KEY: &str = "OBELISK__API__TOKEN";
        const REAL_VALUE: &str = "real-token-value";

        let allowed_hosts = vec![
            host_config(ENV_KEY, REAL_VALUE, vec![Method::GET]),
            host_config(ENV_KEY, REAL_VALUE, vec![Method::PUT]),
        ];

        let (policy, placeholders) = build_http_policy_inner(&allowed_hosts);

        // Exactly one placeholder bound into the guest env for this env var.
        assert_eq!(
            placeholders.len(),
            1,
            "env var must be bound exactly once, got: {placeholders:?}"
        );

        // Both host entries reference that single shared placeholder.
        let placeholder = &policy.hosts[0].secrets[0].placeholder;
        assert_eq!(&policy.hosts[1].secrets[0].placeholder, placeholder);
        assert_eq!(placeholders.get(ENV_KEY), Some(placeholder));

        // The guest ships whichever placeholder is in its env; assert both the
        // GET-scoped and PUT-scoped requests get it substituted for the real value.
        for method in [Method::GET, Method::PUT] {
            let mut request = hyper::Request::builder()
                .method(method.clone())
                .uri("https://api.example.com/v1/items")
                .header("authorization", format!("Bearer {placeholder}"))
                .body(empty_body())
                .unwrap();

            policy.apply(&mut request).unwrap();

            assert_eq!(
                request.headers().get("authorization").unwrap(),
                &format!("Bearer {REAL_VALUE}"),
                "placeholder not substituted for {method} request"
            );
        }
    }
}
