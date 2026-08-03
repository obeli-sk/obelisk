use crate::http_request_policy::{
    AllowedHostConfig, AllowedHostPolicy, GlobalHttpConfig, HttpRequestPolicy, PlaceholderSecret,
    SecretResolver, generate_placeholder,
};
use secrecy::SecretString;
use wasmtime_wasi::WasiCtxBuilder;

/// Build an [`HttpRequestPolicy`] from resolved allowed-host configs, generating
/// one random placeholder per unique secret name and binding each into `wasi_ctx`
/// exactly once.
///
/// Secret values are fetched from `resolver` here, at execution-run policy build
/// time, rather than baked into the verified [`AllowedHostConfig`]. A name the
/// resolver cannot resolve (unknown, or outside the component's restricted view)
/// mints no placeholder and injects nothing, so the request fails closed.
///
/// `global_allowlist` is the operator-owned allowlist from `server.toml`. Egress
/// must additionally match it and secret injection is intersected against it
/// (see [`HttpRequestPolicy`]). The global allowlist contributes authorization only:
/// it mints no placeholders, resolves no values, and binds nothing into the guest env.
pub(crate) fn build_http_policy(
    allowed_hosts: &[AllowedHostConfig],
    global_http_config: &GlobalHttpConfig,
    resolver: &dyn SecretResolver,
    wasi_ctx: &mut WasiCtxBuilder,
) -> HttpRequestPolicy {
    let (mut policy, placeholders) = build_http_policy_inner(allowed_hosts, resolver);
    for (env_key, placeholder) in &placeholders {
        wasi_ctx.env(env_key, placeholder);
    }
    policy.global_allowlist = Some(build_authorization_hosts(global_http_config.entries()));
    policy
}

/// Build the operator global allowlist's host list. Each entry authorizes a
/// `(secret name, replacement target)` pair but carries no placeholder or real value, so no
/// guest env binding is produced.
fn build_authorization_hosts(allowed_hosts: &[AllowedHostConfig]) -> Vec<AllowedHostPolicy> {
    allowed_hosts
        .iter()
        .map(|host_config| AllowedHostPolicy {
            pattern: host_config.pattern.clone(),
            request_url_regex: host_config.request_url_regex.clone(),
            secrets: host_config
                .secret_names
                .iter()
                .map(|name| PlaceholderSecret {
                    name: name.clone(),
                    placeholder: String::new(),
                    real_value: SecretString::from(String::new()),
                    replace_in: host_config.replace_in.clone(),
                })
                .collect(),
        })
        .collect()
}

/// Pure core of [`build_http_policy`]: resolves declared secret names to values
/// via `resolver` and returns the policy plus the `env_key -> placeholder` map
/// that must be set on the guest env.
///
/// The same secret name may be declared on multiple `allowed_host` entries (e.g.
/// one per method/path regex). Since the value is identical across entries, it is
/// resolved once and a single shared placeholder is generated per name and bound
/// once. A name the resolver cannot resolve is skipped (no placeholder, no
/// binding), so a request referencing it fails closed.
fn build_http_policy_inner(
    allowed_hosts: &[AllowedHostConfig],
    resolver: &dyn SecretResolver,
) -> (
    HttpRequestPolicy,
    hashbrown::HashMap<
        String, // env key (== secret name)
        String, // placeholder
    >,
) {
    // Resolve each distinct name once; `None` (unknown / restricted) drops it, so
    // any host referencing that name mints no placeholder and fails closed.
    let mut by_name: hashbrown::HashMap<String, (String, SecretString)> = hashbrown::HashMap::new();
    let mut placeholders: hashbrown::HashMap<String, String> = hashbrown::HashMap::new();
    let mut hosts = Vec::with_capacity(allowed_hosts.len());
    for host_config in allowed_hosts {
        let mut secrets = Vec::with_capacity(host_config.secret_names.len());
        for name in &host_config.secret_names {
            if !by_name.contains_key(name) {
                let Some(value) = resolver.secret_lookup(name) else {
                    continue;
                };
                by_name.insert(name.clone(), (generate_placeholder(), value));
            }
            let (placeholder, value) = &by_name[name];
            placeholders.insert(name.clone(), placeholder.clone());
            secrets.push(PlaceholderSecret {
                name: name.clone(),
                placeholder: placeholder.clone(),
                real_value: value.clone(),
                replace_in: host_config.replace_in.clone(),
            });
        }
        hosts.push(AllowedHostPolicy {
            pattern: host_config.pattern.clone(),
            request_url_regex: host_config.request_url_regex.clone(),
            secrets,
        });
    }
    (
        HttpRequestPolicy {
            hosts,
            global_allowlist: None,
        },
        placeholders,
    )
}

#[cfg(test)]
mod tests {
    use super::build_http_policy_inner;
    use crate::http_request_policy::{
        AllowedHostConfig, HostPattern, MethodsPattern, ReplacementLocation, SecretResolver,
    };
    use hyper::http::Method;
    use secrecy::SecretString;
    use wasmtime_wasi_http::p2::body::HyperOutgoingBody;

    /// Test resolver backed by a fixed name -> value map.
    #[derive(Debug)]
    struct MapResolver(hashbrown::HashMap<String, SecretString>);
    impl SecretResolver for MapResolver {
        fn secret_lookup(&self, name: &str) -> Option<SecretString> {
            self.0.get(name).cloned()
        }
    }

    fn empty_body() -> HyperOutgoingBody {
        http_body_util::combinators::UnsyncBoxBody::new(http_body_util::BodyExt::map_err(
            http_body_util::Empty::<hyper::body::Bytes>::new(),
            |_| unreachable!(),
        ))
    }

    fn host_config(name: &str, methods: Vec<Method>) -> AllowedHostConfig {
        AllowedHostConfig {
            pattern: HostPattern::parse_with_methods(
                "api.example.com",
                MethodsPattern::Specific(methods),
            )
            .unwrap(),
            request_url_regex: None,
            secret_names: vec![name.to_string()],
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
            host_config(ENV_KEY, vec![Method::GET]),
            host_config(ENV_KEY, vec![Method::PUT]),
        ];
        let resolver = MapResolver(hashbrown::HashMap::from([(
            ENV_KEY.to_string(),
            SecretString::from(REAL_VALUE),
        )]));

        let (policy, placeholders) = build_http_policy_inner(&allowed_hosts, &resolver);

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
