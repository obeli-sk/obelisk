use crate::config::env_var::{
    StartupEnvVars, collect_env_var_references, interpolate_startup_env_vars,
};
use crate::config::secret_registry::{
    PublicEnvConfigToml, PublicEnvRef, PublicEnvToml, SecretsToml,
};
use crate::config::server::{AllowExecActivities, OutboundHttpToml};
use anyhow::{Context as _, ensure};
use concepts::component_id::Digest;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub(crate) fn resolve_app_config(
    explicit: Option<std::path::PathBuf>,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    let path = std::path::PathBuf::from("app.toml");
    match std::fs::symlink_metadata(&path) {
        Ok(_) => Ok(Some(path)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context("cannot inspect ./app.toml"),
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppConfigToml {
    #[serde(default)]
    #[schemars(
        length(min = 4, max = 25),
        regex(pattern = "^[a-z0-9][a-z0-9-]{2,23}[a-z0-9]$")
    )]
    pub(crate) app_name: Option<String>,
    #[serde(default)]
    pub(crate) cloud: CloudConfigToml,
    #[serde(default)]
    pub(crate) secrets: SecretsToml,
    #[serde(default)]
    pub(crate) public_env: PublicEnvToml,
    #[serde(default)]
    pub(crate) allowed_exec_activities: AllowExecActivities,
    #[serde(default)]
    pub(crate) outbound_http: OutboundHttpToml,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudConfigToml {
    /// VM memory in MB; omission uses the cloud default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) memory_mb: Option<std::num::NonZeroU32>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub(crate) integrations: std::collections::BTreeMap<String, CloudIntegrationToml>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudIntegrationToml {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) env: String,
}

#[derive(Debug, Default, Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppPolicyV1 {
    format: AppPolicyFormatV1,
    secrets: std::collections::BTreeMap<String, AppPolicySecretV1>,
    public_env: Vec<PublicEnvRef>,
    allowed_exec_activities: std::collections::BTreeMap<String, Vec<String>>,
    outbound_http: Vec<crate::config::deployment::AllowedHostToml>,
}

#[derive(Debug, Default, Clone, JsonSchema, Serialize)]
enum AppPolicyFormatV1 {
    #[default]
    #[serde(rename = "obelisk-app-config-v1")]
    V1,
}

pub(crate) fn validate_app_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        (4..=25).contains(&name.len())
            && !name.starts_with('-')
            && !name.ends_with('-')
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid app name `{name}`: expected a DNS label (4-25 lowercase ASCII letters, digits, or interior hyphens)"
    );
    Ok(())
}

#[derive(Debug, Clone, JsonSchema, Serialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
enum AppPolicySecretV1 {
    Internal {
        exposed_to: std::collections::BTreeMap<String, Vec<String>>,
    },
    #[serde(untagged)]
    Env {
        optional: bool,
        exposed_to: std::collections::BTreeMap<String, Vec<String>>,
    },
}

impl AppConfigToml {
    pub(crate) fn validate_cloud_integrations(&self) -> anyhow::Result<()> {
        let mut supplied = std::collections::BTreeSet::new();
        for (name, integration) in &self.cloud.integrations {
            ensure!(
                !name.is_empty() && !integration.kind.is_empty(),
                "Cloud integration name and type must not be empty"
            );
            ensure!(
                matches!(
                    self.public_env.get(&integration.env),
                    Some(PublicEnvConfigToml::Env(_))
                ),
                "Cloud integration `{name}` env `{}` must reference an environment-backed [public_env] declaration",
                integration.env
            );
            ensure!(
                !self.secrets.contains_key(&integration.env),
                "Cloud integration `{name}` env must not reference a secret"
            );
            ensure!(
                supplied.insert(&integration.env),
                "Multiple cloud integrations supply `{}`",
                integration.env
            );
        }
        Ok(())
    }

    pub(crate) fn validate_local_integrations(
        &self,
        env_vars: &StartupEnvVars,
    ) -> anyhow::Result<()> {
        for (name, integration) in &self.cloud.integrations {
            if matches!(self.public_env.get(&integration.env), Some(PublicEnvConfigToml::Env(config)) if !config.optional)
            {
                ensure!(
                    env_vars.lookup(&integration.env).is_some(),
                    "{} is required. Cloud integration `{name}` supplies it during cloud deployment; for local execution, set {} explicitly",
                    integration.env,
                    integration.env
                );
            }
        }
        Ok(())
    }

    pub(crate) fn resolve_env_vars(&mut self, env_vars: &StartupEnvVars) -> anyhow::Result<()> {
        self.validate_cloud_integrations()?;
        for allowed_host in &mut self.outbound_http.allowed_hosts {
            let mut references = std::collections::BTreeSet::new();
            for field in allowed_host.interpolated_fields() {
                collect_env_var_references(field, &mut references);
            }
            for name in references {
                ensure!(
                    !self.secrets.contains_key(&name),
                    "app.toml outbound HTTP policy cannot interpolate registered secret `{name}`"
                );
            }
            for field in allowed_host.interpolated_fields_mut() {
                *field = interpolate_startup_env_vars(field, env_vars)?;
            }
        }
        Ok(())
    }
    pub(crate) fn effective_name(&self) -> anyhow::Result<String> {
        let name = match (std::env::var("OBELISK_APP_NAME").ok(), &self.app_name) {
            (Some(env), Some(name)) => {
                ensure!(
                    env == *name,
                    "`OBELISK_APP_NAME` is `{env}`, but app.toml sets `app_name = \"{name}\"`; unset one of them"
                );
                env
            }
            (env, name) => env.or_else(|| name.clone()).context(
                "app name is required: set `app_name` in app.toml or `OBELISK_APP_NAME`",
            )?,
        };
        validate_app_name(&name)?;
        Ok(name)
    }

    pub(crate) fn policy(&self) -> AppPolicyV1 {
        AppPolicyV1 {
            format: AppPolicyFormatV1::V1,
            secrets: self
                .secrets
                .iter()
                .map(|(name, secret)| {
                    let exposed_to = secret
                        .exposed_to()
                        .iter()
                        .map(|(component, digests)| {
                            let mut values =
                                digests.iter().map(ToString::to_string).collect::<Vec<_>>();
                            values.sort();
                            (component.clone(), values)
                        })
                        .collect();
                    let secret = match secret {
                        crate::config::secret_registry::SecretConfigToml::Env(config) => {
                            AppPolicySecretV1::Env {
                                optional: config.optional,
                                exposed_to,
                            }
                        }
                        crate::config::secret_registry::SecretConfigToml::Internal(_) => {
                            AppPolicySecretV1::Internal { exposed_to }
                        }
                    };
                    (name.clone(), secret)
                })
                .collect(),
            public_env: {
                let mut allowed: Vec<_> = self
                    .public_env
                    .iter()
                    .map(|(name, config)| match config {
                        PublicEnvConfigToml::Internal(config) => PublicEnvRef::Internal {
                            name: name.to_owned(),
                            kind: config.kind,
                            source: config.source,
                        },
                        PublicEnvConfigToml::Env(config) if config.optional => {
                            PublicEnvRef::Config {
                                name: name.to_owned(),
                                optional: true,
                            }
                        }
                        PublicEnvConfigToml::Env(_) => PublicEnvRef::Required(name.to_owned()),
                    })
                    .collect();
                allowed.sort_by(|a, b| a.name().cmp(b.name()));
                allowed
            },
            allowed_exec_activities: self
                .allowed_exec_activities
                .iter()
                .map(|(name, digests)| {
                    let mut digests = digests.iter().map(ToString::to_string).collect::<Vec<_>>();
                    digests.sort();
                    (name.clone(), digests)
                })
                .collect(),
            outbound_http: self.outbound_http.allowed_hosts.clone(),
        }
    }
}

impl From<&AppPolicyV1> for rest_types::lifecycle::AppPolicyV1 {
    fn from(policy: &AppPolicyV1) -> Self {
        serde_json::from_value(serde_json::to_value(policy).expect("serialize app policy"))
            .expect("internal app policy must satisfy the OpenAPI contract")
    }
}

impl AppPolicyV1 {
    pub(crate) fn digest(&self) -> AppConfigDigest {
        let bytes = serde_json::to_vec(self).expect("app policy must serialize");
        AppConfigDigest(Digest(Sha256::digest(bytes).into()))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, derive_more::Display, serde_with::SerializeDisplay, JsonSchema,
)]
#[display("app-config:v1:{_0}")]
#[schemars(with = "String")]
pub(crate) struct AppConfigDigest(Digest);

#[cfg(test)]
mod tests {
    use super::AppConfigToml;

    #[test]
    fn cloud_integrations_are_open_typed_bindings_with_local_required_values() {
        let app: AppConfigToml = toml::from_str("[cloud.integrations]\nlimited={type='future-llm',env='OBELISK_TEST_INTEGRATION_URL_UNSET'}\n[public_env]\nOBELISK_TEST_INTEGRATION_URL_UNSET={}\n").unwrap();
        app.validate_cloud_integrations().unwrap();
        let error = app
            .validate_local_integrations(&super::StartupEnvVars::capture())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("limited")
                && error.contains("set OBELISK_TEST_INTEGRATION_URL_UNSET explicitly"),
            "{error}"
        );
        for text in [
            "[cloud.integrations]\nllm={type='llm',env='MISSING'}",
            "[cloud.integrations]\nllm={type='llm',env='URL'}\n[public_env]\nURL={type='internal',source='api-url'}",
            "[cloud.integrations]\nllm={type='llm',env='URL'}\nother={type='llm',env='URL'}\n[public_env]\nURL={}",
        ] {
            let app: AppConfigToml = toml::from_str(text).unwrap();
            assert!(app.validate_cloud_integrations().is_err(), "{text}");
        }
    }

    #[test]
    fn secret_types_default_to_env_and_internal_rejects_optional() {
        let config: AppConfigToml = toml::from_str(
            "[secrets]\nDEFAULT = {}\nEXPLICIT = { type = 'env', optional = true }\nINTERNAL = { type = 'internal' }",
        ).unwrap();
        let policy = serde_json::to_value(config.policy()).unwrap();
        assert_eq!(
            policy["secrets"]["DEFAULT"],
            serde_json::json!({"optional": false, "exposed_to": {}})
        );
        assert_eq!(
            policy["secrets"]["EXPLICIT"],
            serde_json::json!({"optional": true, "exposed_to": {}})
        );
        assert_eq!(
            policy["secrets"]["INTERNAL"],
            serde_json::json!({"type": "internal", "exposed_to": {}})
        );
        for optional in [false, true] {
            let config =
                format!("[secrets]\nTOKEN = {{ type = 'internal', optional = {optional} }}");
            assert!(toml::from_str::<AppConfigToml>(&config).is_err());
        }
    }

    #[test]
    fn digest_is_stable_for_name_order_and_comments() {
        let first: AppConfigToml = toml::from_str(
            r#"
            app_name = "first"
            [cloud]
            memory_mb = 512
            [public_env]
            B = {}
            A = {}
            TRACE_ID = { optional = true }
            [secrets]
            TOKEN = {}
        "#,
        )
        .unwrap();
        let second: AppConfigToml = toml::from_str(
            r#"
            # Same policy for another app.
            app_name = "second"
            [cloud]
            memory_mb = 1024
            [secrets]
            TOKEN = {}
            [public_env]
            TRACE_ID = { optional = true }
            A = {}
            B = {}
        "#,
        )
        .unwrap();
        assert_eq!(first.policy().digest(), second.policy().digest());
        assert!(
            first
                .policy()
                .digest()
                .to_string()
                .starts_with("app-config:v1:sha256:")
        );
        let policy = serde_json::to_value(first.policy()).unwrap();
        assert_eq!(
            policy["public_env"],
            serde_json::json!(["A", "B", { "name": "TRACE_ID", "optional": true }])
        );
    }

    #[test]
    fn generated_policy_preserves_wire_shape() {
        let app: AppConfigToml = toml::from_str(
            r#"
            [public_env]
            REGION = {}
            TRACE_ID = { optional = true }
            API_URL = { type = "internal", source = "api-url" }
            [secrets]
            TOKEN = {}
            OPTIONAL_TOKEN = { optional = true }
            KEY = { type = "internal" }
            [[outbound_http.allowed_host]]
            pattern = "api.example.com"
            methods = "*"
            [[outbound_http.allowed_host]]
            url_prefix = "https://example.com/api/"
            methods = ["GET"]
            secrets = ["TOKEN", { name = "OPTIONAL_TOKEN", optional = true }]
            replace_in = ["headers", "body"]
            [[outbound_http.allowed_host]]
            pattern = "unused.example.com"
        "#,
        )
        .unwrap();
        let policy = app.policy();
        let wire = rest_types::lifecycle::AppPolicyV1::from(&policy);
        assert_eq!(
            serde_json::to_value(wire).unwrap(),
            serde_json::to_value(policy).unwrap()
        );
    }

    #[test]
    fn public_env_requires_named_entries() {
        let err =
            toml::from_str::<AppConfigToml>("[public_env]\nallowed = ['REGION']").unwrap_err();
        assert!(err.to_string().contains("allowed"));
    }

    #[test]
    fn rejects_secret_source_alias() {
        assert!(toml::from_str::<AppConfigToml>("[secrets]\nTOKEN = { env = 'OTHER' }").is_err());
    }

    #[test]
    fn outbound_policy_cannot_interpolate_a_registered_secret() {
        let mut app: AppConfigToml = toml::from_str(
            r#"
            [secrets]
            TOKEN = {}
            [[outbound_http.allowed_host]]
            pattern = "${TOKEN}"
        "#,
        )
        .unwrap();
        let err = app
            .resolve_env_vars(&crate::config::env_var::StartupEnvVars::capture())
            .unwrap_err()
            .to_string();
        assert!(err.contains("registered secret `TOKEN`"));
    }
}
