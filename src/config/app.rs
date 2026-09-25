use crate::config::env_var::{
    StartupEnvVars, collect_env_var_references, interpolate_startup_env_vars,
};
use crate::config::secret_registry::{PublicEnvRef, PublicEnvToml, SecretsToml};
use crate::config::server::{AllowExecActivities, OutboundHttpToml};
use anyhow::{Context as _, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

#[derive(Debug, Default, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppConfigToml {
    #[serde(default)]
    pub(crate) app_name: Option<String>,
    #[serde(default)]
    pub(crate) secrets: SecretsToml,
    #[serde(default)]
    pub(crate) public_env: PublicEnvToml,
    #[serde(default)]
    pub(crate) allowed_exec_activities: AllowExecActivities,
    #[serde(default)]
    pub(crate) outbound_http: OutboundHttpToml,
}

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppPolicyV1 {
    format: AppPolicyFormatV1,
    secrets: std::collections::BTreeMap<String, AppPolicySecretV1>,
    public_env: Vec<PublicEnvRef>,
    allowed_exec_activities: std::collections::BTreeMap<String, Vec<String>>,
    outbound_http: Vec<crate::config::deployment::AllowedHostToml>,
}

#[derive(JsonSchema, Serialize)]
enum AppPolicyFormatV1 {
    #[serde(rename = "obelisk-app-config-v1")]
    V1,
}

pub(crate) fn validate_app_name(name: &str) -> anyhow::Result<()> {
    ensure!(
        name.len() <= 63
            && !name.is_empty()
            && !name.starts_with('-')
            && !name.ends_with('-')
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid app name `{name}`: expected a DNS label (1-63 lowercase ASCII letters, digits, or interior hyphens)"
    );
    Ok(())
}

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct AppPolicySecretV1 {
    optional: bool,
    exposed_to: std::collections::BTreeMap<String, Vec<String>>,
}

impl AppConfigToml {
    pub(crate) fn resolve_env_vars(&mut self, env_vars: &StartupEnvVars) -> anyhow::Result<()> {
        for allowed_host in &mut self.outbound_http.allowed_hosts {
            let mut references = std::collections::BTreeSet::new();
            collect_env_var_references(&allowed_host.pattern, &mut references);
            if let Some(regex) = &allowed_host.request_url_regex {
                collect_env_var_references(regex, &mut references);
            }
            for name in references {
                ensure!(
                    !self.secrets.contains_key(&name),
                    "app.toml outbound HTTP policy cannot interpolate registered secret `{name}`"
                );
            }
            allowed_host.pattern = interpolate_startup_env_vars(&allowed_host.pattern, env_vars)?;
            if let Some(regex) = &mut allowed_host.request_url_regex {
                *regex = interpolate_startup_env_vars(regex, env_vars)?;
            }
        }
        Ok(())
    }
    pub(crate) fn effective_name(&self) -> anyhow::Result<String> {
        let name = std::env::var("OBELISK_APP_NAME")
            .ok()
            .or_else(|| self.app_name.clone())
            .context("app name is required: set `app_name` in app.toml or `OBELISK_APP_NAME`")?;
        validate_app_name(&name)?;
        Ok(name)
    }

    pub(crate) fn policy_json(&self) -> anyhow::Result<Vec<u8>> {
        let policy = AppPolicyV1 {
            format: AppPolicyFormatV1::V1,
            secrets: self
                .secrets
                .iter()
                .map(|(name, secret)| {
                    (
                        name.clone(),
                        AppPolicySecretV1 {
                            optional: secret.optional,
                            exposed_to: secret
                                .exposed_to
                                .iter()
                                .map(|(component, digests)| {
                                    let mut values =
                                        digests.iter().map(ToString::to_string).collect::<Vec<_>>();
                                    values.sort();
                                    (component.clone(), values)
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            public_env: {
                let mut allowed: Vec<_> = self
                    .public_env
                    .iter()
                    .map(|(name, config)| {
                        if config.optional {
                            PublicEnvRef::Config {
                                name: name.to_owned(),
                                optional: true,
                            }
                        } else {
                            PublicEnvRef::Required(name.to_owned())
                        }
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
        };
        serde_json::to_vec(&policy).context("cannot serialize app policy")
    }

    pub(crate) fn digest(&self) -> anyhow::Result<String> {
        let bytes = self.policy_json()?;
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        Ok(format!(
            "app-config:v1:sha256:{}",
            concepts::component_id::Digest(hash)
                .to_string()
                .trim_start_matches("sha256:")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::AppConfigToml;

    #[test]
    fn digest_is_stable_for_name_order_and_comments() {
        let first: AppConfigToml = toml::from_str(
            r#"
            app_name = "one"
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
            app_name = "two"
            [secrets]
            TOKEN = {}
            [public_env]
            TRACE_ID = { optional = true }
            A = {}
            B = {}
        "#,
        )
        .unwrap();
        assert_eq!(first.digest().unwrap(), second.digest().unwrap());
        assert!(first.digest().unwrap().starts_with("app-config:v1:sha256:"));
        let policy: serde_json::Value =
            serde_json::from_slice(&first.policy_json().unwrap()).unwrap();
        assert_eq!(
            policy["public_env"],
            serde_json::json!(["A", "B", { "name": "TRACE_ID", "optional": true }])
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
        let err =
            toml::from_str::<AppConfigToml>("[secrets]\nTOKEN = { env = 'OTHER' }").unwrap_err();
        assert!(err.to_string().contains("unknown field `env`"));
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
