//! Transport-independent types generated from Obelisk's lifecycle OpenAPI contract and
//! app config schema.

#[allow(clippy::all, clippy::pedantic)]
mod generated {
    include!("generated.rs");
}

pub use generated::*;

use std::collections::{BTreeMap, BTreeSet};

/// Invalid app config detected while deriving its policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfigError(String);

impl std::fmt::Display for AppConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AppConfigError {}

macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(AppConfigError(format!($($message)+)));
        }
    };
}

impl AppConfigToml {
    /// Checks the constraints the generated types leave out.
    pub fn validate(&self) -> Result<(), AppConfigError> {
        let name = self.app_name.as_bytes();
        let allowed = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
        ensure!(
            name.iter().all(|b| allowed(b) || *b == b'-')
                && name.first().is_some_and(allowed)
                && name.last().is_some_and(allowed),
            "app_name `{}` must be lowercase alphanumerics and inner hyphens",
            *self.app_name
        );
        self.validate_cloud_integrations()
    }

    /// Checks that each cloud integration supplies a distinct environment-backed public env.
    pub fn validate_cloud_integrations(&self) -> Result<(), AppConfigError> {
        let mut supplied = BTreeSet::new();
        for (name, integration) in &self.cloud.integrations {
            ensure!(
                !name.is_empty() && !integration.type_.is_empty(),
                "Cloud integration name and type must not be empty"
            );
            ensure!(
                matches!(
                    self.public_env.get(&integration.env),
                    Some(PublicEnvConfigToml::EnvPublicEnvConfigToml(_))
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

    /// Derives the policy Obelisk reports for this app config at `/v1/app-policy`.
    pub fn policy(&self) -> Result<AppPolicyV1, AppConfigError> {
        self.validate()?;
        let secrets = self
            .secrets
            .iter()
            .map(|(name, secret)| {
                let value = match secret {
                    SecretConfigToml::EnvSecretConfigToml(config) => {
                        AppPolicyV1SecretsValue::Variant1 {
                            exposed_to: exposed_to(&config.exposed_to)?,
                            optional: config.optional.unwrap_or_default(),
                        }
                    }
                    SecretConfigToml::InternalSecretConfigToml(config) => {
                        AppPolicyV1SecretsValue::Variant0 {
                            exposed_to: exposed_to(&config.exposed_to)?,
                            type_: AppPolicyV1SecretsValueVariant0Type::Internal,
                        }
                    }
                };
                Ok((name.clone(), value))
            })
            .collect::<Result<_, AppConfigError>>()?;
        // BTreeMap iteration keeps public env sorted by name.
        let public_env = self
            .public_env
            .iter()
            .map(|(name, config)| match config {
                PublicEnvConfigToml::InternalPublicEnvConfigToml(config) => {
                    AppPolicyV1PublicEnvItem::Variant2 {
                        name: name.clone(),
                        source: match config.source {
                            InternalPublicEnvSource::ApiUrl => {
                                AppPolicyV1PublicEnvItemVariant2Source::ApiUrl
                            }
                            InternalPublicEnvSource::WebuiUrl => {
                                AppPolicyV1PublicEnvItemVariant2Source::WebuiUrl
                            }
                        },
                        type_: AppPolicyV1PublicEnvItemVariant2Type::Internal,
                    }
                }
                PublicEnvConfigToml::EnvPublicEnvConfigToml(config) if config.optional => {
                    AppPolicyV1PublicEnvItem::Variant1 {
                        name: name.clone(),
                        optional: true,
                    }
                }
                PublicEnvConfigToml::EnvPublicEnvConfigToml(_) => {
                    AppPolicyV1PublicEnvItem::Variant0(name.clone())
                }
            })
            .collect();
        let allowed_exec_activities = self
            .allowed_exec_activities
            .iter()
            .map(|(name, digests)| Ok((name.clone(), sorted_digests(digests)?)))
            .collect::<Result<_, AppConfigError>>()?;
        Ok(AppPolicyV1 {
            allowed_exec_activities,
            format: AppPolicyV1Format::ObeliskAppConfigV1,
            outbound_http: self
                .outbound_http
                .allowed_host
                .iter()
                .map(outbound_http_item)
                .collect(),
            public_env,
            secrets,
        })
    }
}

fn exposed_to(
    exposed_to: &BTreeMap<String, SecretExposureDigests>,
) -> Result<BTreeMap<String, Vec<String>>, AppConfigError> {
    exposed_to
        .iter()
        .map(|(component, digests)| Ok((component.clone(), sorted_digests(digests)?)))
        .collect()
}

/// Canonical `sha256:<lowercase hex>` digests, sorted, rejecting duplicates.
fn sorted_digests(digests: &SecretExposureDigests) -> Result<Vec<String>, AppConfigError> {
    let digests = match digests {
        SecretExposureDigests::String(digest) => std::slice::from_ref(digest),
        SecretExposureDigests::Array(digests) => digests.as_slice(),
    };
    let mut canonical = BTreeSet::new();
    for digest in digests {
        let hex = digest.strip_prefix("sha256:").unwrap_or_default();
        ensure!(
            hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid secret exposure digest `{digest}`"
        );
        ensure!(
            canonical.insert(format!("sha256:{}", hex.to_ascii_lowercase())),
            "duplicate secret exposure digest `{digest}`"
        );
    }
    Ok(canonical.into_iter().collect())
}

fn outbound_http_item(host: &AllowedHostToml) -> AppPolicyV1OutboundHttpItem {
    let methods = |methods: &Option<MethodsInput>| {
        methods.as_ref().map(|methods| match methods {
            MethodsInput::MethodsInputStar(MethodsInputStar::X) => {
                AppPolicyV1OutboundHttpItemMethods::String(
                    AppPolicyV1OutboundHttpItemMethodsString::X,
                )
            }
            MethodsInput::Array(methods) => {
                AppPolicyV1OutboundHttpItemMethods::Array(methods.clone())
            }
        })
    };
    let replace_in = |replace_in: &[ReplaceIn]| {
        replace_in
            .iter()
            .map(|location| match location {
                ReplaceIn::Headers => AppPolicyV1OutboundHttpItemReplaceInItem::Headers,
                ReplaceIn::Body => AppPolicyV1OutboundHttpItemReplaceInItem::Body,
                ReplaceIn::Params => AppPolicyV1OutboundHttpItemReplaceInItem::Params,
            })
            .collect()
    };
    let secrets = |secrets: &[SecretRef]| {
        secrets
            .iter()
            .map(|secret| match secret {
                SecretRef::String(name)
                | SecretRef::SecretRefTableToml(SecretRefTableToml {
                    name,
                    optional: false,
                }) => AppPolicyV1OutboundHttpItemSecretsItem::String(name.clone()),
                SecretRef::SecretRefTableToml(SecretRefTableToml {
                    name,
                    optional: true,
                }) => AppPolicyV1OutboundHttpItemSecretsItem::Object {
                    name: name.clone(),
                    optional: true,
                },
            })
            .collect()
    };
    match host {
        AllowedHostToml::Variant0 {
            methods: host_methods,
            pattern,
            replace_in: host_replace_in,
            request_url_regex,
            secrets: host_secrets,
        } => AppPolicyV1OutboundHttpItem {
            methods: methods(host_methods),
            pattern: Some(pattern.clone()),
            replace_in: replace_in(host_replace_in),
            request_url_regex: request_url_regex.clone(),
            secrets: secrets(host_secrets),
            url_prefix: None,
            url_regex_suffix: None,
        },
        AllowedHostToml::Variant1 {
            methods: host_methods,
            replace_in: host_replace_in,
            request_url_regex: (),
            secrets: host_secrets,
            url_prefix,
            url_regex_suffix,
        } => AppPolicyV1OutboundHttpItem {
            methods: methods(host_methods),
            pattern: None,
            replace_in: replace_in(host_replace_in),
            request_url_regex: None,
            secrets: secrets(host_secrets),
            url_prefix: Some(url_prefix.clone()),
            url_regex_suffix: url_regex_suffix.clone(),
        },
    }
}
