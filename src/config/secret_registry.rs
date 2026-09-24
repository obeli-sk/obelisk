//! App-owned secret registry built from `app.toml`.

use crate::command::server::RuntimeConfigAvailability;
use crate::config::env_var::StartupEnvVars;
use anyhow::bail;
use concepts::component_id::SecretExposureDigest;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use worker_common::SecretResolver;

pub(crate) const API_TOKEN: &str = "OBELISK_API_TOKEN";
pub(crate) const API_TOKEN_LEGACY: &str = "OBELISK__API__TOKEN";

#[derive(Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
#[schemars(with = "String")]
pub(crate) struct SecretExposureDigests(Vec<SecretExposureDigest>);

impl SecretExposureDigests {
    #[cfg(test)]
    pub(crate) fn new(digests: Vec<SecretExposureDigest>) -> Self {
        Self(digests)
    }

    pub(crate) fn contains(&self, digest: &SecretExposureDigest) -> bool {
        self.0.contains(digest)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &SecretExposureDigest> {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for SecretExposureDigests {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(SecretExposureDigest),
            Many(Vec<SecretExposureDigest>),
        }
        let values = match OneOrMany::deserialize(deserializer)? {
            OneOrMany::One(value) => vec![value],
            OneOrMany::Many(values) => values,
        };
        let mut seen = HashSet::new();
        for value in &values {
            if !seen.insert(value.clone()) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate secret exposure digest `{value}`"
                )));
            }
        }
        Ok(Self(values))
    }
}

impl Serialize for SecretExposureDigests {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_slice() {
            [value] => value.serialize(serializer),
            values => values.serialize(serializer),
        }
    }
}

/// Source and exposure authorization for a secret in the `[secrets]` table.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecretConfigToml {
    /// Internal source override used by older tests; app.toml always uses the secret name.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) env: String,
    /// Allow `env` to be unset; the secret is then absent and only optional references accept it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) optional: bool,
    #[serde(default)]
    pub(crate) exposed_to: BTreeMap<String, SecretExposureDigests>,
}

/// The `[secrets]` table: logical name -> source and exposure authorization.
pub(crate) type SecretsToml = IndexMap<String, SecretConfigToml>;

/// Public environment variables that deployments may read.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicEnvToml {
    #[serde(default)]
    pub(crate) allowed: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublicEnvViolation {
    #[error(
        "environment variable `{0}` is not declared in app.toml `[public_env].allowed`; add it, or remove the deployment reference:\n\n[public_env]\nallowed = [\"{0}\"]"
    )]
    Undeclared(String),
    #[error(
        "environment variable `{0}` is registered as sensitive and cannot be exposed to deployments as plaintext; reference its logical secret name through an activity secret mechanism, or remove the plaintext reference"
    )]
    Sensitive(String),
}

#[derive(Debug, Clone)]
pub(crate) struct SecretRegistry {
    /// Logical secret name -> resolved value.
    values: HashMap<String, SecretString>,
    /// Optional secrets whose source was unset at startup.
    absent: HashSet<String>,
    /// Used to reject `public_env_lookup`, contains both logical and `env` names.
    sensitive: HashSet<String>,
    /// Process environment variable names that deployments may read.
    public_allowed: HashSet<String>,
    /// Values captured for the public allowlist during startup.
    public_values: HashMap<String, String>,
    exposure_grants: HashMap<String, BTreeMap<String, SecretExposureDigests>>,
    environment_audit: serde_json::Value,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum EnvVarSecretsCleanup {
    Wipe,
    Noop,
}

impl SecretRegistry {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        SecretRegistry {
            values: HashMap::default(),
            absent: HashSet::default(),
            sensitive: HashSet::default(),
            public_allowed: HashSet::default(),
            public_values: HashMap::default(),
            exposure_grants: HashMap::default(),
            environment_audit: serde_json::json!({
                "public_env": {},
                "secrets": {},
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn empty_with_public_env(allowed: impl IntoIterator<Item = String>) -> Self {
        Self {
            public_allowed: allowed.into_iter().collect(),
            ..Self::empty()
        }
    }

    /// Look up an operator-allowed public value captured during startup.
    pub(crate) fn public_env_lookup(
        &self,
        name: &str,
    ) -> Result<Option<String>, PublicEnvViolation> {
        if self.sensitive.contains(name) {
            Err(PublicEnvViolation::Sensitive(name.to_owned()))
        } else if !self.public_allowed.contains(name) {
            Err(PublicEnvViolation::Undeclared(name.to_owned()))
        } else {
            Ok(self.public_values.get(name).cloned())
        }
    }

    pub(crate) fn deployment_env_lookup(
        &self,
        name: &str,
    ) -> Result<Option<String>, PublicEnvViolation> {
        self.public_env_lookup(name)
    }

    pub(crate) fn public_env_is_allowed(&self, name: &str) -> bool {
        self.public_allowed.contains(name) && !self.sensitive.contains(name)
    }

    pub(crate) fn public_env_is_sensitive(&self, name: &str) -> bool {
        self.sensitive.contains(name)
    }

    pub(crate) fn allow_unavailable_public_env(
        mut self,
        names: impl IntoIterator<Item = String>,
    ) -> Self {
        self.public_allowed.extend(names);
        self
    }

    /// `None` when the secret is unregistered or registered as optional and absent.
    pub(crate) fn secret_lookup(&self, name: &str) -> Option<SecretString> {
        self.values.get(name).cloned()
    }

    pub(crate) fn is_registered(&self, name: &str) -> bool {
        self.values.contains_key(name) || self.absent.contains(name)
    }

    /// Registered as optional and its source was unset at startup.
    pub(crate) fn is_absent(&self, name: &str) -> bool {
        self.absent.contains(name)
    }

    pub(crate) fn environment_audit(&self) -> serde_json::Value {
        self.environment_audit.clone()
    }

    /// Build a registry directly from name -> value pairs, without touching the process
    /// environment. Every provided name is treated as sensitive. Test-only.
    #[cfg(test)]
    pub(crate) fn from_test_values(
        values: impl IntoIterator<Item = (String, SecretString)>,
    ) -> Self {
        let values: HashMap<String, SecretString> = values.into_iter().collect();
        let sensitive = values.keys().cloned().collect();
        Self {
            values,
            absent: HashSet::default(),
            sensitive,
            public_allowed: HashSet::default(),
            public_values: HashMap::default(),
            exposure_grants: HashMap::default(),
            environment_audit: serde_json::json!({
                "public_env": {},
                "secrets": {},
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_public_env(mut self, allowed: impl IntoIterator<Item = String>) -> Self {
        self.public_allowed = allowed.into_iter().collect();
        self.public_values = self
            .public_allowed
            .iter()
            .filter_map(|name| std::env::var(name).ok().map(|value| (name.clone(), value)))
            .collect();
        self
    }

    #[cfg(test)]
    pub(crate) fn with_exposure_grants(
        mut self,
        secret_name: impl Into<String>,
        grants: impl IntoIterator<Item = (String, SecretExposureDigest)>,
    ) -> Self {
        self.exposure_grants.insert(
            secret_name.into(),
            grants
                .into_iter()
                .map(|(name, digest)| (name, SecretExposureDigests::new(vec![digest])))
                .collect(),
        );
        self
    }

    #[cfg(test)]
    pub(crate) fn with_exposure_config(mut self, secrets: &SecretsToml) -> Self {
        self.exposure_grants = secrets
            .iter()
            .map(|(name, config)| (name.clone(), config.exposed_to.clone()))
            .collect();
        self
    }

    /// Build the registry from the resolved server configuration
    ///
    /// If [`EnvVarCleanupStrategy::Wipe`] is set, MUST run during early, single-threaded startup, before the tokio runtime is
    /// constructed: it calls `std::env::remove_var`, which is only sound without concurrent readers.
    pub(crate) fn resolve(
        secrets: SecretsToml,
        public_env: PublicEnvToml,
        env_var_cleanup: EnvVarSecretsCleanup,
        runtime_config_availability: RuntimeConfigAvailability,
        was_legacy_token_wiped: Option<&SecretString>,
        env_vars: &StartupEnvVars,
    ) -> anyhow::Result<Self> {
        let mut values = HashMap::new();
        let mut absent = HashSet::new();

        // Always sensitive, even when the operator did not register them as secrets.
        let mut sensitive = HashSet::from([API_TOKEN_LEGACY.to_string(), API_TOKEN.to_string()]);

        let mut missing_env_vars = BTreeSet::new();
        let mut secret_audit = std::collections::BTreeMap::new();
        let mut exposure_grants = HashMap::new();
        for (logical_name, config) in secrets {
            let SecretConfigToml {
                env,
                optional,
                exposed_to,
            } = config;
            let env = if env.is_empty() {
                logical_name.clone()
            } else {
                env
            };
            let present = env_vars.lookup(&env).is_some()
                || was_legacy_token_wiped.is_some_and(|_| env == API_TOKEN_LEGACY);
            secret_audit.insert(
                logical_name.clone(),
                serde_json::json!({
                    "present": present,
                    "optional": optional,
                    "exposed_to": exposed_to,
                }),
            );
            if let Some(value) = env_vars.lookup(&env) {
                values.insert(logical_name.clone(), SecretString::from(value));
            } else if let Some(value) = was_legacy_token_wiped
                && env == API_TOKEN_LEGACY
            {
                // backcompat: avoid failing here if [[secrets]] contains the token and it was wiped already.
                values.insert(logical_name.clone(), value.clone());
            } else if optional {
                absent.insert(logical_name.clone());
            } else {
                missing_env_vars.insert(env.clone());
                values.insert(logical_name.clone(), SecretString::from(String::new()));
            }
            exposure_grants.insert(logical_name.clone(), exposed_to);
            sensitive.insert(env);
            sensitive.insert(logical_name);
        }
        if runtime_config_availability == RuntimeConfigAvailability::Strict
            && !missing_env_vars.is_empty()
        {
            bail!("secrets sourced from environment variables are not set: {missing_env_vars:?}");
        }
        if env_var_cleanup == EnvVarSecretsCleanup::Wipe {
            for src in &sensitive {
                // SAFETY: `resolve_and_wipe` runs during single-threaded startup, before the
                // tokio runtime is constructed, so there are no concurrent environment readers.
                unsafe { std::env::remove_var(src) };
            }
        }

        let public_allowed: HashSet<_> = public_env.allowed.into_iter().collect();
        let public_values: HashMap<String, String> = public_allowed
            .iter()
            .filter_map(|name| env_vars.lookup(name).map(|value| (name.clone(), value)))
            .collect();
        let public_env_audit: std::collections::BTreeMap<_, _> = public_allowed
            .iter()
            .map(|name| (name.clone(), public_values.contains_key(name)))
            .collect();
        Ok(Self {
            values,
            absent,
            sensitive,
            public_allowed,
            public_values,
            exposure_grants,
            environment_audit: serde_json::json!({
                "public_env": public_env_audit,
                "secrets": secret_audit,
            }),
        })
    }

    pub(crate) fn exposure_allowed(
        &self,
        secret_name: &str,
        component_name: &str,
        digest: &SecretExposureDigest,
    ) -> bool {
        self.exposure_grants
            .get(secret_name)
            .and_then(|components| components.get(component_name))
            .is_some_and(|digests| digests.contains(digest))
    }
}

/// A component-scoped view over the operator [`SecretRegistry`].
///
/// Handed to activities and webhooks so they resolve secret values *by name, on
/// demand* (at execution-run policy build / process spawn), never baking values
/// into long-lived verified configs. Lookups are restricted to the subset of
/// names the component declared: an undeclared name resolves to `None` even if
/// the operator registered it, so a component can only reach the secrets it
/// asked for. Env-backed today via the shared registry, Vault-backed later
/// without changing this boundary.
#[derive(Debug, Clone)]
pub(crate) struct RestrictedSecretRegistry {
    registry: Arc<SecretRegistry>,
    allowed: Arc<HashSet<String>>,
}

impl RestrictedSecretRegistry {
    pub(crate) fn new(
        registry: Arc<SecretRegistry>,
        allowed: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            registry,
            allowed: Arc::new(allowed.into_iter().collect()),
        }
    }
}

impl SecretResolver for RestrictedSecretRegistry {
    fn secret_lookup(&self, name: &str) -> Option<SecretString> {
        if self.allowed.contains(name) {
            self.registry.secret_lookup(name)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::env_var::StartupEnvVars;
    use secrecy::ExposeSecret as _;

    // Security: Default server.toml does not make the obelisk token available.
    #[test]
    fn default_does_not_make_token_available() {
        let env_vars = StartupEnvVars::capture();
        let registry = SecretRegistry::resolve(
            SecretsToml::new(),
            PublicEnvToml::default(),
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap();

        assert!(registry.values.is_empty());
        assert_eq!(
            HashSet::from([API_TOKEN_LEGACY.to_string(), API_TOKEN.to_string()]),
            registry.sensitive
        );
    }

    #[test]
    fn resolve_reads_value_wipes_source_and_rejects_lookup() {
        // A source name distinct from the logical name exercises the rename mapping.
        const SRC: &str = "OBELISK_TEST_SECRET_SRC_7A3F";
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::set_var(SRC, "s3cret") };
        let env_vars = StartupEnvVars::capture();

        let mut secrets = SecretsToml::new();
        secrets.insert(
            "LOGICAL".to_string(),
            SecretConfigToml {
                env: SRC.to_string(),
                optional: false,
                exposed_to: BTreeMap::new(),
            },
        );
        let registry = SecretRegistry::resolve(
            secrets,
            PublicEnvToml::default(),
            EnvVarSecretsCleanup::Wipe,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap();

        // Value is available under the logical name only.
        assert_eq!(
            registry.secret_lookup("LOGICAL").unwrap().expose_secret(),
            "s3cret"
        );
        assert!(registry.secret_lookup(SRC).is_none());
        assert_eq!(
            registry.environment_audit(),
            serde_json::json!({
                "public_env": {},
                "secrets": {
                    "LOGICAL": {"present": true, "optional": false, "exposed_to": {}},
                },
            })
        );

        // The source variable was wiped.
        assert!(std::env::var(SRC).is_err());

        // Both the logical name and the source name are sensitive: public lookup rejects them.
        assert!(registry.public_env_lookup("LOGICAL").is_err());
        assert!(registry.public_env_lookup(SRC).is_err());
        // Public values must be explicitly allowlisted.
        assert!(
            registry
                .public_env_lookup("OBELISK_TEST_UNREGISTERED_X")
                .is_err()
        );
    }

    #[test]
    fn missing_source_is_a_startup_error() {
        let env_vars = StartupEnvVars::capture();
        let mut secrets = SecretsToml::new();
        secrets.insert(
            "LOGICAL".to_string(),
            SecretConfigToml {
                env: "OBELISK_TEST_DEFINITELY_UNSET_2B9C".to_string(),
                optional: false,
                exposed_to: BTreeMap::new(),
            },
        );
        let err = SecretRegistry::resolve(
            secrets,
            PublicEnvToml::default(),
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not set"), "unexpected error: {err}");
        assert!(
            err.contains("OBELISK_TEST_DEFINITELY_UNSET_2B9C"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unset_optional_source_is_absent_not_empty() {
        const OPTIONAL: &str = "OBELISK_TEST_OPTIONAL_UNSET_5E1A";
        const EMPTY: &str = "OBELISK_TEST_OPTIONAL_EMPTY_5E1A";
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::set_var(EMPTY, "") };
        let env_vars = StartupEnvVars::capture();
        let mut secrets = SecretsToml::new();
        for name in [OPTIONAL, EMPTY] {
            secrets.insert(
                name.to_string(),
                SecretConfigToml {
                    env: name.to_string(),
                    optional: true,
                    exposed_to: BTreeMap::new(),
                },
            );
        }
        let registry = SecretRegistry::resolve(
            secrets,
            PublicEnvToml::default(),
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap();

        assert!(registry.is_registered(OPTIONAL));
        assert!(registry.is_absent(OPTIONAL));
        assert!(registry.secret_lookup(OPTIONAL).is_none());
        // An empty value is present.
        assert!(!registry.is_absent(EMPTY));
        assert_eq!(registry.secret_lookup(EMPTY).unwrap().expose_secret(), "");
        assert_eq!(
            registry.environment_audit()["secrets"][OPTIONAL],
            serde_json::json!({"present": false, "optional": true, "exposed_to": {}})
        );
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::remove_var(EMPTY) };
    }

    #[test]
    fn public_allowlist_does_not_require_values_and_rejects_other_variables() {
        const ALLOWED: &str = "OBELISK_TEST_OPTIONAL_PUBLIC_3C8D";
        const DENIED: &str = "OBELISK_TEST_DENIED_PUBLIC_3C8D";
        let env_vars = StartupEnvVars::capture();
        let registry = SecretRegistry::resolve(
            SecretsToml::new(),
            PublicEnvToml {
                allowed: vec![ALLOWED.to_string()],
            },
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap();

        assert_eq!(registry.deployment_env_lookup(ALLOWED).unwrap(), None);
        assert!(registry.deployment_env_lookup(DENIED).is_err());
        assert!(registry.deployment_env_lookup("PATH").is_err());
    }

    #[test]
    fn public_values_are_captured_when_registry_is_resolved() {
        const ALLOWED: &str = "OBELISK_TEST_CAPTURED_PUBLIC_4D9E";
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::set_var(ALLOWED, "initial") };
        let env_vars = StartupEnvVars::capture();
        let registry = SecretRegistry::resolve(
            SecretsToml::new(),
            PublicEnvToml {
                allowed: vec![ALLOWED.to_string()],
            },
            EnvVarSecretsCleanup::Noop,
            RuntimeConfigAvailability::Strict,
            None,
            &env_vars,
        )
        .unwrap();

        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::set_var(ALLOWED, "changed") };
        assert_eq!(
            registry.deployment_env_lookup(ALLOWED).unwrap().as_deref(),
            Some("initial")
        );
        assert_eq!(
            registry.environment_audit()["public_env"][ALLOWED],
            serde_json::json!(true)
        );
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::remove_var(ALLOWED) };
    }
}
