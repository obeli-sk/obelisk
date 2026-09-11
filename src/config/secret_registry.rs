//! Operator-owned secret registry built from `server.toml`.

use crate::command::server::RuntimeConfigAvailability;
use crate::config::env_var::StartupEnvVars;
use anyhow::bail;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use wasm_workers::http_request_policy::SecretResolver;

pub(crate) const API_TOKEN: &str = "OBELISK_API_TOKEN";
pub(crate) const API_TOKEN_LEGACY: &str = "OBELISK__API__TOKEN";

/// Source of a secret in the `[secrets]` table.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, deny_unknown_fields)]
pub(crate) enum SecretSourceToml {
    /// Read the secret from a process environment variable at startup.
    Env { env: String },
}

/// The `[secrets]` table: logical name -> source.
pub(crate) type SecretsToml = IndexMap<String, SecretSourceToml>;

/// Public environment variables that deployments may read.
#[derive(Debug, Default, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicEnvToml {
    #[serde(default)]
    pub(crate) allowed: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("environment variable `{0}` is not available to deployments")]
pub(crate) struct SecretViolation(pub(crate) String);

#[derive(Debug, Clone)]
pub(crate) struct SecretRegistry {
    /// Logical secret name -> resolved value.
    values: HashMap<String, SecretString>,
    /// Used to reject `public_env_lookup`, contains both logical and `env` names.
    sensitive: HashSet<String>,
    /// Process environment variable names that deployments may read.
    public_allowed: HashSet<String>,
    /// Values captured for the public allowlist during startup.
    public_values: HashMap<String, String>,
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
            sensitive: HashSet::default(),
            public_allowed: HashSet::default(),
            public_values: HashMap::default(),
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
    pub(crate) fn public_env_lookup(&self, name: &str) -> Result<Option<String>, SecretViolation> {
        if self.sensitive.contains(name) || !self.public_allowed.contains(name) {
            Err(SecretViolation(name.to_owned()))
        } else {
            Ok(self.public_values.get(name).cloned())
        }
    }

    pub(crate) fn deployment_env_lookup(
        &self,
        name: &str,
    ) -> Result<Option<String>, SecretViolation> {
        self.public_env_lookup(name)
    }

    pub(crate) fn secret_lookup(&self, name: &str) -> Option<SecretString> {
        self.values.get(name).cloned()
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
            sensitive,
            public_allowed: HashSet::default(),
            public_values: HashMap::default(),
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

        // Always sensitive, even when the operator did not register them as secrets.
        let mut sensitive = HashSet::from([API_TOKEN_LEGACY.to_string(), API_TOKEN.to_string()]);

        let mut missing_env_vars = BTreeSet::new();
        let mut secret_audit = std::collections::BTreeMap::new();
        for (logical_name, source) in secrets {
            match source {
                SecretSourceToml::Env { env } => {
                    let present = env_vars.lookup(&env).is_some()
                        || was_legacy_token_wiped.is_some_and(|_| env == API_TOKEN_LEGACY);
                    secret_audit.insert(
                        logical_name.clone(),
                        serde_json::json!({"env": env, "present": present}),
                    );
                    let value = if let Some(value) = env_vars.lookup(&env) {
                        SecretString::from(value)
                    } else if let Some(value) = was_legacy_token_wiped
                        && env == API_TOKEN_LEGACY
                    {
                        // backcompat: avoid failing here if [[secrets]] contains the token and it was wiped already.
                        value.clone()
                    } else {
                        missing_env_vars.insert(env.clone());
                        SecretString::from(String::new())
                    };
                    values.insert(logical_name.clone(), value);
                    sensitive.insert(env);
                    sensitive.insert(logical_name);
                }
            }
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
            sensitive,
            public_allowed,
            public_values,
            environment_audit: serde_json::json!({
                "public_env": public_env_audit,
                "secrets": secret_audit,
            }),
        })
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
            SecretSourceToml::Env {
                env: SRC.to_string(),
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
                    "LOGICAL": {"env": SRC, "present": true},
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
            SecretSourceToml::Env {
                env: "OBELISK_TEST_DEFINITELY_UNSET_2B9C".to_string(),
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
