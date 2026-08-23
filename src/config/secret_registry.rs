//! Operator-owned secret registry built from `server.toml`.

use crate::command::server::RuntimeConfigAvailability;
use anyhow::bail;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use wasm_workers::http_request_policy::SecretResolver;

pub(crate) const API_TOKEN_CLIENT: &str = "OBELISK_API_TOKEN";
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

#[derive(Debug, thiserror::Error)]
#[error("attempted to load secret `{0}` as an environment variable")]
pub(crate) struct SecretViolation(pub(crate) String);

#[derive(Debug, Clone)]
pub(crate) struct SecretRegistry {
    /// Logical secret name -> resolved value.
    values: HashMap<String, SecretString>,
    /// Used to reject `public_env_lookup`, contains both logical and `env` names.
    sensitive: HashSet<String>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum EnvVarCleanupStrategy {
    Wipe,
    Noop,
}

impl SecretRegistry {
    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        SecretRegistry {
            values: HashMap::default(),
            sensitive: HashSet::default(),
        }
    }

    /// Public (non-secret) environment lookup. Rejects the name when it is sensitive
    /// (a secret's logical name or a secret's source env var name); otherwise reads it
    /// from the process environment, returning `None` when unset.
    pub(crate) fn public_env_lookup(&self, name: &str) -> Result<Option<String>, SecretViolation> {
        if self.sensitive.contains(name) {
            Err(SecretViolation(name.to_owned()))
        } else {
            Ok(std::env::var(name).ok())
        }
    }

    pub(crate) fn secret_lookup(&self, name: &str) -> Option<SecretString> {
        self.values.get(name).cloned()
    }

    /// Build a registry directly from name -> value pairs, without touching the process
    /// environment. Every provided name is treated as sensitive. Test-only.
    #[cfg(test)]
    pub(crate) fn from_test_values(
        values: impl IntoIterator<Item = (String, SecretString)>,
    ) -> Self {
        let values: HashMap<String, SecretString> = values.into_iter().collect();
        let sensitive = values.keys().cloned().collect();
        Self { values, sensitive }
    }

    /// Build the registry from the resolved server configuration
    ///
    /// If [`EnvVarCleanupStrategy::Wipe`] is set, MUST run during early, single-threaded startup, before the tokio runtime is
    /// constructed: it calls `std::env::remove_var`, which is only sound without concurrent readers.
    pub(crate) fn resolve(
        secrets: SecretsToml,
        env_var_cleanup: EnvVarCleanupStrategy,
        runtime_config_availability: RuntimeConfigAvailability,
    ) -> anyhow::Result<Self> {
        let mut values = HashMap::new();

        // Always sensitive, even when the operator did not register them as secrets.
        let mut sensitive =
            HashSet::from([API_TOKEN_LEGACY.to_string(), API_TOKEN_CLIENT.to_string()]);

        let mut missing_env_vars = BTreeSet::new();
        for (logical_name, source) in secrets {
            match source {
                SecretSourceToml::Env { env } => {
                    let value = if let Ok(value) = std::env::var(&env) {
                        value
                    } else {
                        missing_env_vars.insert(env.clone());
                        String::new()
                    };
                    values.insert(logical_name.clone(), SecretString::from(value));
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
        if env_var_cleanup == EnvVarCleanupStrategy::Wipe {
            for src in &sensitive {
                // SAFETY: `resolve_and_wipe` runs during single-threaded startup, before the
                // tokio runtime is constructed, so there are no concurrent environment readers.
                unsafe { std::env::remove_var(src) };
            }
        }

        Ok(Self { values, sensitive })
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
    use secrecy::ExposeSecret as _;

    #[test]
    fn resolve_reads_value_wipes_source_and_rejects_lookup() {
        // A source name distinct from the logical name exercises the rename mapping.
        const SRC: &str = "OBELISK_TEST_SECRET_SRC_7A3F";
        // SAFETY: test-only, unique var name, no concurrent access.
        unsafe { std::env::set_var(SRC, "s3cret") };

        let mut secrets = SecretsToml::new();
        secrets.insert(
            "LOGICAL".to_string(),
            SecretSourceToml::Env {
                env: SRC.to_string(),
            },
        );
        let registry = SecretRegistry::resolve(
            secrets,
            EnvVarCleanupStrategy::Wipe,
            RuntimeConfigAvailability::Strict,
        )
        .unwrap();

        // Value is available under the logical name only.
        assert_eq!(
            registry.secret_lookup("LOGICAL").unwrap().expose_secret(),
            "s3cret"
        );
        assert!(registry.secret_lookup(SRC).is_none());

        // The source variable was wiped.
        assert!(std::env::var(SRC).is_err());

        // Both the logical name and the source name are sensitive: public lookup rejects them.
        assert!(registry.public_env_lookup("LOGICAL").is_err());
        assert!(registry.public_env_lookup(SRC).is_err());
        // An unregistered name still resolves normally (here: unset -> None).
        assert!(
            registry
                .public_env_lookup("OBELISK_TEST_UNREGISTERED_X")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_source_is_a_startup_error() {
        let mut secrets = SecretsToml::new();
        secrets.insert(
            "LOGICAL".to_string(),
            SecretSourceToml::Env {
                env: "OBELISK_TEST_DEFINITELY_UNSET_2B9C".to_string(),
            },
        );
        let err = SecretRegistry::resolve(
            secrets,
            EnvVarCleanupStrategy::Noop,
            RuntimeConfigAvailability::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("not set"), "unexpected error: {err}");
        assert!(
            err.contains("OBELISK_TEST_DEFINITELY_UNSET_2B9C"),
            "unexpected error: {err}"
        );
    }
}
