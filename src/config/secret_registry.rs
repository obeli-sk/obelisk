//! Operator-owned secret registry.
//!
//! The `[secrets]` table in `server.toml` maps a logical secret name to a source
//! (currently only an environment variable). At startup, before the tokio runtime
//! is constructed and while the process is still single-threaded, env-backed
//! secrets are resolved into `SecretString` and their source variables are removed
//! from the process environment (see [`SecretRegistry::resolve_and_wipe`]).
//!
//! The resulting [`SecretRegistry`] is a plain value (not a global) that is passed
//! by ownership into the server code. It exposes:
//! - the set of sensitive names (logical names plus env source names), used to
//!   reject `${VAR}` interpolation of a registered secret, and
//! - a name -> `SecretString` getter ([`SecretRegistry::secret_lookup`]), the only
//!   way a deployment-referenced secret becomes a plaintext value (exec stdin or
//!   HTTP placeholder injection).
//!
//! See `meta/designs/secret-registry.md`.

use anyhow::Context as _;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexMap;
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// Source of a secret in the `[secrets]` table. Untagged so `{ env = "VAR" }`
/// parses directly; more variants (`{ file = "..." }`, Vault, ...) are added later.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged, deny_unknown_fields)]
pub(crate) enum SecretSourceToml {
    /// Read the secret from a process environment variable at startup.
    Env { env: String },
}

/// The `[secrets]` table: logical name -> source. `IndexMap` preserves author order.
pub(crate) type SecretsToml = IndexMap<String, SecretSourceToml>;

/// A deployment or config value referenced a name that is a registered secret.
/// Registered secrets are inject-only and must never be interpolated as plaintext.
#[derive(Debug, thiserror::Error)]
#[error(
    "cannot interpolate secret `{0}`: registered secrets are inject-only and cannot be \
     interpolated into configuration"
)]
pub(crate) struct SecretViolation(pub(crate) String);

#[derive(Debug, Clone)]
pub(crate) struct SecretRegistry {
    /// Logical secret name -> resolved value.
    values: HashMap<String, SecretString>,
    /// Used to reject `public_env_lookup`.
    sensitive: HashSet<String>,
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
    /// (a secret's logical name or a secret's source env var); otherwise reads it
    /// from the process environment, returning `None` when unset.
    pub(crate) fn public_env_lookup(&self, name: &str) -> Result<Option<String>, SecretViolation> {
        if self.sensitive.contains(name) {
            Err(SecretViolation(name.to_owned()))
        } else {
            Ok(std::env::var(name).ok())
        }
    }

    /// Resolve a registered secret by its logical name into its value.
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

    /// Build the registry from the resolved server configuration and wipe env-backed
    /// source variables.
    ///
    /// MUST run during early, single-threaded startup, before the tokio runtime is
    /// constructed: it calls `std::env::remove_var`, which is only sound without
    /// concurrent readers.
    pub(crate) fn resolve_and_wipe(secrets: SecretsToml) -> anyhow::Result<Self> {
        let mut values = HashMap::new();

        // Seed the sensitive set with the API-token env vars so a deployment can neither
        // interpolate nor read them, even if the operator forgot to register them.
        let mut sensitive = HashSet::from([
            "OBELISK__API__TOKEN".to_string(),
            "OBELISK_API_TOKEN".to_string(),
        ]);

        for (name, source) in secrets {
            match source {
                SecretSourceToml::Env { env } => {
                    let value = std::env::var(&env).with_context(|| {
                        format!("secret `{name}` source environment variable `{env}` is not set")
                    })?;
                    values.insert(name.clone(), SecretString::from(value));
                    sensitive.insert(env);
                    // A logical name is sensitive too, so it cannot be interpolated as plaintext.
                    sensitive.insert(name);
                }
            }
        }
        for src in &sensitive {
            // SAFETY: `resolve_and_wipe` runs during single-threaded startup, before the
            // tokio runtime is constructed, so there are no concurrent environment readers.
            unsafe { std::env::remove_var(src) };
        }

        Ok(Self { values, sensitive })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret as _;

    #[test]
    fn resolve_and_wipe_reads_value_wipes_source_and_rejects_lookup() {
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
        let registry = SecretRegistry::resolve_and_wipe(secrets).unwrap();

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
        let err = SecretRegistry::resolve_and_wipe(secrets)
            .unwrap_err()
            .to_string();
        assert!(err.contains("is not set"), "unexpected error: {err}");
    }
}
