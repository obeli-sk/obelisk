use crate::config::secret_registry::{SecretRegistry, SecretViolation};
use secrecy::SecretString;

pub use concepts::env_var::EnvVarConfig;

/// Error from `${VAR}` interpolation of an operator/deployment config value.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EnvVarError {
    #[error("environment variable not set: `{0}`")]
    Missing(String),
    #[error(transparent)]
    Secret(#[from] SecretViolation),
}

#[derive(Debug, thiserror::Error)]
#[error("environment variables not set: `{0:?}`")]
pub(crate) struct EnvVarsMissing(pub(crate) Vec<String>);

pub(crate) fn interpolate_env_vars_plaintext(
    input: &str,
    secret_registry: &SecretRegistry,
) -> Result<String, EnvVarError> {
    interpolate_env_vars_inner(input, secret_registry)
}
pub(crate) fn interpolate_env_vars_secret(
    input: &str,
    secret_registry: &SecretRegistry,
) -> Result<SecretString, EnvVarError> {
    interpolate_env_vars_inner(input, secret_registry).map(SecretString::from)
}

fn interpolate_env_vars_inner(
    input: &str,
    secret_registry: &SecretRegistry,
) -> Result<String, EnvVarError> {
    interpolate_core(
        input,
        &|key| {
            secret_registry
                .public_env_lookup(key)
                .map_err(EnvVarError::from)
        },
        &|key| EnvVarError::Missing(key),
    )
}

/// Interpolate a path template, resolving synthetic path variables (e.g. `DATA_DIR`) before
/// process environment variables. `synthetics` maps each recognized synthetic name to its value,
/// or `None` when that directory is unavailable in the current context. Synthetic names take
/// precedence over same-named process environment variables. A reference to an unset variable
/// (no `${VAR:-default}` fallback) is an error rather than a literal `${VAR}`.
pub(crate) fn interpolate_path_template(
    input: &str,
    synthetics: &[(&'static str, Option<String>)],
    secret_registry: &SecretRegistry,
) -> Result<String, anyhow::Error> {
    let lookup = |key: &str| -> Result<Option<String>, anyhow::Error> {
        match synthetics.iter().find(|(name, _)| *name == key) {
            Some((_, val)) => Ok(val.clone()),
            None => secret_registry
                .public_env_lookup(key)
                .map_err(anyhow::Error::from),
        }
    };
    let on_missing = |key: String| -> anyhow::Error {
        if synthetics.iter().any(|(name, _)| *name == key) {
            anyhow::anyhow!("path variable `${{{key}}}` is not available in this context")
        } else {
            anyhow::anyhow!("environment variable not set: `{key}`")
        }
    };
    interpolate_core(input, &lookup, &on_missing)
}

/// Shared `${VAR}` / `${VAR:-default}` / `${VAR-default}` parser. `lookup` returns the value of a
/// variable (`Some`) or reports it as unset (`None`); `on_missing` builds the error for a reference
/// to an unset variable that has no default.
fn interpolate_core<E>(
    input: &str,
    lookup: &dyn Fn(&str) -> Result<Option<String>, E>,
    on_missing: &dyn Fn(String) -> E,
) -> Result<String, E> {
    let mut out = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // skip '{'
            let mut key = String::new();
            let mut closed = false;
            // Some(true) = `:-` (unset or empty), Some(false) = `-` (unset only)
            let mut default_mode: Option<bool> = None;
            let mut default_str = String::new();

            let mut depth = 0usize;
            while let Some(&ch) = chars.peek() {
                chars.next();
                if default_mode.is_none() {
                    if ch == '}' {
                        closed = true;
                        break;
                    } else if ch == '-' {
                        // `:-` if key ends with `:`, otherwise bare `-`
                        let colon_dash = key.ends_with(':');
                        if colon_dash {
                            key.pop();
                        }
                        default_mode = Some(colon_dash);
                    } else {
                        key.push(ch);
                    }
                } else {
                    // Track brace depth so nested `${...}` doesn't close the outer expression
                    if ch == '{' {
                        depth += 1;
                    } else if ch == '}' {
                        if depth == 0 {
                            closed = true;
                            break;
                        }
                        depth -= 1;
                    }
                    default_str.push(ch);
                }
            }

            if !closed {
                // Unclosed `${` — treat as literal text
                out.push_str("${");
                out.push_str(&key);
            } else {
                match default_mode {
                    None => match lookup(&key)? {
                        Some(val) => out.push_str(&val),
                        None => return Err(on_missing(key)),
                    },
                    Some(colon_dash) => {
                        let val = lookup(&key)?;
                        let use_default =
                            val.is_none() || (colon_dash && val.as_deref() == Some(""));
                        if use_default {
                            // Recursively interpolate the default value
                            out.push_str(&interpolate_core(&default_str, lookup, on_missing)?);
                        } else {
                            out.push_str(val.as_deref().unwrap());
                        }
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::secret_registry::SecretRegistry;

    /// Interpolate against an empty registry (no secrets, plain process-env lookup).
    fn interp(input: &str) -> Result<String, EnvVarError> {
        interpolate_env_vars_inner(input, &SecretRegistry::empty())
    }

    #[test]
    fn registered_secret_interpolation_is_rejected() {
        let registry = SecretRegistry::from_test_values([(
            "OPENAI_API_KEY".to_string(),
            SecretString::from("sk-test"),
        )]);
        // Even though the secret exists, interpolating it as plaintext is refused.
        let err =
            interpolate_env_vars_plaintext("Bearer ${OPENAI_API_KEY}", &registry).unwrap_err();
        assert!(matches!(err, EnvVarError::Secret(_)), "got {err:?}");
    }

    #[test]
    fn no_interpolation() {
        assert_eq!(interp("hello world").unwrap(), "hello world");
    }

    #[test]
    fn single_interpolation() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_VAR_1", "value1") };
        assert_eq!(interp("${TEST_ENV_VAR_1}").unwrap(), "value1");
    }

    #[test]
    fn interpolation_with_prefix_suffix() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_VAR_2", "middle") };
        assert_eq!(
            interp("prefix ${TEST_ENV_VAR_2} suffix").unwrap(),
            "prefix middle suffix"
        );
    }

    #[test]
    fn multiple_interpolations() {
        // SAFETY: test-only, no concurrent access to these env vars.
        unsafe { std::env::set_var("TEST_ENV_VAR_A", "aaa") };
        // SAFETY: test-only, no concurrent access to these env vars.
        unsafe { std::env::set_var("TEST_ENV_VAR_B", "bbb") };
        assert_eq!(
            interp("${TEST_ENV_VAR_A}-${TEST_ENV_VAR_B}").unwrap(),
            "aaa-bbb"
        );
    }

    #[test]
    fn missing_env_var() {
        let result = interp("${NONEXISTENT_TEST_VAR_XYZ}");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("NONEXISTENT_TEST_VAR_XYZ")
        );
    }

    #[test]
    fn dollar_without_brace_is_literal() {
        assert_eq!(interp("$hello").unwrap(), "$hello");
    }

    #[test]
    fn empty_string() {
        assert_eq!(interp("").unwrap(), "");
    }

    // --- `${VAR:-default}`: use default when unset OR empty ---

    #[test]
    fn colon_dash_unset_uses_default() {
        assert_eq!(
            interp("${NONEXISTENT_COLON_DASH_XYZ:-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn colon_dash_empty_uses_default() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_COLON_DASH_EMPTY", "") };
        assert_eq!(
            interp("${TEST_ENV_COLON_DASH_EMPTY:-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn colon_dash_set_uses_value() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_COLON_DASH_SET", "actual") };
        assert_eq!(
            interp("${TEST_ENV_COLON_DASH_SET:-fallback}").unwrap(),
            "actual"
        );
    }

    // --- `${VAR-default}`: use default only when unset ---

    #[test]
    fn bare_dash_unset_uses_default() {
        assert_eq!(
            interp("${NONEXISTENT_BARE_DASH_XYZ-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn bare_dash_empty_keeps_empty() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_BARE_DASH_EMPTY", "") };
        assert_eq!(interp("${TEST_ENV_BARE_DASH_EMPTY-fallback}").unwrap(), "");
    }

    #[test]
    fn bare_dash_set_uses_value() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_BARE_DASH_SET", "actual") };
        assert_eq!(
            interp("${TEST_ENV_BARE_DASH_SET-fallback}").unwrap(),
            "actual"
        );
    }

    // --- default value containing another interpolation ---

    #[test]
    fn colon_dash_default_is_interpolated() {
        // SAFETY: test-only, no concurrent access to these env vars.
        unsafe { std::env::set_var("TEST_ENV_NESTED_FALLBACK", "nested_val") };
        assert_eq!(
            interp("${NONEXISTENT_NESTED_XYZ:-${TEST_ENV_NESTED_FALLBACK}}").unwrap(),
            "nested_val"
        );
    }

    // --- path templates: synthetic path variables + env interpolation ---

    fn interp_path(
        input: &str,
        synthetics: &[(&'static str, Option<String>)],
    ) -> Result<String, anyhow::Error> {
        interpolate_path_template(input, synthetics, &SecretRegistry::empty())
    }

    #[test]
    fn path_template_synthetic_resolves() {
        let synthetics = [("DATA_DIR", Some("/data".to_string()))];
        assert_eq!(
            interp_path("${DATA_DIR}/obelisk-sqlite", &synthetics).unwrap(),
            "/data/obelisk-sqlite"
        );
    }

    #[test]
    fn path_template_synthetic_wins_over_env() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEMP_DIR", "/env-temp") };
        let synthetics = [("TEMP_DIR", Some("/synthetic-temp".to_string()))];
        assert_eq!(
            interp_path("${TEMP_DIR}/x", &synthetics).unwrap(),
            "/synthetic-temp/x"
        );
    }

    #[test]
    fn path_template_falls_back_to_env() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_PATH_ENV_VAR", "/from-env") };
        let synthetics = [("DATA_DIR", Some("/data".to_string()))];
        assert_eq!(
            interp_path("${TEST_PATH_ENV_VAR}/x", &synthetics).unwrap(),
            "/from-env/x"
        );
    }

    #[test]
    fn path_template_rejects_registered_secret_name() {
        let registry = SecretRegistry::from_test_values([(
            "PATH_SECRET".to_string(),
            SecretString::from("/secret"),
        )]);
        let err = interpolate_path_template("${PATH_SECRET}/x", &[], &registry).unwrap_err();
        assert!(err.to_string().contains("PATH_SECRET"));
    }

    #[test]
    fn path_template_unknown_var_errors() {
        let synthetics = [("DATA_DIR", Some("/data".to_string()))];
        let err = interp_path("${NONEXISTENT_PATH_XYZ}/x", &synthetics).unwrap_err();
        assert!(err.to_string().contains("NONEXISTENT_PATH_XYZ"));
    }

    #[test]
    fn path_template_unavailable_synthetic_errors_clearly() {
        let synthetics = [("SERVER_CONFIG_DIR", None)];
        let err = interp_path("${SERVER_CONFIG_DIR}/x", &synthetics).unwrap_err();
        assert!(err.to_string().contains("not available"));
        assert!(err.to_string().contains("SERVER_CONFIG_DIR"));
    }

    #[test]
    fn path_template_unavailable_synthetic_uses_default() {
        let synthetics = [("DATA_DIR", None)];
        assert_eq!(
            interp_path("${DATA_DIR:-./local}/x", &synthetics).unwrap(),
            "./local/x"
        );
    }
}
