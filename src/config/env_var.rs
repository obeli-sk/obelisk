use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

#[derive(Clone, derive_more::Debug, Hash, JsonSchema, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvVarConfig {
    /// Forward from host: `"KEY"`
    Key(String),
    /// Set to value: `{key = "KEY", value = "foo"}` (supports `${VAR}` interpolation)
    KeyValue {
        key: String,
        #[debug(skip)]
        value: String,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("environment variable not set: `{0}`")]
pub(crate) struct EnvVarMissing(pub(crate) String);

#[derive(Debug, thiserror::Error)]
#[error("environment variables not set: `{0:?}`")]
pub(crate) struct EnvVarsMissing(pub(crate) Vec<String>);

pub(crate) fn interpolate_env_vars_plaintext(input: &str) -> Result<String, EnvVarMissing> {
    interpolate_env_vars_inner(input)
}
pub(crate) fn interpolate_env_vars_secret(input: &str) -> Result<SecretString, EnvVarMissing> {
    interpolate_env_vars_inner(input).map(SecretString::from)
}

fn interpolate_env_vars_inner(input: &str) -> Result<String, EnvVarMissing> {
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
                    None => {
                        let val = std::env::var(&key).map_err(|_| EnvVarMissing(key))?;
                        out.push_str(&val);
                    }
                    Some(colon_dash) => {
                        let val = std::env::var(&key).ok();
                        let use_default =
                            val.is_none() || (colon_dash && val.as_deref() == Some(""));
                        if use_default {
                            // Recursively interpolate the default value
                            out.push_str(&interpolate_env_vars_inner(&default_str)?);
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

    #[test]
    fn no_interpolation() {
        assert_eq!(
            interpolate_env_vars_inner("hello world").unwrap(),
            "hello world"
        );
    }

    #[test]
    fn single_interpolation() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_VAR_1", "value1") };
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_VAR_1}").unwrap(),
            "value1"
        );
    }

    #[test]
    fn interpolation_with_prefix_suffix() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_VAR_2", "middle") };
        assert_eq!(
            interpolate_env_vars_inner("prefix ${TEST_ENV_VAR_2} suffix").unwrap(),
            "prefix middle suffix"
        );
    }

    #[test]
    fn multiple_interpolations() {
        // SAFETY: test-only, no concurrent access to these env vars.
        unsafe {
            std::env::set_var("TEST_ENV_VAR_A", "aaa");
            std::env::set_var("TEST_ENV_VAR_B", "bbb");
        }
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_VAR_A}-${TEST_ENV_VAR_B}").unwrap(),
            "aaa-bbb"
        );
    }

    #[test]
    fn missing_env_var() {
        let result = interpolate_env_vars_inner("${NONEXISTENT_TEST_VAR_XYZ}");
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
        assert_eq!(interpolate_env_vars_inner("$hello").unwrap(), "$hello");
    }

    #[test]
    fn empty_string() {
        assert_eq!(interpolate_env_vars_inner("").unwrap(), "");
    }

    // --- `${VAR:-default}`: use default when unset OR empty ---

    #[test]
    fn colon_dash_unset_uses_default() {
        assert_eq!(
            interpolate_env_vars_inner("${NONEXISTENT_COLON_DASH_XYZ:-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn colon_dash_empty_uses_default() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_COLON_DASH_EMPTY", "") };
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_COLON_DASH_EMPTY:-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn colon_dash_set_uses_value() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_COLON_DASH_SET", "actual") };
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_COLON_DASH_SET:-fallback}").unwrap(),
            "actual"
        );
    }

    // --- `${VAR-default}`: use default only when unset ---

    #[test]
    fn bare_dash_unset_uses_default() {
        assert_eq!(
            interpolate_env_vars_inner("${NONEXISTENT_BARE_DASH_XYZ-fallback}").unwrap(),
            "fallback"
        );
    }

    #[test]
    fn bare_dash_empty_keeps_empty() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_BARE_DASH_EMPTY", "") };
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_BARE_DASH_EMPTY-fallback}").unwrap(),
            ""
        );
    }

    #[test]
    fn bare_dash_set_uses_value() {
        // SAFETY: test-only, no concurrent access to this env var.
        unsafe { std::env::set_var("TEST_ENV_BARE_DASH_SET", "actual") };
        assert_eq!(
            interpolate_env_vars_inner("${TEST_ENV_BARE_DASH_SET-fallback}").unwrap(),
            "actual"
        );
    }

    // --- default value containing another interpolation ---

    #[test]
    fn colon_dash_default_is_interpolated() {
        // SAFETY: test-only, no concurrent access to these env vars.
        unsafe { std::env::set_var("TEST_ENV_NESTED_FALLBACK", "nested_val") };
        assert_eq!(
            interpolate_env_vars_inner("${NONEXISTENT_NESTED_XYZ:-${TEST_ENV_NESTED_FALLBACK}}")
                .unwrap(),
            "nested_val"
        );
    }
}
