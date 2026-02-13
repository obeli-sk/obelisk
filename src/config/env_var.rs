use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Deserializer};

#[derive(Clone, derive_more::Debug, Hash, JsonSchema)]
pub struct EnvVarConfig {
    pub key: String,
    #[debug(skip)]
    pub val: Option<String>,
}

struct EnvVarConfigVisitor;

impl serde::de::Visitor<'_> for EnvVarConfigVisitor {
    type Value = EnvVarConfig;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .write_str("either key of environment varaible to be forwarded from host, or key=value")
    }

    fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(match input.split_once('=') {
            None => EnvVarConfig {
                key: input.to_string(),
                val: None,
            },
            Some((k, input)) => EnvVarConfig {
                key: k.to_string(),
                val: Some(input.to_string()),
            },
        })
    }
}
impl<'de> Deserialize<'de> for EnvVarConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(EnvVarConfigVisitor)
    }
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

            while let Some(&ch) = chars.peek() {
                chars.next();
                if ch == '}' {
                    closed = true;
                    break;
                }
                key.push(ch);
            }

            if !closed {
                // Unclosed `${` — treat as literal text
                out.push_str("${");
                out.push_str(&key);
            } else {
                let val = std::env::var(&key).map_err(|_| EnvVarMissing(key))?;
                out.push_str(&val);
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
}
