use anyhow::anyhow;
use schemars::JsonSchema;
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

pub(crate) fn replace_env_vars(input: &str) -> Result<String, anyhow::Error> {
    let mut out = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // skip '{'
            let mut key = String::new();

            while let Some(&ch) = chars.peek() {
                chars.next();
                if ch == '}' {
                    break;
                }
                key.push(ch);
            }

            let val = std::env::var(&key)
                .map_err(|_| anyhow!("environment variable not set: `{key}`"))?;
            out.push_str(&val);
        } else {
            out.push(c);
        }
    }

    Ok(out)
}
