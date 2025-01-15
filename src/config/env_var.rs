use serde::{Deserialize, Deserializer};

#[derive(Clone, derive_more::Debug, Hash)]
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
