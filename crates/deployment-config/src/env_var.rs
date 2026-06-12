use schemars::JsonSchema;
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
