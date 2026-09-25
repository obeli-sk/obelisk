use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, derive_more::Debug, Hash, JsonSchema, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum EnvVarConfig {
    // backcompat: 0.41 string entries remain required forwards from the host.
    /// Forward from host: `"KEY"`
    Key(String),
    /// Forward when present: `{key = "KEY", optional = true}`.
    OptionalKey { key: String, optional: bool },
    /// Set to value: `{key = "KEY", value = "foo"}` (supports `${VAR}` interpolation)
    KeyValue {
        key: String,
        #[debug(skip)]
        value: String,
    },
}
