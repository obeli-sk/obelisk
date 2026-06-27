//! Deployment configuration data model shared between the obelisk server and the webui.
//!
//! Contains the resolved deployment configuration ([`DeploymentResolved`]), the stored
//! manifest representation used for deployment submission and DB storage, plus the data
//! types it is composed of. Some of these types double as the TOML representation
//! (their original `*Toml` names are kept).
//!
//! Behavior that requires the server runtime (OCI fetching, executor configuration,
//! env var resolution) lives in the obelisk binary as extension traits.

use crate::component_id::{ComponentDigest, ContentDigest, InvalidNameError, check_name};
use crate::env_var::EnvVarConfig;
use crate::naming::{FunctionFqn, StrVariant};
use hashbrown::HashMap;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

pub const OCI_SCHEMA_PREFIX: &str = "oci://";

/// Default for `non_blocking_event_batching` of the `await` blocking strategy.
/// Must stay in sync with `wasm_workers::workflow::workflow_worker::DEFAULT_NON_BLOCKING_EVENT_BATCHING`,
/// which is asserted at compile time in the obelisk binary.
pub const DEFAULT_NON_BLOCKING_EVENT_BATCHING: u32 = 100;

/// Activity, Webhook, Workflow or a Http server
#[derive(
    Debug,
    Clone,
    Hash,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::Into,
    JsonSchema,
    derive_more::Deref,
)]
#[display("{_0}")]
pub struct ConfigName(#[schemars(with = "String")] StrVariant);
impl ConfigName {
    pub fn new(name: StrVariant) -> Result<Self, InvalidNameError<ConfigName>> {
        Ok(Self(check_name(name, "_.-")?))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}
impl<'de> Deserialize<'de> for ConfigName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let name = String::deserialize(deserializer)?;
        ConfigName::new(StrVariant::from(name)).map_err(serde::de::Error::custom)
    }
}

impl serde::Serialize for ConfigName {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(s)
    }
}

impl ConfigName {
    /// Derive a `ConfigName` from a `FunctionFqn` using the short form `{ifc_name}.{function_name}`.
    #[must_use]
    pub fn from_ffqn(ffqn: &FunctionFqn) -> Self {
        let ifc_name = ffqn.ifc_fqn.ifc_name();
        let function_name: &str = &ffqn.function_name;
        // WIT identifiers are kebab-case ([a-z0-9-]), so the derived name
        // contains only [a-z0-9-.] — always valid for ConfigName.
        Self(StrVariant::from(format!("{ifc_name}.{function_name}")))
    }
}

/// Location of a WASM component.
/// The OCI reference is kept as a string (without the `oci://` prefix); it is
/// validated and normalized by the obelisk server before canonicalization.
#[derive(
    Debug, Clone, Hash, JsonSchema, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
#[schemars(with = "String")]
pub enum ComponentLocationToml {
    Path(String), // String because it can contain path prefix - $DEPLOYMENT_DIR/
    /// No `oci://` prefix.
    Oci(String),
}

impl FromStr for ComponentLocationToml {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix(OCI_SCHEMA_PREFIX) {
            Ok(ComponentLocationToml::Oci(location.to_string()))
        } else {
            Ok(ComponentLocationToml::Path(s.to_string()))
        }
    }
}

impl Display for ComponentLocationToml {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentLocationToml::Path(p) => write!(f, "{p}"),
            ComponentLocationToml::Oci(r) => write!(f, "{OCI_SCHEMA_PREFIX}{r}"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ComponentCommon {
    pub name: ConfigName,
    pub location: ComponentLocationToml,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LockingStrategy {
    /// Select all exported FFQNs
    ByFfqns,
    /// Select by component digest
    ByComponentDigest,
    /// Only applicable for workflows: Same as `ByFffqns`, with automatic upgrade
    Auto,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ExecConfigToml {
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_lock_expiry")]
    pub lock_expiry: DurationConfig,
    #[serde(default = "default_tick_sleep")]
    pub tick_sleep: DurationConfig,
    #[serde(default)]
    pub locking_strategy: Option<LockingStrategy>,
    #[serde(default)]
    pub instance_limiter: InflightSemaphore,
}

impl Default for ExecConfigToml {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            lock_expiry: default_lock_expiry(),
            tick_sleep: default_tick_sleep(),
            locking_strategy: None,
            instance_limiter: InflightSemaphore::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone, Copy)]
#[serde(untagged)]
pub enum InflightSemaphore {
    Unlimited(Unlimited),
    Some(u32),
}
impl Default for InflightSemaphore {
    fn default() -> Self {
        Self::Unlimited(Unlimited::Unlimited)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Unlimited {
    #[default]
    Unlimited,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DurationConfig {
    Milliseconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
}
impl From<DurationConfig> for Duration {
    fn from(value: DurationConfig) -> Self {
        match value {
            DurationConfig::Milliseconds(millis) => Duration::from_millis(millis),
            DurationConfig::Seconds(secs) => Duration::from_secs(secs),
            DurationConfig::Minutes(mins) => Duration::from_secs(mins * 60),
            DurationConfig::Hours(hrs) => Duration::from_secs(hrs * 60 * 60),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DurationConfigOptional {
    None,
    Milliseconds(u64),
    Seconds(u64),
    Minutes(u64),
    Hours(u64),
}
impl From<DurationConfigOptional> for Option<Duration> {
    fn from(value: DurationConfigOptional) -> Self {
        match value {
            DurationConfigOptional::None => None,
            DurationConfigOptional::Milliseconds(millis) => Some(Duration::from_millis(millis)),
            DurationConfigOptional::Seconds(secs) => Some(Duration::from_secs(secs)),
            DurationConfigOptional::Minutes(mins) => Some(Duration::from_secs(mins * 60)),
            DurationConfigOptional::Hours(hrs) => Some(Duration::from_secs(hrs * 60 * 60)),
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum LogLevelToml {
    Off,
    Trace,
    #[default]
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub enum ComponentStdOutputToml {
    None,
    Stdout,
    Stderr,
    #[default]
    Db,
}

/// Where in the outgoing request placeholders are replaced.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceIn {
    Headers,
    Body,
    Params,
}

/// Input for method restrictions in TOML configuration.
/// Supports both `methods = "*"` and `methods = ["GET", "POST"]` syntax.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub enum MethodsInput {
    /// All methods allowed (from `methods = "*"`).
    Star(MethodsInputStar),
    /// Specific methods list (from `methods = ["GET", "POST"]`).
    List(Vec<String>),
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone)]
pub struct MethodsInputStar(
    #[serde(
        deserialize_with = "deserialize_star",
        serialize_with = "serialize_star"
    )]
    (),
);

fn deserialize_star<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "*" {
        Ok(())
    } else {
        Err(serde::de::Error::custom(format!(
            "expected \"*\", got \"{s}\""
        )))
    }
}

fn serialize_star<S: serde::Serializer>(_: &(), s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str("*")
}

/// An allowed outgoing HTTP host with optional method restrictions and secrets.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct AllowedHostToml {
    /// Host pattern (e.g. `"api.example.com"`, `"*.example.com"`, `"http://localhost:8080"`).
    pub pattern: String,
    /// Allowed HTTP methods.
    /// - Omit to allow nothing (warning emitted).
    /// - `methods = "*"` or `methods = ["*"]` to allow all methods.
    /// - `methods = ["GET", "POST"]` to allow specific methods.
    /// - `methods = []` to allow nothing (warning emitted).
    pub methods: Option<MethodsInput>,
    /// Optional regex restriction checked against `METHOD URL` with query params removed.
    /// For example, `GET https://api.example.com/v1/items`.
    /// Omit to allow all paths accepted by the host and method restrictions.
    pub request_url_regex: Option<String>,
    /// Optional secrets for this host.
    #[serde(default)]
    pub secrets: Option<AllowedHostSecretsToml>,
}

/// Secrets configuration for an allowed host.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct AllowedHostSecretsToml {
    /// Env vars using the same syntax as top-level `env_vars`.
    pub env_vars: Vec<EnvVarConfig>,
    /// Where in the request to perform replacement.
    /// Default: empty (no replacement — deny by default).
    #[serde(default)]
    pub replace_in: Vec<ReplaceIn>,
}

/// A parameter declaration for a JS activity function.
#[derive(Debug, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct JsParamToml {
    /// Parameter name (used in WIT metadata).
    /// Stored in kebab-case for WIT compatibility.
    pub name: String,
    /// WIT type string, e.g. `string`, `u32`, `list<string>`, `option<u64>`.
    #[serde(rename = "type")]
    pub wit_type: String,
}
impl<'de> Deserialize<'de> for JsParamToml {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            name: String,
            #[serde(rename = "type")]
            wit_type: String,
        }
        let raw = Raw::deserialize(deserializer)?;
        let name = if raw.name.contains('_') {
            let kebab = raw.name.replace('_', "-");
            tracing::warn!(
                "param name `{}` contains '_', converting to kebab-case: `{kebab}`",
                raw.name
            );
            kebab
        } else {
            raw.name
        };
        Ok(JsParamToml {
            name,
            wit_type: raw.wit_type,
        })
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityWasmComponentConfigToml {
    #[serde(flatten)]
    pub common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub content_digest: Option<ContentDigest>,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the content digest of the WASM file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub env_vars: Vec<EnvVarConfig>,
    #[serde(default)]
    pub logs_store_min_level: LogLevelToml,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub allowed_hosts: Vec<AllowedHostToml>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityStubFileConfigToml {
    #[serde(flatten)]
    pub common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub content_digest: Option<ContentDigest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityExternalFileConfigToml {
    #[serde(flatten)]
    pub common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub content_digest: Option<ContentDigest>,
    /// Override the auto-computed component digest used for locking.
    /// If set, this value is used instead of the content digest of the WASM file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub component_digest: Option<ComponentDigest>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityStubExtInlineConfigResolved {
    pub name: ConfigName,
    #[schemars(with = "String")]
    pub ffqn: FunctionFqn,
    #[serde(default)]
    pub params: Option<Vec<JsParamToml>>,
    #[serde(default)]
    pub return_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub enum ActivityStubComponentConfigResolved {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigResolved),
}

impl ActivityStubComponentConfigResolved {
    #[must_use]
    pub fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.as_str(),
            Self::Inline(i) => i.name.as_str(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub enum ActivityExternalComponentConfigResolved {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigResolved),
}

impl ActivityExternalComponentConfigResolved {
    #[must_use]
    pub fn name_str(&self) -> &str {
        match self {
            Self::File(f) => f.common.name.as_str(),
            Self::Inline(i) => i.name.as_str(),
        }
    }
}

/// Resolved location of a script source (JS or exec) after file-provider resolution.
///
/// - `Content` is owned by the deployment (inline content, or a file that lived under
///   the deployment directory and was read from disk/CAS); `file_name` is the
///   deployment-relative path (which may include subfolders), used for source names and
///   backtraces.
/// - `Oci` is an external registry reference.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScriptLocationResolved {
    #[schemars(with = "String")]
    Content { content: String, file_name: String },
    /// OCI-sourced script. No `oci://` prefix.
    #[schemars(with = "String")]
    Oci { image: String },
}

/// Resolved backtrace source: the inlined source `content` plus the deployment-relative
/// `file_name` it was read from (used to recreate the file, mirroring subfolders, on export).
#[derive(JsonSchema, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktraceSourceResolved {
    pub content: String,
    pub file_name: String,
}

#[derive(JsonSchema, Debug, Default, Clone, Serialize, Deserialize)]
pub struct ComponentBacktraceConfigResolved {
    #[schemars(with = "std::collections::HashMap<String, BacktraceSourceResolved>")]
    pub frame_files_to_sources: HashMap<String, BacktraceSourceResolved>,
}

impl ComponentBacktraceConfigResolved {
    /// Map each frame-symbol key to its source content (dropping the recreate path),
    /// as needed by the runtime backtrace lookup.
    #[must_use]
    pub fn into_frame_files(self) -> HashMap<String, String> {
        self.frame_files_to_sources
            .into_iter()
            .map(|(k, v)| (k, v.content))
            .collect()
    }
}

/// Resolved form of `ActivityJsComponentConfigToml`.
#[derive(JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityJsComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub ffqn: FunctionFqn,
    pub params: Vec<JsParamToml>,
    pub exec: ExecConfigToml,
    pub max_retries: u32,
    pub retry_exp_backoff: DurationConfig,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub logs_store_min_level: LogLevelToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub allowed_hosts: Vec<AllowedHostToml>,
    pub return_type: Option<String>,
}

/// Secret entry: resolved from environment variables at startup.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct SecretEnvVarToml {
    /// Name used to reference this secret in the `secrets.stdin` function.
    pub name: String,
    /// Value supporting `${VAR}` interpolation from host environment.
    #[serde(default)]
    pub value: String,
}

/// Secrets configuration for exec activities.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ExecSecretsToml {
    /// Secret entries resolved from environment variables.
    #[serde(default)]
    pub env_vars: Vec<SecretEnvVarToml>,
}

/// Resolved form of `ActivityExecComponentConfigToml`.
#[derive(JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ActivityExecComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub ffqn: FunctionFqn,
    pub params: Vec<JsParamToml>,
    pub return_type: Option<String>,
    pub component_digest: Option<ComponentDigest>,
    pub exec: ExecConfigToml,
    pub max_retries: u32,
    pub retry_exp_backoff: DurationConfig,
    pub forward_stdout: ComponentStdOutputToml,
    pub forward_stderr: ComponentStdOutputToml,
    pub logs_store_min_level: LogLevelToml,
    pub env_vars: Vec<EnvVarConfig>,
    pub max_output_bytes: u64,
    pub secrets: Option<ExecSecretsToml>,
    #[serde(default)]
    pub params_via_stdin: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, PartialEq)]
#[serde(untagged)] // Try variants without needing a specific outer tag
pub enum BlockingStrategyConfigToml {
    // Try the more specific map format first
    Tagged(BlockingStrategyConfigCustomized),
    // If it's not the map format, try the simple string format
    Simple(BlockingStrategyConfigSimple),
}
impl Default for BlockingStrategyConfigToml {
    fn default() -> Self {
        Self::Simple(BlockingStrategyConfigSimple::default())
    }
}
// Enum to handle the tagged map case ({ kind = "await", ... })
#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")] // Expects a map with "kind" field
pub enum BlockingStrategyConfigCustomized {
    Await(BlockingStrategyAwaitConfig),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BlockingStrategyAwaitConfig {
    #[serde(default = "default_non_blocking_event_batching")]
    pub non_blocking_event_batching: u32,
}
#[derive(Debug, Deserialize, Serialize, Clone, Copy, JsonSchema, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BlockingStrategyConfigSimple {
    Interrupt,
    #[default]
    Await,
}

/// Resolved form of `WorkflowWasmComponentConfigToml`.
#[derive(JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WorkflowWasmComponentConfigResolved {
    pub common: ComponentCommon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub exec: ExecConfigToml,
    pub retry_exp_backoff: DurationConfig,
    pub blocking_strategy: BlockingStrategyConfigToml,
    pub backtrace: ComponentBacktraceConfigResolved,
    pub stub_wasi: bool,
    pub lock_extension: bool,
    pub logs_store_min_level: LogLevelToml,
}

/// Resolved form of `WorkflowJsComponentConfigToml`.
#[derive(JsonSchema, Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct WorkflowJsComponentConfigResolved {
    pub name: ConfigName,
    pub location: ScriptLocationResolved,
    pub content_digest: Option<ContentDigest>,
    pub component_digest: Option<ComponentDigest>,
    pub ffqn: FunctionFqn,
    pub params: Vec<JsParamToml>,
    pub exec: ExecConfigToml,
    pub retry_exp_backoff: DurationConfig,
    pub blocking_strategy: BlockingStrategyConfigToml,
    pub logs_store_min_level: LogLevelToml,
    pub return_type: Option<String>,
    pub lock_extension: bool,
}

pub mod webhook {
    use super::{
        AllowedHostToml, ComponentBacktraceConfigResolved, ComponentCommon, ComponentStdOutputToml,
        ConfigName, LogLevelToml, ScriptLocationResolved,
    };
    use crate::component_id::ContentDigest;
    use crate::env_var::EnvVarConfig;
    use crate::naming::StrVariant;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[must_use]
    pub fn default_external_server_name() -> ConfigName {
        ConfigName::new(StrVariant::Static("external")).expect("valid name")
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(untagged)]
    pub enum WebhookRoute {
        String(String),
        WebhookRouteDetail(WebhookRouteDetail),
    }

    impl Default for WebhookRoute {
        fn default() -> Self {
            WebhookRoute::String(String::new())
        }
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub struct WebhookRouteDetail {
        // Empty means all methods.
        #[serde(default)]
        pub methods: Vec<String>,
        pub route: String,
    }

    /// Resolved form of `WebhookWasmComponentConfigToml`.
    #[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct WebhookWasmComponentConfigResolved {
        #[serde(flatten)]
        pub common: ComponentCommon,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub content_digest: Option<ContentDigest>,
        #[serde(default = "default_external_server_name")]
        pub http_server: ConfigName,
        pub routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub env_vars: Vec<EnvVarConfig>,
        #[serde(default)]
        pub backtrace: ComponentBacktraceConfigResolved,
        #[serde(default)]
        pub logs_store_min_level: LogLevelToml,
        #[serde(default, rename = "allowed_host")]
        pub allowed_hosts: Vec<AllowedHostToml>,
    }

    /// Resolved form of `WebhookJsComponentConfigToml`.
    #[derive(Debug, Deserialize, Serialize, Clone, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct WebhookJsComponentConfigResolved {
        pub name: ConfigName,
        pub location: ScriptLocationResolved,
        #[serde(default)]
        pub content_digest: Option<ContentDigest>,
        #[serde(default = "default_external_server_name")]
        pub http_server: ConfigName,
        pub routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub logs_store_min_level: LogLevelToml,
        #[serde(default)]
        pub env_vars: Vec<EnvVarConfig>,
        #[serde(default, rename = "allowed_host")]
        pub allowed_hosts: Vec<AllowedHostToml>,
    }
}

pub mod cron {
    use super::{ConfigName, ExecConfigToml};
    use crate::naming::FunctionFqn;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub struct CronComponentConfigToml {
        pub name: ConfigName,
        /// Fully qualified function name of the target to invoke on each schedule tick.
        #[schemars(with = "String")]
        pub ffqn: FunctionFqn,
        /// JSON-encoded parameters to pass to the target function.
        #[serde(default = "default_schedule_params")]
        pub params: String,
        /// Cron expression or `@once`, `@daily`, `@hourly`, `@weekly`, `@monthly`, `@yearly`.
        pub schedule: String,
        #[serde(default)]
        pub exec: ExecConfigToml,
    }

    #[must_use]
    pub fn default_schedule_params() -> String {
        "[]".to_string()
    }
}

/// Resolved deployment configuration after resolving deployment-owned text sources.
///
/// This is a runtime/verification shape derived from the stored manifest plus a file
/// provider. It is not the stored deployment source of truth. Deployment-owned scripts
/// and backtrace sources are inlined as content; deployment-owned WASM locations remain
/// relative path + content digest until `DeploymentRunnable` materializes them from the
/// CAS into a runnable cache path. OCI references remain external references.
#[derive(Debug, Deserialize, Serialize, Default, Clone, JsonSchema)]
pub struct DeploymentResolved {
    pub activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    pub activities_stub: Vec<ActivityStubComponentConfigResolved>,
    pub activities_external: Vec<ActivityExternalComponentConfigResolved>,
    pub activities_js: Vec<ActivityJsComponentConfigResolved>,
    pub activities_exec: Vec<ActivityExecComponentConfigResolved>,
    pub workflows_wasm: Vec<WorkflowWasmComponentConfigResolved>,
    pub workflows_js: Vec<WorkflowJsComponentConfigResolved>,
    pub webhooks_wasm: Vec<webhook::WebhookWasmComponentConfigResolved>,
    pub webhooks_js: Vec<webhook::WebhookJsComponentConfigResolved>,
    #[serde(default)]
    pub crons: Vec<cron::CronComponentConfigToml>,
}

// Serde defaults shared by the canonical types and the TOML types in the obelisk binary.

#[must_use]
pub const fn default_max_retries() -> u32 {
    5
}

#[must_use]
pub const fn default_retry_exp_backoff() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}

#[must_use]
pub const fn default_non_blocking_event_batching() -> u32 {
    DEFAULT_NON_BLOCKING_EVENT_BATCHING
}

#[must_use]
pub const fn default_batch_size() -> u32 {
    5
}

#[must_use]
pub const fn default_lock_expiry() -> DurationConfig {
    DurationConfig::Seconds(1)
}

#[must_use]
pub const fn default_tick_sleep() -> DurationConfig {
    DurationConfig::Milliseconds(200)
}

#[must_use]
pub const fn default_lock_extension() -> bool {
    true
}

#[must_use]
pub const fn default_max_output_bytes() -> u64 {
    4096
}
