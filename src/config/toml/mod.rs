use super::{config_holder::PathPrefixes, env_var::EnvVarConfig};
use crate::args::TomlComponentType;
use crate::command::server::{FrameFilesToSource, FrameSource};
use crate::config::config_holder::{CACHE_DIR_PREFIX, DATA_DIR_PREFIX};
use crate::config::env_var::{
    EnvVarError, EnvVarsMissing, interpolate_env_vars_plaintext, interpolate_env_vars_secret,
};
use crate::config::file_provider::{
    parse_js_graph_from_cas, parse_wit_files_from_cas, read_package_blob, verify_content_digest,
};
use crate::config::secret_registry::{
    RestrictedSecretRegistry, SecretRegistry, SecretViolation, SecretsToml,
};
use crate::config::toml::cron::CronComponentConfigToml;
use crate::config::{content_digest_to_exec_file, wasm_cache_metadata_dir};
use crate::oci;
use anyhow::{Context, ensure};
use anyhow::{anyhow, bail};
use concepts::ContentDigest;
use concepts::ReturnType;
use concepts::cas::Cas;
use concepts::component_id::Digest;
use concepts::{
    ComponentId, ComponentRetryConfig, ComponentType, FunctionFqn, StrVariant,
    component_id::ComponentDigest, prefixed_ulid::ExecutorId, storage::LogLevel,
};
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use hashbrown::HashMap;
use log::{LoggingConfig, LoggingStyle};
use regex::Regex;
use schemars::JsonSchema;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};
use sha2::{Digest as _, Sha256};
use std::fmt::Display;
use std::str::FromStr;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tracing::{debug, instrument, warn};
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity::activity_exec_worker::ExecSecrets;
use wasm_workers::cron::cron_worker::CronOrOnce;
use wasm_workers::http_hooks::ConfigSectionHint;
use wasm_workers::http_request_policy::HostPatternError;
use wasm_workers::{
    activity::activity_worker::ActivityConfig,
    envvar::EnvVar,
    http_request_policy::{
        AllowedHostConfig, GlobalHttpConfig, HostPattern, MethodsPattern, ReplacementLocation,
        SecretResolver,
    },
    std_output_stream::StdOutputConfig,
    workflow::workflow_worker::{
        DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy, WorkflowConfig,
        WorkflowConfigMode,
    },
};
use webhook::{HttpServer, WebhookJsComponentConfigToml, WebhookWasmComponentConfigToml};

pub(crate) mod model;

pub(crate) use crate::config::toml::model::{
    ActivityExecComponentConfigResolved, ActivityExternalComponentConfigResolved,
    ActivityExternalFileConfigToml, ActivityJsComponentConfigResolved,
    ActivityStubComponentConfigResolved, ActivityStubExtInlineConfigResolved,
    ActivityStubFileConfigToml, ActivityWasmComponentConfigToml, AllowedHostToml,
    BacktraceSourceResolved, BlockingStrategyConfigToml, ComponentBacktraceConfigResolved,
    ComponentCommon, ComponentLocationToml, ComponentStdOutputToml, ConfigName, DeploymentResolved,
    DurationConfig, DurationConfigOptional, ExecConfigToml, FunctionInterfaceResolved,
    InflightSemaphore, InlineFunctionInterfaceResolved, JsParamToml, LockingStrategy, LogLevelToml,
    MethodsInput, MethodsInputStar, OCI_SCHEMA_PREFIX, ReplaceIn, ScriptLocationResolved,
    Unlimited, WitSourceResolved, WorkflowJsComponentConfigResolved,
    WorkflowWasmComponentConfigResolved, default_lock_extension, default_lock_extension_leeway,
    default_max_output_bytes, default_max_retries, default_retry_exp_backoff,
};

const DEFAULT_SQLITE_DIR_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}obelisk-sqlite", DATA_DIR_PREFIX);
const DEFAULT_SQLITE_DIR: &str = "obelisk-sqlite";
pub(crate) const SQLITE_FILE_NAME: &str = "obelisk.sqlite";
const DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}wasm", CACHE_DIR_PREFIX);
const DEFAULT_WASM_DIRECTORY: &str = "cache/wasm";
const DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}codegen", CACHE_DIR_PREFIX);
const DEFAULT_CODEGEN_CACHE_DIRECTORY: &str = "cache/codegen";
pub(crate) const MAX_DEPLOYMENT_FILE_BYTES: u32 = 20 * 1024 * 1024; // 20MiB

// backcompat: accept component digest overrides until 0.42.
fn warn_deprecated_component_digest_override(
    component_name: &str,
    component_digest: Option<&ComponentDigest>,
) {
    if let Some(component_digest) = component_digest {
        warn!(
            component_name,
            %component_digest,
            "`component_digest` override is deprecated and will be removed in 0.42"
        );
    }
}

#[derive(Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentToml {
    #[serde(default, rename = "activity_wasm")]
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,
    #[serde(default, rename = "activity_stub")]
    pub(crate) activities_stub: Vec<ActivityStubComponentConfigToml>,
    #[serde(default, rename = "activity_external")]
    pub(crate) activities_external: Vec<ActivityExternalComponentConfigToml>,
    #[serde(default, rename = "activity_js")]
    pub(crate) activities_js: Vec<ActivityJsComponentConfigToml>,
    #[serde(default, rename = "activity_exec")]
    pub(crate) activities_exec: Vec<ActivityExecComponentConfigToml>,
    #[serde(default, rename = "workflow_wasm")]
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,
    #[serde(default, rename = "workflow_js")]
    pub(crate) workflows_js: Vec<WorkflowJsComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_wasm")]
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,
    #[serde(default, rename = "webhook_endpoint_js")]
    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    #[serde(default, rename = "cron")]
    pub(crate) crons: Vec<CronComponentConfigToml>,
}

/// A `DeploymentToml` that has passed name-uniqueness validation.
///
/// Components that support auto-derived names (`activity_js`, `activity_exec`,
/// `workflow_js`) are stored as `(Config, ConfigName)` tuples with the resolved name
/// pulled out of the `Option`.
#[derive(Default)]
pub(crate) struct DeploymentTomlValidated {
    pub(crate) activities_exec: Vec<(ActivityExecComponentConfigToml, ConfigName)>,
    pub(crate) activities_external: Vec<(ActivityExternalComponentConfigToml, ConfigName)>,
    pub(crate) activities_js: Vec<(ActivityJsComponentConfigToml, ConfigName)>,
    pub(crate) activities_stub: Vec<(ActivityStubComponentConfigToml, ConfigName)>,
    pub(crate) activities_wasm: Vec<ActivityWasmComponentConfigToml>,

    pub(crate) workflows_js: Vec<(WorkflowJsComponentConfigToml, ConfigName)>,
    pub(crate) workflows_wasm: Vec<WorkflowWasmComponentConfigToml>,

    pub(crate) webhooks_js: Vec<WebhookJsComponentConfigToml>,
    pub(crate) webhooks_wasm: Vec<WebhookWasmComponentConfigToml>,

    pub(crate) crons: Vec<CronComponentConfigToml>,

    pub(crate) component_names_to_types: hashbrown::HashMap<String, crate::args::TomlComponentType>,
}
impl DeploymentTomlValidated {
    /// Resolve every deployment-owned reference by reading its blob from `cas` by digest.
    ///
    /// The manifest must be processed (every deployment-owned reference carries a digest in
    /// `content_digest` / `component_files`); resolution never touches the submitter's disk.
    pub(crate) async fn resolve(self, cas: &dyn Cas) -> Result<DeploymentResolved, anyhow::Error> {
        resolve_local_refs(self, cas).await
    }
}

impl DeploymentToml {
    // backcompat: Delete ${DEPLOYMENT_DIR} in 0.42
    /// Expand `${DEPLOYMENT_DIR}/` prefixes in WASM component paths,
    /// verify that every component name is unique, and return a `DeploymentTomlValidated`
    /// that also carries the name→type index and the deployment directory.
    pub(crate) fn validate(
        mut self,
        deployment_dir: &std::path::Path,
    ) -> Result<DeploymentTomlValidated, anyhow::Error> {
        self.expand_deployment_dir_prefix(deployment_dir)?;
        self.normalize_oci_locations()?;
        self.validate_wit_sources()?;

        // Build the name→type index and check for duplicates.
        let mut component_names_to_types = hashbrown::HashMap::new();
        // Add components with mandatory names
        let iter = self
            .activities_wasm
            .iter()
            .map(|c| (c.common.name.as_str(), TomlComponentType::ActivityWasm))
            .chain(
                self.workflows_wasm
                    .iter()
                    .map(|c| (c.common.name.as_str(), TomlComponentType::WorkflowWasm)),
            )
            .chain(self.webhooks_wasm.iter().map(|c| {
                (
                    c.common.name.as_str(),
                    TomlComponentType::WebhookEndpointWasm,
                )
            }))
            .chain(
                self.webhooks_js
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::WebhookEndpointJs)),
            )
            .chain(
                self.crons
                    .iter()
                    .map(|c| (c.name.as_str(), TomlComponentType::Cron)),
            );

        for (name, component_type) in iter {
            if component_names_to_types
                .insert(name.to_string(), component_type)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }

        let activities_js = Self::resolve_names(self.activities_js);
        let activities_exec = Self::resolve_names(self.activities_exec);
        let activities_stub = Self::resolve_stub_names(self.activities_stub);
        let activities_external = Self::resolve_external_names(self.activities_external);
        let workflows_js = Self::resolve_names(self.workflows_js);

        // Add components with optional names (now resolved)

        for (_, name) in &activities_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_exec {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExec)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_stub {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityStub)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &activities_external {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::ActivityExternal)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        for (_, name) in &workflows_js {
            if component_names_to_types
                .insert(name.to_string(), TomlComponentType::WorkflowJs)
                .is_some()
            {
                bail!("duplicate component name `{name}` in deployment");
            }
        }
        Ok(DeploymentTomlValidated {
            activities_exec,
            activities_external,
            activities_js,
            activities_stub,
            activities_wasm: self.activities_wasm,

            workflows_js,
            workflows_wasm: self.workflows_wasm,

            webhooks_js: self.webhooks_js,
            webhooks_wasm: self.webhooks_wasm,

            crons: self.crons,

            component_names_to_types,
        })
    }

    fn validate_wit_sources(&self) -> anyhow::Result<()> {
        fn validate(section: &str, interface: &FunctionInterfaceToml) -> anyhow::Result<()> {
            if let FunctionInterfaceToml::Authored(AuthoredFunctionInterfaceToml { wit }) =
                interface
            {
                sanitize_deployment_relative_path(wit)
                    .with_context(|| format!("invalid `{section}.wit`"))?;
            }
            Ok(())
        }

        for config in &self.activities_js {
            validate("activity_js", &config.interface)?;
        }
        for config in &self.activities_exec {
            validate("activity_exec", &config.interface)?;
        }
        for config in &self.workflows_js {
            validate("workflow_js", &config.interface)?;
        }
        for config in &self.activities_stub {
            if let ActivityStubComponentConfigToml::Inline(inline) = config {
                validate("activity_stub", &inline.interface)?;
            }
        }
        for config in &self.activities_external {
            if let ActivityExternalComponentConfigToml::Inline(inline) = config {
                validate("activity_external", &inline.interface)?;
            }
        }
        Ok(())
    }

    // Resolve optional names from FFQN.
    fn resolve_names<T: HasOptionalNameAndFfqn>(configs: Vec<T>) -> Vec<(T, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = c
                    .config_name()
                    .cloned()
                    .unwrap_or_else(|| ConfigName::from_ffqn(c.ffqn()));
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityStubComponentConfigToml` enum variants.
    /// File variants always have an explicit name; Inline variants may derive from FFQN.
    fn resolve_stub_names(
        configs: Vec<ActivityStubComponentConfigToml>,
    ) -> Vec<(ActivityStubComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityStubComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityStubComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve names for `ActivityExternalComponentConfigToml` enum variants.
    fn resolve_external_names(
        configs: Vec<ActivityExternalComponentConfigToml>,
    ) -> Vec<(ActivityExternalComponentConfigToml, ConfigName)> {
        configs
            .into_iter()
            .map(|c| {
                let name = match &c {
                    ActivityExternalComponentConfigToml::File(f) => f.common.name.clone(),
                    ActivityExternalComponentConfigToml::Inline(i) => i
                        .name
                        .clone()
                        .unwrap_or_else(|| ConfigName::from_ffqn(&i.ffqn)),
                };
                (c, name)
            })
            .collect()
    }

    /// Resolve a WASM component file path to an absolute path. A `${DEPLOYMENT_DIR}/<suffix>`
    /// path and a bare relative path are both anchored to the deployment directory and must
    /// stay within it (no `..` escape); authored absolute paths are rejected. This makes every
    /// path in a deployment.toml deployment-relative.
    fn expand_deployment_dir(
        s: &mut String,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        // A `${DEPLOYMENT_DIR}/x` path and a bare relative `x` are equivalent.
        let candidate = strip_deployment_dir_prefix(s).unwrap_or(s.as_str());
        if std::path::Path::new(candidate).is_absolute() {
            bail!("absolute local paths are not allowed in deployment manifests: `{s}`");
        }
        let rel = sanitize_deployment_relative_path(candidate)
            .with_context(|| format!("invalid deployment-relative path `{s}`"))?;
        *s = deployment_dir.join(rel).to_string_lossy().into_owned();
        Ok(())
    }

    /// Validate and normalize OCI references of WASM components so that the resolved
    /// form matches the previous `oci_client::Reference`-based serialization.
    fn normalize_oci_locations(&mut self) -> Result<(), anyhow::Error> {
        fn normalize(loc: &mut ComponentLocationToml) -> Result<(), anyhow::Error> {
            if let ComponentLocationToml::Oci(image) = loc {
                let reference = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                *image = reference.to_string();
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                normalize(&mut c.common.location)?;
            }
        }
        for c in &mut self.workflows_wasm {
            normalize(&mut c.common.location)?;
        }
        for c in &mut self.webhooks_wasm {
            normalize(&mut c.common.location)?;
        }
        Ok(())
    }

    /// Expand `${DEPLOYMENT_DIR}` prefixes in WASM component paths (which are read lazily
    /// at runtime and therefore must be absolute in the resolved form), rejecting `..`
    /// escapes.
    fn expand_deployment_dir_prefix(
        &mut self,
        deployment_dir: &std::path::Path,
    ) -> anyhow::Result<()> {
        fn expand_loc(
            loc: &mut ComponentLocationToml,
            deployment_dir: &std::path::Path,
        ) -> anyhow::Result<()> {
            if let ComponentLocationToml::Path(p) = loc {
                DeploymentToml::expand_deployment_dir(p, deployment_dir)?;
            }
            Ok(())
        }
        for c in &mut self.activities_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.activities_stub {
            if let ActivityStubComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        for c in &mut self.activities_external {
            if let ActivityExternalComponentConfigToml::File(c) = c {
                expand_loc(&mut c.common.location, deployment_dir)?;
            }
        }
        // Script (JS/exec) locations and backtrace sources are NOT expanded here. Their
        // `${DEPLOYMENT_DIR}` prefix is handled when resolving deployment-owned refs
        // (`resolve_script_toml` / `resolve_backtrace`), so
        // deployment-owned files preserve deployment-relative names.
        for c in &mut self.workflows_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        for c in &mut self.webhooks_wasm {
            expand_loc(&mut c.common.location, deployment_dir)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerConfigToml {
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) source_path: Option<PathBuf>,
    #[serde(default, rename = "obelisk-version")]
    pub(crate) obelisk_version: Option<String>,
    /// Operator-owned secret registry. Maps a logical secret name to a source
    /// (currently only `{ env = "VAR" }`). Env-backed secrets are resolved and
    /// their source variables wiped from the process environment at startup, before
    /// the tokio runtime starts. Deployments reference these names in `activity_exec`
    /// `secrets` and `allowed_host[].secrets`; they cannot interpolate them.
    #[serde(default)]
    pub(crate) secrets: SecretsToml,
    /// Permit deployments to run host processes through `activity_exec`.
    /// `false` denies all (default), `true` allows any, a map from exec activity
    /// names to `sha256:...` content digests allows only the named scripts.
    #[serde(default)]
    pub(crate) allow_exec_activities: AllowExecActivities,
    /// Operator-owned allowlist for component-originated HTTP requests.
    /// An empty allowlist denies every outbound request.
    #[serde(default)]
    pub(crate) outbound_http: OutboundHttpToml,
    /// Per-file size limit for deployment-owned blobs attached to a submit request.
    #[serde(default)]
    pub(crate) max_deployment_file_bytes: MaxDeploymentFileBytes,
    #[serde(default)]
    pub(crate) api: ApiConfig,
    #[serde(default)]
    pub(crate) database: DatabaseConfigToml,
    #[serde(default)]
    pub(crate) webui: WebUIConfig,
    #[serde(default)]
    pub(crate) external: ExternalServerConfig,
    #[serde(default, rename = "wasm")]
    pub(crate) wasm_global_config: WasmGlobalConfigToml,
    #[serde(default, rename = "workflows")]
    pub(crate) workflows_global_config: WorkflowsGlobalConfigToml,
    #[serde(default)]
    pub(crate) timers_watcher: TimersWatcherTomlConfig,
    #[serde(default)]
    pub(crate) cancel_watcher: CancelWatcherTomlConfig,
    #[cfg(feature = "otlp")]
    #[serde(default)]
    pub(crate) otlp: Option<otlp::OtlpConfig>,
    #[serde(default)]
    pub(crate) log: LoggingConfig,
    #[serde(default, rename = "http_server")]
    pub(crate) http_servers: Vec<HttpServer>,
}

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutboundHttpToml {
    /// Global outbound HTTP entries use the same grammar as deployment
    /// `allowed_host` entries.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}

/// Per-file size limit (in bytes) for deployment-owned blobs, defaulting to 20 MiB.
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(transparent)]
pub(crate) struct MaxDeploymentFileBytes(pub(crate) u32);
impl Default for MaxDeploymentFileBytes {
    fn default() -> Self {
        Self(MAX_DEPLOYMENT_FILE_BYTES)
    }
}

/// Exec activity policy: deny all, allow any, or allow only named scripts whose
/// content digest matches the configured digest of the exact script text.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) enum AllowExecActivities {
    #[default]
    Deny,
    AllowAny,
    Allowlist(BTreeMap<String, ContentDigest>),
    // backcompat: 0.40.x accepted an unnamed list of content digests.
    LegacyAllowlist(Vec<ContentDigest>),
}
impl<'de> Deserialize<'de> for AllowExecActivities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AllowExecActivitiesVisitor;
        impl<'de> serde::de::Visitor<'de> for AllowExecActivitiesVisitor {
            type Value = AllowExecActivities;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a boolean or a map from exec activity names to `sha256:...` content digests",
                )
            }

            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(if v {
                    AllowExecActivities::AllowAny
                } else {
                    AllowExecActivities::Deny
                })
            }

            // The `OBELISK__ALLOW_EXEC_ACTIVITIES` env override arrives as a string.
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v.parse::<bool>() {
                    Ok(v) => self.visit_bool(v),
                    Err(_) => Err(E::invalid_value(serde::de::Unexpected::Str(v), &self)),
                }
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut digests = Vec::new();
                while let Some(digest) = seq.next_element::<ContentDigest>()? {
                    digests.push(digest);
                }
                Ok(AllowExecActivities::LegacyAllowlist(digests))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut digests = BTreeMap::new();
                while let Some((name, digest)) = map.next_entry::<String, ContentDigest>()? {
                    if digests.insert(name.clone(), digest).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate exec activity name `{name}`"
                        )));
                    }
                }
                Ok(AllowExecActivities::Allowlist(digests))
            }
        }
        deserializer.deserialize_any(AllowExecActivitiesVisitor)
    }
}

impl JsonSchema for AllowExecActivities {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("AllowExecActivities")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "anyOf": [
                {"type": "boolean"},
                {"type": "object", "additionalProperties": {"type": "string"}},
                // backcompat: 0.40.x accepted an unnamed list of content digests.
                {"type": "array", "items": {"type": "string"}}
            ]
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_api_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
    /// Accepted API bearer tokens as `sha256:<hex>` digests of the token text.
    /// Hashes are not secrets, so this file stays safe to commit.
    /// Generate an entry with `obelisk generate token`.
    #[serde(default)]
    pub(crate) token_hashes: Vec<Digest>,
    /// Plaintext accepted token, intended for env injection only
    /// (`OBELISK__API__TOKEN`); do not write it into the config file.
    #[serde(default, deserialize_with = "deserialize_opt_secret_string")]
    #[schemars(with = "Option<String>")]
    pub(crate) token: Option<SecretString>,
}
impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_api_listening_addr(),
            token_hashes: Vec::new(),
            token: None,
        }
    }
}
fn deserialize_opt_secret_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<SecretString>, D::Error> {
    Ok(Option::<String>::deserialize(deserializer)?.map(SecretString::from))
}
fn default_api_listening_addr() -> SocketAddr {
    "127.0.0.1:5005".parse().expect("valid default address")
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DatabaseConfigToml {
    Sqlite(SqliteConfigToml),
    Postgres(PostgresConfigToml),
}
impl DatabaseConfigToml {
    pub fn get_subscription_interruption(&self) -> Option<Duration> {
        match self {
            DatabaseConfigToml::Sqlite(_) => None,
            DatabaseConfigToml::Postgres(postgres_config_toml) => {
                postgres_config_toml.subscription_interruption.into()
            }
        }
    }
}
impl Default for DatabaseConfigToml {
    fn default() -> DatabaseConfigToml {
        DatabaseConfigToml::Sqlite(SqliteConfigToml::default())
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgresConfigToml {
    host: String,
    user: String,
    password: String,
    db_name: String,
    /// Interrupts listening for notifications periodically, needed for Postgres with a local-only subscription mechanism.
    #[serde(default = "default_subscription_interruption")]
    pub subscription_interruption: DurationConfigOptional,
    #[serde(default)]
    provision_policy: PostgresProvisionPolicy,
}

impl PostgresConfigToml {
    pub fn as_config(
        &self,
        secret_registry: &SecretRegistry,
    ) -> Result<PostgresConfig, anyhow::Error> {
        Ok(PostgresConfig {
            host: interpolate_env_vars_plaintext(&self.host, secret_registry)?,
            user: interpolate_env_vars_plaintext(&self.user, secret_registry)?,
            password: interpolate_env_vars_secret(&self.password, secret_registry)?,
            db_name: interpolate_env_vars_plaintext(&self.db_name, secret_registry)?,
        })
    }
    pub fn as_provision_policy(&self) -> postgres_dao::ProvisionPolicy {
        match self.provision_policy {
            PostgresProvisionPolicy::Never => postgres_dao::ProvisionPolicy::NeverCreate,
            PostgresProvisionPolicy::Auto => postgres_dao::ProvisionPolicy::Auto,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum PostgresProvisionPolicy {
    #[default]
    Never,
    /// Create database if it does not exist.
    Auto,
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SqliteConfigToml {
    #[serde(default)]
    directory: Option<String>,
    #[serde(default = "default_sqlite_queue_capacity")]
    queue_capacity: usize,
    #[serde(default)]
    pragma: std::collections::HashMap<String, String>, // hashbrown is not supported by schemars
    #[serde(default)]
    metrics_threshold: Option<DurationConfig>,
}
impl Default for SqliteConfigToml {
    fn default() -> Self {
        Self {
            directory: None,
            queue_capacity: default_sqlite_queue_capacity(),
            pragma: std::collections::HashMap::default(),
            metrics_threshold: Option::default(),
        }
    }
}
impl SqliteConfigToml {
    pub(crate) async fn get_sqlite_dir(
        &self,
        path_prefixes: &PathPrefixes,
        secret_registry: &SecretRegistry,
    ) -> Result<PathBuf, anyhow::Error> {
        let sqlite_file = self.directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_SQLITE_DIR_IF_PROJECT_DIRS
            } else {
                DEFAULT_SQLITE_DIR
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(sqlite_file, secret_registry)
            .await
    }

    pub(crate) fn as_sqlite_config(&self) -> SqliteConfig {
        SqliteConfig {
            queue_capacity: self.queue_capacity,
            pragma_override: Some(self.pragma.clone().into_iter().collect()),
            metrics_threshold: self.metrics_threshold.map(Duration::from),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebUIConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_webui_listening_addr")]
    pub(crate) listening_addr: String,
}
impl Default for WebUIConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_webui_listening_addr(),
        }
    }
}
fn default_webui_listening_addr() -> String {
    "127.0.0.1:8080".to_string()
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalServerConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_external_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
}
impl Default for ExternalServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_external_listening_addr(),
        }
    }
}
fn default_external_listening_addr() -> SocketAddr {
    "127.0.0.1:9090".parse().expect("valid default address")
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmGlobalConfigToml {
    #[serde(default)]
    pub(crate) codegen_cache: CodegenCache,
    #[serde(default)]
    cache_directory: Option<String>,
    #[serde(default)]
    pub(crate) allocator_config: WasmtimeAllocatorConfig,
    #[serde(default)]
    pub(crate) global_executor_instance_limiter: InflightSemaphore,
    #[serde(default)]
    pub(crate) global_webhook_instance_limiter: InflightSemaphore,
    #[serde(default)]
    pub(crate) fuel: ValueOrUnlimited<u64>,
    #[serde(default)]
    pub(crate) build_semaphore: ValueOrUnlimited<u64>,
    #[serde(default = "default_parallel_compilation")]
    pub(crate) parallel_compilation: bool,
    #[serde(default)]
    pub(crate) wasmtime_pooling_config: WasmtimePoolingAllocatorConfig,
    #[serde(default = "default_debug")]
    pub(crate) debug: bool,
}
impl Default for WasmGlobalConfigToml {
    fn default() -> Self {
        WasmGlobalConfigToml {
            codegen_cache: CodegenCache::default(),
            cache_directory: Option::default(),
            allocator_config: WasmtimeAllocatorConfig::default(),
            global_executor_instance_limiter: InflightSemaphore::default(),
            global_webhook_instance_limiter: InflightSemaphore::default(),
            fuel: ValueOrUnlimited::default(),
            build_semaphore: ValueOrUnlimited::default(),
            parallel_compilation: default_parallel_compilation(),
            wasmtime_pooling_config: WasmtimePoolingAllocatorConfig::default(),
            debug: default_debug(),
        }
    }
}

impl WasmGlobalConfigToml {
    pub(crate) async fn get_wasm_cache_directory(
        &self,
        path_prefixes: &PathPrefixes,
        secret_registry: &SecretRegistry,
    ) -> Result<PathBuf, anyhow::Error> {
        let wasm_directory = self.cache_directory.as_deref().unwrap_or_else(|| {
            if path_prefixes.project_dirs.is_some() {
                DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS
            } else {
                DEFAULT_WASM_DIRECTORY
            }
        });
        path_prefixes
            .server_config_replace_path_prefix_mkdir(wasm_directory, secret_registry)
            .await
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowsGlobalConfigToml {
    /// Deprecated: set `lock_extension_leeway` on each `[[workflow_wasm]]` / `[[workflow_js]]`
    /// instead. When set, it overrides the per-workflow value for every workflow. Will be
    /// removed in 0.42.
    #[serde(default)]
    pub(crate) lock_extension_leeway: Option<DurationConfig>,
    /// Maximum number of captured writes a single replay pass returns. On reaching it, replay
    /// stops and returns that many writes as an advanceable prefix; advancing them and replaying
    /// again resumes from the persisted tip. Keeps a non-terminating workflow (e.g. an unresolved
    /// `joinNextTry` poll loop, whose replay never blocks) advanceable in bounded batches instead
    /// of collecting captured writes forever.
    #[serde(default = "default_max_replay_captured_writes")]
    pub(crate) max_replay_captured_writes: usize,
    /// Maximum number of history events a real workflow run may write before yielding.
    #[serde(default = "default_max_events_per_run")]
    #[schemars(range(min = 1))]
    pub(crate) max_events_per_run: usize,
    /// Number of newly written non-blocking events between database response refreshes.
    #[serde(default = "default_response_refresh_interval")]
    #[schemars(range(min = 1))]
    pub(crate) response_refresh_interval: usize,
}

impl Default for WorkflowsGlobalConfigToml {
    fn default() -> Self {
        Self {
            lock_extension_leeway: None,
            max_replay_captured_writes: default_max_replay_captured_writes(),
            max_events_per_run: default_max_events_per_run(),
            response_refresh_interval: default_response_refresh_interval(),
        }
    }
}

const fn default_max_replay_captured_writes() -> usize {
    100
}

const fn default_max_events_per_run() -> usize {
    100
}

const fn default_response_refresh_interval() -> usize {
    32
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodegenCache {
    #[serde(default = "default_codegen_enabled")]
    enabled: bool,
    #[serde(default)]
    directory: Option<String>,
}

impl Default for CodegenCache {
    fn default() -> Self {
        Self {
            enabled: default_codegen_enabled(),
            directory: None,
        }
    }
}

impl CodegenCache {
    pub(crate) async fn get_directory(
        &self,
        path_prefixes: &PathPrefixes,
        secret_registry: &SecretRegistry,
    ) -> Result<Option<PathBuf>, anyhow::Error> {
        if self.enabled {
            let directory = self.directory.as_deref().unwrap_or_else(|| {
                if path_prefixes.project_dirs.is_some() {
                    DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS
                } else {
                    DEFAULT_CODEGEN_CACHE_DIRECTORY
                }
            });
            path_prefixes
                .server_config_replace_path_prefix_mkdir(directory, secret_registry)
                .await
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimersWatcherTomlConfig {
    #[serde(default = "default_timers_watcher_enabled")]
    pub(crate) enabled: bool,

    #[serde(default = "default_timers_watcher_leeway")]
    // TODO: Add `derivative`,
    // #[derivative(Default(value = "default_timers_watcher_leeway()"))]
    pub(crate) leeway: DurationConfig,
    #[serde(default = "default_timers_watcher_tick_sleep")]
    pub(crate) tick_sleep: DurationConfig,
}
impl Default for TimersWatcherTomlConfig {
    fn default() -> Self {
        Self {
            enabled: default_timers_watcher_enabled(),
            leeway: default_timers_watcher_leeway(),
            tick_sleep: default_timers_watcher_tick_sleep(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelWatcherTomlConfig {
    #[serde(default = "default_cancel_watcher_tick_sleep")]
    pub(crate) tick_sleep: DurationConfig,
}
impl Default for CancelWatcherTomlConfig {
    fn default() -> Self {
        Self {
            tick_sleep: default_cancel_watcher_tick_sleep(),
        }
    }
}

// Components

#[derive(Debug, Clone, Hash)]
pub(crate) struct ComponentCommonVerified {
    pub(crate) name: ConfigName,
    pub(crate) location: ComponentLocationToml,
}

pub(crate) trait ComponentLocationFetchExt {
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentLocationFetchExt for ComponentLocationToml {
    /// Fetch wasm file and calculate its content digest.
    ///
    /// Read wasm file either from local fs, or pull from an OCI registry and cache it.
    async fn fetch(
        &self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ContentDigest, PathBuf), anyhow::Error> {
        use utils::sha256sum::calculate_sha256_file;

        debug!("Fetching {self:?}");
        let stopwatch = std::time::Instant::now();

        let (actual_digest, path) = match &self {
            ComponentLocationToml::Path(wasm_path) => {
                let wasm_path = PathBuf::from(wasm_path);
                if !wasm_path.exists() {
                    bail!("file does not exist: {wasm_path:?}");
                }
                let actual_digest = calculate_sha256_file(&wasm_path)
                    .await
                    .with_context(|| format!("cannot compute hash of file `{wasm_path:?}`"))?;
                (actual_digest, wasm_path)
            }
            ComponentLocationToml::Oci(image) => {
                let image = oci_client::Reference::from_str(image)
                    .map_err(|e| anyhow!("invalid OCI reference `{image}`: {e}"))?;
                let (digest, path, _, _) =
                    oci::pull_to_cache_dir(&image, wasm_cache_dir, metadata_dir)
                        .await
                        .context("try cleaning the cache directory with `--clean-cache`")?;
                (digest, path)
            }
        };
        let stopwatch = stopwatch.elapsed();
        debug!("Fetching done in {stopwatch:?}");
        Ok((actual_digest, path))
    }
}

/// Trait for config structs that have an optional `name` and a required `ffqn`.
trait HasOptionalNameAndFfqn {
    fn config_name(&self) -> Option<&ConfigName>;
    fn ffqn(&self) -> &FunctionFqn;
}

impl HasOptionalNameAndFfqn for ActivityJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityExecComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for WorkflowJsComponentConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

impl HasOptionalNameAndFfqn for ActivityStubExtInlineConfigToml {
    fn config_name(&self) -> Option<&ConfigName> {
        self.name.as_ref()
    }
    fn ffqn(&self) -> &FunctionFqn {
        &self.ffqn
    }
}

/// Location of a JavaScript source file.
/// Supports local file paths and OCI registry references (`oci://...`).
/// On-disk format only; replaced by [`ScriptLocationResolved`] before hash computation.
#[derive(Debug, Clone, Hash, JsonSchema, SerializeDisplay, DeserializeFromStr)]
#[schemars(with = "String")]
pub(crate) enum ScriptLocationPathOrOci {
    Path(String),
    Oci(oci_client::Reference),
}
impl Display for ScriptLocationPathOrOci {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptLocationPathOrOci::Path(p) => write!(f, "{p}"),
            ScriptLocationPathOrOci::Oci(r) => write!(f, "{OCI_SCHEMA_PREFIX}{r}"),
        }
    }
}
impl FromStr for ScriptLocationPathOrOci {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(location) = s.strip_prefix(OCI_SCHEMA_PREFIX) {
            Ok(ScriptLocationPathOrOci::Oci(
                oci_client::Reference::from_str(location)
                    .map_err(|e| anyhow::anyhow!("invalid OCI reference: {e}"))?,
            ))
        } else {
            Ok(ScriptLocationPathOrOci::Path(s.to_string()))
        }
    }
}

pub(crate) trait ComponentCommonFetchExt {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error>;
}

impl ComponentCommonFetchExt for ComponentCommon {
    async fn fetch(
        self,
        wasm_cache_dir: &Path,
        metadata_dir: &Path,
    ) -> Result<(ComponentCommonVerified, ContentDigest, PathBuf), anyhow::Error> {
        let (content_digest, wasm_path) = self.location.fetch(wasm_cache_dir, metadata_dir).await?;

        let verified = ComponentCommonVerified {
            name: self.name,
            location: self.location,
        };
        Ok((verified, content_digest, wasm_path))
    }
}

fn verify_fetched_content_digest(
    actual: &ContentDigest,
    expected: Option<&ContentDigest>,
    what: &str,
) -> anyhow::Result<()> {
    ensure!(
        expected.is_none_or(|expected| expected == actual),
        "content digest mismatch for {what}: expected {}, got {actual}",
        expected.expect("checked above")
    );
    Ok(())
}

fn locking_strategy_into_executor(value: LockingStrategy) -> executor::executor::LockingStrategy {
    match value {
        LockingStrategy::ByFfqns => executor::executor::LockingStrategy::ByFfqns,
        LockingStrategy::ByComponentDigest => {
            executor::executor::LockingStrategy::ByComponentDigest
        }
        LockingStrategy::Auto => executor::executor::LockingStrategy::Auto,
    }
}

pub(crate) trait ExecConfigTomlExt {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error>;
}

impl ExecConfigTomlExt for ExecConfigToml {
    fn into_exec_exec_config(
        self,
        component_id: ComponentId,
        task_limiter_global: Option<Arc<tokio::sync::Semaphore>>,
        retry_config: ComponentRetryConfig,
    ) -> Result<executor::executor::ExecConfig, anyhow::Error> {
        Ok(executor::executor::ExecConfig {
            lock_expiry: self.lock_expiry.into(),
            tick_sleep: self.tick_sleep.into(),
            batch_size: self.batch_size,
            locking_strategy: locking_strategy(self.locking_strategy, component_id.component_type)?,
            component_id,
            task_limiter_global,
            task_limiter_local: self.instance_limiter.as_semaphore(),
            executor_id: ExecutorId::generate(),
            retry_config,
        })
    }
}

fn locking_strategy(
    locking_strategy_override: Option<LockingStrategy>,
    component_type: ComponentType,
) -> Result<executor::executor::LockingStrategy, anyhow::Error> {
    if component_type == ComponentType::Cron {
        ensure!(
            locking_strategy_override.is_none(),
            "locking strategy cannot be overridden for cron"
        );
        // needed for seed execution deduplication.
        return Ok(executor::executor::LockingStrategy::ByComponentDigest);
    }
    // Auto is only valid for workflows
    if component_type != ComponentType::Workflow
        && locking_strategy_override == Some(LockingStrategy::Auto)
    {
        bail!("Locking strategy `auto` is only available for workflows");
    }
    Ok(locking_strategy_override.map(locking_strategy_into_executor).unwrap_or_else(||
    match component_type {
        ComponentType::Activity => executor::executor::LockingStrategy::ByFfqns,
        ComponentType::Workflow => executor::executor::LockingStrategy::Auto,
        other => unreachable!(
            "unexpected type {other}, only workflows, activities, and crons expose locking strategy"
        ),
    }))
}

pub(crate) trait LogLevelTomlExt {
    fn into_log_level(self) -> Option<LogLevel>;
}
impl LogLevelTomlExt for LogLevelToml {
    fn into_log_level(self) -> Option<LogLevel> {
        match self {
            LogLevelToml::Off => None,
            LogLevelToml::Trace => Some(LogLevel::Trace),
            LogLevelToml::Debug => Some(LogLevel::Debug),
            LogLevelToml::Info => Some(LogLevel::Info),
            LogLevelToml::Warn => Some(LogLevel::Warn),
            LogLevelToml::Error => Some(LogLevel::Error),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityStubExtInlineConfigToml {
    /// Component name. Optional — defaults to `{ifc_name}.{function_name}` from `ffqn`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Generated CAS references for the parser-selected WIT files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthoredFunctionInterfaceToml {
    /// Authored WIT directory containing the exported `ffqn`.
    pub(crate) wit: String,
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct InlineFunctionInterfaceToml {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) params: Option<Vec<JsParamToml>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) return_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum FunctionInterfaceToml {
    Authored(AuthoredFunctionInterfaceToml),
    Inline(InlineFunctionInterfaceToml),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityStubComponentConfigToml {
    File(ActivityStubFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum ActivityExternalComponentConfigToml {
    File(ActivityExternalFileConfigToml),
    Inline(ActivityStubExtInlineConfigToml),
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) component_id: ComponentId,
}

#[derive(Debug)]
pub(crate) struct ActivityStubExtInlineConfigVerified {
    pub(crate) component_id: ComponentId,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
}

struct VerifiedFunctionInterface {
    params: Vec<concepts::ParameterType>,
    return_type: concepts::ReturnTypeExtendable,
    authored_component: Option<WasmComponent>,
}

fn verify_function_interface(
    interface: FunctionInterfaceResolved,
    ffqn: &FunctionFqn,
    component_type: ComponentType,
    default_params: Vec<concepts::ParameterType>,
    default_return_type: &str,
) -> anyhow::Result<VerifiedFunctionInterface> {
    match interface {
        FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
            params,
            return_type,
        }) => {
            let params = match params {
                None => default_params,
                Some(params) => params
                    .iter()
                    .map(|param| {
                        let type_wrapper = val_json::type_wrapper::parse_wit_type(&param.wit_type)
                            .map_err(|err| {
                                anyhow!("invalid param type `{}`: {err}", param.wit_type)
                            })?;
                        Ok(concepts::ParameterType {
                            type_wrapper,
                            name: StrVariant::from(param.name.clone()),
                            wit_type: StrVariant::from(param.wit_type.clone()),
                        })
                    })
                    .collect::<anyhow::Result<_>>()?,
            };
            let return_type_str = return_type.as_deref().unwrap_or(default_return_type);
            let type_wrapper = val_json::type_wrapper::parse_wit_type(return_type_str)
                .map_err(|err| anyhow!("invalid return_type `{return_type_str}`: {err}"))?;
            let return_type = concepts::ReturnType::detect(
                type_wrapper,
                StrVariant::from(return_type_str.to_string()),
            );
            let ReturnType::Extendable(return_type) = return_type else {
                bail!(
                    "return_type must be `result`, `result<T>`, `result<T, string>`, or \
                     `result<T, variant {{ execution-failed, ... }}>`, got `{return_type_str}`"
                )
            };
            Ok(VerifiedFunctionInterface {
                params,
                return_type,
                authored_component: None,
            })
        }
        FunctionInterfaceResolved::Authored { wit } => {
            let root = wit.root;
            let component = WasmComponent::new_from_wit_resolve_for_ffqn(
                wit.resolve,
                wit.main_pkg_id,
                component_type,
                ffqn,
            )
            .with_context(|| format!("cannot verify authored WIT directory `{root}`"))?;
            let exports = component.exported_functions(false);
            ensure!(
                exports.len() == 1 && exports[0].ffqn == *ffqn,
                "authored WIT must expose exactly selected function `{ffqn}`"
            );
            let metadata = &exports[0];
            let ReturnType::Extendable(return_type) = &metadata.return_type else {
                bail!("authored WIT function `{ffqn}` must return an extendable result")
            };
            Ok(VerifiedFunctionInterface {
                params: metadata.parameter_types.0.clone(),
                return_type: return_type.clone(),
                authored_component: Some(component),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActivityStubConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(Box<ActivityStubExtInlineConfigVerified>),
}

pub(crate) trait ActivityStubComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error>;
}

impl ActivityStubComponentConfigResolvedExt for ActivityStubComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityStubConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let expected_content_digest = file.content_digest;
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                verify_fetched_content_digest(
                    &content_digest,
                    expected_content_digest.as_ref(),
                    &common.location.to_string(),
                )?;
                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(common.name),
                    ComponentDigest(content_digest.0),
                )?;
                Ok(ActivityStubConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let default_params = vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }];
                let verified = verify_function_interface(
                    inline.interface,
                    &ffqn,
                    ComponentType::ActivityStub,
                    default_params,
                    "result<string, string>",
                )?;
                let parsed_params = verified.params;
                let return_type = verified.return_type;

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_stub_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                if let Some(component) = &verified.authored_component {
                    hasher.update(component.wit().as_bytes());
                }
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::ActivityStub,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityStubConfigVerified::Inline(Box::new(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                        user_wasm_component: verified.authored_component,
                    },
                )))
            }
        }
    }
}
#[derive(Debug)]
pub(crate) enum ActivityExternalConfigVerified {
    File(ActivityStubExtConfigVerified),
    Inline(Box<ActivityStubExtInlineConfigVerified>),
}

pub(crate) trait ActivityExternalComponentConfigResolvedExt {
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error>;
}

impl ActivityExternalComponentConfigResolvedExt for ActivityExternalComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name_str(), component_id))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
    ) -> Result<ActivityExternalConfigVerified, anyhow::Error> {
        match self {
            Self::File(file) => {
                let component_digest_override = file.component_digest;
                let expected_content_digest = file.content_digest;
                let (common, content_digest, wasm_path) =
                    file.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
                warn_deprecated_component_digest_override(
                    common.name.as_str(),
                    component_digest_override.as_ref(),
                );
                verify_fetched_content_digest(
                    &content_digest,
                    expected_content_digest.as_ref(),
                    &common.location.to_string(),
                )?;
                let component_digest =
                    component_digest_override.unwrap_or(ComponentDigest(content_digest.0));
                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(common.name),
                    component_digest,
                )?;
                Ok(ActivityExternalConfigVerified::File(
                    ActivityStubExtConfigVerified {
                        wasm_path,
                        component_id,
                    },
                ))
            }
            Self::Inline(inline) => {
                let ffqn = inline.ffqn;
                let default_params = vec![concepts::ParameterType {
                    type_wrapper: val_json::type_wrapper::TypeWrapper::List(Box::new(
                        val_json::type_wrapper::TypeWrapper::String,
                    )),
                    name: StrVariant::Static("params"),
                    wit_type: StrVariant::Static("list<string>"),
                }];
                let verified = verify_function_interface(
                    inline.interface,
                    &ffqn,
                    ComponentType::Activity,
                    default_params,
                    "result<string, string>",
                )?;
                let parsed_params = verified.params;
                let return_type = verified.return_type;

                // Compute component digest: SHA256 of prefix + ffqn + params + return_type
                let mut hasher = Sha256::new();
                hasher.update(b"activity_external_inline:");
                hasher.update(ffqn.to_string().as_bytes());
                for p in &parsed_params {
                    hasher.update(p.wit_type.as_ref().as_bytes());
                }
                hasher.update(return_type.wit_type.as_bytes());
                if let Some(component) = &verified.authored_component {
                    hasher.update(component.wit().as_bytes());
                }
                let hash: [u8; 32] = hasher.finalize().into();
                let component_digest = ComponentDigest(Digest(hash));

                let component_id = ComponentId::new(
                    ComponentType::Activity,
                    StrVariant::from(inline.name),
                    component_digest,
                )?;

                Ok(ActivityExternalConfigVerified::Inline(Box::new(
                    ActivityStubExtInlineConfigVerified {
                        component_id,
                        ffqn,
                        params: parsed_params,
                        return_type,
                        user_wasm_component: verified.authored_component,
                    },
                )))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct ActivityWasmConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityWasmConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }
}

pub(crate) trait ActivityWasmComponentConfigTomlExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error>;
}

impl ActivityWasmComponentConfigTomlExt for ActivityWasmComponentConfigToml {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityWasmConfigVerified, anyhow::Error> {
        let expected_content_digest = self.content_digest;
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        verify_fetched_content_digest(
            &content_digest,
            expected_content_digest.as_ref(),
            &common.location.to_string(),
        )?;
        warn_deprecated_component_digest_override(
            common.name.as_str(),
            self.component_digest.as_ref(),
        );

        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;

        // Validate no collision between env_vars and secret env names
        validate_no_env_collision(&env_vars, &allowed_hosts)?;

        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            global_http_config,
            secrets,
            config_section_hint: ConfigSectionHint::ActivityWasm,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityWasmConfigVerified {
            wasm_path,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// CAS references for the closed module graph, populated during deployment preparation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    /// The synthesized return type must be `result<T, string>`.
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
    #[serde(default, rename = "allowed_host")]
    pub(crate) allowed_hosts: Vec<AllowedHostToml>,
}
#[derive(Debug)]
pub(crate) struct ActivityJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS activities
    pub(crate) js_entry_path: String,
    pub(crate) js_files: BTreeMap<String, String>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) activity_config: ActivityConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.activity_config.component_id
    }

    pub(crate) fn as_frame_sources(&self) -> FrameFilesToSource {
        self.js_files
            .clone()
            .into_iter()
            .map(|(name, content)| (name, FrameSource::Content(content)))
            .collect()
    }
}

// --- activity_exec config ---

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct ActivityExecComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the exec script.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline script content embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the exec script.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Generated CAS references for parser-selected WIT files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the exec activity.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_max_retries")]
    pub(crate) max_retries: u32,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) forward_stdout: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) forward_stderr: ComponentStdOutputToml,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
    #[serde(default)]
    pub(crate) env_vars: Vec<EnvVarConfig>,
    /// Maximum bytes collected from stdout to form the response.
    /// Exceeding the limit fails the execution.
    /// Not used when `return_type` is result (default), since the response carries no data.
    #[serde(default = "default_max_output_bytes")]
    pub(crate) max_output_bytes: u64,
    /// Registered secret names (from the operator-owned `server.toml` `[secrets]`
    /// table) to expose to the script in the stdin JSON `secrets` object.
    #[serde(default)]
    pub(crate) secrets: Vec<String>,
    /// Pass parameters to the program via the stdin JSON `params` array instead
    /// of argv. Use this for large payloads that would exceed the `execve` argument-size
    /// limit. Defaults to `false` (parameters passed as command-line arguments).
    #[serde(default)]
    pub(crate) params_via_stdin: bool,
}

#[derive(Debug)]
pub(crate) struct ResolvedExecProgram {
    /// Path to the immutable cached script file the worker executes directly.
    pub(crate) program: PathBuf,
    /// Content digest of the script text (component identity and allowlist line).
    pub(crate) content_digest: ContentDigest,
}

async fn write_inline_exec_file_to_cache_dir(
    exec_path: &Path,
    exec_cache_dir: &Path,
    content: &[u8],
) -> anyhow::Result<()> {
    if let Ok(existing) = tokio::fs::read(exec_path).await
        && existing == content
    {
        return Ok(());
    }
    let tmp = tempfile::NamedTempFile::new_in(exec_cache_dir)?;
    tokio::fs::write(tmp.path(), content).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o755)).await?;
    }
    tmp.persist(exec_path)?;
    Ok(())
}

pub(crate) trait ActivityExecComponentConfigResolvedExt {
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram>;
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error>;
}

impl ActivityExecComponentConfigResolvedExt for ActivityExecComponentConfigResolved {
    /// Resolve the program to a form the worker can execute.
    async fn resolve(
        &self,
        wasm_cache_dir: &std::path::Path,
    ) -> anyhow::Result<ResolvedExecProgram> {
        let exec_cache_dir = wasm_cache_dir.join("exec");
        match &self.location {
            ScriptLocationResolved::Content { content, .. } => {
                let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                let content_digest = ContentDigest(Digest(hash));
                if let Some(expected) = self.content_digest.as_ref() {
                    ensure!(
                        *expected == content_digest,
                        "content digest mismatch for inline exec content: expected {expected}, got {content_digest}"
                    );
                }
                tokio::fs::create_dir_all(&exec_cache_dir).await?;
                let exec_path = content_digest_to_exec_file(&exec_cache_dir, &content_digest);
                write_inline_exec_file_to_cache_dir(
                    &exec_path,
                    &exec_cache_dir,
                    content.as_bytes(),
                )
                .await?;
                Ok(ResolvedExecProgram {
                    program: exec_path,
                    content_digest,
                })
            }
            ScriptLocationResolved::Graph { .. } => {
                bail!("activity_exec does not support module graphs")
            }
            ScriptLocationResolved::Oci { image } => {
                tokio::fs::create_dir_all(&exec_cache_dir).await?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                let result =
                    crate::oci::pull_exec_to_cache(image, &exec_cache_dir, &metadata_dir).await?;
                if let Some(expected) = self.content_digest.as_ref() {
                    let actual = utils::sha256sum::calculate_sha256_file(&result.exec_path).await?;
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for OCI exec `{image}`: expected {expected}, got {actual}"
                    );
                }
                Ok(ResolvedExecProgram {
                    program: result.exec_path,
                    content_digest: result.content_digest,
                })
            }
        }
    }

    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    fn fetch_and_verify(
        self,
        resolved_program: ResolvedExecProgram,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Result<ActivityExecConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Activity,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_exec:");
            hasher.update(resolved_program.content_digest.0.0);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        // Carry only the declared names plus a component-scoped resolver; values are
        // fetched by name when the child's stdin is assembled, never baked here.
        let resolved_secrets = if self.secrets.is_empty() {
            None
        } else {
            let resolver =
                restricted_secret_registry(secret_registry, &[], self.secrets.iter().cloned());
            Some(ExecSecrets {
                names: self.secrets,
                resolver,
            })
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityExecConfigVerified {
            program: resolved_program.program,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            env_vars,
            max_output_bytes: self.max_output_bytes,
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            secrets: resolved_secrets,
            params_via_stdin: self.params_via_stdin,
            component_id: component_id.clone(),
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ActivityExecConfigVerified {
    pub(crate) program: PathBuf,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) env_vars: Arc<[EnvVar]>,
    pub(crate) max_output_bytes: u64,
    pub(crate) forward_stdout: Option<StdOutputConfig>,
    pub(crate) forward_stderr: Option<StdOutputConfig>,
    pub(crate) secrets: Option<wasm_workers::activity::activity_exec_worker::ExecSecrets>,
    pub(crate) params_via_stdin: bool,
    pub(crate) component_id: ComponentId,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
}

impl ActivityExecConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.component_id
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
pub(crate) struct WorkflowJsComponentConfigToml {
    /// Component name. Optional when `ffqn` is specified — defaults to `{ifc_name}.{function_name}`.
    #[serde(default)]
    pub(crate) name: Option<ConfigName>,
    /// Location of the JavaScript source file.
    /// Supports local file paths and OCI registry references (`oci://...`).
    #[serde(default)]
    pub(crate) location: Option<ScriptLocationPathOrOci>,
    /// Inline JavaScript source embedded in the TOML.
    /// Exactly one of `location` or `content` must be set.
    #[serde(default)]
    pub(crate) content: Option<String>,
    /// Content digest of the JS source file.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// CAS references for the closed module graph, populated during deployment preparation.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[schemars(with = "String")]
    pub(crate) ffqn: FunctionFqn,
    /// Custom parameters for the JS workflow function.
    /// Each entry has a `name` and a WIT `type` (e.g. `string`, `u32`, `list<string>`).
    /// Defaults to no parameters.
    /// The synthesized return type must be an extendable `result`.
    #[serde(flatten)]
    pub(crate) interface: FunctionInterfaceToml,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    /// Starts extending the lock shortly before it expires, at `expires_at` minus this leeway.
    #[serde(default = "default_lock_extension_leeway")]
    pub(crate) lock_extension_leeway: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

#[derive(Debug)]
pub(crate) struct WorkflowJsConfigVerified {
    pub(crate) wasm_path: Arc<Path>, // same for all JS workflows
    pub(crate) js_entry_path: String,
    pub(crate) js_files: BTreeMap<String, String>,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) params: Vec<concepts::ParameterType>,
    pub(crate) return_type: concepts::ReturnTypeExtendable,
    pub(crate) user_wasm_component: Option<WasmComponent>,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) lock_extension_leeway: Duration,
}

impl WorkflowJsConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }

    pub(crate) fn frame_sources(js_files: BTreeMap<String, String>) -> FrameFilesToSource {
        js_files
            .into_iter()
            .map(|(name, content)| (name, FrameSource::Content(content)))
            .collect()
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowWasmComponentConfigToml {
    #[serde(flatten)]
    pub(crate) common: ComponentCommon,
    /// Optional content digest of the WASM file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub(crate) content_digest: Option<ContentDigest>,
    /// Generated CAS references for backtrace source files.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schemars(skip)]
    pub(crate) component_files: BTreeMap<String, ContentDigest>,
    /// Deprecated override of the auto-computed component digest used for locking.
    /// This option will be removed in 0.42.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub(crate) component_digest: Option<ComponentDigest>,
    #[serde(default)]
    pub(crate) exec: ExecConfigToml,
    #[serde(default = "default_retry_exp_backoff")]
    pub(crate) retry_exp_backoff: DurationConfig,
    #[serde(default)]
    pub(crate) blocking_strategy: BlockingStrategyConfigToml,
    #[serde(default)]
    pub(crate) backtrace: ComponentBacktraceConfig,
    #[serde(default)]
    pub(crate) stub_wasi: bool,
    #[serde(default = "default_lock_extension")]
    pub(crate) lock_extension: bool,
    /// Starts extending the lock shortly before it expires, at `expires_at` minus this leeway.
    #[serde(default = "default_lock_extension_leeway")]
    pub(crate) lock_extension_leeway: DurationConfig,
    #[serde(default)]
    pub(crate) logs_store_min_level: LogLevelToml,
}

pub(crate) trait BlockingStrategyConfigTomlExt {
    fn into_blocking_strategy(
        self,
        subscription_interruption: Option<Duration>,
    ) -> JoinNextBlockingStrategy;
}
impl BlockingStrategyConfigTomlExt for BlockingStrategyConfigToml {
    fn into_blocking_strategy(
        self,
        subscription_interruption: Option<Duration>,
    ) -> JoinNextBlockingStrategy {
        use crate::config::toml::model::{
            BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
            BlockingStrategyConfigSimple,
        };
        match self {
            BlockingStrategyConfigToml::Tagged(BlockingStrategyConfigCustomized::Await(
                BlockingStrategyAwaitConfig {
                    non_blocking_event_batching,
                },
            )) => JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
                subscription_interruption,
            },
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Interrupt) => {
                JoinNextBlockingStrategy::Interrupt
            }
            BlockingStrategyConfigToml::Simple(BlockingStrategyConfigSimple::Await) => {
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                    subscription_interruption,
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentBacktraceConfig {
    /// Maps a frame-symbol key to a backtrace source file path. On-disk format only;
    /// resolved to `ComponentBacktraceConfigResolved` before hash
    /// computation. A relative path is deployment-dir-relative (a leading
    /// `${DEPLOYMENT_DIR}/` is accepted for backcompat); absolute paths are rejected.
    #[serde(rename = "sources")]
    #[schemars(with = "std::collections::HashMap<String, String>")]
    pub(crate) frame_files_to_sources: HashMap<String, BacktraceSourceToml>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
#[serde(untagged)]
pub(crate) enum BacktraceSourceToml {
    Path(String),
    // backcompat: Remove in 0.42
    Detailed {
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(with = "Option<String>")]
        content_digest: Option<ContentDigest>,
    },
}

impl BacktraceSourceToml {
    fn path(&self) -> &str {
        match self {
            Self::Path(path) | Self::Detailed { path, .. } => path,
        }
    }

    fn content_digest(&self) -> Option<&ContentDigest> {
        match self {
            Self::Path(_) => None,
            Self::Detailed { content_digest, .. } => content_digest.as_ref(),
        }
    }
}

impl From<String> for BacktraceSourceToml {
    fn from(value: String) -> Self {
        Self::Path(value)
    }
}
pub(crate) struct JsContent {
    pub(crate) entry_path: String,
    pub(crate) files: BTreeMap<String, String>,
}

fn hash_js_graph(hasher: &mut Sha256, entry_path: &str, files: &BTreeMap<String, String>) {
    if files.len() == 1
        && let Some((only_path, only_source)) = files.iter().next()
        && only_path == entry_path
    {
        hasher.update(only_source.as_bytes());
        return;
    }
    hasher.update(b"v1\0");
    for (path, source) in files {
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hasher.update(source.as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(entry_path.as_bytes());
}

pub(crate) trait JsLocationResolvedExt {
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent>;
}

impl JsLocationResolvedExt for ScriptLocationResolved {
    /// Return the JS source content and file name.
    /// For `Content`, returns them directly (validating digest if provided).
    /// For `Oci`, pulls from the registry (or cache) under `wasm_cache_dir/js/`.
    async fn get_content(
        &self,
        wasm_cache_dir: &Path,
        expected_digest: Option<&ContentDigest>,
    ) -> anyhow::Result<JsContent> {
        match self {
            ScriptLocationResolved::Content { content, file_name } => {
                if let Some(expected) = expected_digest {
                    let hash: [u8; 32] = Sha256::digest(content.as_bytes()).into();
                    let actual = ContentDigest(Digest(hash));
                    ensure!(
                        *expected == actual,
                        "content digest mismatch for inline JS `{file_name}`: expected {expected}, got {actual}"
                    );
                }
                // Preserve the historical basename used by single-file stack frames and
                // backtrace source lookup. True module graphs use deployment-relative keys.
                let entry_path = std::path::Path::new(file_name)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(file_name)
                    .to_string();
                let files = BTreeMap::from([(entry_path.clone(), content.clone())]);
                Ok(JsContent { entry_path, files })
            }
            ScriptLocationResolved::Graph { entry_path, files } => Ok(JsContent {
                entry_path: entry_path.clone(),
                files: files.iter().cloned().collect(),
            }),
            ScriptLocationResolved::Oci { image } => {
                let js_cache_dir = wasm_cache_dir.join("js");
                tokio::fs::create_dir_all(&js_cache_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create JS cache directory {js_cache_dir:?}")
                    })?;
                let metadata_dir = wasm_cache_metadata_dir(wasm_cache_dir);
                tokio::fs::create_dir_all(&metadata_dir)
                    .await
                    .with_context(|| {
                        format!("cannot create metadata directory {metadata_dir:?}")
                    })?;
                let crate::oci::JsCacheResult { js_path, .. } =
                    crate::oci::pull_js_to_cache(image, &js_cache_dir, &metadata_dir)
                        .await
                        .with_context(|| format!("cannot pull JS from OCI: {image}"))?;
                if let Some(expected) = expected_digest {
                    let hash = utils::sha256sum::calculate_sha256_file(&js_path).await?;
                    ensure!(
                        *expected == hash,
                        "content digest mismatch for OCI JS `{image}`: expected {expected}, got {hash}"
                    );
                }
                let file_name = js_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("cached.js")
                    .to_string();
                let source = tokio::fs::read_to_string(&js_path)
                    .await
                    .with_context(|| format!("cannot read cached JS file {js_path:?}"))?;
                Ok(JsContent {
                    entry_path: file_name.clone(),
                    files: BTreeMap::from([(file_name, source)]),
                })
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct WorkflowConfigVerified {
    pub(crate) wasm_path: PathBuf,
    pub(crate) workflow_config: WorkflowConfig,
    pub(crate) exec_config: executor::executor::ExecConfig,
    pub(crate) frame_files_to_sources: FrameFilesToSource,
    pub(crate) logs_store_min_level: Option<LogLevel>,
    pub(crate) lock_extension_leeway: Duration,
}

impl WorkflowConfigVerified {
    pub fn component_id(&self) -> &ComponentId {
        &self.workflow_config.component_id
    }
}

// Resolved component config types live in the `model` submodule.

pub(crate) trait ActivityJsComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error>;
}

impl ActivityJsComponentConfigResolvedExt for ActivityJsComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        secret_registry: &Arc<SecretRegistry>,
        global_http_config: GlobalHttpConfig,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
    ) -> Result<ActivityJsConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Activity,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        let JsContent {
            entry_path: js_entry_path,
            files: js_files,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"activity_js:");
            hash_js_graph(&mut hasher, &js_entry_path, &js_files);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Activity,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let env_vars =
            resolve_env_vars_plaintext(self.env_vars, ignore_missing_env_vars, secret_registry)?;
        let (allowed_hosts, _advisories) =
            resolve_allowed_hosts(self.allowed_hosts, ignore_missing_env_vars, secret_registry)?;
        validate_no_env_collision(&env_vars, &allowed_hosts)?;
        let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
        let activity_config = ActivityConfig {
            component_id: component_id.clone(),
            forward_stdout: self.forward_stdout.into_std_output_config(),
            forward_stderr: self.forward_stderr.into_std_output_config(),
            env_vars,
            fuel,
            allowed_hosts,
            global_http_config,
            secrets,
            config_section_hint: ConfigSectionHint::ActivityJs,
        };
        let retry_config = ComponentRetryConfig {
            max_retries: Some(self.max_retries),
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(ActivityJsConfigVerified {
            wasm_path,
            js_entry_path,
            js_files,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            activity_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
        })
    }
}

pub(crate) trait WorkflowWasmComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowConfigVerified, anyhow::Error>;
}

impl WorkflowWasmComponentConfigResolvedExt for WorkflowWasmComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.common.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowConfigVerified, anyhow::Error> {
        let retry_exp_backoff = Duration::from(self.retry_exp_backoff);
        if retry_exp_backoff == Duration::ZERO {
            bail!(
                "invalid `retry_exp_backoff` setting for workflow `{}` - duration must not be zero",
                self.common.name
            );
        }
        let expected_content_digest = self.content_digest;
        let (common, content_digest, wasm_path) =
            self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
        verify_fetched_content_digest(
            &content_digest,
            expected_content_digest.as_ref(),
            &common.location.to_string(),
        )?;
        let wasm_path = WasmComponent::convert_core_module_to_component(
            &wasm_path,
            &content_digest,
            &wasm_cache_dir,
        )
        .await?
        .unwrap_or(wasm_path);
        warn_deprecated_component_digest_override(
            common.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self
            .component_digest
            .unwrap_or(ComponentDigest(content_digest.0));
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(common.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: self.stub_wasi,
            fuel,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: self
                    .blocking_strategy
                    .into_blocking_strategy(subscription_interruption),
                lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
                max_events_per_run,
                response_refresh_interval,
            },
        };
        let frame_files_to_sources: FrameFilesToSource = self
            .backtrace
            .into_frame_files()
            .into_iter()
            .map(|(name, digest)| (name, FrameSource::Digest(digest)))
            .collect();
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff,
        };
        Ok(WorkflowConfigVerified {
            wasm_path,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            frame_files_to_sources,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
            lock_extension_leeway: self.lock_extension_leeway.into(),
        })
    }
}

pub(crate) trait WorkflowJsComponentConfigResolvedExt {
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error>;
}

impl WorkflowJsComponentConfigResolvedExt for WorkflowJsComponentConfigResolved {
    #[instrument(skip_all, fields(component_name = self.name.as_str()))]
    async fn fetch_and_verify(
        self,
        wasm_path: Arc<Path>,
        wasm_cache_dir: Arc<Path>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        subscription_interruption: Option<Duration>,
        max_events_per_run: usize,
        response_refresh_interval: usize,
    ) -> Result<WorkflowJsConfigVerified, anyhow::Error> {
        let verified = verify_function_interface(
            self.interface,
            &self.ffqn,
            ComponentType::Workflow,
            Vec::new(),
            "result",
        )?;
        let parsed_params = verified.params;
        let return_type = verified.return_type;
        let JsContent {
            entry_path: js_entry_path,
            files: js_files,
        } = self
            .location
            .get_content(&wasm_cache_dir, self.content_digest.as_ref())
            .await?;
        warn_deprecated_component_digest_override(
            self.name.as_str(),
            self.component_digest.as_ref(),
        );
        let component_digest = self.component_digest.unwrap_or_else(|| {
            let mut hasher = Sha256::new();
            hasher.update(b"workflow_js:");
            hash_js_graph(&mut hasher, &js_entry_path, &js_files);
            hasher.update(self.ffqn.to_string().as_bytes());
            for p in &parsed_params {
                hasher.update(p.wit_type.as_ref().as_bytes());
            }
            hasher.update(return_type.wit_type.as_bytes());
            if let Some(component) = &verified.authored_component {
                hasher.update(component.wit().as_bytes());
            }
            let hash: [u8; 32] = hasher.finalize().into();
            ComponentDigest(Digest(hash))
        });
        let component_id = ComponentId::new(
            ComponentType::Workflow,
            StrVariant::from(self.name),
            component_digest,
        )?;
        let workflow_config = WorkflowConfig {
            component_id: component_id.clone(),
            stub_wasi: false,
            fuel,
            mode: WorkflowConfigMode::Real {
                join_next_blocking_strategy: self
                    .blocking_strategy
                    .into_blocking_strategy(subscription_interruption),
                lock_extension: self.lock_extension.then_some(self.exec.lock_expiry.into()),
                max_events_per_run,
                response_refresh_interval,
            },
        };
        let retry_config = ComponentRetryConfig {
            max_retries: None,
            retry_exp_backoff: self.retry_exp_backoff.into(),
        };
        Ok(WorkflowJsConfigVerified {
            wasm_path,
            js_entry_path,
            js_files,
            ffqn: self.ffqn,
            params: parsed_params,
            return_type,
            user_wasm_component: verified.authored_component,
            workflow_config,
            exec_config: self.exec.into_exec_exec_config(
                component_id,
                global_executor_instance_limiter,
                retry_config,
            )?,
            logs_store_min_level: self.logs_store_min_level.into_log_level(),
            lock_extension_leeway: self.lock_extension_leeway.into(),
        })
    }
}

/// Resolve a `DeploymentToml` to `DeploymentResolved` by reading all local JS and backtrace
/// source files.
async fn resolve_local_refs(
    deployment: DeploymentTomlValidated,
    cas: &dyn Cas,
) -> anyhow::Result<DeploymentResolved> {
    let mut activities_js = Vec::with_capacity(deployment.activities_js.len());
    for (mut a, name) in deployment.activities_js {
        let interface =
            resolve_function_interface(a.interface, &mut a.component_files, cas).await?;
        activities_js.push(ActivityJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: a.location,
                    content: a.content,
                    component_files: a.component_files,
                },
                format!("{name}.js"),
                cas,
                a.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: a.content_digest,
            component_digest: a.component_digest,
            ffqn: a.ffqn,
            interface,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            allowed_hosts: a.allowed_hosts,
        });
    }

    let mut workflows_wasm = Vec::with_capacity(deployment.workflows_wasm.len());
    for w in deployment.workflows_wasm {
        workflows_wasm.push(WorkflowWasmComponentConfigResolved {
            common: w.common,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            backtrace: resolve_backtrace(&w.backtrace, &w.component_files)?,
            stub_wasi: w.stub_wasi,
            lock_extension: w.lock_extension,
            lock_extension_leeway: w.lock_extension_leeway,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut workflows_js = Vec::with_capacity(deployment.workflows_js.len());
    for (mut w, name) in deployment.workflows_js {
        let interface =
            resolve_function_interface(w.interface, &mut w.component_files, cas).await?;
        workflows_js.push(WorkflowJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: w.location,
                    content: w.content,
                    component_files: w.component_files,
                },
                format!("{name}.js"),
                cas,
                w.content_digest.as_ref(),
            )
            .await?,
            name,
            content_digest: w.content_digest,
            component_digest: w.component_digest,
            ffqn: w.ffqn,
            interface,
            exec: w.exec,
            retry_exp_backoff: w.retry_exp_backoff,
            blocking_strategy: w.blocking_strategy,
            lock_extension: w.lock_extension,
            lock_extension_leeway: w.lock_extension_leeway,
            logs_store_min_level: w.logs_store_min_level,
        });
    }

    let mut webhooks_wasm = Vec::with_capacity(deployment.webhooks_wasm.len());
    for w in deployment.webhooks_wasm {
        webhooks_wasm.push(webhook::WebhookWasmComponentConfigResolved {
            common: w.common,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            env_vars: w.env_vars,
            backtrace: resolve_backtrace(&w.backtrace, &w.component_files)?,
            backtrace_persist: w.backtrace_persist,
            logs_store_min_level: w.logs_store_min_level,
            allowed_hosts: w.allowed_hosts,
            is_webui: false,
        });
    }

    let mut webhooks_js = Vec::with_capacity(deployment.webhooks_js.len());
    for w in deployment.webhooks_js {
        webhooks_js.push(webhook::WebhookJsComponentConfigResolved {
            location: resolve_script_toml(
                ScriptToml::JavaScript {
                    location: w.location,
                    content: w.content,
                    component_files: w.component_files,
                },
                format!("{}.js", w.name),
                cas,
                w.content_digest.as_ref(),
            )
            .await?,
            name: w.name,
            content_digest: w.content_digest,
            http_server: w.http_server,
            routes: w.routes,
            forward_stdout: w.forward_stdout,
            forward_stderr: w.forward_stderr,
            logs_store_min_level: w.logs_store_min_level,
            env_vars: w.env_vars,
            backtrace_persist: w.backtrace_persist,
            allowed_hosts: w.allowed_hosts,
        });
    }

    let mut activities_exec = Vec::with_capacity(deployment.activities_exec.len());
    for (mut a, name) in deployment.activities_exec {
        let interface =
            resolve_function_interface(a.interface, &mut a.component_files, cas).await?;
        ensure!(
            a.component_files.is_empty(),
            "activity_exec component_files contains files not selected by its WIT"
        );
        let location = resolve_script_toml(
            ScriptToml::Exec {
                location: a.location,
                content: a.content,
            },
            name.to_string(),
            cas,
            a.content_digest.as_ref(),
        )
        .await?;
        activities_exec.push(ActivityExecComponentConfigResolved {
            name,
            location,
            content_digest: a.content_digest,
            ffqn: a.ffqn,
            interface,
            component_digest: a.component_digest,
            exec: a.exec,
            max_retries: a.max_retries,
            retry_exp_backoff: a.retry_exp_backoff,
            forward_stdout: a.forward_stdout,
            forward_stderr: a.forward_stderr,
            logs_store_min_level: a.logs_store_min_level,
            env_vars: a.env_vars,
            max_output_bytes: a.max_output_bytes,
            secrets: a.secrets,
            params_via_stdin: a.params_via_stdin,
        });
    }

    // Build resolved stubs/externals with their names filled in.
    let mut activities_stub = Vec::with_capacity(deployment.activities_stub.len());
    for (c, name) in deployment.activities_stub {
        activities_stub.push(match c {
            ActivityStubComponentConfigToml::File(f) => {
                ActivityStubComponentConfigResolved::File(f)
            }
            ActivityStubComponentConfigToml::Inline(mut i) => {
                let interface =
                    resolve_function_interface(i.interface, &mut i.component_files, cas).await?;
                ensure!(
                    i.component_files.is_empty(),
                    "activity_stub component_files contains files not selected by its WIT"
                );
                ActivityStubComponentConfigResolved::Inline(ActivityStubExtInlineConfigResolved {
                    name,
                    ffqn: i.ffqn,
                    interface,
                })
            }
        });
    }
    let mut activities_external = Vec::with_capacity(deployment.activities_external.len());
    for (c, name) in deployment.activities_external {
        activities_external.push(match c {
            ActivityExternalComponentConfigToml::File(f) => {
                ActivityExternalComponentConfigResolved::File(f)
            }
            ActivityExternalComponentConfigToml::Inline(mut i) => {
                let interface =
                    resolve_function_interface(i.interface, &mut i.component_files, cas).await?;
                ensure!(
                    i.component_files.is_empty(),
                    "activity_external component_files contains files not selected by its WIT"
                );
                ActivityExternalComponentConfigResolved::Inline(
                    ActivityStubExtInlineConfigResolved {
                        name,
                        ffqn: i.ffqn,
                        interface,
                    },
                )
            }
        });
    }

    let resolved = DeploymentResolved {
        source_path: None,
        activities_wasm: deployment.activities_wasm,
        activities_stub,
        activities_external,
        activities_js,
        activities_exec,
        workflows_wasm,
        workflows_js,
        webhooks_wasm,
        webhooks_js,
        crons: deployment.crons,
    };
    validate_owned_source_file_names(&resolved)?;
    Ok(resolved)
}

async fn resolve_function_interface(
    interface: FunctionInterfaceToml,
    component_files: &mut BTreeMap<String, ContentDigest>,
    cas: &dyn Cas,
) -> anyhow::Result<FunctionInterfaceResolved> {
    let root = match interface {
        FunctionInterfaceToml::Authored(AuthoredFunctionInterfaceToml { wit }) => wit,
        FunctionInterfaceToml::Inline(InlineFunctionInterfaceToml {
            params,
            return_type,
        }) => {
            return Ok(FunctionInterfaceResolved::Inline(
                InlineFunctionInterfaceResolved {
                    params,
                    return_type,
                },
            ));
        }
    };
    let root = sanitize_deployment_relative_path(&root)?;
    // TODO: Cache parsed WIT packages by root during deployment resolution.
    let parsed = parse_wit_files_from_cas(cas, &root, component_files).await?;
    let prefix = format!("{root}/");
    for (path, _) in &parsed.files {
        ensure!(
            path.starts_with(&prefix),
            "parsed WIT file `{path}` is outside configured WIT directory `{root}`"
        );
        component_files.remove(path);
    }
    Ok(FunctionInterfaceResolved::Authored {
        wit: Box::new(WitSourceResolved {
            root,
            resolve: parsed.resolve,
            main_pkg_id: parsed.main_pkg_id,
        }),
    })
}

/// Reject deployments where two deployment-owned source files (inline/owned scripts and
/// recreated workflow/webhook backtrace sources) would be written to the same `file_name`
/// with differing content. Such a deployment hashes and runs fine, but could never be
/// retrieved with `deployment get`, which writes every owned source to disk at its
/// `file_name` and refuses to clobber. Identical re-uses of a name are allowed (they dedupe
/// to a single file). This surfaces the failure at submit time rather than on a later round-trip.
fn validate_owned_source_file_names(resolved: &DeploymentResolved) -> anyhow::Result<()> {
    // Compare by content digest: scripts carry inline content (hashed here), backtrace
    // sources already carry their CAS digest. Differing digests at the same `file_name` are
    // the collision `deployment get` cannot round-trip.
    fn digest_of(content: &str) -> ContentDigest {
        ContentDigest(Digest(Sha256::digest(content.as_bytes()).into()))
    }
    fn register<'a>(
        seen: &mut HashMap<&'a str, ContentDigest>,
        file_name: &'a str,
        digest: &ContentDigest,
    ) -> anyhow::Result<()> {
        if let Some(existing) = seen.insert(file_name, digest.clone()) {
            ensure!(
                existing == *digest,
                "two deployment-owned source files would be written to `{file_name}`; rename \
                 one of the colliding scripts or backtrace sources so the deployment can be \
                 retrieved with `deployment get`"
            );
        }
        Ok(())
    }

    let mut seen: HashMap<&str, ContentDigest> = HashMap::new();

    let script_locations = resolved
        .activities_js
        .iter()
        .map(|c| &c.location)
        .chain(resolved.activities_exec.iter().map(|c| &c.location))
        .chain(resolved.workflows_js.iter().map(|c| &c.location))
        .chain(resolved.webhooks_js.iter().map(|c| &c.location));
    for loc in script_locations {
        match loc {
            ScriptLocationResolved::Content { content, file_name } => {
                register(&mut seen, file_name, &digest_of(content))?;
            }
            ScriptLocationResolved::Graph { files, .. } => {
                for (file_name, content) in files {
                    register(&mut seen, file_name, &digest_of(content))?;
                }
            }
            ScriptLocationResolved::Oci { .. } => {}
        }
    }

    let backtraces = resolved
        .workflows_wasm
        .iter()
        .map(|c| &c.backtrace)
        .chain(resolved.webhooks_wasm.iter().map(|c| &c.backtrace));
    for bt in backtraces {
        for source in bt.frame_files_to_sources.values() {
            register(&mut seen, &source.file_name, &source.content_digest)?;
        }
    }
    Ok(())
}

/// The literal prefix used to anchor a path at the deployment directory.
pub(crate) const DEPLOYMENT_DIR_PREFIX: &str = "${DEPLOYMENT_DIR}";

/// Strip an optional `${DEPLOYMENT_DIR}` (and following `/`) prefix, returning the remainder.
pub(crate) fn strip_deployment_dir_prefix(s: &str) -> Option<&str> {
    s.strip_prefix(DEPLOYMENT_DIR_PREFIX)
        .map(|rest| rest.strip_prefix('/').unwrap_or(rest))
}

/// Normalize a deployment-owned relative path to forward-slash form, rejecting anything
/// that would escape the deployment directory (`..`, absolute paths, drive prefixes).
pub(crate) fn sanitize_deployment_relative_path(rel: &str) -> anyhow::Result<String> {
    use std::path::Component;
    let mut parts: Vec<&str> = Vec::new();
    for comp in std::path::Path::new(rel).components() {
        match comp {
            Component::Normal(s) => parts.push(
                s.to_str()
                    .with_context(|| format!("non-UTF8 path component in `{rel}`"))?,
            ),
            Component::CurDir => {}
            Component::ParentDir => {
                bail!(
                    "path must not contain `..` (cannot escape the deployment directory): `{rel}`"
                )
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("path must be relative to the deployment directory: `{rel}`")
            }
        }
    }
    ensure!(!parts.is_empty(), "empty deployment-relative path: `{rel}`");
    Ok(parts.join("/"))
}

enum ScriptToml {
    JavaScript {
        location: Option<ScriptLocationPathOrOci>,
        content: Option<String>,
        component_files: BTreeMap<String, ContentDigest>,
    },
    Exec {
        location: Option<ScriptLocationPathOrOci>,
        content: Option<String>,
    },
}

enum ModuleGraphResolution {
    JavaScript(BTreeMap<String, ContentDigest>),
    Disabled,
}

/// Resolve a script source (JS or exec) TOML location to its resolved form.
///
/// - inline `content` → `Content { content, file_name: default_file_name }` (owned).
/// - a **relative** `Path` (bare, or `${DEPLOYMENT_DIR}/…`) → read + inline as `Content`,
///   with `file_name` preserving the deployment-relative subpath (owned). `..` escapes error.
/// - an **absolute** `Path` → rejected.
/// - an `Oci` reference → `Oci { image }`.
///
/// When `content_digest` is set it is verified here against the relevant bytes (inline
/// content or the owned file). `Oci` digests are verified at runtime.
async fn resolve_script_toml(
    script: ScriptToml,
    default_file_name: String,
    cas: &dyn Cas,
    content_digest: Option<&ContentDigest>,
) -> anyhow::Result<ScriptLocationResolved> {
    let (location, content, module_graph) = match script {
        ScriptToml::JavaScript {
            location,
            content,
            component_files,
        } => (
            location,
            content,
            ModuleGraphResolution::JavaScript(component_files),
        ),
        ScriptToml::Exec { location, content } => {
            (location, content, ModuleGraphResolution::Disabled)
        }
    };
    match (location, content) {
        (None, Some(content)) => {
            if let ModuleGraphResolution::JavaScript(component_files) = &module_graph {
                ensure!(
                    component_files.is_empty(),
                    "inline scripts cannot set `component_files`"
                );
            }
            verify_content_digest(content.as_bytes(), content_digest, &default_file_name)?;
            Ok(ScriptLocationResolved::Content {
                content,
                file_name: default_file_name,
            })
        }
        (Some(ScriptLocationPathOrOci::Path(path)), None) => {
            if std::path::Path::new(&path).is_absolute() {
                bail!("absolute local paths are not allowed in deployment manifests: `{path}`")
            }
            let path = strip_deployment_dir_prefix(&path).unwrap_or(&path);
            let path = sanitize_deployment_relative_path(path)?;
            if let ModuleGraphResolution::JavaScript(component_files) = module_graph {
                // `component_files` is the manifest's declared closure (`path -> digest`).
                // Ensure the entry is in it (an entry-only manifest declares none), then walk
                // the import graph and require it be fully contained: an import missing from
                // the package is rejected here rather than trapping at runtime.
                let declared: std::collections::BTreeSet<String> =
                    component_files.keys().cloned().collect();
                let mut known = component_files;
                match (known.get(&path), content_digest) {
                    (Some(entry_digest), Some(expected)) => ensure!(
                        expected == entry_digest,
                        "content_digest for `{path}` does not match its component_files digest"
                    ),
                    (None, Some(cd)) => {
                        known.insert(path.clone(), cd.clone());
                    }
                    _ => {}
                }
                let files = parse_js_graph_from_cas(cas, &path, &known).await?;
                let entry_source = files
                    .iter()
                    .find_map(|(module_path, source)| (module_path == &path).then_some(source))
                    .context("parsed JS graph does not contain its entry")?;
                verify_content_digest(entry_source.as_bytes(), content_digest, &path)?;
                // Reject stray declared files: every `component_files` entry must be reachable
                // from the entry's imports, so the stored file set is exactly what runs and the
                // deployment -> digest mapping the orphan GC relies on carries no dead blobs.
                let reached: std::collections::BTreeSet<&str> = files
                    .iter()
                    .map(|(module_path, _)| module_path.as_str())
                    .collect();
                let stray: Vec<&str> = declared
                    .iter()
                    .map(String::as_str)
                    .filter(|declared_path| !reached.contains(declared_path))
                    .collect();
                ensure!(
                    stray.is_empty(),
                    "component_files for `{path}` declares {stray:?} that its module graph does not import"
                );
                if files.len() > 1 {
                    return Ok(ScriptLocationResolved::Graph {
                        entry_path: path,
                        files,
                    });
                }
            }
            let digest = content_digest.with_context(|| {
                // TODO: resolved deployment must have content_digest set, this can only happen on a corrupted deployment record.
                // This should be fixed by having a separate schema for DB deployment.
                format!("deployment-owned script `{path}` is missing a content digest")
            })?;
            let content = read_package_blob(cas, digest, &path).await?;
            let content = String::from_utf8(content)
                .with_context(|| format!("script file {path:?} is not valid UTF-8"))?;
            Ok(ScriptLocationResolved::Content {
                content,
                file_name: path,
            })
        }
        (Some(ScriptLocationPathOrOci::Oci(reference)), None) => {
            if let ModuleGraphResolution::JavaScript(component_files) = module_graph {
                ensure!(
                    component_files.is_empty(),
                    "OCI scripts cannot set `component_files`"
                );
            }
            Ok(ScriptLocationResolved::Oci { image: reference })
        }
        (None, None) | (Some(_), Some(_)) => {
            bail!("exactly one of `location` or `content` must be set for script components")
        }
    }
}

fn resolve_backtrace(
    backtrace: &ComponentBacktraceConfig,
    component_files: &BTreeMap<String, ContentDigest>,
) -> anyhow::Result<ComponentBacktraceConfigResolved> {
    let mut frame_files_to_sources = HashMap::new();
    for (key, source) in &backtrace.frame_files_to_sources {
        let path = source.path();
        // Classify the source path like a script: a relative path (bare or
        // `${DEPLOYMENT_DIR}/…`) is deployment-relative and its subpath is mirrored on export.
        let file_name = if let Some(rest) = strip_deployment_dir_prefix(path) {
            sanitize_deployment_relative_path(rest)?
        } else if std::path::Path::new(path).is_absolute() {
            bail!("absolute local paths are not allowed in deployment manifests: `{path}`")
        } else {
            sanitize_deployment_relative_path(path)?
        };
        // The processed manifest carries every deployment-owned backtrace source's digest in
        // `component_files`; the bytes are in the CAS, so the digest is a complete reference.
        // backcompat: 0.41 processed manifests stored the digest beside each source path
        // (`source.content_digest()`); remove that fallback in 0.42.
        let content_digest = component_files
            .get(&file_name)
            .or_else(|| source.content_digest())
            .with_context(|| {
                format!("backtrace source `{file_name}` has no digest in `component_files`")
            })?
            .clone();
        frame_files_to_sources.insert(
            key.clone(),
            BacktraceSourceResolved {
                content_digest,
                file_name,
            },
        );
    }
    Ok(ComponentBacktraceConfigResolved {
        frame_files_to_sources,
    })
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WasmtimeAllocatorConfig {
    #[default]
    Auto,
    OnDemand,
    Pooling,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WasmtimePoolingAllocatorConfig {
    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in linear memories.
    #[serde(default)]
    pooling_memory_keep_resident: Option<usize>,

    /// How many bytes to keep resident between instantiations for the
    /// pooling allocator in tables.
    #[serde(default)]
    pooling_table_keep_resident: Option<usize>,

    /// Enable memory protection keys for the pooling allocator; this can
    /// optimize the size of memory slots.
    #[serde(default)]
    memory_protection_keys: Option<bool>,

    /// The maximum number of WebAssembly instances which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pooling_total_core_instances: Option<u32>,

    /// The maximum number of WebAssembly components which can be created
    /// with the pooling allocator.
    #[serde(default)]
    pooling_total_component_instances: Option<u32>,

    /// The maximum number of WebAssembly memories which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_memories: Option<u32>,

    /// The maximum number of WebAssembly tables which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_tables: Option<u32>,

    /// The maximum number of WebAssembly stacks which can be created with
    /// the pooling allocator.
    #[serde(default)]
    pooling_total_stacks: Option<u32>,

    /// The maximum runtime size of each linear memory in the pooling
    /// allocator, in bytes.
    #[serde(default)]
    pooling_max_memory_size: Option<usize>,
}

impl From<WasmtimePoolingAllocatorConfig> for wasm_workers::engines::PoolingOptions {
    fn from(value: WasmtimePoolingAllocatorConfig) -> wasm_workers::engines::PoolingOptions {
        wasm_workers::engines::PoolingOptions {
            pooling_memory_keep_resident: value.pooling_memory_keep_resident,
            pooling_table_keep_resident: value.pooling_table_keep_resident,
            memory_protection_keys: value.memory_protection_keys,
            pooling_total_core_instances: value.pooling_total_core_instances,
            pooling_total_component_instances: value.pooling_total_component_instances,
            pooling_total_memories: value.pooling_total_memories,
            pooling_total_tables: value.pooling_total_tables,
            pooling_total_stacks: value.pooling_total_stacks,
            pooling_max_memory_size: value.pooling_max_memory_size,
        }
    }
}

#[cfg(feature = "otlp")]
pub(crate) mod otlp {
    use super::{Deserialize, log};
    use log::EnvFilter;
    use schemars::JsonSchema;

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct OtlpConfig {
        pub(crate) enabled: bool,
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default = "default_service_name")]
        pub(crate) service_name: String,
        #[serde(default = "default_otlp_endpoint")]
        pub(crate) otlp_endpoint: String,
    }

    fn default_service_name() -> String {
        "obelisk-server".to_string()
    }

    fn default_otlp_endpoint() -> String {
        // Default port as per https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
        "http://localhost:4317".to_string()
    }
}

pub(crate) mod log {
    use crate::config::toml::default_console_enabled;

    use super::{Deserialize, JsonSchema, default_console_style};
    use serde_with::serde_as;
    use std::str::FromStr;

    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct LoggingConfig {
        #[serde(default)]
        pub(crate) file: Option<AppenderRollingFile>,
        #[serde(default)]
        pub(crate) console: AppenderConsole,
    }

    #[derive(Debug, Deserialize, JsonSchema, Default, Copy, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum SpanConfig {
        /// spans are ignored (this is the default)
        #[default]
        None,
        /// one event when span is created
        New,
        /// one event per enter of a span
        Enter,
        /// one event per exit of a span
        Exit,
        /// one event when the span is dropped
        Close,
        /// one event per enter/exit of a span
        Active,
        /// events at all points (new, enter, exit, drop)
        Full,
    }

    impl From<SpanConfig> for tracing_subscriber::fmt::format::FmtSpan {
        fn from(value: SpanConfig) -> Self {
            match value {
                SpanConfig::None => Self::NONE,
                SpanConfig::New => Self::NEW,
                SpanConfig::Enter => Self::ENTER,
                SpanConfig::Exit => Self::EXIT,
                SpanConfig::Close => Self::CLOSE,
                SpanConfig::Active => Self::ACTIVE,
                SpanConfig::Full => Self::FULL,
            }
        }
    }

    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum LoggingStyle {
        #[default]
        Plain,
        PlainCompact,
        Json,
    }

    #[serde_as]
    #[derive(Debug, Deserialize, JsonSchema, Default, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderCommon {
        #[serde(default)]
        pub(crate) level: EnvFilter,
        #[serde(default)]
        pub(crate) span: SpanConfig,
        #[serde(default)]
        pub(crate) target: bool,
    }

    #[derive(Debug, serde_with::DeserializeFromStr, JsonSchema, Clone)]
    pub(crate) struct EnvFilter(
        #[schemars(with = "String")] pub(crate) tracing_subscriber::EnvFilter,
    );
    impl FromStr for EnvFilter {
        type Err = tracing_subscriber::filter::ParseError;

        fn from_str(directives: &str) -> Result<Self, Self::Err> {
            tracing_subscriber::EnvFilter::builder()
                .parse(directives)
                .map(Self)
        }
    }
    impl Default for EnvFilter {
        fn default() -> Self {
            Self::from_str("info,app=debug").expect("empty directive must not fail to parse")
        }
    }

    #[derive(Copy, Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
    pub(crate) enum AppenderConsoleWriter {
        #[default]
        Stderr,
        Stdout,
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderConsole {
        #[serde(default = "default_console_enabled")]
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        #[serde(default = "default_console_style")]
        pub(crate) style: LoggingStyle,
        #[serde(default)]
        pub(crate) writer: AppenderConsoleWriter,
    }
    impl Default for AppenderConsole {
        fn default() -> Self {
            Self {
                enabled: default_console_enabled(),
                common: AppenderCommon::default(),
                style: default_console_style(),
                writer: AppenderConsoleWriter::default(),
            }
        }
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct AppenderRollingFile {
        pub(crate) enabled: bool,
        #[serde(flatten, default)]
        pub(crate) common: AppenderCommon,
        pub(crate) directory: String,
        pub(crate) prefix: String,
        #[serde(default)]
        pub(crate) rotation: Rotation,
        #[serde(default)]
        pub(crate) style: LoggingStyle,
    }

    #[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
    #[serde(rename_all = "snake_case")]
    pub(crate) enum Rotation {
        Minutely,
        Hourly,
        Daily,
        #[default]
        Never,
    }
    impl From<Rotation> for tracing_appender::rolling::Rotation {
        fn from(value: Rotation) -> Self {
            match value {
                Rotation::Minutely => Self::MINUTELY,
                Rotation::Hourly => Self::HOURLY,
                Rotation::Daily => Self::DAILY,
                Rotation::Never => Self::NEVER,
            }
        }
    }
}

pub(crate) trait ComponentStdOutputTomlExt {
    fn into_std_output_config(self) -> Option<StdOutputConfig>;
}
impl ComponentStdOutputTomlExt for ComponentStdOutputToml {
    fn into_std_output_config(self) -> Option<StdOutputConfig> {
        match self {
            ComponentStdOutputToml::None => None,
            ComponentStdOutputToml::Stdout => Some(StdOutputConfig::Stdout),
            ComponentStdOutputToml::Stderr => Some(StdOutputConfig::Stderr),
            ComponentStdOutputToml::Db => Some(StdOutputConfig::Db),
        }
    }
}

pub(crate) mod webhook {
    use super::{
        AllowedHostToml, ComponentBacktraceConfig, ComponentCommon, ComponentCommonFetchExt,
        ComponentStdOutputToml, ComponentStdOutputTomlExt, ConfigName, JsContent,
        JsLocationResolvedExt, LogLevelTomlExt, ScriptLocationPathOrOci, SecretResolver,
        hash_js_graph, resolve_allowed_hosts, resolve_env_vars_plaintext,
        restricted_secret_registry, validate_no_env_collision,
    };
    use crate::command::server::{FrameFilesToSource, FrameSource};
    use crate::config::secret_registry::SecretRegistry;
    use crate::config::{env_var::EnvVarConfig, toml::LogLevelToml};
    use anyhow::Context;
    use concepts::{
        ComponentId, ComponentType, ContentDigest, StrVariant,
        component_id::{ComponentDigest, Digest},
        storage::LogLevel,
    };
    pub(crate) use crate::config::toml::model::webhook::{
        WebhookJsComponentConfigResolved, WebhookRoute, WebhookRouteDetail,
        WebhookWasmComponentConfigResolved, default_external_server_name,
    };
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use sha2::{Digest as _, Sha256};
    use std::{
        collections::BTreeMap,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };
    use tracing::instrument;
    use wasm_workers::{
        envvar::EnvVar, http_hooks::ConfigSectionHint, http_request_policy::AllowedHostConfig,
        std_output_stream::StdOutputConfig,
    };

    #[derive(Debug, Deserialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct HttpServer {
        pub(crate) name: ConfigName,
        pub(crate) listening_addr: SocketAddr,
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookWasmComponentConfigToml {
        #[serde(flatten)]
        pub(crate) common: ComponentCommon,
        /// Optional content digest of the WASM file.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(with = "Option<String>")]
        pub(crate) content_digest: Option<ContentDigest>,
        /// Generated CAS references for backtrace source files.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[schemars(skip)]
        pub(crate) component_files: BTreeMap<String, ContentDigest>,
        #[serde(default = "default_external_server_name")]
        pub(crate) http_server: ConfigName,
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        #[serde(default)]
        pub(crate) backtrace: ComponentBacktraceConfig,
        /// Capture and persist backtraces for requests handled by this webhook.
        #[serde(default)]
        pub(crate) backtrace_persist: bool,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookWasmComponentConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) wasm_path: PathBuf,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<StdOutputConfig>,
        pub(crate) forward_stderr: Option<StdOutputConfig>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) frame_files_to_sources: FrameFilesToSource,
        pub(crate) backtrace_persist: bool,
        pub(crate) subscription_interruption: Option<Duration>,
        pub(crate) logs_store_min_level: Option<LogLevel>,
        pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
        /// Component-scoped resolver for the endpoint's declared secret names.
        pub(crate) secrets: Arc<dyn SecretResolver>,
        pub(crate) is_webui: bool,
        /// The TOML config section type for error messages
        pub(crate) config_section_hint: ConfigSectionHint,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookRouteVerified {
        pub(crate) methods: Vec<http::Method>,
        pub(crate) route: String,
    }

    impl TryFrom<WebhookRoute> for WebhookRouteVerified {
        type Error = anyhow::Error;

        fn try_from(value: WebhookRoute) -> Result<Self, Self::Error> {
            Ok(match value {
                WebhookRoute::String(route) => Self {
                    methods: Vec::new(),
                    route,
                },
                WebhookRoute::WebhookRouteDetail(WebhookRouteDetail { methods, route }) => {
                    let methods = methods
                        .into_iter()
                        .map(|method| {
                            http::Method::from_bytes(method.as_bytes())
                                .with_context(|| format!("cannot parse route method `{method}`"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Self { methods, route }
                }
            })
        }
    }

    #[derive(Debug, Deserialize, Serialize, JsonSchema, Clone)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct WebhookJsComponentConfigToml {
        pub(crate) name: ConfigName,
        /// Location of the JavaScript source file.
        /// Supports local file paths and OCI registry references (`oci://...`).
        #[serde(default)]
        pub(crate) location: Option<ScriptLocationPathOrOci>,
        /// Inline JavaScript source embedded in the TOML.
        /// Exactly one of `location` or `content` must be set.
        #[serde(default)]
        pub(crate) content: Option<String>,
        /// Content digest of the JS source file.
        #[serde(default)]
        #[schemars(with = "Option<String>")]
        pub(crate) content_digest: Option<ContentDigest>,
        /// CAS references for the closed module graph, populated during deployment preparation.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        #[schemars(skip)]
        pub(crate) component_files: BTreeMap<String, ContentDigest>,
        /// The HTTP server to bind this webhook to.
        #[serde(default = "default_external_server_name")]
        pub(crate) http_server: ConfigName,
        /// Routes that this webhook responds to.
        pub(crate) routes: Vec<WebhookRoute>,
        #[serde(default)]
        pub(crate) forward_stdout: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) forward_stderr: ComponentStdOutputToml,
        #[serde(default)]
        pub(crate) logs_store_min_level: LogLevelToml,
        #[serde(default)]
        pub(crate) env_vars: Vec<EnvVarConfig>,
        /// Capture and persist backtraces for requests handled by this webhook.
        #[serde(default)]
        pub(crate) backtrace_persist: bool,
        /// Allowed outgoing HTTP hosts with optional method restrictions and secrets.
        #[serde(default, rename = "allowed_host")]
        pub(crate) allowed_hosts: Vec<AllowedHostToml>,
    }

    #[derive(Debug)]
    pub(crate) struct WebhookJsConfigVerified {
        pub(crate) wasm_path: Arc<Path>,
        pub(crate) component_id: ComponentId,
        pub(crate) js_entry_path: String,
        pub(crate) js_files: BTreeMap<String, String>,
        pub(crate) routes: Vec<WebhookRouteVerified>,
        pub(crate) forward_stdout: Option<StdOutputConfig>,
        pub(crate) forward_stderr: Option<StdOutputConfig>,
        pub(crate) env_vars: Arc<[EnvVar]>,
        pub(crate) backtrace_persist: bool,
        pub(crate) logs_store_min_level: Option<LogLevel>,
        pub(crate) allowed_hosts: Arc<[AllowedHostConfig]>,
        /// Component-scoped resolver for the endpoint's declared secret names.
        pub(crate) secrets: Arc<dyn SecretResolver>,
        /// The TOML config section type for error messages
        pub(crate) config_section_hint: ConfigSectionHint,
    }

    impl WebhookJsConfigVerified {
        pub(crate) fn as_frame_sources(&self) -> FrameFilesToSource {
            self.js_files
                .clone()
                .into_iter()
                .map(|(name, content)| (name, FrameSource::Content(content)))
                .collect()
        }
    }

    pub(crate) trait WebhookWasmComponentConfigResolvedExt {
        async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            secret_registry: &Arc<SecretRegistry>,
            subscription_interruption: Option<Duration>,
        ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error>;
    }

    impl WebhookWasmComponentConfigResolvedExt for WebhookWasmComponentConfigResolved {
        #[instrument(skip_all, fields(component_name = self.common.name.as_str()), err)]
        async fn fetch_and_verify(
            self,
            wasm_cache_dir: Arc<Path>,
            metadata_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            secret_registry: &Arc<SecretRegistry>,
            subscription_interruption: Option<Duration>,
        ) -> Result<(ConfigName, WebhookWasmComponentConfigVerified), anyhow::Error> {
            let expected_content_digest = self.content_digest;
            let (common, content_digest, wasm_path) =
                self.common.fetch(&wasm_cache_dir, &metadata_dir).await?;
            super::verify_fetched_content_digest(
                &content_digest,
                expected_content_digest.as_ref(),
                &common.location.to_string(),
            )?;
            let frame_files_to_sources: FrameFilesToSource = self
                .backtrace
                .into_frame_files()
                .into_iter()
                .map(|(name, digest)| (name, FrameSource::Digest(digest)))
                .collect();
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(common.name.clone()),
                ComponentDigest(content_digest.0),
            )?;
            let env_vars = resolve_env_vars_plaintext(
                self.env_vars,
                ignore_missing_env_vars,
                secret_registry,
            )?;
            let (allowed_hosts, _advisories) = resolve_allowed_hosts(
                self.allowed_hosts,
                ignore_missing_env_vars,
                secret_registry,
            )?;
            validate_no_env_collision(&env_vars, &allowed_hosts)?;
            let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
            Ok((
                common.name,
                WebhookWasmComponentConfigVerified {
                    component_id,
                    wasm_path,
                    routes: self
                        .routes
                        .into_iter()
                        .map(WebhookRouteVerified::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    forward_stdout: self.forward_stdout.into_std_output_config(),
                    forward_stderr: self.forward_stderr.into_std_output_config(),
                    env_vars,
                    frame_files_to_sources,
                    backtrace_persist: self.backtrace_persist,
                    subscription_interruption,
                    logs_store_min_level: self.logs_store_min_level.into_log_level(),
                    allowed_hosts,
                    secrets,
                    is_webui: self.is_webui,
                    config_section_hint: ConfigSectionHint::WebhookEndpointWasm,
                },
            ))
        }
    }

    pub(crate) trait WebhookJsComponentConfigResolvedExt {
        async fn fetch_and_verify(
            self,
            wasm_path: Arc<Path>,
            wasm_cache_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            secret_registry: &Arc<SecretRegistry>,
        ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error>;
    }

    impl WebhookJsComponentConfigResolvedExt for WebhookJsComponentConfigResolved {
        #[instrument(skip_all, fields(component_name = self.name.as_str()))]
        async fn fetch_and_verify(
            self,
            wasm_path: Arc<Path>,
            wasm_cache_dir: Arc<Path>,
            ignore_missing_env_vars: bool,
            secret_registry: &Arc<SecretRegistry>,
        ) -> Result<(ConfigName, WebhookJsConfigVerified), anyhow::Error> {
            let JsContent {
                entry_path: js_entry_path,
                files: js_files,
            } = self
                .location
                .get_content(&wasm_cache_dir, self.content_digest.as_ref())
                .await?;
            let mut hasher = Sha256::new();
            hasher.update(b"webhook_js:");
            hash_js_graph(&mut hasher, &js_entry_path, &js_files);
            let hash: [u8; 32] = hasher.finalize().into();
            let component_id = ComponentId::new(
                ComponentType::WebhookEndpoint,
                StrVariant::from(self.name.clone()),
                ComponentDigest(Digest(hash)),
            )?;
            let env_vars = resolve_env_vars_plaintext(
                self.env_vars,
                ignore_missing_env_vars,
                secret_registry,
            )?;
            let (allowed_hosts, _advisories) = resolve_allowed_hosts(
                self.allowed_hosts,
                ignore_missing_env_vars,
                secret_registry,
            )?;
            validate_no_env_collision(&env_vars, &allowed_hosts)?;
            let secrets = restricted_secret_registry(secret_registry, &allowed_hosts, None);
            Ok((
                self.name,
                WebhookJsConfigVerified {
                    wasm_path,
                    component_id,
                    js_entry_path,
                    js_files,
                    routes: self
                        .routes
                        .into_iter()
                        .map(WebhookRouteVerified::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    forward_stdout: self.forward_stdout.into_std_output_config(),
                    forward_stderr: self.forward_stderr.into_std_output_config(),
                    env_vars,
                    backtrace_persist: self.backtrace_persist,
                    logs_store_min_level: self.logs_store_min_level.into_log_level(),
                    allowed_hosts,
                    secrets,
                    config_section_hint: ConfigSectionHint::WebhookEndpointJs,
                },
            ))
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(untagged)]
pub(crate) enum ValueOrUnlimited<T> {
    Unlimited(Unlimited),
    Some(T),
}
impl<T> Default for ValueOrUnlimited<T> {
    fn default() -> Self {
        Self::Unlimited(Unlimited::Unlimited)
    }
}
impl<T> From<ValueOrUnlimited<T>> for Option<T> {
    fn from(value: ValueOrUnlimited<T>) -> Self {
        match value {
            ValueOrUnlimited::Some(val) => Some(val),
            ValueOrUnlimited::Unlimited(Unlimited::Unlimited) => None,
        }
    }
}

pub(crate) trait InflightSemaphoreExt {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>>;
}
impl InflightSemaphoreExt for InflightSemaphore {
    fn as_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        match self {
            InflightSemaphore::Unlimited(_) => None,
            InflightSemaphore::Some(permits) => Some(Arc::new(tokio::sync::Semaphore::new(
                usize::try_from(*permits).expect("usize >= u32"),
            ))),
        }
    }
}

// TODO: Move to env_var module
fn resolve_env_vars_plaintext(
    env_vars: Vec<EnvVarConfig>,
    ignore_missing: bool,
    secret_registry: &SecretRegistry,
) -> Result<Arc<[EnvVar]>, EnvVarError> {
    // A registered secret can never be forwarded to a guest as a plaintext env var, even
    // with `ignore_missing`: `EnvVarError::Secret` is always fatal, only `Missing` is skipped.
    let empty_if_missing = |key: String, err: EnvVarError| match err {
        EnvVarError::Missing(_) if ignore_missing => Ok(EnvVar {
            key,
            val: String::new(),
        }),
        other => Err(other),
    };
    env_vars
        .into_iter()
        .map(|env_var| match env_var {
            EnvVarConfig::KeyValue { key, value } => {
                match interpolate_env_vars_plaintext(&value, secret_registry) {
                    Ok(val) => Ok(EnvVar { key, val }),
                    Err(err) => empty_if_missing(key, err),
                }
            }
            EnvVarConfig::Key(key) => match secret_registry.public_env_lookup(&key) {
                Ok(Some(val)) => Ok(EnvVar { key, val }),
                Ok(None) => empty_if_missing(key.clone(), EnvVarError::Missing(key)),
                Err(violation) => Err(EnvVarError::Secret(violation)),
            },
        })
        .collect::<Result<_, _>>()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveAllowedHostsError {
    #[error(transparent)]
    HostPattern(#[from] HostPatternError),
    #[error(transparent)]
    EnvVarsMissing(#[from] EnvVarsMissing),
    #[error(transparent)]
    SecretViolation(#[from] SecretViolation),
    #[error("cannot parse HTTP method `{0}`")]
    InvalidMethod(String),
    #[error("use `methods = \"*\"` to allow all methods, not `methods = [\"*\"]`")]
    InvalidMethodStar,
    #[error("cannot parse request_url_regex `{pattern}`: {err}")]
    InvalidRequestUrlRegex { pattern: String, err: regex::Error },
}

/// An `allowed_host` entry that is valid but likely misconfigured. Carries the entry's TOML
/// fingerprint so a located reporter (`config_prepass`) can map it back to a source line.
/// Resolution collects these instead of logging them, so a single deduplicated, source-located
/// report is emitted once by the pre-pass rather than scattered across the resolvers.
#[derive(Debug)]
pub(crate) struct AllowedHostAdvisory {
    pub(crate) fingerprint: String,
    pub(crate) message: String,
}

/// The TOML serialization of an entry, used as a stable key to join a resolver advisory to the
/// `[[*.allowed_host]]` block it came from in the source file.
pub(crate) fn allowed_host_fingerprint(entry: &AllowedHostToml) -> String {
    toml::to_string(entry).expect("allowed host must serialize")
}

pub(crate) fn resolve_allowed_hosts(
    entries: Vec<AllowedHostToml>,
    ignore_missing_env_vars: bool,
    secret_registry: &SecretRegistry,
) -> Result<(Arc<[AllowedHostConfig]>, Vec<AllowedHostAdvisory>), ResolveAllowedHostsError> {
    let mut advisories = Vec::new();
    let hosts = entries
        .into_iter()
        .filter_map(|entry| {
            let fingerprint = allowed_host_fingerprint(&entry);
            let mut advise = |message: String| {
                advisories.push(AllowedHostAdvisory {
                    fingerprint: fingerprint.clone(),
                    message,
                });
            };
            // Convert MethodsInput to MethodsPattern
            let methods = match entry.methods {
                None => {
                    // Omitted methods: nothing allowed, warn and skip
                    advise(format!(
                        "allowed_host `{}` has no `methods` field - no requests will be allowed; \
                         use `methods = \"*\"` to allow all methods",
                        entry.pattern
                    ));
                    return None;
                }
                Some(MethodsInput::Star(_)) => {
                    // `methods = "*"` - all methods allowed
                    MethodsPattern::AllMethods
                }
                Some(MethodsInput::List(list)) => {
                    if list.is_empty() {
                        // Empty list: nothing allowed, warn and skip
                        advise(format!(
                            "allowed_host `{}` has empty `methods = []` - no requests will be allowed",
                            entry.pattern
                        ));
                        return None;
                    }
                    // Parse specific methods
                    match list
                        .into_iter()
                        .map(|m| {
                            http::Method::from_bytes(m.as_bytes()).map_err(|_| {
                                if m == "*" {
                                    ResolveAllowedHostsError::InvalidMethodStar
                                } else {
                                    ResolveAllowedHostsError::InvalidMethod(m)
                                }
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(methods) => MethodsPattern::Specific(methods),
                        Err(e) => return Some(Err(e)),
                    }
                }
            };

            let pattern_str = match interpolate_env_vars_plaintext(&entry.pattern, secret_registry) {
                Ok(s) => s,
                Err(EnvVarError::Missing(var)) => {
                    if ignore_missing_env_vars {
                        advise(format!(
                            "allowed_host pattern `{}` references missing env var `{var}`, skipping",
                            entry.pattern
                        ));
                        return None;
                    }
                    return Some(Err(ResolveAllowedHostsError::EnvVarsMissing(
                        EnvVarsMissing(vec![var]),
                    )));
                }
                Err(EnvVarError::Secret(violation)) => return Some(Err(violation.into())),
            };
            let request_url_regex = match entry.request_url_regex {
                Some(pattern) => {
                    let pattern = match interpolate_env_vars_plaintext(&pattern, secret_registry) {
                        Ok(s) => s,
                        Err(EnvVarError::Missing(var)) => {
                            if ignore_missing_env_vars {
                                advise(format!(
                                    "allowed_host request_url_regex `{pattern}` references missing env var `{var}`, skipping"
                                ));
                                return None;
                            }
                            return Some(Err(ResolveAllowedHostsError::EnvVarsMissing(
                                EnvVarsMissing(vec![var]),
                            )));
                        }
                        Err(EnvVarError::Secret(violation)) => {
                            return Some(Err(violation.into()));
                        }
                    };
                    match Regex::new(&pattern) {
                        Ok(regex) => Some(regex),
                        Err(err) => {
                            return Some(Err(ResolveAllowedHostsError::InvalidRequestUrlRegex {
                                pattern,
                                err,
                            }));
                        }
                    }
                }
                None => None,
            };
            let pattern = match HostPattern::parse_with_methods(&pattern_str, methods) {
                Ok(p) => p,
                Err(e) => return Some(Err(e.into())),
            };

            let (secret_names, replace_in) = if entry.secrets.is_empty() {
                if !entry.replace_in.is_empty() {
                    advise(format!(
                        "allowed_host `{}` has `replace_in` but no `secrets` - nothing to inject",
                        entry.pattern
                    ));
                }
                (Vec::new(), hashbrown::HashSet::new())
            } else {
                if entry.replace_in.is_empty() {
                    advise(format!(
                        "allowed_host `{}` has empty `replace_in` - secrets will never be injected",
                        entry.pattern
                    ));
                }
                if pattern.scheme.allows_unencrypted() {
                    advise(format!(
                        "secrets allowed for potentially unencrypted host `{pattern}`"
                    ));
                }

                let replace_in = entry
                    .replace_in
                    .into_iter()
                    .map(|r| match r {
                        ReplaceIn::Headers => ReplacementLocation::Headers,
                        ReplaceIn::Body => ReplacementLocation::Body,
                        ReplaceIn::Params => ReplacementLocation::Params,
                    })
                    .collect();
                // Carry only the declared names: values are resolved lazily per
                // execution run via the component's `RestrictedSecretRegistry`, never
                // baked into this verified config. An unregistered name is handled by
                // `config_prepass::preflight` (continue/bail/fix) and fails closed at
                // runtime when the resolver cannot supply it.
                (entry.secrets, replace_in)
            };

            Some(Ok(AllowedHostConfig {
                pattern,
                request_url_regex,
                secret_names,
                replace_in,
            }))
        })
        .collect::<Result<Arc<[AllowedHostConfig]>, _>>()?;
    Ok((hosts, advisories))
}

/// Build a component-scoped [`SecretResolver`] over the operator registry, limited
/// to the union of secret names the component declared across its `allowed_host`
/// entries plus `extra_names` (exec-activity `secrets`). Values are fetched through
/// this at execution time, never baked into the verified config.
fn restricted_secret_registry(
    secret_registry: &Arc<SecretRegistry>,
    allowed_hosts: &[AllowedHostConfig],
    extra_names: impl IntoIterator<Item = String>,
) -> Arc<dyn SecretResolver> {
    let names = allowed_hosts
        .iter()
        .flat_map(|h| h.secret_names.iter().cloned())
        .chain(extra_names);
    Arc::new(RestrictedSecretRegistry::new(
        secret_registry.clone(),
        names,
    ))
}

fn validate_no_env_collision(
    env_vars: &[EnvVar],
    allowed_hosts: &[AllowedHostConfig],
) -> Result<(), anyhow::Error> {
    let env_var_keys: hashbrown::HashSet<_> = env_vars.iter().map(|e| e.key.as_str()).collect();
    for host in allowed_hosts {
        for key in &host.secret_names {
            ensure!(
                !env_var_keys.contains(key.as_str()),
                "secret env var `{key}` collides with an `env_vars` entry"
            );
        }
    }
    Ok(())
}

const fn default_true() -> bool {
    true
}
const fn default_parallel_compilation() -> bool {
    true
}
const fn default_debug() -> bool {
    false
}
const fn default_codegen_enabled() -> bool {
    true
}

const fn default_subscription_interruption() -> DurationConfigOptional {
    DurationConfigOptional::Seconds(1)
}
fn default_console_enabled() -> bool {
    true
}
fn default_console_style() -> LoggingStyle {
    LoggingStyle::PlainCompact
}

fn default_sqlite_queue_capacity() -> usize {
    SqliteConfig::default().queue_capacity
}
fn default_timers_watcher_enabled() -> bool {
    true
}
fn default_timers_watcher_leeway() -> DurationConfig {
    DurationConfig::Milliseconds(500)
}
fn default_timers_watcher_tick_sleep() -> DurationConfig {
    DurationConfig::Milliseconds(100)
}

fn default_cancel_watcher_tick_sleep() -> DurationConfig {
    DurationConfig::Seconds(1)
}

pub(crate) mod cron {
    use super::*;

    pub(crate) use crate::config::toml::model::cron::CronComponentConfigToml;

    #[derive(Debug)]
    pub(crate) struct CronConfigVerified {
        pub(crate) component_id: ComponentId,
        pub(crate) target_ffqn: FunctionFqn,
        pub(crate) params_json: Vec<serde_json::Value>,
        pub(crate) cron_schedule: CronOrOnce,
        pub(crate) exec_config: executor::executor::ExecConfig,
    }

    pub(crate) trait CronComponentConfigTomlExt {
        fn verify(self) -> Result<CronConfigVerified, anyhow::Error>;
    }

    impl CronComponentConfigTomlExt for CronComponentConfigToml {
        fn verify(self) -> Result<CronConfigVerified, anyhow::Error> {
            let name = self.name.to_string();
            let cron_schedule = if self.schedule == "@once" {
                CronOrOnce::Once
            } else {
                CronOrOnce::Cron(Box::new(
                    croner::Cron::new(&self.schedule)
                        .with_seconds_optional()
                        .parse()
                        .with_context(|| {
                            format!(
                                "invalid cron expression `{}` for schedule `{name}`",
                                self.schedule
                            )
                        })?,
                ))
            };
            // Validate params JSON
            let serde_json::Value::Array(params_json) =
                serde_json::from_str::<serde_json::Value>(&self.params).with_context(|| {
                    format!(
                        "invalid JSON params for schedule `{name}`: `{}`",
                        self.params
                    )
                })?
            else {
                bail!("invalid params for schedule `{name}` - expected JSON array")
            };
            // Compute component digest from schedule config
            let mut hasher = Sha256::new();
            sha2::Digest::update(&mut hasher, name.as_bytes());
            sha2::Digest::update(&mut hasher, self.ffqn.to_string().as_bytes());
            sha2::Digest::update(&mut hasher, self.params.as_bytes());
            sha2::Digest::update(&mut hasher, self.schedule.as_bytes());
            let hash: [u8; 32] = sha2::Digest::finalize(hasher).into();
            let component_digest = ComponentDigest(Digest(hash));
            let component_id = ComponentId::new(
                ComponentType::Cron,
                StrVariant::from(name),
                component_digest,
            )?;
            let exec_config = self.exec.into_exec_exec_config(
                component_id.clone(),
                None, // no global instance limiter for crons
                ComponentRetryConfig::CRON,
            )?;
            Ok(CronConfigVerified {
                component_id,
                target_ffqn: self.ffqn,
                params_json,
                cron_schedule,
                exec_config,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn cron_verification_accepts_optional_seconds_field() {
            for schedule in ["*/10 * * * *", "*/10 * * * * *"] {
                let config: CronComponentConfigToml = toml::from_str(&format!(
                    r#"
                    name = "frequent"
                    ffqn = "testing:integration/activity.run"
                    schedule = "{schedule}"
                    "#
                ))
                .unwrap();

                assert!(matches!(
                    config.verify().unwrap().cron_schedule,
                    CronOrOnce::Cron(_)
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use concepts::{ContentDigest, component_id::Digest};
    use sha2::{Digest as _, Sha256};

    fn digest_of(bytes: &[u8]) -> ContentDigest {
        ContentDigest(Digest(Sha256::digest(bytes).into()))
    }

    mod outbound_http {
        use super::super::*;

        #[test]
        fn server_allowlist_uses_deployment_allowed_host_shape() {
            let config: ServerConfigToml = toml::from_str(
                r#"
                [secrets]
                API_KEY = { env = "API_KEY_SOURCE" }

                [[outbound_http.allowed_host]]
                pattern = "api.example.com"
                methods = ["POST"]
                request_url_regex = "^POST https://api\\.example\\.com/v1/"
                secrets = ["API_KEY"]
                replace_in = ["headers"]
                "#,
            )
            .unwrap();

            let entry = &config.outbound_http.allowed_hosts[0];
            assert_eq!(entry.pattern, "api.example.com");
            assert_eq!(entry.secrets, ["API_KEY"]);
            assert!(matches!(
                entry.methods,
                Some(MethodsInput::List(ref methods)) if methods.as_slice() == ["POST"]
            ));
            assert!(matches!(entry.replace_in.as_slice(), [ReplaceIn::Headers]));
        }

        #[test]
        fn omitted_server_allowlist_is_empty() {
            let config: ServerConfigToml = toml::from_str("").unwrap();
            assert!(config.outbound_http.allowed_hosts.is_empty());
        }
    }

    mod blocking_strategy {
        use super::super::*;
        use crate::config::toml::model::{
            BlockingStrategyAwaitConfig, BlockingStrategyConfigCustomized,
            BlockingStrategyConfigSimple, default_non_blocking_event_batching,
        };
        use serde::Deserialize;

        // Helper struct to deserialize into
        #[derive(Deserialize, Debug, PartialEq)]
        struct TestConfig {
            strategy: BlockingStrategyConfigToml,
        }

        #[test]
        fn deserialize_simple_interrupt() {
            let toml_str = r#"
strategy = "interrupt"
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Simple(
                    BlockingStrategyConfigSimple::Interrupt,
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse interrupt string");
            assert_eq!(actual, expected);

            // Verify From impl result
            assert_eq!(
                actual.strategy.into_blocking_strategy(None),
                JoinNextBlockingStrategy::Interrupt
            );
        }

        #[test]
        fn deserialize_simple_await() {
            let toml_str = r#"
strategy = "await"
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Simple(
                    BlockingStrategyConfigSimple::Await, // The default variant of Simple
                ),
            };
            let actual: TestConfig = toml::from_str(toml_str).expect("Should parse await string");
            assert_eq!(actual, expected);

            // Verify From impl result (uses default batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(None),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                    subscription_interruption: None,
                }
            );
        }

        #[test]
        fn deserialize_tagged_await_default_batching() {
            let toml_str = r#"
strategy = { kind = "await" }
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Tagged(
                    BlockingStrategyConfigCustomized::Await(BlockingStrategyAwaitConfig {
                        non_blocking_event_batching: default_non_blocking_event_batching(),
                    }),
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse tagged await with default batching");
            assert_eq!(actual, expected);

            // Verify From impl result (uses default batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(None),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
                    subscription_interruption: None,
                }
            );
        }

        #[test]
        fn deserialize_tagged_await_custom_batching() {
            let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 99 }
"#;
            let expected = TestConfig {
                strategy: BlockingStrategyConfigToml::Tagged(
                    BlockingStrategyConfigCustomized::Await(BlockingStrategyAwaitConfig {
                        non_blocking_event_batching: 99,
                    }),
                ),
            };
            let actual: TestConfig =
                toml::from_str(toml_str).expect("Should parse tagged await with custom batching");
            assert_eq!(actual, expected);

            // Verify From impl result (uses custom batching)
            assert_eq!(
                actual.strategy.into_blocking_strategy(None),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 99,
                    subscription_interruption: None,
                }
            );
        }

        #[test]
        fn deserialize_invalid_string_should_fail() {
            let toml_str = r#"
strategy = "unknown"
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on unknown string");
            // Check for a more specific error if needed, e.g., contains "unknown variant"
        }

        #[test]
        fn deserialize_invalid_kind_in_tagged_should_fail() {
            let toml_str = r#"
strategy = { kind = "interrupt", non_blocking_event_batching = 10 }
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on invalid kind in map");
        }

        #[test]
        fn deserialize_invalid_structure_missing_kind_should_fail() {
            let toml_str = r#"
strategy = { name = "await", non_blocking_event_batching = 10 } # Missing 'kind'
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            // Fails `Tagged` because 'kind' is missing. Fails `Simple` because it's not a string.
            assert!(result.is_err(), "Should fail on map missing 'kind'");
        }

        #[test]
        fn deserialize_invalid_type_should_fail() {
            let toml_str = r"
strategy = 123
";
            let result = toml::from_str::<TestConfig>(toml_str);
            // Fails `Tagged` because not a map. Fails `Simple` because not a string.
            assert!(result.is_err(), "Should fail on incorrect type (integer)");
        }

        #[test]
        fn deserialize_tagged_await_with_extra_field_should_fail() {
            // TOML allows extra fields by default, Serde ignores them if not in the struct
            let toml_str = r#"
strategy = { kind = "await", non_blocking_event_batching = 25, extra_stuff = "hello" }
"#;
            let result = toml::from_str::<TestConfig>(toml_str);
            assert!(result.is_err(), "Should fail on `extra_stuff`");
        }
    }

    mod allow_exec_activities {
        use super::super::*;

        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(default)]
            allow: AllowExecActivities,
        }

        const DIGEST: &str =
            "sha256:abababababababababababababababababababababababababababababababab";

        #[test]
        fn deserialize_bool_map_and_legacy_digest_list() {
            let actual: TestConfig = toml::from_str("allow = true").unwrap();
            assert_eq!(AllowExecActivities::AllowAny, actual.allow);
            let actual: TestConfig = toml::from_str("allow = false").unwrap();
            assert_eq!(AllowExecActivities::Deny, actual.allow);
            let actual: TestConfig = toml::from_str("").unwrap();
            assert_eq!(AllowExecActivities::Deny, actual.allow);
            let actual: TestConfig =
                toml::from_str(&format!("[allow]\ngreet = \"{DIGEST}\"")).unwrap();
            assert_eq!(
                AllowExecActivities::Allowlist(BTreeMap::from([(
                    "greet".to_string(),
                    DIGEST.parse().unwrap()
                )])),
                actual.allow
            );
            let actual: TestConfig = toml::from_str(&format!("allow = [\"{DIGEST}\"]")).unwrap();
            assert_eq!(
                AllowExecActivities::LegacyAllowlist(vec![DIGEST.parse().unwrap()]),
                actual.allow
            );
        }

        #[test]
        fn deserialize_bool_string_as_sent_by_env_override() {
            // `OBELISK__ALLOW_EXEC_ACTIVITIES=true` reaches serde as a string.
            let actual: TestConfig = toml::from_str(r#"allow = "true""#).unwrap();
            assert_eq!(AllowExecActivities::AllowAny, actual.allow);
            toml::from_str::<TestConfig>(r#"allow = "yes""#).unwrap_err();
        }
    }

    mod allowed_hosts {
        use super::super::*;

        fn allowed_host_with_regex(request_url_regex: &str) -> AllowedHostToml {
            AllowedHostToml {
                pattern: "api.example.com".to_string(),
                methods: Some(MethodsInput::List(vec!["GET".to_string()])),
                request_url_regex: Some(request_url_regex.to_string()),
                secrets: Vec::new(),
                replace_in: Vec::new(),
            }
        }

        #[test]
        fn request_url_regex_interpolates_env_vars() {
            let (hosts, _advisories) = resolve_allowed_hosts(
                vec![allowed_host_with_regex(
                    r"^GET https://${OBELISK_TEST_REQUEST_URL_REGEX_DOMAIN:-api\.example\.com}/v1/",
                )],
                false,
                &std::sync::Arc::new(SecretRegistry::empty()),
            )
            .unwrap();

            let regex = hosts[0].request_url_regex.as_ref().unwrap();
            assert!(regex.is_match("GET https://api.example.com/v1/items"));
            assert!(!regex.is_match("GET https://apiXexampleYcom/v1/items"));
        }

        #[test]
        fn request_url_regex_missing_env_var_fails_when_not_ignored() {
            const VAR: &str = "OBELISK_TEST_MISSING_REQUEST_URL_REGEX_DOMAIN_9E5F58E0";
            let error = resolve_allowed_hosts(
                vec![allowed_host_with_regex(&format!(
                    "^GET https://${{{VAR}}}/"
                ))],
                false,
                &std::sync::Arc::new(SecretRegistry::empty()),
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains(VAR), "unexpected error: {error}");
        }

        #[test]
        fn request_url_regex_missing_env_var_skips_when_ignored() {
            const VAR: &str = "OBELISK_TEST_MISSING_REQUEST_URL_REGEX_DOMAIN_IGNORED_9E5F58E0";
            let (hosts, _advisories) = resolve_allowed_hosts(
                vec![allowed_host_with_regex(&format!(
                    "^GET https://${{{VAR}}}/"
                ))],
                true,
                &std::sync::Arc::new(SecretRegistry::empty()),
            )
            .unwrap();
            assert!(hosts.is_empty());
        }
    }

    mod env_vars {
        use super::super::*;

        #[test]
        fn missing_key_value_interpolation_honors_ignore_missing() {
            const VAR: &str = "OBELISK_TEST_MISSING_KEY_VALUE_ENV_VAR_1C5D78B2";
            let env_vars = vec![EnvVarConfig::KeyValue {
                key: "RENAMED_ENV_VAR".to_string(),
                value: format!("${{{VAR}}}"),
            }];

            let error =
                resolve_env_vars_plaintext(env_vars.clone(), false, &SecretRegistry::empty())
                    .unwrap_err()
                    .to_string();
            assert!(error.contains(VAR), "unexpected error: {error}");

            let resolved =
                resolve_env_vars_plaintext(env_vars, true, &SecretRegistry::empty()).unwrap();
            assert_eq!(resolved[0].key, "RENAMED_ENV_VAR");
            assert_eq!(resolved[0].val, "");
        }
    }

    mod component_location {
        use super::super::*;

        #[test]
        fn parse_local_path() {
            let location: ComponentLocationToml = "./my-component.wasm".parse().unwrap();
            assert!(
                matches!(location, ComponentLocationToml::Path(p) if p == "./my-component.wasm")
            );
        }

        #[test]
        fn parse_oci_reference() {
            let location: ComponentLocationToml =
                "oci://ghcr.io/obeli-sk/obelisk:v0.34.1".parse().unwrap();
            assert!(matches!(location, ComponentLocationToml::Oci(_)));
        }
    }

    mod activity_stub {
        use crate::config::toml::tests::digest_of;

        use super::super::*;

        #[test]
        fn deserialize_file_mode() {
            let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
"#;
            let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
            assert!(matches!(stub, ActivityStubComponentConfigToml::File(_)));
        }

        #[test]
        fn deserialize_inline_mode() {
            let toml_str = r#"
name = "my_stub"
ffqn = "ns:pkg/ifc.fn"
params = [{ name = "id", type = "u64" }]
return_type = "result<string, string>"
"#;
            let stub: ActivityStubComponentConfigToml = toml::from_str(toml_str).unwrap();
            assert!(matches!(stub, ActivityStubComponentConfigToml::Inline(_)));
        }

        #[test]
        fn reject_both_location_and_ffqn() {
            let toml_str = r#"
name = "my_stub"
location = "./stub.wasm"
ffqn = "ns:pkg/ifc.fn"
"#;
            toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
        }

        #[test]
        fn reject_neither_location_nor_ffqn() {
            let toml_str = r#"
name = "my_stub"
"#;
            toml::from_str::<ActivityStubComponentConfigToml>(toml_str).unwrap_err();
        }

        #[tokio::test]
        async fn file_mode_rejects_mismatched_content_digest() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("stub.wasm");
            tokio::fs::write(&path, b"actual").await.unwrap();
            let stub = ActivityStubComponentConfigResolved::File(ActivityStubFileConfigToml {
                common: ComponentCommon {
                    name: ConfigName::new(StrVariant::from("my_stub")).unwrap(),
                    location: ComponentLocationToml::Path(path.to_string_lossy().into_owned()),
                },
                content_digest: Some(digest_of(b"different")),
            });

            let err = stub
                .fetch_and_verify(dir.path().into(), dir.path().into())
                .await
                .unwrap_err()
                .to_string();

            assert!(
                err.contains("content digest mismatch"),
                "unexpected error: {err}"
            );
        }
    }

    mod activity_exec {
        use secrecy::{ExposeSecret as _, SecretString};

        use crate::config::toml::tests::digest_of;

        use super::super::*;

        /// A config that references the registered secret name `MY_SECRET`.
        fn exec_config_with_secret() -> ActivityExecComponentConfigResolved {
            ActivityExecComponentConfigResolved {
                name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
                location: ScriptLocationResolved::Content {
                    content: "#!/usr/bin/env bash\necho null\n".into(),
                    file_name: "exec-test".into(),
                },
                content_digest: None,
                ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
                interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                    params: Some(vec![]),
                    return_type: Some("result<string, string>".into()),
                }),
                component_digest: None,
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                max_output_bytes: default_max_output_bytes(),
                secrets: vec!["MY_SECRET".to_string()],
                params_via_stdin: false,
            }
        }

        fn exec_config_with_source(
            location: ScriptLocationResolved,
            content_digest: Option<ContentDigest>,
        ) -> ActivityExecComponentConfigResolved {
            ActivityExecComponentConfigResolved {
                name: ConfigName::new(StrVariant::from("exec-test")).unwrap(),
                location,
                content_digest,
                ffqn: "testing:integration/exec-secret.expose".parse().unwrap(),
                interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                    params: Some(vec![]),
                    return_type: Some("result<string, string>".into()),
                }),
                component_digest: None,
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                max_output_bytes: default_max_output_bytes(),
                secrets: Vec::new(),
                params_via_stdin: false,
            }
        }

        fn inline_program() -> ResolvedExecProgram {
            ResolvedExecProgram {
                program: PathBuf::from("/tmp/fake-exec-script.sh"),
                content_digest: digest_of(b"#!/usr/bin/env bash\necho null\n"),
            }
        }

        /// A declared secret name is always carried; an unregistered one simply
        /// resolves to nothing at use (the child never receives it).
        /// `config_prepass::preflight` owns the fatal/continue/fix decision.
        #[test]
        fn fetch_and_verify_activity_exec_secret_dropped_when_unregistered() {
            let config = exec_config_with_secret();
            let verified = config
                .fetch_and_verify(
                    inline_program(),
                    false,
                    &std::sync::Arc::new(SecretRegistry::empty()),
                    None,
                )
                .unwrap();
            let secrets = verified.secrets.expect("declared secret name is carried");
            assert!(secrets.names.contains(&"MY_SECRET".to_string()));
            // Unregistered: the resolver supplies no value, so it is dropped at use.
            assert!(secrets.resolver.secret_lookup("MY_SECRET").is_none());
        }

        #[test]
        fn fetch_and_verify_activity_exec_secret_resolves_from_registry() {
            let config = exec_config_with_secret();
            let registry = std::sync::Arc::new(SecretRegistry::from_test_values([(
                "MY_SECRET".to_string(),
                SecretString::from("s3cret_value"),
            )]));
            let verified = config
                .fetch_and_verify(inline_program(), false, &registry, None)
                .unwrap();
            let secrets = verified.secrets.expect("secret must be declared");
            // Only the name is carried; the value is fetched on demand via the resolver.
            assert!(secrets.names.contains(&"MY_SECRET".to_string()));
            assert_eq!(
                secrets
                    .resolver
                    .secret_lookup("MY_SECRET")
                    .expect("resolver supplies the declared secret")
                    .expose_secret(),
                "s3cret_value"
            );
        }

        #[test]
        fn fetch_and_verify_activity_exec_hashes_resolved_source_not_oci_reference() {
            let source = b"#!/usr/bin/env bash\necho null\n".to_vec();
            let inline = exec_config_with_source(
                ScriptLocationResolved::Content {
                    content: String::from_utf8(source.clone()).unwrap(),
                    file_name: "exec-test".into(),
                },
                None,
            );
            let oci = exec_config_with_source(
                ScriptLocationResolved::Oci {
                    image: "registry.example.com/ns/exec:latest".parse().unwrap(),
                },
                None,
            );

            let inline_verified = inline
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: PathBuf::from("/tmp/fake-exec-script.sh"),
                        content_digest: digest_of(&source),
                    },
                    true,
                    &std::sync::Arc::new(SecretRegistry::empty()),
                    None,
                )
                .unwrap();
            let oci_verified = oci
                .fetch_and_verify(
                    ResolvedExecProgram {
                        program: PathBuf::from("/tmp/fake-exec-script.sh"),
                        content_digest: digest_of(&source),
                    },
                    true,
                    &std::sync::Arc::new(SecretRegistry::empty()),
                    None,
                )
                .unwrap();

            assert_eq!(inline_verified.component_id, oci_verified.component_id);
        }

        #[tokio::test]
        async fn resolve_activity_exec_validates_inline_content_digest() {
            let config = exec_config_with_source(
                ScriptLocationResolved::Content {
                    content: "#!/usr/bin/env bash\necho null\n".into(),
                    file_name: "exec-test".into(),
                },
                Some(
                    "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                        .parse()
                        .unwrap(),
                ),
            );
            let error = config
                .resolve(std::path::Path::new("/tmp"))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("content digest mismatch"),
                "unexpected error: {error}"
            );
        }
    }

    mod script_location {
        use crate::config::toml::tests::digest_of;

        use super::super::*;
        use concepts::cas::InMemoryCas;

        fn javascript(
            location: Option<ScriptLocationPathOrOci>,
            content: Option<String>,
            component_files: BTreeMap<String, ContentDigest>,
        ) -> ScriptToml {
            ScriptToml::JavaScript {
                location,
                content,
                component_files,
            }
        }

        #[tokio::test]
        async fn inline_content_becomes_owned() {
            let cas = InMemoryCas::default();
            let location = resolve_script_toml(
                javascript(
                    None,
                    Some("export const x = 1;".to_string()),
                    BTreeMap::new(),
                ),
                "foo.js".to_string(),
                &cas,
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationResolved::Content { content, file_name }
                    if content == "export const x = 1;" && file_name == "foo.js"
            );
        }

        #[tokio::test]
        async fn relative_file_is_owned_and_mirrors_subpath() {
            let cas = InMemoryCas::default();
            let source = "export default 'owned content';";
            let digest = cas.write_blob(source.as_bytes()).await.unwrap();

            // Bare relative path (implicit `${DEPLOYMENT_DIR}` prefix).
            let location = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Path("scripts/a.js".to_string())),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                Some(&digest),
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationResolved::Content { content, file_name }
                    if content == source && file_name == "scripts/a.js"
            );
        }

        #[tokio::test]
        async fn explicit_deployment_dir_prefix_is_owned() {
            let cas = InMemoryCas::default();
            let digest = cas
                .write_blob(b"export default 'owned content';")
                .await
                .unwrap();

            let location = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Path(
                        "${DEPLOYMENT_DIR}/scripts/a.js".to_string(),
                    )),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                Some(&digest),
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationResolved::Content { file_name, .. } if file_name == "scripts/a.js"
            );
        }

        #[tokio::test]
        async fn absolute_path_is_rejected() {
            let cas = InMemoryCas::default();
            let abs = "/tmp/outside.js".to_string();
            let err = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Path(abs.clone())),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                None,
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("absolute local paths are not allowed"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn parent_dir_escape_is_rejected() {
            let cas = InMemoryCas::default();
            for raw in ["../escape.js", "${DEPLOYMENT_DIR}/../escape.js"] {
                let err = resolve_script_toml(
                    javascript(
                        Some(ScriptLocationPathOrOci::Path(raw.to_string())),
                        None,
                        BTreeMap::new(),
                    ),
                    "ignored.js".to_string(),
                    &cas,
                    None,
                )
                .await
                .unwrap_err()
                .to_string();
                assert!(err.contains("`..`"), "unexpected error for `{raw}`: {err}");
            }
        }

        #[tokio::test]
        async fn oci_becomes_oci() {
            let cas = InMemoryCas::default();
            let reference =
                oci_client::Reference::from_str("docker.io/library/example:latest").unwrap();
            let location = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Oci(reference)),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                None,
            )
            .await
            .unwrap();
            assert_matches::assert_matches!(
                location,
                ScriptLocationResolved::Oci { image }
                    if image.to_string() == "docker.io/library/example:latest"
            );
        }

        #[tokio::test]
        async fn content_digest_verified_at_submit() {
            let cas = InMemoryCas::default();
            let content = "export const x = 1;";

            // Matching digest succeeds.
            resolve_script_toml(
                javascript(None, Some(content.to_string()), BTreeMap::new()),
                "foo.js".to_string(),
                &cas,
                Some(&digest_of(content.as_bytes())),
            )
            .await
            .expect("matching digest should pass");

            // Mismatching digest on inline content fails.
            let wrong = digest_of(b"different");
            let err = resolve_script_toml(
                javascript(None, Some(content.to_string()), BTreeMap::new()),
                "foo.js".to_string(),
                &cas,
                Some(&wrong),
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                err.contains("content digest mismatch"),
                "unexpected error: {err}"
            );
        }

        #[tokio::test]
        async fn relative_file_missing_blob_is_rejected() {
            // A relative script whose pinned digest is not in the CAS cannot be resolved: in the
            // content-addressed model a wrong digest is a missing blob, not a hash mismatch.
            let cas = InMemoryCas::default();
            let missing = digest_of(b"nope");
            let err = resolve_script_toml(
                javascript(
                    Some(ScriptLocationPathOrOci::Path("script.js".to_string())),
                    None,
                    BTreeMap::new(),
                ),
                "ignored.js".to_string(),
                &cas,
                Some(&missing),
            )
            .await
            .unwrap_err();
            let err = format!("{err:#}");
            assert!(
                err.contains("not present in the CAS"),
                "unexpected error: {err}"
            );
        }
    }

    mod export {
        use super::super::*;

        fn js_activity(
            name: &str,
            location: ScriptLocationResolved,
        ) -> ActivityJsComponentConfigResolved {
            ActivityJsComponentConfigResolved {
                name: ConfigName::new(StrVariant::from(name.to_string())).unwrap(),
                location,
                content_digest: None,
                component_digest: None,
                ffqn: "ns:pkg/ifc.fn".parse().unwrap(),
                interface: FunctionInterfaceResolved::Inline(InlineFunctionInterfaceResolved {
                    params: Some(vec![]),
                    return_type: None,
                }),
                exec: ExecConfigToml::default(),
                max_retries: default_max_retries(),
                retry_exp_backoff: default_retry_exp_backoff(),
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                logs_store_min_level: LogLevelToml::default(),
                env_vars: vec![],
                allowed_hosts: vec![],
            }
        }

        #[test]
        fn submit_rejects_owned_file_name_collision() {
            // Two distinct owned scripts resolving to the same `file_name` must be rejected
            // at submit time, since `deployment get` could never write both to disk.
            let mut deployment = DeploymentResolved::default();
            deployment.activities_js.push(js_activity(
                "a",
                ScriptLocationResolved::Content {
                    content: "export const a = 1;".to_string(),
                    file_name: "foo".to_string(),
                },
            ));
            deployment.activities_js.push(js_activity(
                "b",
                ScriptLocationResolved::Content {
                    content: "export const b = 2;".to_string(),
                    file_name: "foo".to_string(),
                },
            ));
            let err = validate_owned_source_file_names(&deployment)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("two deployment-owned source files would be written to `foo`"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn submit_allows_identical_owned_content_under_same_name() {
            // Same file_name with identical content dedupes on export, so it must pass submit.
            let mut deployment = DeploymentResolved::default();
            for name in ["a", "b"] {
                deployment.activities_js.push(js_activity(
                    name,
                    ScriptLocationResolved::Content {
                        content: "export const shared = 1;".to_string(),
                        file_name: "shared.js".to_string(),
                    },
                ));
            }
            validate_owned_source_file_names(&deployment).unwrap();
        }
    }

    mod backtrace {
        use crate::config::toml::tests::digest_of;

        use super::super::*;

        #[test]
        fn wasm_deployment_dir_escape_rejected_but_subpath_ok() {
            let dir = std::path::Path::new("/dep");

            let mut escape = "${DEPLOYMENT_DIR}/../evil.wasm".to_string();
            let err = format!(
                "{:#}",
                DeploymentToml::expand_deployment_dir(&mut escape, dir).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");

            let mut ok = "${DEPLOYMENT_DIR}/components/a.wasm".to_string();
            DeploymentToml::expand_deployment_dir(&mut ok, dir).unwrap();
            assert_eq!(ok, "/dep/components/a.wasm");

            // Bare relative paths are anchored to the deployment dir too.
            let mut bare = "components/a.wasm".to_string();
            DeploymentToml::expand_deployment_dir(&mut bare, dir).unwrap();
            assert_eq!(bare, "/dep/components/a.wasm");

            let mut bare_escape = "../evil.wasm".to_string();
            let err = format!(
                "{:#}",
                DeploymentToml::expand_deployment_dir(&mut bare_escape, dir).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");

            // Author-provided absolute paths are rejected.
            let mut abs = "/other/a.wasm".to_string();
            let err = format!(
                "{:#}",
                DeploymentToml::expand_deployment_dir(&mut abs, dir).unwrap_err()
            );
            assert!(
                err.contains("absolute local paths are not allowed"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn resolved_retains_relative_subpath() {
            let digest = digest_of(b"SRC");
            let component_files =
                BTreeMap::from([("crates/foo/src/lib.rs".to_string(), digest.clone())]);

            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                "${DEPLOYMENT_DIR}/crates/foo/src/lib.rs".to_string().into(),
            );
            let resolved = resolve_backtrace(&bt, &component_files).unwrap();
            let src = resolved
                .frame_files_to_sources
                .get(".../src/lib.rs")
                .unwrap();
            assert_eq!(src.content_digest, digest);
            assert_eq!(src.file_name, "crates/foo/src/lib.rs");
        }

        #[test]
        fn bare_relative_source_is_deployment_dir_relative() {
            // A bare relative backtrace source (no `${DEPLOYMENT_DIR}` prefix) resolves to the
            // same deployment-relative file name as the explicit-prefix form.
            let digest = digest_of(b"SRC");
            let component_files =
                BTreeMap::from([("crates/foo/src/lib.rs".to_string(), digest.clone())]);

            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                "crates/foo/src/lib.rs".to_string().into(),
            );
            let resolved = resolve_backtrace(&bt, &component_files).unwrap();
            let src = resolved
                .frame_files_to_sources
                .get(".../src/lib.rs")
                .unwrap();
            assert_eq!(src.content_digest, digest);
            assert_eq!(src.file_name, "crates/foo/src/lib.rs");
        }

        #[test]
        fn source_parent_dir_escape_is_rejected() {
            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                "frame".to_string(),
                "${DEPLOYMENT_DIR}/../escape.rs".to_string().into(),
            );
            let err = format!(
                "{:#}",
                resolve_backtrace(&bt, &BTreeMap::new()).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");
        }

        #[test]
        fn absolute_source_is_rejected() {
            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources.insert(
                ".../src/lib.rs".to_string(),
                "/nested/lib.rs".to_string().into(),
            );
            let err = resolve_backtrace(&bt, &BTreeMap::new())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("absolute local paths are not allowed"),
                "unexpected error: {err}"
            );
        }
    }
}
