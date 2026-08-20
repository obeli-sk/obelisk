//! TOML configuration for the obelisk binary, split by concern:
//! - [`authored`]: the user-authored deployment manifest and its validation.
//! - [`processed`]: resolving + fetching/verifying a manifest into runtime-ready configs.
//! - [`server`]: server/runtime config (`obelisk.toml`), orthogonal to the manifest.
//! - [`model`]: serde data shapes shared between the authored and processed sides.
//!
//! This module keeps only cross-cutting helpers (env/secret/host resolution, digest
//! verification, path sanitizing, shared extension traits) and re-exports the submodules
//! so `crate::config::toml::<Type>` paths stay stable. The `webhook` and `cron`
//! submodules keep their own authored/processed types together.

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

pub(crate) use crate::config::toml::model::cron::CronComponentConfigToml;
pub(crate) use crate::config::toml::model::webhook::{
    WebhookJsComponentConfigResolved, WebhookRoute, WebhookRouteDetail,
    WebhookWasmComponentConfigResolved, default_external_server_name,
};

mod authored;
mod resolve;
mod server;
pub(crate) use authored::*;
pub(crate) use resolve::*;
pub(crate) use server::*;

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
                "${DEPLOYMENT_DIR}/crates/foo/src/lib.rs".to_string(),
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
                "crates/foo/src/lib.rs".to_string(),
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
                "${DEPLOYMENT_DIR}/../escape.rs".to_string(),
            );
            let err = format!(
                "{:#}",
                resolve_backtrace(&bt, &BTreeMap::new()).unwrap_err()
            );
            assert!(err.contains("`..`"), "unexpected error: {err}");
        }

        #[test]
        #[should_panic(expected = "must be rejected before resolution")]
        fn absolute_source_panics_after_validation() {
            // Absolute backtrace sources are rejected by the pre-resolve validation pass,
            // so reaching `resolve_backtrace` with one is an internal invariant violation.
            let mut bt = ComponentBacktraceConfig::default();
            bt.frame_files_to_sources
                .insert(".../src/lib.rs".to_string(), "/nested/lib.rs".to_string());
            let _ = resolve_backtrace(&bt, &BTreeMap::new());
        }
    }
}
