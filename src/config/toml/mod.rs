//! TOML configuration for the obelisk binary, split by concern:
//! - [`authored`]: the user-authored deployment manifest and its validation.
//! - [`resolve`]: resolving + fetching/verifying a manifest into runtime-ready configs.
//! - [`server`]: server/runtime config (`obelisk.toml`), orthogonal to the manifest.
//! - [`common`]: serde data-shape primitives shared across the stages.
//!
//! This module is only module wiring: it declares the submodules and re-exports them so
//! `crate::config::toml::<Type>` paths stay stable.

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

mod authored;
mod common;
mod resolve;
mod server;
pub(crate) use authored::*;
pub(crate) use common::*;
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
        use crate::config::toml::common::{
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
mod tests;
