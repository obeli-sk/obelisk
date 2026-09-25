//! Server/runtime configuration (`obelisk.toml`): listeners, database, WASM engine
//! globals, watchers, allocator, and telemetry. Orthogonal to the deployment manifest.

use self::log::{LoggingConfig, LoggingStyle};
use crate::config::config_holder::{CACHE_DIR_PREFIX, PathPrefixes};
use crate::config::deployment::{
    AllowedHostToml, ByteSizeConfig, ConfigName, DurationConfig, DurationConfigOptional,
    InflightSemaphore, ValueOrUnlimited,
};
use crate::config::env_var::{
    StartupEnvVars, interpolate_env_vars_plaintext, interpolate_env_vars_secret,
    interpolate_startup_env_vars,
};
use crate::config::secret_registry::{
    PublicEnvToml, SecretExposureDigests, SecretRegistry, SecretsToml,
};
use anyhow::Context as _;
use concepts::component_id::Digest;
use concepts::persisted_value::DEFAULT_MAX_PERSISTED_VALUE_SIZE_BYTES;
use db_postgres::postgres_dao::{self, PostgresConfig};
use db_sqlite::sqlite_dao::SqliteConfig;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Default, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServerConfigToml {
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) app_name: String,
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) app_config_digest: Option<String>,
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) app_policy_json: Option<String>,
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) source_path: Option<PathBuf>,
    #[cfg(feature = "tokio-console")]
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) tokio_console_enabled: bool,
    #[serde(default, rename = "obelisk-version")]
    pub(crate) obelisk_version: Option<String>,
    /// App-owned secret registry supplied after app.toml is loaded.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) secrets: SecretsToml,
    /// App-owned public environment allowance supplied after app.toml is loaded.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) public_env: PublicEnvToml,
    /// App-reviewed exec components used by the runtime after startup validation.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) allowed_exec_activities: AllowExecActivities,
    /// Platform exec gate: `false` (default), `"*"`, or a reviewed component/digest set.
    #[serde(default, rename = "allowed_exec_activities")]
    pub(crate) platform_exec_activities: PlatformExecActivities,
    /// App-owned outbound HTTP allowance supplied after app.toml is loaded.
    #[serde(skip)]
    #[schemars(skip)]
    pub(crate) outbound_http: OutboundHttpToml,
    #[serde(default)]
    pub(crate) limits: LimitsToml,
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
    #[serde(default)]
    pub(crate) v8: V8ConfigToml,
    #[serde(default)]
    pub(crate) webhooks: WebhooksGlobalConfigToml,
    #[serde(default, rename = "workflows")]
    pub(crate) workflows_global_config: WorkflowsGlobalConfigToml,
    #[serde(default)]
    pub(crate) timers_watcher: TimersWatcherTomlConfig,
    #[serde(default)]
    pub(crate) cancel_watcher: CancelWatcherTomlConfig,
    #[serde(default)]
    pub(crate) maintenance: MaintenanceTomlConfig,
    #[cfg(feature = "otlp")]
    #[serde(default)]
    pub(crate) otlp: Option<otlp::OtlpConfig>,
    #[serde(default)]
    pub(crate) log: LoggingConfig,
    #[serde(default, rename = "http_server")]
    pub(crate) http_servers: Vec<HttpServer>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct V8ConfigToml {
    /// Stack size for each native V8 isolate thread. How many isolates there may be and how large
    /// one isolate's heap may get are the `v8` cells of `[limits]`.
    #[serde(default = "default_v8_thread_stack_size")]
    pub(crate) thread_stack_size: ByteSizeConfig,
}

impl Default for V8ConfigToml {
    fn default() -> Self {
        Self {
            thread_stack_size: default_v8_thread_stack_size(),
        }
    }
}

const fn default_v8_thread_stack_size() -> ByteSizeConfig {
    ByteSizeConfig::Mib(4)
}
impl ServerConfigToml {
    pub(crate) fn resolve_env_vars(
        &mut self,
        path_prefixes: &PathPrefixes,
        env_vars: &StartupEnvVars,
    ) -> Result<(), anyhow::Error> {
        #[cfg(feature = "tokio-console")]
        {
            self.tokio_console_enabled = env_vars
                .lookup("TOKIO_CONSOLE")
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or_default();
        }
        if let DatabaseConfigToml::Postgres(postgres) = &mut self.database {
            postgres.host = interpolate_startup_env_vars(&postgres.host, env_vars)?;
            postgres.user = interpolate_startup_env_vars(&postgres.user, env_vars)?;
            postgres.password = interpolate_startup_env_vars(&postgres.password, env_vars)?;
            postgres.db_name = interpolate_startup_env_vars(&postgres.db_name, env_vars)?;
        }
        if let DatabaseConfigToml::Sqlite(sqlite) = &mut self.database
            && let Some(directory) = &mut sqlite.directory
        {
            *directory = path_prefixes.resolve_server_path(directory, env_vars)?;
        }
        if let Some(directory) = &mut self.wasm_global_config.cache_directory {
            *directory = path_prefixes.resolve_server_path(directory, env_vars)?;
        }
        if let Some(directory) = &mut self.wasm_global_config.codegen_cache.directory {
            *directory = path_prefixes.resolve_server_path(directory, env_vars)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsToml {
    /// Maximum compact JSON-encoded size of one newly persisted execution
    /// value. Each execution snapshots the effective positive value so config
    /// changes do not alter replay. Historical executions created before this
    /// setting was persisted retain their legacy unlimited contract.
    #[serde(default = "default_max_persisted_value_size_bytes")]
    pub(crate) max_persisted_value_size_bytes: u64,
    /// Per-file size limit for deployment-owned blobs attached to a submit request.
    #[serde(default)]
    pub(crate) max_deployment_file_bytes: MaxDeploymentFileBytes,
    /// Maximum size of a single API transport message: the gRPC encoded message
    /// size and the equivalent REST request body limit. Must be positive; the
    /// default is 512 MiB. This is a transport bound and is distinct from
    /// `max_persisted_value_size_bytes`.
    #[serde(default = "default_max_transport_message_size_bytes")]
    pub(crate) max_transport_message_size_bytes: u64,
    /// Concurrency and per-slot memory of the activity runtimes.
    #[serde(default)]
    pub(crate) activities: ActivityCellsToml,
    /// Concurrency and per-slot memory of the workflow runtimes.
    #[serde(default)]
    pub(crate) workflows: WorkflowCellsToml,
    /// Concurrency and per-slot memory of the webhook runtimes.
    #[serde(default)]
    pub(crate) webhooks: WebhookCellsToml,
}

impl Default for LimitsToml {
    fn default() -> Self {
        Self {
            max_persisted_value_size_bytes: DEFAULT_MAX_PERSISTED_VALUE_SIZE_BYTES,
            max_deployment_file_bytes: MaxDeploymentFileBytes::default(),
            max_transport_message_size_bytes: default_max_transport_message_size_bytes(),
            activities: ActivityCellsToml::default(),
            workflows: WorkflowCellsToml::default(),
            webhooks: WebhookCellsToml::default(),
        }
    }
}

/// One `(workload, runtime)` cell: how many execution slots it grants and how large one slot may
/// get. Both keys accept `"unlimited"`; an omitted key takes the cell's default.
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellToml {
    #[serde(default)]
    pub(crate) count: Option<InflightSemaphore>,
    #[serde(default)]
    pub(crate) memory: Option<ValueOrUnlimited<ByteSizeConfig>>,
}

/// A cell whose slot is an operating system process, so its memory is not wasmtime's business.
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessCellToml {
    #[serde(default)]
    pub(crate) count: Option<InflightSemaphore>,
}

/// The activity runtimes are four different resources: a wasmtime instance, a V8 isolate with its
/// own heap and thread, an external process, and an emulated machine. A VM storm must not crowd
/// out plain exec activities, so each gets its own reservation. A VM cell is keyed by backend
/// because the backend decides what a slot costs.
#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActivityCellsToml {
    /// Activities running as WASM components, including Boa JavaScript activities.
    #[serde(default)]
    pub(crate) wasm: CellToml,
    /// Activities running on native V8.
    #[serde(default)]
    pub(crate) v8: CellToml,
    /// `activity_exec` activities, each slot an operating system process.
    #[serde(default)]
    pub(crate) process: ProcessCellToml,
    /// `activity_vm` activities on the Bochs backend.
    #[serde(default)]
    pub(crate) vm_bochs: CellToml,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowCellsToml {
    /// Workflows running as WASM components, including Boa JavaScript workflows.
    #[serde(default)]
    pub(crate) wasm: CellToml,
    /// Workflows running on native V8.
    #[serde(default)]
    pub(crate) v8: CellToml,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebhookCellsToml {
    /// Webhook endpoints running as WASM components, including Boa JavaScript endpoints.
    #[serde(default)]
    pub(crate) wasm: CellToml,
    /// Webhook endpoints running on native V8.
    #[serde(default)]
    pub(crate) v8: CellToml,
}

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// One resolved `(workload, runtime)` cell. Its semaphore is shared by every executor of the cell,
/// and for a native V8 cell it is the one `V8Executor` admits from.
#[derive(Debug, Clone)]
pub(crate) struct ConcurrencyCell {
    semaphore: Arc<tokio::sync::Semaphore>,
    /// `None` when the cell grants unlimited slots.
    count: Option<usize>,
    /// Memory one slot may hold; `None` when unbounded.
    memory: Option<u64>,
}

impl ConcurrencyCell {
    fn new(count: Option<usize>, memory: Option<u64>) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(
                count.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS),
            )),
            count,
            memory,
        }
    }

    /// A cell that bounds nothing, for tests and direct callers.
    #[cfg(test)]
    pub(crate) fn unlimited() -> Self {
        Self::new(None, None)
    }

    /// The executor's `task_limiter_cell`. `None` for an unlimited cell, which skips the
    /// acquisition rather than taking a permit that can never be refused.
    pub(crate) fn task_limiter(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        self.count.map(|_| self.semaphore.clone())
    }

    pub(crate) fn memory(&self) -> Option<u64> {
        self.memory
    }

    fn v8(&self) -> Result<wasm_workers::v8_executor::V8Cell, anyhow::Error> {
        let max_heap_size = self
            .memory
            .map(|memory| {
                usize::try_from(memory).context("a native V8 cell's `memory` does not fit usize")
            })
            .transpose()?;
        Ok(wasm_workers::v8_executor::V8Cell::new(
            self.semaphore.clone(),
            self.count.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS),
            max_heap_size,
        ))
    }
}

/// Every execution slot in the process is charged to exactly one of these. They are independent
/// reservations with no process-wide total: their sum is the process bound.
#[derive(Debug, Clone)]
pub(crate) struct ConcurrencyCells {
    pub(crate) activities_wasm: ConcurrencyCell,
    pub(crate) activities_v8: ConcurrencyCell,
    pub(crate) activities_process: ConcurrencyCell,
    pub(crate) activities_vm_bochs: ConcurrencyCell,
    pub(crate) workflows_wasm: ConcurrencyCell,
    pub(crate) workflows_v8: ConcurrencyCell,
    pub(crate) webhooks_wasm: ConcurrencyCell,
    pub(crate) webhooks_v8: ConcurrencyCell,
}

/// The cell each kind of component charges to, with the JavaScript kinds resolved against the
/// server-wide runtime mode once, so every configuration site just reads its own field.
#[derive(Debug, Clone)]
pub(crate) struct ComponentCells {
    pub(crate) activities_wasm: ConcurrencyCell,
    pub(crate) activities_js: ConcurrencyCell,
    pub(crate) activities_process: ConcurrencyCell,
    pub(crate) activities_vm: ConcurrencyCell,
    pub(crate) workflows_wasm: ConcurrencyCell,
    pub(crate) workflows_js: ConcurrencyCell,
    pub(crate) webhooks_wasm: ConcurrencyCell,
}

impl ConcurrencyCells {
    /// A Boa component is a WASM component, so its JavaScript source is irrelevant to what its
    /// slot costs and it is charged to the wasm cell.
    pub(crate) fn for_js_runtime(&self, native_v8: bool) -> ComponentCells {
        ComponentCells {
            activities_wasm: self.activities_wasm.clone(),
            activities_js: if native_v8 {
                self.activities_v8.clone()
            } else {
                self.activities_wasm.clone()
            },
            activities_process: self.activities_process.clone(),
            activities_vm: self.activities_vm_bochs.clone(),
            workflows_wasm: self.workflows_wasm.clone(),
            workflows_js: if native_v8 {
                self.workflows_v8.clone()
            } else {
                self.workflows_wasm.clone()
            },
            webhooks_wasm: self.webhooks_wasm.clone(),
        }
    }

    pub(crate) fn v8_executor_config(
        &self,
        thread_stack_size: usize,
    ) -> Result<wasm_workers::v8_executor::V8ExecutorConfig, anyhow::Error> {
        Ok(wasm_workers::v8_executor::V8ExecutorConfig {
            workflows: self.workflows_v8.v8()?,
            activities: self.activities_v8.v8()?,
            webhooks: self.webhooks_v8.v8()?,
            thread_stack_size,
        })
    }
}

impl LimitsToml {
    pub(crate) fn resolve_cells(&self) -> Result<ConcurrencyCells, anyhow::Error> {
        Ok(ConcurrencyCells {
            activities_wasm: self.activities.wasm.resolve("activities.wasm", 500, GIB)?,
            activities_v8: self.activities.v8.resolve("activities.v8", 16, 256 * MIB)?,
            activities_process: resolve_count(
                "activities.process",
                self.activities.process.count,
                32,
            )
            .map(|count| ConcurrencyCell::new(count, None))?,
            activities_vm_bochs: self
                .activities
                .vm_bochs
                .resolve("activities.vm_bochs", 8, GIB)?,
            workflows_wasm: self
                .workflows
                .wasm
                .resolve("workflows.wasm", 500, 512 * MIB)?,
            workflows_v8: self.workflows.v8.resolve("workflows.v8", 100, 256 * MIB)?,
            webhooks_wasm: self
                .webhooks
                .wasm
                .resolve("webhooks.wasm", 500, 512 * MIB)?,
            webhooks_v8: self.webhooks.v8.resolve("webhooks.v8", 16, 256 * MIB)?,
        })
    }
}

impl CellToml {
    fn resolve(
        self,
        cell: &str,
        default_count: usize,
        default_memory: u64,
    ) -> Result<ConcurrencyCell, anyhow::Error> {
        let count = resolve_count(cell, self.count, default_count)?;
        let memory = match self.memory {
            None => Some(default_memory),
            Some(memory) => Option::<ByteSizeConfig>::from(memory).map(u64::from),
        };
        anyhow::ensure!(
            memory != Some(0),
            "`limits.{cell}.memory` must be greater than zero"
        );
        Ok(ConcurrencyCell::new(count, memory))
    }
}

/// A cell granting zero slots would leave its workload permanently pending, so it is rejected
/// rather than accepted as a way of switching a runtime off.
fn resolve_count(
    cell: &str,
    count: Option<InflightSemaphore>,
    default_count: usize,
) -> Result<Option<usize>, anyhow::Error> {
    let count = match count {
        None => Some(default_count),
        Some(InflightSemaphore::Unlimited(_)) => None,
        Some(InflightSemaphore::Some(count)) => Some(usize::try_from(count).expect("usize >= u32")),
    };
    anyhow::ensure!(
        count != Some(0),
        "`limits.{cell}.count` must be greater than zero"
    );
    Ok(count)
}

const fn default_max_persisted_value_size_bytes() -> u64 {
    DEFAULT_MAX_PERSISTED_VALUE_SIZE_BYTES
}

const fn default_max_transport_message_size_bytes() -> u64 {
    crate::api::DEFAULT_MAX_TRANSPORT_MESSAGE_SIZE_BYTES as u64
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone)]
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

/// Exec activity policy: component name -> reviewed secret exposure digests.
pub(crate) type AllowExecActivities = BTreeMap<String, SecretExposureDigests>;

#[derive(Debug, Default, Clone)]
pub(crate) enum PlatformExecActivities {
    #[default]
    Disabled,
    All,
    Allowlist(AllowExecActivities),
}

impl<'de> Deserialize<'de> for PlatformExecActivities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = PlatformExecActivities;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("false, \"*\", or a component/digest allowlist")
            }

            fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
                if value {
                    Err(E::custom("use `\"*\"` to allow all exec activities"))
                } else {
                    Ok(PlatformExecActivities::Disabled)
                }
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value == "*" {
                    Ok(PlatformExecActivities::All)
                } else {
                    Err(E::custom(format!(
                        "invalid exec allowance `{value}`; expected `\"*\"`"
                    )))
                }
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut entries = AllowExecActivities::new();
                while let Some((name, digests)) = map.next_entry()? {
                    entries.insert(name, digests);
                }
                Ok(PlatformExecActivities::Allowlist(entries))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl JsonSchema for PlatformExecActivities {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PlatformExecActivities".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "oneOf": [
                { "const": false },
                { "const": "*" },
                generator.subschema_for::<AllowExecActivities>()
            ]
        })
    }
}

pub(crate) fn audit_exec_activities(entries: &AllowExecActivities) -> serde_json::Value {
    serde_json::json!({
        "mode": if entries.is_empty() { "deny" } else { "allowlist" },
        "entries": entries.iter().map(|(name, digests)| {
            (name, digests.iter().map(ToString::to_string).collect::<Vec<_>>())
        }).collect::<BTreeMap<_, _>>(),
    })
}

#[derive(Debug, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebhooksGlobalConfigToml {
    /// Wall-clock deadline for a webhook handler to accept the request and return its HTTP
    /// response, regardless of component runtime. Once the response has been returned, this
    /// deadline does not limit the lifetime of a streaming response body.
    #[serde(default = "default_webhook_request_timeout")]
    pub(crate) request_timeout: DurationConfig,
}

impl Default for WebhooksGlobalConfigToml {
    fn default() -> Self {
        Self {
            request_timeout: default_webhook_request_timeout(),
        }
    }
}

const fn default_webhook_request_timeout() -> DurationConfig {
    DurationConfig::Seconds(30)
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApiConfig {
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
    #[serde(default = "default_api_listening_addr")]
    pub(crate) listening_addr: SocketAddr,
    /// Accepted API bearer tokens as `sha256:<hex>` digests of the token text.
    /// Only hashes of high-entropy random tokens are safe to commit: SHA-256 is fast and unsalted,
    /// so hashes of passwords or other guessable tokens are vulnerable to offline guessing.
    /// Tokens should contain at least 32 characters. Always generate an entry with
    /// `obelisk generate token`.
    #[serde(default)]
    pub(crate) token_hashes: Vec<Digest>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            listening_addr: default_api_listening_addr(),
            token_hashes: Vec::new(),
        }
    }
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
        let default_dir = if path_prefixes.project_dirs.is_some() {
            format!("${{DATA_DIR}}/apps/{}/sqlite", path_prefixes.app_name)
        } else {
            format!("apps/{}/sqlite", path_prefixes.app_name)
        };
        let sqlite_file = self.directory.as_deref().unwrap_or(&default_dir);
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

#[derive(Debug, Default, Deserialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct MaintenanceTomlConfig {
    #[serde(default)]
    pub(crate) gc: GarbageCollectionTomlConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct GarbageCollectionTomlConfig {
    #[serde(default = "default_gc_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_gc_interval")]
    pub(crate) interval: DurationConfig,
    #[serde(default = "default_gc_batch_size")]
    pub(crate) batch_size: u32,
    #[serde(default = "default_gc_batch_delay")]
    pub(crate) batch_delay: DurationConfig,
    #[serde(default)]
    pub(crate) retention: RetentionTomlConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionTomlConfig {
    #[serde(default)]
    pub(crate) executions: RetentionPolicyTomlConfig,
    #[serde(default)]
    pub(crate) deployments: RetentionPolicyTomlConfig,
    #[serde(default)]
    pub(crate) system_events: RetentionPolicyTomlConfig,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetentionPolicyTomlConfig {
    #[serde(default = "default_retention_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_retention_max_age")]
    pub(crate) max_age: DurationConfig,
}

impl Default for RetentionPolicyTomlConfig {
    fn default() -> Self {
        Self {
            enabled: default_retention_enabled(),
            max_age: default_retention_max_age(),
        }
    }
}

impl Default for GarbageCollectionTomlConfig {
    fn default() -> Self {
        Self {
            enabled: default_gc_enabled(),
            interval: default_gc_interval(),
            batch_size: default_gc_batch_size(),
            batch_delay: default_gc_batch_delay(),
            retention: RetentionTomlConfig::default(),
        }
    }
}

impl Default for CancelWatcherTomlConfig {
    fn default() -> Self {
        Self {
            tick_sleep: default_cancel_watcher_tick_sleep(),
        }
    }
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
    use super::{Deserialize, JsonSchema, default_console_enabled, default_console_style};
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

const fn default_gc_enabled() -> bool {
    true
}

const fn default_gc_interval() -> DurationConfig {
    DurationConfig::Seconds(30)
}

const fn default_gc_batch_size() -> u32 {
    1000
}

const fn default_gc_batch_delay() -> DurationConfig {
    DurationConfig::Milliseconds(25)
}

const fn default_retention_enabled() -> bool {
    true
}

const fn default_retention_max_age() -> DurationConfig {
    DurationConfig::Hours(30 * 24)
}

// HTTP server declaration (referenced by ServerConfigToml)

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpServer {
    pub(crate) name: ConfigName,
    pub(crate) listening_addr: SocketAddr,
}

// Default on-disk locations and size limits for the server's data/cache directories.

pub(crate) const SQLITE_FILE_NAME: &str = "obelisk.sqlite";
const DEFAULT_WASM_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}wasm", CACHE_DIR_PREFIX);
const DEFAULT_WASM_DIRECTORY: &str = "cache/wasm";
const DEFAULT_CODEGEN_CACHE_DIRECTORY_IF_PROJECT_DIRS: &str =
    const_format::formatcp!("{}codegen", CACHE_DIR_PREFIX);
const DEFAULT_CODEGEN_CACHE_DIRECTORY: &str = "cache/codegen";
pub(crate) const MAX_DEPLOYMENT_FILE_BYTES: u32 = 20 * 1024 * 1024; // 20MiB

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::deployment::{MethodsInput, ReplaceIn};

    mod limits {
        use super::*;

        fn cells(toml: &str) -> ConcurrencyCells {
            toml::from_str::<ServerConfigToml>(toml)
                .unwrap()
                .limits
                .resolve_cells()
                .unwrap()
        }

        /// Exceeding the wasmtime pooling allocator's per-engine limit arrives as an
        /// instantiation failure, which both workers classify as `FatalError::CannotInstantiate`:
        /// a transient capacity problem would permanently fail an execution instead of leaving it
        /// pending. Every wasm cell's default therefore stays below that limit, so saturation is
        /// answered by backpressure.
        #[test]
        fn wasm_cell_defaults_must_stay_below_the_pooling_allocator_limit() {
            /// wasmtime's `PoolingAllocationConfig` default on a 64-bit host, which obelisk
            /// leaves unset for each of its engines.
            const POOLING_TOTAL_COMPONENT_INSTANCES: usize = 1000;
            let cells = cells("");
            for (name, cell) in [
                ("activities.wasm", &cells.activities_wasm),
                ("workflows.wasm", &cells.workflows_wasm),
                ("webhooks.wasm", &cells.webhooks_wasm),
                ("activities.vm_bochs", &cells.activities_vm_bochs),
            ] {
                let count = cell.count.expect("a wasm cell must have a real default");
                assert!(
                    count < POOLING_TOTAL_COMPONENT_INSTANCES,
                    "`{name}` default {count} must leave headroom below the pooling allocator"
                );
            }
        }

        #[test]
        fn omitted_cells_must_take_their_defaults() {
            let cells = cells("");
            assert_eq!(Some(500), cells.activities_wasm.count);
            assert_eq!(Some(GIB), cells.activities_wasm.memory);
            assert_eq!(Some(32), cells.activities_process.count);
            assert_eq!(None, cells.activities_process.memory);
            assert_eq!(Some(100), cells.workflows_v8.count);
            assert_eq!(Some(256 * MIB), cells.workflows_v8.memory);
        }

        #[test]
        fn a_cell_must_accept_unlimited_on_either_key() {
            let cells = cells(
                r#"
                [limits.activities.wasm]
                count = "unlimited"
                memory.mib = 64
                [limits.workflows.wasm]
                memory = "unlimited"
                "#,
            );
            assert_eq!(None, cells.activities_wasm.count);
            assert!(cells.activities_wasm.task_limiter().is_none());
            assert_eq!(Some(64 * MIB), cells.activities_wasm.memory);
            assert_eq!(Some(500), cells.workflows_wasm.count);
            assert_eq!(None, cells.workflows_wasm.memory);
        }

        /// A cell granting zero slots would leave its workload pending forever.
        #[test]
        fn a_zero_cell_must_be_rejected() {
            let err = toml::from_str::<ServerConfigToml>(
                r"
                [limits.activities.wasm]
                count = 0
                ",
            )
            .unwrap()
            .limits
            .resolve_cells()
            .unwrap_err();
            assert!(
                err.to_string().contains("limits.activities.wasm.count"),
                "{err}"
            );
        }

        /// The keys are a breaking rename, and `deny_unknown_fields` is what turns a config still
        /// carrying the old ones into a load failure rather than silently lost limits.
        #[test]
        fn the_replaced_global_limiters_must_be_rejected() {
            for old in [
                "[wasm]\nglobal_executor_instance_limiter = 10",
                "[wasm]\nglobal_webhook_instance_limiter = 10",
                "[v8]\nmax_workflows = 10",
                "[v8]\nmax_heap_size = 1024",
            ] {
                assert!(
                    toml::from_str::<ServerConfigToml>(old).is_err(),
                    "`{old}` must not load silently"
                );
            }
        }

        /// The cells shipped in `server-help.toml` must actually load: that file is what
        /// `obelisk server generate-config` hands the operator, and a stale line in it is
        /// documentation that cannot be caught by the rest of the suite.
        #[test]
        fn the_documented_cells_must_load() {
            let documented = crate::config::config_holder::OBELISK_HELP_SERVER_TOML
                .lines()
                .skip_while(|line| !line.starts_with("# [limits.activities.wasm]"))
                .take_while(|line| line.starts_with('#'))
                .map(|line| line.trim_start_matches("# ").trim_start_matches('#'))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                documented.contains("[limits.webhooks.v8]"),
                "the documented cells must be found: {documented}"
            );
            let cells = cells(&documented);
            assert_eq!(Some(500), cells.activities_wasm.count);
            assert_eq!(Some(GIB), cells.activities_wasm.memory);
            assert_eq!(Some(32), cells.activities_process.count);
            assert_eq!(Some(8), cells.activities_vm_bochs.count);
            assert_eq!(Some(100), cells.workflows_v8.count);
            assert_eq!(Some(16), cells.webhooks_v8.count);
            assert_eq!(Some(256 * MIB), cells.webhooks_v8.memory);
        }

        #[test]
        fn a_byte_size_must_name_its_unit() {
            assert!(
                toml::from_str::<ServerConfigToml>(
                    r"
                    [limits.activities.wasm]
                    memory = 1048576
                    "
                )
                .is_err(),
                "a bare integer must not be accepted as a byte size"
            );
        }
    }

    mod webhooks {
        use super::*;

        #[test]
        fn request_timeout_is_runtime_independent() {
            let default_config: ServerConfigToml = toml::from_str("").unwrap();
            assert!(matches!(
                default_config.webhooks.request_timeout,
                DurationConfig::Seconds(30)
            ));

            let config: ServerConfigToml = toml::from_str(
                r"
                [webhooks]
                request_timeout.milliseconds = 250
                ",
            )
            .unwrap();
            assert!(matches!(
                config.webhooks.request_timeout,
                DurationConfig::Milliseconds(250)
            ));
        }

        #[test]
        fn request_timeout_is_not_a_v8_setting() {
            toml::from_str::<ServerConfigToml>(
                r"
                [v8]
                webhook_request_timeout.seconds = 1
                ",
            )
            .unwrap_err();
        }
    }

    mod outbound_http {
        use super::*;

        #[test]
        fn app_allowlist_uses_deployment_allowed_host_shape() {
            let config: crate::config::app::AppConfigToml = toml::from_str(
                r#"
                [secrets]
                API_KEY = {}

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
            assert_eq!(entry.secrets, ["API_KEY".into()]);
            assert!(matches!(
                entry.methods,
                Some(MethodsInput::List(ref methods)) if methods.as_slice() == ["POST"]
            ));
            assert!(matches!(entry.replace_in.as_slice(), [ReplaceIn::Headers]));
        }

        #[test]
        fn omitted_app_allowlist_is_empty() {
            let config: crate::config::app::AppConfigToml = toml::from_str("").unwrap();
            assert!(config.outbound_http.allowed_hosts.is_empty());
        }
    }

    mod allow_exec_activities {
        use super::*;

        #[derive(serde::Deserialize, Debug)]
        struct TestConfig {
            #[serde(default)]
            allow: AllowExecActivities,
        }

        const DIGEST: &str =
            "sha256:abababababababababababababababababababababababababababababababab";

        #[test]
        fn platform_gate_accepts_false_wildcard_or_reviewed_entries() {
            let default: ServerConfigToml = toml::from_str("").unwrap();
            assert!(matches!(
                default.platform_exec_activities,
                PlatformExecActivities::Disabled
            ));
            let disabled: ServerConfigToml =
                toml::from_str("allowed_exec_activities = false").unwrap();
            assert!(matches!(
                disabled.platform_exec_activities,
                PlatformExecActivities::Disabled
            ));
            let all: ServerConfigToml = toml::from_str("allowed_exec_activities = '*' ").unwrap();
            assert!(matches!(
                all.platform_exec_activities,
                PlatformExecActivities::All
            ));
            let reviewed: ServerConfigToml =
                toml::from_str(&format!("[allowed_exec_activities]\nworker = '{DIGEST}'")).unwrap();
            assert!(
                matches!(reviewed.platform_exec_activities, PlatformExecActivities::Allowlist(ref entries) if entries.contains_key("worker"))
            );
            let next_digest = DIGEST.replace("ab", "cd");
            let allowlist_toml =
                format!("[allowed_exec_activities]\nworker = ['{DIGEST}', '{next_digest}']");
            let reviewed: ServerConfigToml = toml::from_str(&allowlist_toml).unwrap();
            assert!(
                matches!(reviewed.platform_exec_activities, PlatformExecActivities::Allowlist(ref entries) if entries["worker"].iter().count() == 2)
            );
            let app_toml = format!(
                "{allowlist_toml}\n[secrets]\nTOKEN = {{ exposed_to = {{ worker = ['{DIGEST}', '{next_digest}'] }} }}"
            );
            let app: crate::config::app::AppConfigToml = toml::from_str(&app_toml).unwrap();
            assert_eq!(app.allowed_exec_activities["worker"].iter().count(), 2);
            assert_eq!(app.secrets["TOKEN"].exposed_to["worker"].iter().count(), 2);
            assert!(toml::from_str::<ServerConfigToml>("allowed_exec_activities = true").is_err());
            assert!(toml::from_str::<ServerConfigToml>("allowed_exec_activities = 'all'").is_err());
        }

        #[test]
        fn deserialize_scalar_or_list_map() {
            toml::from_str::<TestConfig>("allow = true").unwrap_err();
            toml::from_str::<TestConfig>("allow = false").unwrap_err();
            let actual: TestConfig = toml::from_str("").unwrap();
            assert!(actual.allow.is_empty());
            let actual: TestConfig =
                toml::from_str(&format!("[allow]\ngreet = \"{DIGEST}\"")).unwrap();
            let digests = actual.allow.get("greet").unwrap();
            assert_eq!(digests.iter().next().unwrap().to_string(), DIGEST);
            let actual: TestConfig =
                toml::from_str(&format!("[allow]\ngreet = [\"{DIGEST}\"]")).unwrap();
            assert_eq!(actual.allow["greet"].iter().count(), 1);
        }
    }

    mod retention {
        use super::*;

        #[test]
        fn omitted_retention_uses_enabled_thirty_day_defaults() {
            let config: ServerConfigToml = toml::from_str("").unwrap();
            let retention = config.maintenance.gc.retention;
            for policy in [
                retention.executions,
                retention.deployments,
                retention.system_events,
            ] {
                assert!(policy.enabled);
                assert!(matches!(policy.max_age, DurationConfig::Hours(720)));
            }
        }

        #[test]
        fn execution_and_deployment_retention_can_be_configured_independently() {
            let config: ServerConfigToml = toml::from_str(
                r"
                [maintenance.gc.retention.executions]
                enabled = false

                [maintenance.gc.retention.deployments]
                max_age.hours = 48

                [maintenance.gc.retention.system_events]
                max_age.hours = 168
                ",
            )
            .unwrap();
            let retention = config.maintenance.gc.retention;
            assert!(!retention.executions.enabled);
            assert!(matches!(
                retention.executions.max_age,
                DurationConfig::Hours(720)
            ));
            assert!(retention.deployments.enabled);
            assert!(matches!(
                retention.deployments.max_age,
                DurationConfig::Hours(48)
            ));
            assert!(retention.system_events.enabled);
            assert!(matches!(
                retention.system_events.max_age,
                DurationConfig::Hours(168)
            ));
        }
    }
}
