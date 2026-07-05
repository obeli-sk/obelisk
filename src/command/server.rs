use crate::args::Server;
use crate::args::shadow::PKG_VERSION;
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::PathPrefixes;
use crate::config::config_holder::load_deployment_canonical;
use crate::config::content_digest_to_wasm_file;
use crate::config::env_var::EnvVarConfig;
use crate::config::file_provider::CasFileProvider;
use crate::config::manifest::DeploymentManifest;
use crate::config::manifest::DeploymentManifestFile;
use crate::config::toml::ActivityExecComponentConfigResolvedExt as _;
use crate::config::toml::ActivityExecConfigVerified;
use crate::config::toml::ActivityExternalComponentConfigResolved;
use crate::config::toml::ActivityExternalComponentConfigResolvedExt as _;
use crate::config::toml::ActivityExternalConfigVerified;
use crate::config::toml::ActivityJsComponentConfigResolvedExt as _;
use crate::config::toml::ActivityJsConfigVerified;
use crate::config::toml::ActivityStubComponentConfigResolved;
use crate::config::toml::ActivityStubComponentConfigResolvedExt as _;
use crate::config::toml::ActivityStubConfigVerified;
use crate::config::toml::ActivityStubExtConfigVerified;
use crate::config::toml::ActivityStubExtInlineConfigVerified;
use crate::config::toml::ActivityWasmComponentConfigTomlExt as _;
use crate::config::toml::ActivityWasmConfigVerified;
use crate::config::toml::CancelWatcherTomlConfig;
use crate::config::toml::ComponentCommon;
use crate::config::toml::ComponentLocationFetchExt as _;
use crate::config::toml::ComponentLocationToml;
use crate::config::toml::ComponentStdOutputToml;
use crate::config::toml::ConfigName;
use crate::config::toml::DatabaseConfigToml;
use crate::config::toml::DeploymentResolved;
use crate::config::toml::InflightSemaphoreExt as _;
use crate::config::toml::LogLevelToml;
use crate::config::toml::SQLITE_FILE_NAME;
use crate::config::toml::ServerConfigToml;
use crate::config::toml::TimersWatcherTomlConfig;
use crate::config::toml::WasmtimeAllocatorConfig;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::WorkflowJsComponentConfigResolvedExt as _;
use crate::config::toml::WorkflowJsConfigVerified;
use crate::config::toml::WorkflowWasmComponentConfigResolvedExt as _;
use crate::config::toml::cron::CronComponentConfigTomlExt as _;
use crate::config::toml::cron::CronConfigVerified;
use crate::config::toml::webhook;
use crate::config::toml::webhook::HttpServer;
use crate::config::toml::webhook::WebhookJsComponentConfigResolvedExt as _;
use crate::config::toml::webhook::WebhookJsConfigVerified;
use crate::config::toml::webhook::WebhookRoute;
use crate::config::toml::webhook::WebhookRouteVerified;
use crate::config::toml::webhook::WebhookWasmComponentConfigResolvedExt as _;
use crate::config::toml::webhook::WebhookWasmComponentConfigVerified;
use crate::config::toml::{AllowedHostToml, MethodsInput, MethodsInputStar};
use crate::config::wasm_cache_metadata_dir;
use crate::init;
use crate::init::Guard;
use crate::project_dirs;
use crate::server::grpc_server::GrpcServer;
use crate::server::web_api_server::WebApiState;
use crate::server::web_api_server::app_router;
use anyhow::Context;
use anyhow::bail;
use chrono::Utc;
use concepts::ComponentId;
use concepts::ComponentType;
use concepts::ContentDigest;
use concepts::ExecutionId;
use concepts::FnName;
use concepts::FunctionExtension;
use concepts::FunctionFqn;
use concepts::FunctionRegistry;
use concepts::IfcFqnName;
use concepts::ParameterType;
use concepts::Params;
use concepts::ReturnTypeExtendable;
use concepts::SUFFIX_FN_SCHEDULE;
use concepts::StrVariant;
use concepts::component_id::ComponentDigest;
use concepts::component_id::Digest;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::CreateRequest;
use concepts::storage::DbErrorWrite;
use concepts::storage::DbErrorWriteNonRetriable;
use concepts::storage::DbExternalApi;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use concepts::storage::DeploymentComponentRecord;
use concepts::storage::DeploymentFileRecord;
use concepts::storage::EnqueueOutcome;
use concepts::storage::LogInfoAppendRow;
use concepts::storage::LogLevel;
use concepts::storage::{ComponentMetadataRecord, DeploymentRecord, DeploymentStatus};
use concepts::time::ClockFn;
use concepts::time::Now;
use concepts::time::TokioSleep;
use db_postgres::postgres_dao::PostgresPool;
use db_sqlite::sqlite_dao::SqlitePool;
use directories::BaseDirs;
use directories::ProjectDirs;
use executor::AbortOnDropHandle;
use executor::executor::ExecutorTaskHandle;
use executor::executor::WorkerTasksHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher;
use executor::expired_timers_watcher::TimersWatcherConfig;
use executor::worker::Worker;
use futures_util::future::OptionFuture;
use grpc::extractor::accept_trace;
use grpc::grpc_gen;
use hashbrown::HashMap;
use indexmap::IndexMap;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::fmt::Debug;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::TryAcquireError;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tonic::codec::CompressionEncoding;
use tonic::service::RoutesBuilder;
use tonic_web::GrpcWebLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing::Instrument;
use tracing::Span;
use tracing::info_span;
use tracing::instrument;
use tracing::warn;
use tracing::{debug, info, trace};
use utils::wasm_tools::WasmComponent;
use val_json::wast_val::WastValWithType;
use wasm_workers::RunnableComponent;
use wasm_workers::activity::activity_exec_worker::ActivityExecWorkerCompiled;
use wasm_workers::activity::activity_js_worker::ActivityJsWorkerCompiled;
use wasm_workers::activity::activity_worker::ActivityWorkerCompiled;
use wasm_workers::activity::cancel_registry::CancelRegistry;
use wasm_workers::component_logger::LogStrageConfig;
use wasm_workers::cron::cron_worker;
use wasm_workers::cron::cron_worker::CronOrOnce;
use wasm_workers::cron::cron_worker::CronWorker;
use wasm_workers::engines::EngineConfig;
use wasm_workers::engines::Engines;
use wasm_workers::engines::PoolingConfig;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::log_db_forwarder;
use wasm_workers::registry::ComponentConfig;
use wasm_workers::registry::ComponentConfigImportable;
use wasm_workers::registry::ComponentConfigRegistry;
use wasm_workers::registry::ComponentConfigRegistryRO;
use wasm_workers::registry::ReplayWorker;
use wasm_workers::registry::ReplayWorkerRegistry;
use wasm_workers::registry::WitOrigin;
use wasm_workers::webhook::webhook_registry::WebhookRegistry;
use wasm_workers::webhook::webhook_trigger;
use wasm_workers::webhook::webhook_trigger::MethodAwareRouter;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointCompiled;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointConfig;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointInstanceLinked;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointJsConfig;
use wasm_workers::webhook::webhook_trigger::WebhookServerState;
use wasm_workers::workflow::deadline_tracker::{
    DeadlineTrackerFactoryForReplay, DeadlineTrackerFactoryTokio,
};
use wasm_workers::workflow::host_exports::history_event_schedule_at_from_wast_val;
use wasm_workers::workflow::workflow_js_worker::WorkflowJsWorkerCompiled;
use wasm_workers::workflow::workflow_js_worker::WorkflowJsWorkerLinked;
use wasm_workers::workflow::workflow_worker::JoinNextBlockingStrategy;
use wasm_workers::workflow::workflow_worker::WorkflowConfig;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerCompiled;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerLinked;
use wasmtime::Engine;

pub(crate) struct DeploymentContext {
    pub(crate) deployment_id: DeploymentId,
    pub(crate) component_registry_ro: wasm_workers::registry::ComponentConfigRegistryRO,
    pub(crate) exec_task_handles: Vec<ExecutorTaskHandle>,
    pub(crate) replay_workers: Arc<ReplayWorkerRegistry>,
    pub(crate) closed: bool,
}

pub(crate) type DeploymentContextHandle = Arc<tokio::sync::RwLock<DeploymentContext>>;

struct PreparedDeploymentSwitch {
    deployment_id: DeploymentId,
    digest: ContentDigest,
    compiled_linked: ServerCompiledLinked,
}

#[derive(Clone)]
pub(crate) struct DeploymentSwitchManagerHandle {
    inner: Arc<DeploymentSwitchManager>,
}

/// Permit for the switch lane (hot redeploy / enqueue), held for its `Drop` which releases
/// the slot. A distinct type from [`SubmitPermit`] so the two lanes cannot be mixed up.
struct SwitchPermit(#[expect(dead_code)] OwnedSemaphorePermit);

/// Permit for the submit lane, held for its `Drop` which releases a slot.
struct SubmitPermit(#[expect(dead_code)] OwnedSemaphorePermit);

struct DeploymentSwitchManager {
    /// Independent lane for submit, so a long hot switch does not block submissions.
    /// Holds `submit_concurrency` permits; `close()` drains all of them.
    submit_lane: Arc<Semaphore>,
    /// Serializes hot redeploys and cold switch/enqueue: at most one in flight,
    /// surplus requests get `Busy`. Must stay at exactly 1 permit: `close()` drains this
    /// lane with a single `acquire_owned()`, which only accounts for one permit.
    switch_gate: Arc<Semaphore>,
    /// Permit count of `submit_gate`, retained so `close()` can drain every permit.
    submit_concurrency: u32,
    latest_prepared: Mutex<Option<PreparedDeploymentSwitch>>,
    server_verified: ServerVerified,
    prepared_dirs: PreparedDirs,
    db_pool: Arc<dyn DbPool>,
    termination_watcher: watch::Receiver<()>,
    deployment_ctx: DeploymentContextHandle,
    webhook_registry: Arc<WebhookRegistry>,
    cancel_registry: CancelRegistry,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
}

impl DeploymentSwitchManagerHandle {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        server_verified: ServerVerified,
        prepared_dirs: PreparedDirs,
        db_pool: Arc<dyn DbPool>,
        termination_watcher: watch::Receiver<()>,
        deployment_ctx: DeploymentContextHandle,
        webhook_registry: Arc<WebhookRegistry>,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
        submit_concurrency: u32,
    ) -> Self {
        Self {
            inner: Arc::new(DeploymentSwitchManager {
                submit_lane: Arc::new(Semaphore::new(submit_concurrency as usize)),
                switch_gate: Arc::new(Semaphore::new(1)), // at most 1
                submit_concurrency,
                latest_prepared: Mutex::new(None),
                server_verified,
                prepared_dirs,
                db_pool,
                termination_watcher,
                deployment_ctx,
                webhook_registry,
                cancel_registry,
                log_forwarder_sender,
            }),
        }
    }

    fn try_acquire_switch_permit(&self) -> Result<SwitchPermit, SwitchError> {
        match self.inner.switch_gate.clone().try_acquire_owned() {
            Ok(permit) => Ok(SwitchPermit(permit)),
            Err(TryAcquireError::NoPermits) => Err(SwitchError::Busy),
            Err(TryAcquireError::Closed) => Err(SwitchError::Other(anyhow::anyhow!(
                "server is being shut down"
            ))),
        }
    }

    fn try_acquire_submit_permit(&self) -> Result<SubmitPermit, SubmitDeploymentError> {
        match self.inner.submit_lane.clone().try_acquire_owned() {
            Ok(permit) => Ok(SubmitPermit(permit)),
            Err(TryAcquireError::NoPermits) => Err(SubmitDeploymentError::Busy),
            Err(TryAcquireError::Closed) => Err(SubmitDeploymentError::Other(anyhow::anyhow!(
                "server is being shut down"
            ))),
        }
    }

    async fn take_latest_prepared(
        &self,
        deployment_id: DeploymentId,
        digest: &ContentDigest,
    ) -> Option<PreparedDeploymentSwitch> {
        let mut latest = self.inner.latest_prepared.lock().await;
        if latest.as_ref().is_some_and(|prepared| {
            prepared.deployment_id == deployment_id && &prepared.digest == digest
        }) {
            latest.take()
        } else {
            None
        }
    }

    async fn store_latest_prepared(&self, prepared: PreparedDeploymentSwitch) {
        let mut latest = self.inner.latest_prepared.lock().await;
        *latest = Some(prepared);
    }

    async fn spawn_critical_hot_switch(
        &self,
        prepared: PreparedDeploymentSwitch,
        switch_permit: SwitchPermit,
    ) -> Result<SwitchOutcome, SwitchError> {
        let (tx, rx) = oneshot::channel();
        let db_pool = self.inner.db_pool.clone();
        let deployment_ctx = self.inner.deployment_ctx.clone();
        let webhook_registry = self.inner.webhook_registry.clone();
        let cancel_registry = self.inner.cancel_registry.clone();
        let log_forwarder_sender = self.inner.log_forwarder_sender.clone();
        // Own the whole commit in a detached task: a dropped caller only stops observing the
        // result via `rx`, it cannot abort the switch. This covers the durable pre-critical
        // commits (cron seeds + activate) as well as the non-cancel-safe critical swap, so a
        // cancellation can never leave the DB activated while the old workers keep running.
        // The permit is held until the task finishes, which is what `close()` drains on.
        tokio::spawn(async move {
            let _switch_permit = switch_permit;
            let deployment_id = prepared.deployment_id;
            let result = async {
                // Pre-critical, fallible commit work, before the critical swap so the section
                // after `exec_task_handles` is taken cannot fail and strand the context.
                // `activate_deployment` is the durable source of truth for which deployment is
                // active, so commit it first: startup reconciles cron seeds (and workers) from
                // the active deployment, so a crash after activate self-heals on restart, while
                // a crash before it leaves no orphan seeds.
                let db_conn = db_pool
                    .external_api_conn()
                    .await
                    .map_err(|e| SwitchError::Other(e.into()))?;
                db_conn
                    .activate_deployment(deployment_id, chrono::Utc::now())
                    .await
                    .map_err(|e| SwitchError::Other(e.into()))?;
                prepared
                    .compiled_linked
                    .create_missing_cron_seeds(&db_pool, deployment_id)
                    .await?;
                switch_hot_redeploy(
                    prepared.compiled_linked,
                    deployment_id,
                    db_pool,
                    deployment_ctx,
                    webhook_registry,
                    cancel_registry,
                    log_forwarder_sender,
                )
                .await
            }
            .await;
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| {
            SwitchError::Other(anyhow::anyhow!("deployment switch critical task exited"))
        })?
    }

    pub(crate) async fn close(&self) {
        // Drain both lanes: an in-flight operation holds its lane's permit(s) until it
        // completes (the hot switch holds the switch permit through its critical task), so
        // acquiring waits for completion. The switch lane has exactly one permit; the
        // submit lane has `submit_concurrency`, so drain all of them. Then close the gates
        // so no new work is accepted; once closed, `try_acquire_*` returns the shutdown
        // error atomically (no separate flag).
        let _switch_permit = self.inner.switch_gate.clone().acquire_owned().await;
        let _submit_permits = self
            .inner
            .submit_lane
            .clone()
            .acquire_many_owned(self.inner.submit_concurrency)
            .await;
        self.inner.switch_gate.close();
        self.inner.submit_lane.close();
    }
}

const EPOCH_MILLIS: u64 = 10;
/// Number of cancelling executions the cancellation driver advances per tick.
const CANCELLATION_DRIVER_BATCH_SIZE: u32 = 100;
/// Default number of concurrent deployment submits the switch manager accepts.
const DEFAULT_SUBMIT_CONCURRENCY: u32 = 1;
const WEBUI_LOCATION: &str = include_str!("../../assets/webui-version.txt");
#[cfg(not(feature = "activity-js-local"))]
pub(crate) const ACTIVITY_JS_LOCATION: &str =
    include_str!("../../assets/activity-js-runtime-version.txt");
#[cfg(not(feature = "workflow-js-local"))]
pub(crate) const WORKFLOW_JS_LOCATION: &str =
    include_str!("../../assets/workflow-js-runtime-version.txt");
#[cfg(not(feature = "webhook-js-local"))]
pub(crate) const WEBHOOK_JS_LOCATION: &str =
    include_str!("../../assets/webhook-js-runtime-version.txt");

const HTTP_SERVER_NAME_WEBUI: &str = "webui";
const HTTP_SERVER_NAME_EXTERNAL: &str = "external";

impl Server {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            Server::Run {
                clean_sqlite_directory,
                clean_cache,
                clean_codegen_cache,
                server_config,
                deployment,
                empty: deployment_empty,
                description,
                suppress_type_checking_errors,
            } => {
                Box::pin(run(
                    project_dirs(),
                    BaseDirs::new(),
                    server_config,
                    deployment,
                    deployment_empty,
                    description,
                    RunParams {
                        dir_params: PrepareDirsParams {
                            clean_cache,
                            clean_codegen_cache,
                        },
                        clean_sqlite_directory,
                        suppress_type_checking_errors,
                    },
                ))
                .await
            }
            Server::Verify {
                clean_cache,
                clean_codegen_cache,
                server_config,
                deployment,
                ignore_missing_env_vars,
                suppress_type_checking_errors,
                skip_db,
            } => {
                verify(
                    project_dirs(),
                    BaseDirs::new(),
                    server_config,
                    deployment,
                    VerifyParams {
                        dir_params: PrepareDirsParams {
                            clean_cache,
                            clean_codegen_cache,
                        },
                        ignore_missing_env_vars,
                        suppress_type_checking_errors,
                        suppress_linking_errors: false,
                    },
                    skip_db,
                )
                .await
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SubmitError {
    #[error("must be a top-level execution id")]
    ExecutionIdMustBeTopLevel,
    #[error("function not found")]
    FunctionNotFound,
    #[error("{0}")]
    ParamsInvalid(String),
    #[error("execution already exists with the same id and different parameters")]
    Conflict,
    #[error(transparent)]
    DbErrorWrite(DbErrorWrite),
}

pub(crate) enum SubmitOutcome {
    Created,
    ExistsWithSameParameters,
}

pub(crate) async fn submit(
    deployment_id: DeploymentId,
    db_connection: &dyn DbExternalApi,
    execution_id: ExecutionId,
    ffqn: FunctionFqn,
    mut params: Vec<serde_json::Value>,
    paused: bool,
    component_registry_ro: &ComponentConfigRegistryRO,
) -> Result<SubmitOutcome, SubmitError> {
    let span = Span::current();
    span.record("execution_id", tracing::field::display(&execution_id));
    if !execution_id.is_top_level() {
        return Err(SubmitError::ExecutionIdMustBeTopLevel);
    }
    // Check that ffqn exists
    let Some((component_id, fn_metadata)) =
        component_registry_ro.find_by_exported_ffqn_submittable(&ffqn)
    else {
        return Err(SubmitError::FunctionNotFound);
    };
    span.record("component_id", tracing::field::display(component_id));

    // Extract `scheduled_at`
    let created_at = Now.now();
    let (scheduled_at, params, fn_metadata) = if fn_metadata.extension
        == Some(FunctionExtension::Schedule)
    {
        // First parameter must be `schedule-at`
        let Some(schedule_at) = params.drain(0..1).next() else {
            return Err(SubmitError::ParamsInvalid(
                "`params` must be an array with first value of type `schedule-at`".to_string(),
            ));
        };
        let schedule_at_type_wrapper = fn_metadata
            .parameter_types
            .iter()
            .map(|ParameterType { type_wrapper, .. }| type_wrapper.clone())
            .next()
            .expect("checked that `fn_metadata` is FunctionExtension::Schedule");
        let wast_val_with_type = json!({
            "type": schedule_at_type_wrapper,
            "value": schedule_at,
        });
        let wast_val_with_type: WastValWithType = serde_json::from_value(wast_val_with_type)
            .map_err(|serde_err| {
                SubmitError::ParamsInvalid(format!(
                    "`params` must be an array with first value of type `schedule-at` - {serde_err}"
                ))
            })?;
        let schedule_at = history_event_schedule_at_from_wast_val(&wast_val_with_type.value)
            .map_err(|serde_err| {
                SubmitError::ParamsInvalid(format!("cannot convert `schedule-at` - {serde_err}"))
            })?;
        // Find the target fn_metadata of the scheduled fn. No need to change the `component_id` as they belong to the same component.
        let ffqn = FunctionFqn {
            ifc_fqn: IfcFqnName::from_parts(
                fn_metadata.ffqn.ifc_fqn.namespace(),
                fn_metadata
                    .ffqn
                    .ifc_fqn
                    .package_strip_obelisk_schedule_suffix()
                    .expect("checked that the ifc is ext"),
                fn_metadata.ffqn.ifc_fqn.ifc_name(),
                fn_metadata.ffqn.ifc_fqn.version(),
            ),
            function_name: FnName::from(
                fn_metadata
                    .ffqn
                    .function_name
                    .to_string()
                    .strip_suffix(SUFFIX_FN_SCHEDULE)
                    .expect("checked that the function is FunctionExtension::Schedule")
                    .to_string(),
            ),
        };
        let (_component_id, fn_metadata) = component_registry_ro
            .find_by_exported_ffqn_submittable(&ffqn)
            .expect("-schedule must have the original counterpart in the component registry");

        (
            schedule_at.as_date_time(created_at).map_err(|err| {
                SubmitError::ParamsInvalid(format!("schedule-at conversion error - {err}"))
            })?,
            params,
            fn_metadata,
        )
    } else {
        assert!(fn_metadata.extension.is_none());
        (created_at, params, fn_metadata)
    };

    let ffqn = &fn_metadata.ffqn;
    span.record("ffqn", tracing::field::display(ffqn));

    let params = Params::from_json_values(
        Arc::from(params),
        fn_metadata
            .parameter_types
            .iter()
            .map(|ParameterType { type_wrapper, .. }| type_wrapper),
    )
    .map_err(|err| SubmitError::ParamsInvalid(format!("argument `params` invalid - {err}")))?;

    // Associate the (root) request execution with the request span. Makes possible to find the trace by execution id.
    let metadata = concepts::ExecutionMetadata::from_parent_span(&span);
    let res = db_connection
        .create(CreateRequest {
            created_at,
            execution_id: execution_id.clone(),
            metadata,
            ffqn: ffqn.clone(),
            params: params.clone(),
            parent: None,
            scheduled_at,
            component_id: component_id.clone(),
            deployment_id,
            scheduled_by: None,
            paused,
        })
        .await;
    match res {
        Ok(_) => Ok(SubmitOutcome::Created),
        Err(DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict)) => {
            let create_req = db_connection
                .get_create_request(&execution_id)
                .await
                .map_err(|err| SubmitError::DbErrorWrite(err.into()))?;
            if create_req.params == params {
                Ok(SubmitOutcome::ExistsWithSameParameters)
            } else {
                Err(SubmitError::Conflict)
            }
        }
        Err(err) => Err(SubmitError::DbErrorWrite(err)),
    }
}

pub(crate) struct RunParams {
    pub(crate) dir_params: PrepareDirsParams,
    pub(crate) clean_sqlite_directory: bool,
    pub(crate) suppress_type_checking_errors: bool,
}

pub(crate) async fn run(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    server_config: Option<PathBuf>,
    deployment: Option<PathBuf>,
    deployment_empty: bool,
    description: Option<String>,
    params: RunParams,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, server_config)?;
    let config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&config)?;
    let deployment = if let Some(deployment_path) = deployment {
        Some(LocalDeployment::from_path(&deployment_path).await?)
    } else if deployment_empty {
        Some(LocalDeployment::empty())
    } else {
        None
    };
    if description.is_some() && deployment.is_none() {
        anyhow::bail!("--description requires --deployment or --empty");
    }

    let (termination_sender, termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });

    let prepared_dirs =
        prepare_dirs(&config, &params.dir_params, &config_holder.path_prefixes).await?;
    Box::pin(run_internal(
        config,
        deployment,
        description,
        config_holder.path_prefixes,
        params,
        prepared_dirs,
        termination_watcher,
    ))
    .await?;
    Ok(())
}

#[derive(Debug, Default, Clone)]
pub(crate) struct VerifyParams {
    pub(crate) dir_params: PrepareDirsParams,
    pub(crate) ignore_missing_env_vars: bool,
    pub(crate) suppress_type_checking_errors: bool,
    pub(crate) suppress_linking_errors: bool,
}

pub(crate) async fn verify(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    server_config: Option<PathBuf>,
    deployment: Option<PathBuf>,
    verify_params: VerifyParams,
    skip_db: bool,
) -> Result<(), anyhow::Error> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, server_config)?;
    let config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&config)?;
    let deployment_opt = if let Some(deployment_path) = deployment {
        Some(load_deployment_canonical(&deployment_path).await?)
    } else {
        None
    };

    let (termination_sender, mut termination_watcher) = watch::channel(());
    let prepared_dirs = prepare_dirs(
        &config,
        &verify_params.dir_params,
        &config_holder.path_prefixes,
    )
    .await?;
    let engines = create_engines(&config, &prepared_dirs)?;
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    let mut db_pool = if !skip_db {
        verify_db_schema(&config.database, &config_holder.path_prefixes).await?
    } else {
        None
    };
    let (deployment, deployment_id) = if let Some(deployment) = deployment_opt {
        (deployment, DeploymentId::generate())
    } else {
        get_deployment_canonical_from_db(
            &config.database,
            &config_holder.path_prefixes,
            &mut db_pool,
        )
        .await?
    };
    let server_verified = Box::pin(server_verify(config, engines)).await?;
    let cas: Option<Arc<dyn concepts::cas::Cas>> = if let Some((pool, _)) = db_pool.as_ref() {
        Some(pool.cas_conn().await?.into())
    } else {
        None
    };
    deployment_verify_config_compile_link(
        server_verified,
        &prepared_dirs,
        deployment,
        cas,
        deployment_id,
        verify_params,
        &mut termination_watcher,
    )
    .await?;
    if let Some((_, db_close)) = db_pool {
        db_close.await;
    }
    Ok(())
}

fn ignore_not_found(err: std::io::Error) -> Result<(), std::io::Error> {
    if err.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(err)
    }
}

type DbPoolCloseableContainer = Option<(Arc<dyn DbPool>, Pin<Box<dyn Future<Output = ()> + Send>>)>;

async fn verify_db_schema(
    db_config_toml: &DatabaseConfigToml,
    path_prefixes: &PathPrefixes,
) -> Result<DbPoolCloseableContainer, anyhow::Error> {
    let result: DbPoolCloseableContainer = match db_config_toml {
        DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
            let db_dir = sqlite_config_toml.get_sqlite_dir(path_prefixes).await?;
            let sqlite_config = sqlite_config_toml.as_sqlite_config();
            let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
            if sqlite_file.exists() {
                let db_pool = Arc::new(
                    SqlitePool::new(&sqlite_file, sqlite_config)
                        .await
                        .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?,
                );
                info!("SQLite database schema verified");
                let db_close = Box::pin({
                    let db_pool = db_pool.clone();
                    async move { db_pool.close().await }
                });
                Some((db_pool, db_close))
            } else {
                info!("SQLite database does not exist yet, skipping schema verification");
                None
            }
        }
        DatabaseConfigToml::Postgres(postgres_config_toml) => {
            let db_pool = Arc::new(
                PostgresPool::new(
                    postgres_config_toml.as_config()?,
                    postgres_config_toml.as_provision_policy(),
                )
                .await
                .context("cannot initialize postgres connection pool")?,
            );
            info!("PostgreSQL database schema verified");
            let db_close = Box::pin({
                let db_pool = db_pool.clone();
                async move { db_pool.close().await }
            });
            Some((db_pool, db_close))
        }
    };
    Ok(result)
}

#[derive(Debug, Default, Clone)]
pub(crate) struct PrepareDirsParams {
    pub(crate) clean_cache: bool,
    pub(crate) clean_codegen_cache: bool,
}

pub(crate) async fn prepare_dirs(
    config: &ServerConfigToml,
    params: &PrepareDirsParams,
    path_prefixes: &PathPrefixes,
) -> Result<PreparedDirs, anyhow::Error> {
    let wasm_cache_dir = config
        .wasm_global_config
        .get_wasm_cache_directory(path_prefixes)
        .await?;
    let codegen_cache_dir = config
        .wasm_global_config
        .codegen_cache
        .get_directory(path_prefixes)
        .await?;
    debug!("Using codegen cache? {codegen_cache_dir:?}");
    if params.clean_cache {
        tokio::fs::remove_dir_all(&wasm_cache_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete wasm cache directory {wasm_cache_dir:?}"))?;
    }
    if (params.clean_cache || params.clean_codegen_cache)
        && let Some(codegen_cache) = &codegen_cache_dir
    {
        // delete codegen_cache
        tokio::fs::remove_dir_all(codegen_cache)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete codegen cache directory {codegen_cache:?}"))?;
        tokio::fs::create_dir_all(codegen_cache)
            .await
            .with_context(|| format!("cannot create codegen cache directory {codegen_cache:?}"))?;
    }
    tokio::fs::create_dir_all(&wasm_cache_dir)
        .await
        .with_context(|| format!("cannot create wasm cache directory {wasm_cache_dir:?}"))?;
    let metadata_dir = wasm_cache_metadata_dir(&wasm_cache_dir);
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .with_context(|| format!("cannot create wasm metadata directory {metadata_dir:?}"))?;
    Ok(PreparedDirs {
        codegen_cache_dir: codegen_cache_dir.map(Arc::from),
        wasm_cache_dir: Arc::from(wasm_cache_dir),
        metadata_dir: Arc::from(metadata_dir),
    })
}

#[derive(Debug, Clone)]
#[expect(clippy::struct_field_names)]
pub(crate) struct PreparedDirs {
    codegen_cache_dir: Option<Arc<Path>>,
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
}

/// Verifies configuration without database schema check.
#[instrument(skip_all)]
pub(crate) async fn server_verify(
    config: ServerConfigToml,
    engines: Engines,
) -> Result<ServerVerified, anyhow::Error> {
    info!("Verifying server configuration");
    // Check obelisk-version compatibility if specified
    if let Some(version_req_str) = &config.obelisk_version {
        let version_req = semver::VersionReq::parse(version_req_str)
            .with_context(|| format!("Invalid obelisk-version requirement: {version_req_str}"))?;
        let current_version = semver::Version::parse(PKG_VERSION)
            .with_context(|| format!("Invalid current version: {PKG_VERSION}"))?;
        if !version_req.matches(&current_version) {
            bail!(
                "Obelisk version mismatch: config requires {version_req_str}, but running version is {PKG_VERSION}",
            );
        }
        info!("Obelisk version {PKG_VERSION} matches requirement {version_req_str}",);
    }
    // Verify server
    Box::pin(ServerVerified::new(engines, config)).await
}

/// Verifies configuration without database schema check.
#[instrument(skip_all, fields(%deployment_id))]
pub(crate) async fn deployment_verify_config_compile_link(
    server_verified: ServerVerified,
    prepared_dirs: &PreparedDirs,
    deployment: DeploymentResolved,
    cas: Option<Arc<dyn concepts::cas::Cas>>,
    deployment_id: DeploymentId,
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<ServerCompiledLinked, anyhow::Error> {
    info!("Verifying deployment configuration, compiling WASM components");
    let deployment_verified = deployment_verify_config(
        &server_verified,
        prepared_dirs,
        deployment,
        cas,
        params.clone(),
        termination_watcher,
    )
    .await?;
    deployment_compile_link(
        server_verified,
        deployment_verified,
        deployment_id,
        params,
        termination_watcher,
    )
    .await
}

#[instrument(skip_all, fields(%deployment_id))]
pub(crate) async fn deployment_compile_link(
    server_verified: ServerVerified,
    deployment_verified: DeploymentVerified,
    deployment_id: DeploymentId,
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<ServerCompiledLinked, anyhow::Error> {
    let compiled_and_linked = ServerCompiledLinked::new(
        deployment_id,
        deployment_verified,
        server_verified.launch,
        termination_watcher,
        params.suppress_type_checking_errors,
        params.suppress_linking_errors,
    )
    .await?;
    if compiled_and_linked.supressed_errors.is_none() {
        info!("Obelisk configuration was verified");
    } else {
        warn!("Obelisk configuration was verified with supressed errors");
    }
    Ok(compiled_and_linked)
}

#[instrument(skip_all)]
pub(crate) async fn deployment_verify_config(
    server_verified: &ServerVerified,
    prepared_dirs: &PreparedDirs,
    deployment: DeploymentResolved,
    cas: Option<Arc<dyn concepts::cas::Cas>>,
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<DeploymentVerified, anyhow::Error> {
    // Materialize deployment-owned WASM blobs from the CAS onto disk before compiling.
    let deployment =
        DeploymentRunnable::resolve(deployment, cas.as_deref(), &prepared_dirs.wasm_cache_dir)
            .await?;
    let deployment_verified = Box::pin(DeploymentVerified::fetch_and_verify_all(
        deployment,
        server_verified.http_servers.clone(),
        prepared_dirs.wasm_cache_dir.clone(),
        prepared_dirs.metadata_dir.clone(),
        params.ignore_missing_env_vars,
        server_verified.global_backtrace_persist,
        server_verified.global_executor_instance_limiter.clone(),
        server_verified.fuel,
        termination_watcher,
        server_verified.database_subscription_interruption,
        server_verified.api_addr_if_webui_enabled.clone(),
    ))
    .await?;
    trace!("Verified deployment: {deployment_verified:#?}");
    Ok(deployment_verified)
}

/// Look up the current deployment from the database.
/// Prefers Enqueued over Active; errors if neither exists.
async fn get_deployment_canonical_from_db(
    database: &DatabaseConfigToml,
    path_prefixes: &PathPrefixes,
    db_pool_container: &mut DbPoolCloseableContainer,
) -> anyhow::Result<(DeploymentResolved, DeploymentId)> {
    let conn = if let Some((pool, _)) = db_pool_container.as_ref() {
        pool.external_api_conn()
            .await
            .context("cannot get db connection for deployment lookup")?
    } else {
        match database {
            DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
                let db_dir = sqlite_config_toml.get_sqlite_dir(path_prefixes).await?;
                let sqlite_config = sqlite_config_toml.as_sqlite_config();
                let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
                let db_pool = Arc::new(
                    SqlitePool::new(&sqlite_file, sqlite_config)
                        .await
                        .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?,
                );
                let db_close = Box::pin({
                    let db_pool = db_pool.clone();
                    async move { db_pool.close().await }
                });
                // update the container
                let (db_pool, _) = db_pool_container.insert((db_pool.clone(), db_close));
                db_pool
                    .external_api_conn()
                    .await
                    .context("cannot get db connection for deployment lookup")?
            }
            DatabaseConfigToml::Postgres(postgres_config_toml) => {
                let db_pool = Arc::new(
                    PostgresPool::new(
                        postgres_config_toml.as_config()?,
                        postgres_config_toml.as_provision_policy(),
                    )
                    .await
                    .context("cannot initialize postgres connection pool")?,
                );
                let db_close = Box::pin({
                    let db_pool = db_pool.clone();
                    async move { db_pool.close().await }
                });
                // update the container
                let (db_pool, _) = db_pool_container.insert((db_pool.clone(), db_close));
                db_pool
                    .external_api_conn()
                    .await
                    .context("cannot get db connection for deployment lookup")?
            }
        }
    };

    let record = conn
        .get_current_deployment()
        .await
        .context("cannot query current deployment")?;

    let record = record
        .context("no Enqueued or Active deployment found in database; provide --deployment")?;

    let pool = db_pool_container
        .as_ref()
        .map(|(pool, _)| pool.clone())
        .expect("db pool was set above");
    let deployment = deployment_resolved_from_manifest(pool.as_ref(), &record.deployment_toml)
        .await
        .with_context(|| {
            format!(
                "cannot canonicalize deployment manifest for {:?}",
                record.deployment_id
            )
        })?;
    Ok((deployment, record.deployment_id))
}

/// A deployment authored locally and passed via `server run -d <toml>` / `--empty`.
///
/// Carries the verbatim manifest (stored as the source of truth), its referenced file
/// blobs (uploaded to the CAS so later restarts can canonicalize from it), and the
/// already-canonicalized form used to compile/link this run.
pub(crate) struct LocalDeployment {
    deployment_toml: String,
    canonical: DeploymentResolved,
    files: Vec<DeploymentManifestFile>,
}

impl LocalDeployment {
    pub(crate) async fn from_path(deployment_path: &Path) -> anyhow::Result<Self> {
        let canonical = load_deployment_canonical(deployment_path).await?;
        let prepared =
            crate::config::manifest::prepare_deployment_manifest_from_disk(deployment_path).await?;
        Ok(Self {
            deployment_toml: prepared.deployment_toml,
            canonical,
            files: prepared.files,
        })
    }

    pub(crate) fn empty() -> Self {
        Self {
            deployment_toml: String::new(),
            canonical: DeploymentResolved::default(),
            files: Vec::new(),
        }
    }
}

/// Root for canonicalizing a stored manifest against the CAS.
///
/// Empty on purpose: a stored manifest has no submitter host to anchor relative paths to.
/// Deployment-owned references are addressed by content digest in the CAS, so joining a
/// `${DEPLOYMENT_DIR}` / relative path against an empty root leaves it relative. That keeps
/// deployment-owned WASM locations relative in the resulting `DeploymentResolved` (no
/// fabricated host paths); they become concrete on-disk paths only later, in
/// [`DeploymentRunnable::resolve`], which materializes their blobs from the CAS.
fn cas_deployment_dir() -> std::path::PathBuf {
    std::path::PathBuf::new()
}

/// A [`FileProvider`] that reads from the CAS and records the `(path, digest)` of every blob
/// it serves, so the caller learns exactly which files the manifest references.
struct RecordingCasProvider {
    inner: crate::config::file_provider::CasFileProvider,
    seen: std::sync::Mutex<Vec<concepts::storage::DeploymentFileRecord>>,
}

#[async_trait::async_trait]
impl crate::config::file_provider::FileProvider for RecordingCasProvider {
    async fn read(&self, path: &str, digest: Option<&ContentDigest>) -> anyhow::Result<Vec<u8>> {
        let bytes = self.inner.read(path, digest).await?;
        if let Some(digest) = digest {
            self.seen
                .lock()
                .expect("RecordingCasProvider mutex poisoned")
                .push(concepts::storage::DeploymentFileRecord {
                    path: path.to_string(),
                    digest: digest.clone(),
                });
        }
        Ok(bytes)
    }
}

/// Resolve a stored verbatim TOML manifest by reading its referenced blobs from the CAS,
/// returning the canonical form and the `(path, digest)` of every file it referenced.
///
/// `${...}` env vars and secrets resolve here, in the server's environment.
async fn deployment_resolved_and_files_from_manifest(
    db_pool: &dyn concepts::storage::DbPool,
    deployment_toml: &str,
) -> anyhow::Result<(
    DeploymentResolved,
    Vec<concepts::storage::DeploymentFileRecord>,
)> {
    let cas: Arc<dyn concepts::cas::Cas> = db_pool
        .cas_conn()
        .await
        .context("cannot get CAS connection for deployment canonicalization")?
        .into();
    let provider = RecordingCasProvider {
        inner: CasFileProvider { cas },
        seen: std::sync::Mutex::new(Vec::new()),
    };
    let resolved = crate::config::manifest::manifest_to_resolved(
        deployment_toml,
        &cas_deployment_dir(),
        &provider,
    )
    .await
    .context("cannot resolve deployment manifest from the content-addressed store")?;
    let files = provider
        .seen
        .into_inner()
        .expect("RecordingCasProvider mutex poisoned");
    Ok((resolved, files))
}

/// Resolve a stored verbatim manifest by reading its referenced blobs from the CAS.
async fn deployment_resolved_from_manifest(
    db_pool: &dyn concepts::storage::DbPool,
    deployment_toml: &str,
) -> anyhow::Result<DeploymentResolved> {
    Ok(
        deployment_resolved_and_files_from_manifest(db_pool, deployment_toml)
            .await?
            .0,
    )
}

/// A [`DeploymentResolved`] whose every WASM component location is a concrete, runnable
/// reference: an absolute on-disk path or an OCI image.
///
/// `DeploymentResolved` is host-independent: a deployment-owned WASM is a *relative* path
/// addressed by content digest in the CAS (a stored manifest carries no submitter host).
/// Compiling/linking needs real files, so [`Self::resolve`] materializes each deployment-owned
/// blob from the CAS into the wasm cache and rewrites its location to that path. OCI
/// references already are concrete and pass through unchanged. The current disk-authored
/// local-run canonical expands valid relative WASM paths to absolute internal paths, so it
/// resolves to itself.
///
/// The inner value is private so the only way to obtain one is `resolve`; that makes it
/// impossible to feed an unresolved (relative-path) canonical into compilation.
pub(crate) struct DeploymentRunnable {
    deployment: DeploymentResolved,
}

impl DeploymentRunnable {
    /// Resolve every deployment-owned WASM location against the CAS.
    ///
    /// `cas` may be `None` for disk/offline flows (e.g. `obelisk server verify <toml>` with
    /// `--skip-db`), whose current canonical holds internal absolute paths and OCI refs;
    /// encountering a deployment-owned (relative) WASM there is an error because there is no
    /// store to read it.
    pub(crate) async fn resolve(
        mut deployment: DeploymentResolved,
        cas: Option<&dyn concepts::cas::Cas>,
        wasm_cache_dir: &Path,
    ) -> anyhow::Result<Self> {
        for c in &mut deployment.activities_wasm {
            materialize_wasm_location(
                &mut c.common.location,
                c.content_digest.as_ref(),
                cas,
                wasm_cache_dir,
            )
            .await?;
        }
        for c in &mut deployment.activities_stub {
            if let ActivityStubComponentConfigResolved::File(f) = c {
                materialize_wasm_location(
                    &mut f.common.location,
                    f.content_digest.as_ref(),
                    cas,
                    wasm_cache_dir,
                )
                .await?;
            }
        }
        for c in &mut deployment.activities_external {
            if let ActivityExternalComponentConfigResolved::File(f) = c {
                materialize_wasm_location(
                    &mut f.common.location,
                    f.content_digest.as_ref(),
                    cas,
                    wasm_cache_dir,
                )
                .await?;
            }
        }
        for c in &mut deployment.workflows_wasm {
            materialize_wasm_location(
                &mut c.common.location,
                c.content_digest.as_ref(),
                cas,
                wasm_cache_dir,
            )
            .await?;
        }
        for c in &mut deployment.webhooks_wasm {
            materialize_wasm_location(
                &mut c.common.location,
                c.content_digest.as_ref(),
                cas,
                wasm_cache_dir,
            )
            .await?;
        }
        Ok(Self { deployment })
    }

    fn into_canonical(self) -> DeploymentResolved {
        self.deployment
    }
}

/// Turn a single WASM component location into a concrete on-disk path.
///
/// - `Oci` and already-materialized absolute `Path`s are concrete and left untouched.
/// - A relative `Path` is deployment-owned: its bytes live in the CAS under `content_digest`.
///   Materialize them into `wasm_cache_dir` (keyed by digest, mirroring the OCI pull cache)
///   and rewrite the location to that absolute path.
async fn materialize_wasm_location(
    location: &mut ComponentLocationToml,
    content_digest: Option<&ContentDigest>,
    cas: Option<&dyn concepts::cas::Cas>,
    wasm_cache_dir: &Path,
) -> anyhow::Result<()> {
    let ComponentLocationToml::Path(path) = location else {
        return Ok(()); // OCI reference: pulled later, during fetch.
    };
    if Path::new(path).is_absolute() {
        return Ok(()); // External / disk-authored file, read from disk at runtime.
    }
    let digest = content_digest.with_context(|| {
        format!("deployment-owned WASM component `{path}` is missing a content digest")
    })?;
    let cas = cas.with_context(|| {
        format!("cannot resolve deployment-owned WASM component `{path}` without a content-addressed store")
    })?;
    let target = content_digest_to_wasm_file(wasm_cache_dir, digest);
    if !target.exists() {
        let bytes = cas.read_blob(digest).await?.with_context(|| {
            format!("blob {digest} for WASM component `{path}` not present in the CAS")
        })?;
        tokio::fs::write(&target, &bytes)
            .await
            .with_context(|| format!("cannot write WASM blob to cache file {target:?}"))?;
    }
    *location = ComponentLocationToml::Path(target.to_string_lossy().into_owned());
    Ok(())
}

/// Upload a new deployment's file blobs to the CAS and build its Inactive [`DeploymentRecord`]
/// without inserting it. Deferring the insert until after a successful
/// compile with [`DbExternalApi::insert_deployment_with_components`]
/// means startup, like submit, never persists a deployment that failed verification.
async fn prepare_new_deployment_record(
    db_pool: &dyn concepts::storage::DbPool,
    deployment_id: DeploymentId,
    deployment_toml: String,
    files: Vec<DeploymentManifestFile>,
    description: Option<String>,
) -> anyhow::Result<DeploymentRecord> {
    // Upload referenced blobs to the CAS first, so a later restart can canonicalize from it.
    let cas = db_pool
        .cas_conn()
        .await
        .context("cannot get CAS connection for deployment file upload")?;
    let mut file_records = Vec::with_capacity(files.len());
    for file in files {
        cas.write_blob(&file.bytes)
            .await
            .with_context(|| format!("cannot upload deployment file `{}`", file.path))?;
        file_records.push(DeploymentFileRecord {
            path: file.path,
            digest: file.digest,
        });
    }

    let now = Utc::now();
    let digest = DeploymentRecord::compute_digest(&deployment_toml);
    Ok(DeploymentRecord {
        deployment_id,
        description,
        digest,
        created_at: now,
        last_active_at: None,
        status: DeploymentStatus::Inactive,
        deployment_toml,
        obelisk_version: PKG_VERSION.to_string(),
        created_by: Some("server".to_string()),
        files: file_records,
    })
}

type DbClose = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(crate) fn create_engines(
    config: &ServerConfigToml,
    prepared_dirs: &PreparedDirs,
) -> Result<Engines, anyhow::Error> {
    let fuel: Option<u64> = config.wasm_global_config.fuel.into();
    let consume_fuel = fuel.is_some();
    let engine_config = EngineConfig {
        codegen_cache_dir: prepared_dirs.codegen_cache_dir.clone(),
        consume_fuel,
        parallel_compilation: config.wasm_global_config.parallel_compilation,
        debug: config.wasm_global_config.debug,
        pooling_config: match config.wasm_global_config.allocator_config {
            WasmtimeAllocatorConfig::OnDemand => PoolingConfig::OnDemand,
            WasmtimeAllocatorConfig::Pooling => {
                PoolingConfig::Pooling(config.wasm_global_config.wasmtime_pooling_config.into())
            }
            WasmtimeAllocatorConfig::Auto => PoolingConfig::PoolingWithFallback(
                config.wasm_global_config.wasmtime_pooling_config.into(),
            ),
        },
    };
    Ok(Engines::new(engine_config)?)
}

#[instrument(skip_all, name = "init", fields(deployment_id))]
pub(crate) async fn run_internal(
    config: ServerConfigToml,
    deployment: Option<LocalDeployment>,
    description: Option<String>,
    path_prefixes: PathPrefixes,
    params: RunParams,
    prepared_dirs: PreparedDirs,
    mut termination_watcher: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let api_listening_addr = config.api.enabled.then_some(config.api.listening_addr);

    let global_webhook_instance_limiter = config
        .wasm_global_config
        .global_webhook_instance_limiter
        .as_semaphore();
    let timers_watcher = config.timers_watcher;
    let cancel_watcher = config.cancel_watcher;
    let database = config.database.clone();

    // Open the database pool before compilation so that in the no-deployment case
    // we can read the active/enqueued deployment from it.
    let (db_pool, db_close): (Arc<dyn DbPool>, DbClose) = match &database {
        DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
            let db_dir = sqlite_config_toml.get_sqlite_dir(&path_prefixes).await?;
            let sqlite_config = sqlite_config_toml.as_sqlite_config();
            let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
            if params.clean_sqlite_directory {
                warn!("Deleting sqlite directory {db_dir:?}");
                tokio::fs::remove_dir_all(&db_dir)
                    .await
                    .or_else(ignore_not_found)
                    .with_context(|| format!("cannot delete database directory `{db_dir:?}`"))?;
                tokio::fs::create_dir_all(&db_dir)
                    .await
                    .with_context(|| format!("cannot create database directory {db_dir:?}"))?;
            }
            let db_pool = Arc::new(
                SqlitePool::new(&sqlite_file, sqlite_config)
                    .await
                    .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?,
            );
            let db_close = Box::pin({
                let db_pool = db_pool.clone();
                async move { db_pool.close().await }
            });
            (db_pool, db_close)
        }
        DatabaseConfigToml::Postgres(postgres_config_toml) => {
            let db_pool = Arc::new(
                PostgresPool::new(
                    postgres_config_toml.as_config()?,
                    postgres_config_toml.as_provision_policy(),
                )
                .await
                .context("canont initialize postgres connection pool")?,
            );
            let db_close = Box::pin({
                let db_pool = db_pool.clone();
                async move { db_pool.close().await }
            });
            (db_pool, db_close)
        }
    };
    let span = Span::current();
    let (active_deployment_id, deployment_canonical, new_deployment_record) =
        if let Some(deployment) = deployment {
            // --deployment or `--deployment-empty` provided: prepare, insert+activate after compile.
            let new_deployment_id = DeploymentId::generate();
            span.record("deployment_id", tracing::field::display(&new_deployment_id));
            let record = prepare_new_deployment_record(
                &*db_pool,
                new_deployment_id,
                deployment.deployment_toml,
                deployment.files,
                description,
            )
            .await?;
            (new_deployment_id, deployment.canonical, Some(record))
        } else {
            // No --deployment: pick up from the DB.
            // Activate any Enqueued deployment (queued for this restart), then use active.
            let conn = db_pool
                .external_api_conn()
                .await
                .context("cannot get db connection for deployment lookup")?;
            if let Some(record) = conn
                .get_current_deployment()
                .await
                .context("cannot query current deployment")?
            {
                if record.status == concepts::storage::DeploymentStatus::Enqueued {
                    conn.activate_deployment(record.deployment_id, chrono::Utc::now())
                        .await
                        .context("cannot activate enqueued deployment")?;
                }
                let canonical =
                    deployment_resolved_from_manifest(&*db_pool, &record.deployment_toml).await?;
                span.record(
                    "deployment_id",
                    tracing::field::display(&record.deployment_id),
                );
                if record.status == concepts::storage::DeploymentStatus::Enqueued {
                    info!("Activated enqueued deployment");
                } else {
                    info!("Using the currently active deployment");
                }

                (record.deployment_id, canonical, None)
            } else {
                let new_deployment_id = DeploymentId::generate();
                span.record("deployment_id", tracing::field::display(&new_deployment_id));
                info!("No deployment found in DB; starting with empty deployment");
                let record = prepare_new_deployment_record(
                    &*db_pool,
                    new_deployment_id,
                    String::new(),
                    Vec::new(),
                    None,
                )
                .await?;

                (
                    new_deployment_id,
                    DeploymentResolved::default(),
                    Some(record),
                )
            }
        };
    let engines = create_engines(&config, &prepared_dirs)?;
    let server_verified = server_verify(config, engines).await?;
    let cas: Arc<dyn concepts::cas::Cas> = db_pool.cas_conn().await?.into();
    let compiled_and_linked = Box::pin(deployment_verify_config_compile_link(
        server_verified.clone(),
        &prepared_dirs,
        deployment_canonical,
        Some(cas),
        active_deployment_id,
        VerifyParams {
            dir_params: PrepareDirsParams {
                clean_cache: params.dir_params.clean_cache,
                clean_codegen_cache: params.dir_params.clean_codegen_cache,
            },
            ignore_missing_env_vars: false,
            suppress_type_checking_errors: params.suppress_type_checking_errors,
            suppress_linking_errors: false,
        },
        &mut termination_watcher,
    ))
    .instrument(span.clone())
    .await?;
    // Persist a freshly created deployment only now that it has compiled and verified, matching the submit path.
    if let Some(record) = new_deployment_record {
        let api_conn = db_pool
            .external_api_conn()
            .await
            .context("cannot get db connection for deployment insertion")?;
        let (component_metadata, deployment_components) = build_component_metadata_records(
            active_deployment_id,
            &compiled_and_linked.component_registry_ro,
        );
        api_conn
            .insert_deployment_with_components(record, component_metadata, deployment_components)
            .await
            .context("cannot insert deployment")?;
        api_conn
            .activate_deployment(active_deployment_id, chrono::Utc::now())
            .await
            .context("cannot activate deployment")?;
        info!("Activated new deployment");
    }

    let cancel_registry = CancelRegistry::new();
    let subscription_interruption = database.get_subscription_interruption();

    let server_init = spawn_tasks_and_threads(
        server_verified,
        active_deployment_id,
        db_pool,
        db_close,
        compiled_and_linked,
        global_webhook_instance_limiter,
        timers_watcher,
        cancel_watcher,
        &cancel_registry,
        &termination_watcher,
        prepared_dirs.clone(),
    )
    .instrument(span)
    .await?;
    let mut server_init = server_init;
    let deployment_switch_manager = DeploymentSwitchManagerHandle::new(
        server_init.server_verified.clone(),
        server_init.prepared_dirs.clone(),
        server_init.db_pool.clone(),
        termination_watcher.clone(),
        server_init.deployment_ctx.clone(),
        server_init.webhook_registry.clone(),
        cancel_registry.clone(),
        server_init.log_forwarder_sender.clone(),
        DEFAULT_SUBMIT_CONCURRENCY,
    );
    server_init.deployment_switch_manager = Some(deployment_switch_manager.clone());
    switch_deployment(
        deployment_switch_manager.clone(),
        active_deployment_id,
        SwitchDeploymentAction::Activate,
    )
    .instrument(info_span!("startup deployment manager no-op", %active_deployment_id))
    .await
    .map_err(|err| match err {
        SwitchError::Busy => anyhow::anyhow!("deployment switch manager busy during startup"),
        SwitchError::NotFound => {
            anyhow::anyhow!("active deployment {active_deployment_id} not found during startup")
        }
        SwitchError::Other(err) => err,
    })?;

    let grpc_server = Arc::new(GrpcServer::new(
        server_init.server_verified.clone(),
        server_init.db_pool.clone(),
        termination_watcher.clone(),
        cancel_registry.clone(),
        prepared_dirs.clone(),
        server_init.deployment_ctx.clone(),
        deployment_switch_manager.clone(),
    ));

    let mut grpc = RoutesBuilder::default();

    grpc.add_service(
        grpc_gen::function_repository_server::FunctionRepositoryServer::from_arc(
            grpc_server.clone(),
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip),
    )
    .add_service(
        grpc_gen::execution_repository_server::ExecutionRepositoryServer::from_arc(
            grpc_server.clone(),
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip),
    )
    .add_service(
        grpc_gen::deployment_repository_server::DeploymentRepositoryServer::from_arc(
            grpc_server.clone(),
        )
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip)
        // Submit requests inline deployment-owned blobs; raise the decode limit
        // well above tonic's 4 MiB default. `GetFile` responses are likewise large.
        .max_decoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(crate::MAX_GRPC_MESSAGE_SIZE),
    )
    .add_service(
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(grpc_gen::FILE_DESCRIPTOR_SET)
            .build_v1()?,
    );

    let trace_layer = TraceLayer::new_for_grpc()
        .make_span_with(make_span)
        .on_failure(tower_http::trace::DefaultOnFailure::new().level(tracing::Level::DEBUG));

    let grpc_service = ServiceBuilder::new()
        .layer(GrpcWebLayer::new())
        .layer(trace_layer)
        .map_request(accept_trace)
        .service(grpc.routes());

    let app_router = app_router(WebApiState {
        server_verified: server_init.server_verified.clone(),
        deployment_ctx: server_init.deployment_ctx.clone(),
        db_pool: server_init.db_pool.clone(),
        cancel_registry,
        termination_watcher: termination_watcher.clone(),
        subscription_interruption,
        prepared_dirs: server_init.prepared_dirs.clone(),
        deployment_switch_manager,
    });
    let app: axum::Router<()> = app_router.fallback_service(grpc_service);
    let app_svc = app.into_make_service();

    if let Some(api_listening_addr) = api_listening_addr {
        let listener = TcpListener::bind(api_listening_addr)
            .await
            .with_context(|| format!("cannot bind to {api_listening_addr}"))?;

        axum::serve(listener, app_svc)
            .with_graceful_shutdown(async move {
                info!("Serving HTTP, gRPC and gRPC-Web requests at {api_listening_addr}");
                info!("Obelisk is ready");
                let _: Result<_, _> = termination_watcher.changed().await;
                server_init.close().await; // must be closed here, otherwise HTTP/gRPC streams will not be terminated and server will not exit `serve`.
            })
            .await
            .with_context(|| format!("server error listening on {api_listening_addr}"))?;
        // Normally Axum blocks before this point until all clients are disconnected.
        debug!("Server {api_listening_addr} has been closed");
    } else {
        info!("Obeliskg is ready");
        let _: Result<_, _> = termination_watcher.changed().await;
        server_init.close().await;
    }
    Ok(())
}

fn make_span<B>(request: &axum::http::Request<B>) -> Span {
    let headers = request.headers();
    info_span!(
        "gRPC request",
        "otel.name" = format!("gRPC request {}", request.uri().path()),
        ?headers
    )
}

#[derive(Clone)]
pub(crate) struct ServerVerified {
    launch: ServerVerifiedLaunch,
    http_servers: Vec<HttpServer>,
    fuel: Option<u64>,
    global_backtrace_persist: bool,
    global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    database_subscription_interruption: Option<Duration>,
    api_addr_if_webui_enabled: Option<String>,
}

#[derive(Clone)]
struct ServerVerifiedLaunch {
    engines: Engines,
    build_semaphore: Option<u64>,
    workflows_lock_extension_leeway: Duration,
}

impl ServerVerified {
    #[instrument(name = "ServerVerified::new", skip_all)]
    async fn new(
        engines: Engines,
        config: ServerConfigToml,
    ) -> Result<ServerVerified, anyhow::Error> {
        trace!("Using server toml: {config:#?}");
        let mut http_servers = config.http_servers;
        if config.webui.enabled {
            let webui_listening_addr = config.webui.listening_addr;
            http_servers.push(webhook::HttpServer {
                name: ConfigName::new(HTTP_SERVER_NAME_WEBUI.into()).unwrap(),
                listening_addr: webui_listening_addr
                    .parse()
                    .context("error converting `webui.listening_addr` to a socket address")?,
            });
            if !config.api.enabled {
                anyhow::bail!(
                    "cannot expose webui without enabling the API (`api.enabled = false` is set)"
                );
            }
        }
        if config.external.enabled {
            http_servers.push(webhook::HttpServer {
                name: ConfigName::new(HTTP_SERVER_NAME_EXTERNAL.into()).unwrap(),
                listening_addr: config.external.listening_addr,
            });
        }
        let fuel: Option<u64> = config.wasm_global_config.fuel.into();
        let workflows_lock_extension_leeway =
            config.workflows_global_config.lock_extension_leeway.into();
        let global_backtrace_persist = config.wasm_global_config.backtrace.persist;
        let build_semaphore = config.wasm_global_config.build_semaphore.into();
        let global_executor_instance_limiter = config
            .wasm_global_config
            .global_executor_instance_limiter
            .as_semaphore();
        let database_subscription_interruption = config.database.get_subscription_interruption();

        Ok(Self {
            launch: ServerVerifiedLaunch {
                engines,
                build_semaphore,
                workflows_lock_extension_leeway,
            },
            http_servers,
            fuel,
            global_backtrace_persist,
            global_executor_instance_limiter,
            database_subscription_interruption,
            api_addr_if_webui_enabled: if config.webui.enabled {
                Some(config.api.listening_addr.to_string()) // `config.api.enabled` checked above
            } else {
                None
            },
        })
    }
}

pub(crate) type FrameFilesToSourceContent = HashMap<
    String, // file name, can contain `.../` prefix
    String, //content
>;

pub(crate) type HttpServersToWebhooksAndState = Vec<(
    webhook::HttpServer,
    (Vec<WebhookInstancesAndRoutes>, Arc<WebhookServerState>),
)>;

pub(crate) struct ServerCompiledLinked {
    engines: Engines,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
    pub(crate) workers_linked: Vec<WorkerLinked>,
    pub(crate) http_servers_to_webhooks_and_state: HttpServersToWebhooksAndState,
    supressed_errors: Option<String>,
    frame_files: Vec<(ComponentDigest, FrameFilesToSourceContent)>,
}

impl ServerCompiledLinked {
    async fn new(
        deployment_id: DeploymentId,
        deployment_verified: DeploymentVerified,
        server_verified: ServerVerifiedLaunch,
        termination_watcher: &mut watch::Receiver<()>,
        suppress_type_checking_errors: bool,
        suppress_linking_errors: bool,
    ) -> Result<Self, anyhow::Error> {
        trace!("Verified deployment: {deployment_verified:#?}");
        let DeploymentVerified {
            activities_wasm,
            activities_js,
            activities_exec,
            activities_stub_ext,
            activities_stub_ext_inline,
            workflows,
            workflows_js,
            webhooks_wasm_by_names,
            webhooks_js_by_names,
            crons,
            http_servers_to_webhook_names,
            global_backtrace_persist,
            fuel,
        } = deployment_verified;
        let linked = compile_and_link(
            &server_verified.engines,
            activities_wasm,
            activities_js,
            activities_exec,
            activities_stub_ext,
            activities_stub_ext_inline,
            workflows,
            workflows_js,
            webhooks_wasm_by_names,
            webhooks_js_by_names,
            crons,
            global_backtrace_persist,
            fuel,
            server_verified.build_semaphore,
            server_verified.workflows_lock_extension_leeway,
            termination_watcher,
            suppress_linking_errors,
        )
        .await?;
        if !suppress_type_checking_errors && linked.supressed_errors.is_some() {
            bail!("type checking errors detected");
        }
        let http_server_len = http_servers_to_webhook_names.len();
        let http_servers_to_webhooks = Self::connect_http_servers_to_webhooks(
            &http_servers_to_webhook_names,
            linked.webhooks_wasm_by_names,
        );
        assert_eq!(
            http_server_len,
            http_servers_to_webhooks.len(),
            "must not omit empty http servers"
        );

        let fn_registry: Arc<dyn FunctionRegistry> =
            Arc::from(linked.component_registry_ro.clone());
        let http_servers_to_webhooks_and_state: Vec<_> = http_servers_to_webhooks
            .into_iter()
            .map(|(http_server, webhooks)| {
                let state = Arc::new(build_webhook_server_state(
                    deployment_id,
                    &webhooks,
                    fn_registry.clone(),
                ));
                (http_server, (webhooks, state))
            })
            .collect();
        assert_eq!(
            http_server_len,
            http_servers_to_webhooks_and_state.len(),
            "must not omit empty http servers"
        );

        Ok(ServerCompiledLinked {
            workers_linked: linked.workers,
            component_registry_ro: linked.component_registry_ro,
            engines: server_verified.engines,
            http_servers_to_webhooks_and_state,
            supressed_errors: linked.supressed_errors,
            frame_files: linked.all_frame_files,
        })
    }

    fn connect_http_servers_to_webhooks(
        http_servers_to_webhook_names: &[(webhook::HttpServer, Vec<ConfigName>)],
        mut webhooks_wasm_by_names: IndexMap<ConfigName, WebhookInstancesAndRoutes>,
    ) -> Vec<(webhook::HttpServer, Vec<WebhookInstancesAndRoutes>)> {
        http_servers_to_webhook_names
            .iter()
            .map(|(http_server, webhook_names)| {
                let instances = webhook_names
                    .iter()
                    .map(|name| {
                        webhooks_wasm_by_names
                            .shift_remove(name)
                            .expect("all webhooks must be verified")
                    })
                    .collect();
                (http_server.clone(), instances)
            })
            .collect()
    }

    async fn create_missing_cron_seeds(
        &self,
        db_pool: &Arc<dyn DbPool>,
        deployment_id: DeploymentId,
    ) -> Result<(), anyhow::Error> {
        create_missing_cron_seeds(
            db_pool,
            deployment_id,
            self.workers_linked.iter().filter_map(|worker_linked| {
                if let LinkedWorkerKind::Cron(cron_config) = &worker_linked.worker {
                    Some(cron_config.as_ref())
                } else {
                    None
                }
            }),
        )
        .await
    }
}

pub(crate) async fn upsert_backtrace_sources(
    conn: &dyn DbExternalApi,
    cas: &dyn concepts::cas::Cas,
    server_compiled_linked: &ServerCompiledLinked,
) {
    // Persist source files so GetBacktraceSource can serve them without filesystem access.
    // The bytes go to the content-addressed store (shared with deployment files); only the
    // (component, frame_key) -> digest mapping lives in the DB.
    for (component_digest, frame_files) in &server_compiled_linked.frame_files {
        for (config_key, source) in frame_files {
            // Keys starting with ".../" become suffix keys (strip "...", prepend "/").
            let (frame_key, is_suffix) = if let Some(stripped) = config_key.strip_prefix(".../") {
                (format!("/{stripped}"), true)
            } else {
                (config_key.clone(), false)
            };
            let digest = match cas.write_blob(source.as_bytes()).await {
                Ok(digest) => digest,
                Err(err) => {
                    warn!("Cannot store backtrace source {config_key:?} in CAS: {err:?}");
                    continue;
                }
            };
            if let Err(err) = conn
                .upsert_source_mapping(component_digest, &frame_key, is_suffix, &digest)
                .await
            {
                warn!("Cannot store backtrace source mapping {config_key:?} in DB: {err:?}");
            }
        }
    }
}

fn build_component_metadata_records(
    deployment_id: DeploymentId,
    component_registry_ro: &ComponentConfigRegistryRO,
) -> (
    Vec<ComponentMetadataRecord>,   // List of components as stored in db
    Vec<DeploymentComponentRecord>, // List of relations to each component for the depoloyment ID.
) {
    let components = component_registry_ro.list(true);
    let mut metadata_by_digest = HashMap::<ComponentDigest, ComponentMetadataRecord>::new();
    let mut component_type_by_digest = HashMap::<ComponentDigest, ComponentType>::new();
    let mut deployment_components = Vec::with_capacity(components.len());
    for component in components {
        let component_type = component.component_id.component_type;
        let component_digest = component.component_id.component_digest.clone();
        let exports: Vec<concepts::storage::PersistedFunctionMetadata> = component
            .workflow_or_activity_config
            .as_ref()
            .map(|config| {
                config
                    .exports_ext
                    .clone()
                    .into_iter()
                    .map(Into::into)
                    .collect()
            })
            .unwrap_or_default();
        let imports = component
            .imports
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        let wit_origin = component.wit_origin.to_string();
        match metadata_by_digest.entry(component_digest.clone()) {
            hashbrown::hash_map::Entry::Occupied(occupied) => {
                let existing = occupied.get();
                assert_eq!(
                    component_type_by_digest.get(&component_digest),
                    Some(&component_type),
                    "component digest reused across different component types"
                );
                assert_eq!(
                    existing.imports, imports,
                    "component digest reused with different imports"
                );
                assert_eq!(
                    existing.exports, exports,
                    "component digest reused with different exports"
                );
                assert_eq!(
                    existing.wit, component.wit,
                    "component digest reused with different WIT"
                );
                assert_eq!(
                    existing.wit_origin, wit_origin,
                    "component digest reused with different WIT origins"
                );
            }
            hashbrown::hash_map::Entry::Vacant(vacant) => {
                component_type_by_digest.insert(component_digest.clone(), component_type);
                vacant.insert(ComponentMetadataRecord {
                    component_digest: component_digest.clone(),
                    imports,
                    exports,
                    wit: component.wit.clone(),
                    wit_origin,
                });
            }
        }
        deployment_components.push(DeploymentComponentRecord {
            deployment_id,
            component_name: component.component_id.name,
            component_digest: component.component_id.component_digest,
            component_type,
        });
    }
    (
        metadata_by_digest.into_values().collect(),
        deployment_components,
    )
}

/// Per-file size limit for deployment-owned blobs attached to a submit request.
const MAX_DEPLOYMENT_FILE_BYTES: usize = 512 * 1024 * 1024;

/// A deployment-owned blob attached to a submit request.
pub(crate) struct SuppliedFile {
    pub(crate) path: String,
    /// Optional client-computed digest; the server recomputes and uses its own value.
    pub(crate) supplied_digest: Option<String>,
    pub(crate) content: Vec<u8>,
}

/// A single file-set problem found while validating a submit package. Mirrors the
/// proto `FileIssue` but keeps the command layer free of grpc types.
#[derive(Debug, Clone)]
pub(crate) struct SubmitFileIssue {
    pub(crate) section: String,
    pub(crate) component_name: Option<String>,
    pub(crate) field_path: String,
    pub(crate) path: Option<String>,
    pub(crate) digest: Option<String>,
    pub(crate) message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SubmitDigestMismatch {
    pub(crate) file: SubmitFileIssue,
    pub(crate) supplied_digest: String,
    pub(crate) actual_digest: String,
}

/// Structured file-set validation failure. No deployment is stored when returned.
#[derive(Debug, Default)]
pub(crate) struct SubmitPackageError {
    pub(crate) missing_digest_fields: Vec<SubmitFileIssue>,
    pub(crate) missing_files: Vec<SubmitFileIssue>,
    pub(crate) unexpected_files: Vec<SubmitFileIssue>,
    pub(crate) digest_mismatches: Vec<SubmitDigestMismatch>,
    pub(crate) oversized_files: Vec<SubmitFileIssue>,
}

impl SubmitPackageError {
    fn is_empty(&self) -> bool {
        self.missing_digest_fields.is_empty()
            && self.missing_files.is_empty()
            && self.unexpected_files.is_empty()
            && self.digest_mismatches.is_empty()
            && self.oversized_files.is_empty()
    }
}

/// Submit failure: either a structured file-set error (no deployment stored) or any
/// other error (parse/idempotency/storage).
pub(crate) enum SubmitDeploymentError {
    Busy,
    Package(SubmitPackageError),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for SubmitDeploymentError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

fn issue_from_ref(
    file: &crate::config::manifest::DeploymentFileRef,
    message: &str,
) -> SubmitFileIssue {
    SubmitFileIssue {
        section: file.field.section.clone(),
        component_name: file.field.component_name.clone(),
        field_path: file.field.field_path.clone(),
        path: Some(file.path.clone()),
        digest: Some(file.digest.to_string()),
        message: message.to_string(),
    }
}

fn supplied_issue(path: &str, digest: &ContentDigest, message: &str) -> SubmitFileIssue {
    SubmitFileIssue {
        section: "files".to_string(),
        component_name: None,
        field_path: format!("files[path={path}]"),
        path: Some(path.to_string()),
        digest: Some(digest.to_string()),
        message: message.to_string(),
    }
}

/// Match attached blobs to the manifest's deployment-owned file refs. Returns the
/// blobs that must be written to the CAS (referenced, not already present, attached),
/// or a structured error pointing at each offending reference. Pure: no I/O.
fn validate_submit_package(
    expected: &[crate::config::manifest::DeploymentFileRef],
    supplied: Vec<SuppliedFile>,
    cas_present: &hashbrown::HashSet<ContentDigest>,
    max_bytes: usize,
) -> Result<Vec<DeploymentManifestFile>, SubmitPackageError> {
    use DeploymentManifestFile;

    let mut err = SubmitPackageError::default();
    // `expected` is already deduplicated by digest in `DeploymentManifest`.
    let expected_by_digest: HashMap<ContentDigest, ()> = expected
        .iter()
        .map(|file| (file.digest.clone(), ()))
        .collect();

    let mut provided: HashMap<ContentDigest, Vec<u8>> = HashMap::new();
    for file in supplied {
        let actual = compute_content_digest(&file.content);
        if let Some(supplied_digest) = &file.supplied_digest
            && supplied_digest.parse::<ContentDigest>().ok().as_ref() != Some(&actual)
        {
            err.digest_mismatches.push(SubmitDigestMismatch {
                file: supplied_issue(
                    &file.path,
                    &actual,
                    "attached blob digest does not match the supplied digest",
                ),
                supplied_digest: supplied_digest.clone(),
                actual_digest: actual.to_string(),
            });
            continue;
        }
        if file.content.len() > max_bytes {
            err.oversized_files.push(supplied_issue(
                &file.path,
                &actual,
                &format!("attached blob exceeds the {max_bytes}-byte per-file limit"),
            ));
            continue;
        }
        if expected_by_digest.contains_key(&actual) {
            provided.insert(actual, file.content);
        } else {
            err.unexpected_files.push(supplied_issue(
                &file.path,
                &actual,
                "attached blob is not referenced by the manifest",
            ));
        }
    }

    let mut to_write = Vec::new();
    for file in expected {
        if cas_present.contains(&file.digest) {
            continue;
        }
        if let Some(bytes) = provided.remove(&file.digest) {
            to_write.push(DeploymentManifestFile {
                path: file.path.clone(),
                digest: file.digest.clone(),
                bytes,
            });
        } else {
            err.missing_files.push(issue_from_ref(
                file,
                "referenced file is neither attached nor present in the store",
            ));
        }
    }

    if err.is_empty() {
        Ok(to_write)
    } else {
        Err(err)
    }
}

/// Shared logic for submitting a deployment (used by both gRPC and web API).
/// Validates the manifest and its file set as a package; persists only a complete
/// deployment. Compile/link verification still happens when the deployment is activated.
#[instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn submit_deployment(
    server_verified: ServerVerified,
    deployment_toml: &str,
    runtime_config_check: RuntimeConfigCheck,
    created_by: Option<String>,
    description: Option<String>,
    requested_deployment_id: Option<DeploymentId>,
    prepared_dirs: &PreparedDirs,
    supplied_files: Vec<SuppliedFile>,
    db_pool: Arc<dyn DbPool>,
    termination_watcher: &mut watch::Receiver<()>,
    deployment_switch_manager: Option<DeploymentSwitchManagerHandle>,
) -> Result<DeploymentId, SubmitDeploymentError> {
    info!("Submitting deployment");
    let _submit_permit = if let Some(manager) = &deployment_switch_manager {
        Some(manager.try_acquire_submit_permit()?)
    } else {
        None
    };
    // The deployment digest is the hash of the verbatim manifest; it transitively covers
    // every referenced file because the manifest embeds each file's content digest. It is
    // used only for idempotency, not returned (the client already has the manifest).
    let digest = DeploymentRecord::compute_digest(deployment_toml);

    let conn = db_pool
        .external_api_conn()
        .await
        .map_err(anyhow::Error::from)?;

    // Idempotent submission: if the caller supplied a deployment ID that already
    // exists, return it as a no-op when the content digest matches, and reject a
    // digest mismatch as a conflict. A stored deployment is always complete.
    if let Some(requested_deployment_id) = requested_deployment_id
        && let Some(existing) = conn
            .get_deployment(requested_deployment_id)
            .await
            .map_err(anyhow::Error::from)?
    {
        if existing.digest == digest {
            info!(%requested_deployment_id, "Deployment already exists with matching digest, returning existing ID");
            return Ok(requested_deployment_id);
        }
        return Err(SubmitDeploymentError::Other(anyhow::anyhow!(
            "deployment {requested_deployment_id} already exists with a different content digest \
             (existing {}, submitted {digest}); use a fresh deployment ID",
            existing.digest
        )));
    }

    let manifest = DeploymentManifest::try_from_toml(deployment_toml, &cas_deployment_dir())
        .context("cannot read deployment file references from manifest")?;

    // A referenced digest already in the CAS need not be attached to the request.
    let cas = db_pool.cas_conn().await.map_err(anyhow::Error::from)?;
    let mut cas_present = hashbrown::HashSet::new();
    for file in &manifest.files {
        if cas
            .contains_blob(&file.digest)
            .await
            .map_err(|err| anyhow::anyhow!("cannot query CAS for {}: {err}", file.digest))?
        {
            cas_present.insert(file.digest.clone());
        }
    }

    let to_write = validate_submit_package(
        &manifest.files,
        supplied_files,
        &cas_present,
        MAX_DEPLOYMENT_FILE_BYTES,
    )
    .map_err(SubmitDeploymentError::Package)?;

    // Write missing blobs to the CAS before inserting the deployment row, so the
    // stored deployment never references absent blobs. Orphan blobs from a later
    // failed insert are acceptable GC input.
    for file in &to_write {
        let stored = cas.write_blob(&file.bytes).await.map_err(|err| {
            anyhow::anyhow!("cannot store deployment file {}: {err}", file.digest)
        })?;
        if stored != file.digest {
            return Err(SubmitDeploymentError::Other(anyhow::anyhow!(
                "stored deployment file digest mismatch: expected {}, got {stored}",
                file.digest
            )));
        }
    }

    // Fully validate the deployment before persisting it, so a stored deployment is
    // already known-good: canonicalize from the CAS (resolving env vars/secrets),
    // parse JS, validate WIT/WASM, and compile + link every component. Activation no
    // longer performs first-time package validation. Blobs already written to the CAS
    // when verification fails are acceptable GC input; no deployment row is inserted.
    let deployment_resolved = deployment_resolved_from_manifest(&*db_pool, deployment_toml)
        .await
        .map_err(SubmitDeploymentError::Other)?;
    let cas_arc: Arc<dyn concepts::cas::Cas> = db_pool
        .cas_conn()
        .await
        .map_err(anyhow::Error::from)?
        .into();
    let deployment_id = requested_deployment_id.unwrap_or_else(DeploymentId::generate);
    let compiled_linked = deployment_verify_config_compile_link(
        server_verified,
        prepared_dirs,
        deployment_resolved,
        Some(cas_arc),
        deployment_id,
        VerifyParams {
            dir_params: PrepareDirsParams {
                clean_cache: false,
                clean_codegen_cache: false,
            },
            ignore_missing_env_vars: runtime_config_check.ignore_missing_env_vars(),
            suppress_type_checking_errors: false,
            suppress_linking_errors: false,
        },
        termination_watcher,
    )
    .await
    .map_err(SubmitDeploymentError::Other)?;

    let now = chrono::Utc::now();

    // Persist the deployment row together with its verified component metadata in a single
    // transaction so the DB-backed ListComponents / GetWit see this deployment without waiting
    // for a server restart, and so the submit is cancel-safe: this future runs inline in the
    // request handler and is dropped if the client disconnects, so the row that makes a
    // deployment queryable and its component rows must commit together or not at all.
    // Verification above already compiled and linked every component.
    let (component_metadata, deployment_components) =
        build_component_metadata_records(deployment_id, &compiled_linked.component_registry_ro);
    conn.insert_deployment_with_components(
        DeploymentRecord {
            deployment_id,
            description,
            digest: digest.clone(),
            created_at: now,
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
            created_by,
            deployment_toml: deployment_toml.to_string(),
            files: manifest.file_records(),
        },
        component_metadata,
        deployment_components,
    )
    .await
    .map_err(anyhow::Error::from)?;

    // Backtrace sources are content-addressed and deployment-independent (keyed by component
    // digest), only consulted once a component runs and produces a backtrace, so they are
    // persisted outside the deployment transaction; a partial set self-heals on re-persist.
    upsert_backtrace_sources(conn.as_ref(), cas.as_ref(), &compiled_linked).await;

    // Only cache strict-verified artifacts. A hot redeploy is always strict, so an
    // artifact compiled while tolerating missing env vars (AllowMissing) must not be
    // reused to satisfy a later strict switch; a strict artifact satisfies any consumer.
    if let Some(manager) = deployment_switch_manager
        && !runtime_config_check.ignore_missing_env_vars()
    {
        manager
            .store_latest_prepared(PreparedDeploymentSwitch {
                deployment_id,
                digest,
                compiled_linked,
            })
            .await;
    }

    info!(%deployment_id, "Deployment submitted");
    Ok(deployment_id)
}

fn compute_content_digest(content: &[u8]) -> ContentDigest {
    let hash: [u8; 32] = Sha256::digest(content).into();
    ContentDigest(Digest(hash))
}

/// Outcome of switching a deployment.
#[derive(Debug, Clone, Copy, derive_more::Display)]
pub(crate) enum SwitchOutcome {
    #[display("switched")]
    Switched,
    #[display("restart required")]
    RestartRequired,
}

/// Error returned by [`switch_deployment`].
pub(crate) enum SwitchError {
    /// Another deployment submit/switch operation is queued or running.
    Busy,
    /// The requested deployment ID does not exist.
    NotFound,
    /// Any other failure (verification, compilation, DB write, etc.).
    Other(anyhow::Error),
}

impl From<anyhow::Error> for SwitchError {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

/// Policy for runtime configuration (environment variables and secrets) that a
/// deployment references but that may be absent from the server's environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RuntimeConfigCheck {
    /// Missing environment variables or secrets fail the operation.
    #[default]
    Strict,
    /// Missing environment variables or secrets are tolerated.
    AllowMissing,
}
impl RuntimeConfigCheck {
    fn ignore_missing_env_vars(self) -> bool {
        self == RuntimeConfigCheck::AllowMissing
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SwitchDeploymentAction {
    Activate,
    Enqueue(RuntimeConfigCheck),
}
impl SwitchDeploymentAction {
    fn ignore_missing_env_vars(self) -> bool {
        match self {
            SwitchDeploymentAction::Enqueue(check) => check.ignore_missing_env_vars(),
            SwitchDeploymentAction::Activate => false,
        }
    }
}

/// Enqueue `deployment_id` for the next restart, returning the DB-decided [`EnqueueOutcome`],
/// then release the switch permit needed for serializability.
async fn enqueue_deployment_and_release(
    deployment_switch_manager: &DeploymentSwitchManagerHandle,
    deployment_id: DeploymentId,
    switch_permit: SwitchPermit,
) -> Result<EnqueueOutcome, SwitchError> {
    let db_conn = deployment_switch_manager
        .inner
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;
    let outcome = db_conn
        .enqueue_deployment(deployment_id)
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;
    drop(switch_permit);
    Ok(outcome)
}

#[instrument(skip_all, fields(%deployment_id))]
pub(crate) async fn switch_deployment(
    deployment_switch_manager: DeploymentSwitchManagerHandle,
    deployment_id: DeploymentId,
    action: SwitchDeploymentAction,
) -> Result<SwitchOutcome, SwitchError> {
    let deployment_already_active = deployment_switch_manager
        .inner
        .deployment_ctx
        .read()
        .await
        .deployment_id
        == deployment_id;

    match action {
        SwitchDeploymentAction::Enqueue(_) => {
            let switch_permit = deployment_switch_manager.try_acquire_switch_permit()?;
            if !deployment_already_active {
                // Validate (compile + link) so the queued deployment is known-good against
                // the current host before enqueuing. An already-active deployment is already
                // running, hence known-good, so this is skipped.
                let mut termination_watcher =
                    deployment_switch_manager.inner.termination_watcher.clone();
                prepare_switch_deployment(
                    &deployment_switch_manager,
                    deployment_id,
                    action,
                    &mut termination_watcher,
                )
                .await?;
            }
            // The outcome is decided inside the DB transaction, so it is authoritative even
            // if the active deployment changed since the read above.
            let outcome = enqueue_deployment_and_release(
                &deployment_switch_manager,
                deployment_id,
                switch_permit,
            )
            .await?;
            Ok(match outcome {
                EnqueueOutcome::Enqueued => {
                    info!(%deployment_id, "Deployment enqueued for next restart");
                    SwitchOutcome::RestartRequired
                }
                EnqueueOutcome::AlreadyActive => {
                    info!(%deployment_id, "Deployment already active; it will remain active after restart");
                    SwitchOutcome::Switched
                }
            })
        }
        SwitchDeploymentAction::Activate => {
            if deployment_already_active {
                info!(%deployment_id, "Deployment switch no-op: deployment already active");
                return Ok(SwitchOutcome::Switched);
            }
            let switch_permit = deployment_switch_manager.try_acquire_switch_permit()?;
            let mut termination_watcher =
                deployment_switch_manager.inner.termination_watcher.clone();
            // Cancellable: validation only. The durable commit and critical swap are owned by
            // the detached task spawned below, so they cannot be aborted by a dropped caller.
            let prepared = prepare_switch_deployment(
                &deployment_switch_manager,
                deployment_id,
                action,
                &mut termination_watcher,
            )
            .await?;
            deployment_switch_manager
                .spawn_critical_hot_switch(prepared, switch_permit)
                .await
        }
    }
}

#[instrument(skip_all, fields(%deployment_id))]
async fn prepare_switch_deployment(
    deployment_switch_manager: &DeploymentSwitchManagerHandle,
    deployment_id: DeploymentId,
    action: SwitchDeploymentAction,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<PreparedDeploymentSwitch, SwitchError> {
    let db_conn = deployment_switch_manager
        .inner
        .db_pool
        .external_api_conn()
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;

    let deployment_record = db_conn
        .get_deployment(deployment_id)
        .await
        .map_err(|e| SwitchError::Other(e.into()))?
        .ok_or(SwitchError::NotFound)?;

    if let Some(prepared) = deployment_switch_manager
        .take_latest_prepared(deployment_id, &deployment_record.digest)
        .await
    {
        debug!(%deployment_id, "Using cached prepared deployment artifact");
        return Ok(prepared);
    }

    let missing = db_conn
        .missing_digests(deployment_id)
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;
    if !missing.is_empty() {
        return Err(SwitchError::Other(anyhow::anyhow!(
            "deployment {deployment_id} is missing {} referenced file blob(s): {}",
            missing.len(),
            missing
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let target_deployment = deployment_resolved_from_manifest(
        &*deployment_switch_manager.inner.db_pool,
        &deployment_record.deployment_toml,
    )
    .await
    .map_err(SwitchError::Other)?;

    let cas: Arc<dyn concepts::cas::Cas> = deployment_switch_manager
        .inner
        .db_pool
        .cas_conn()
        .await
        .map_err(|e| SwitchError::Other(e.into()))?
        .into();

    let verify_params = VerifyParams {
        dir_params: PrepareDirsParams {
            clean_cache: false,
            clean_codegen_cache: false,
        },
        ignore_missing_env_vars: action.ignore_missing_env_vars(),
        suppress_type_checking_errors: false,
        suppress_linking_errors: false,
    };

    // Cold switch: validation (compile + link) always runs before enqueuing, so a
    // deployment queued for the next restart is known-good against the current host.
    let compiled_linked = deployment_verify_config_compile_link(
        deployment_switch_manager.inner.server_verified.clone(),
        &deployment_switch_manager.inner.prepared_dirs,
        target_deployment,
        Some(cas),
        deployment_id,
        verify_params,
        termination_watcher,
    )
    .await?;

    Ok(PreparedDeploymentSwitch {
        deployment_id,
        digest: deployment_record.digest,
        compiled_linked,
    })
}

/// Spawn the executor tasks for `deployment_id` and assemble its `DeploymentContext`.
/// Shared by initial startup ([`spawn_tasks_and_threads`]) and hot redeploy
/// ([`switch_hot_redeploy`]) so the two paths cannot drift. Webhook-registry wiring
/// (create vs swap) and where the context is stored differ and stay at the call sites.
fn spawn_deployment_context(
    deployment_id: DeploymentId,
    workers_linked: Vec<WorkerLinked>,
    component_registry_ro: ComponentConfigRegistryRO,
    db_pool: &Arc<dyn DbPool>,
    cancel_registry: &CancelRegistry,
    log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
) -> DeploymentContext {
    let mut exec_task_handles: Vec<ExecutorTaskHandle> = Vec::with_capacity(workers_linked.len());
    let mut replay_workers = ReplayWorkerRegistry::default();
    for pre_spawn in workers_linked {
        let (handle, replay_entry) = pre_spawn.spawn(
            deployment_id,
            db_pool,
            cancel_registry.clone(),
            log_forwarder_sender,
        );
        exec_task_handles.push(handle);
        if let Some((component_id, worker)) = replay_entry {
            replay_workers.insert(component_id, worker);
        }
    }
    DeploymentContext {
        deployment_id,
        component_registry_ro,
        exec_task_handles,
        replay_workers: Arc::new(replay_workers),
        closed: false,
    }
}

/// Write lock pretected switch to the new deployment
#[instrument(skip_all, fields(%deployment_id))]
async fn switch_hot_redeploy(
    server_compiled_linked: ServerCompiledLinked,
    deployment_id: DeploymentId,
    db_pool: Arc<dyn DbPool>,
    deployment_ctx: DeploymentContextHandle,
    webhook_registry: Arc<WebhookRegistry>,
    cancel_registry: CancelRegistry,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
) -> Result<SwitchOutcome, SwitchError> {
    let mut write_guard_ctx = deployment_ctx.write().await;
    if write_guard_ctx.closed {
        return Err(SwitchError::Other(anyhow::anyhow!(
            "server is being shut down"
        )));
    }

    debug!("Closing old executors");
    let old = std::mem::take(&mut write_guard_ctx.exec_task_handles);
    let worker_tasks_handles =
        futures_util::future::join_all(old.into_iter().map(ExecutorTaskHandle::close_outer_task))
            .await;

    debug!("Waiting for workers");
    futures_util::future::join_all(
        worker_tasks_handles
            .into_iter()
            .map(WorkerTasksHandle::close),
    )
    .await;

    debug!("Swapping webhook registry");
    webhook_registry.swap(
        server_compiled_linked
            .http_servers_to_webhooks_and_state
            .iter()
            .map(|(http_server, (_instance, state))| (http_server.name.to_string(), state.clone())),
    );

    *write_guard_ctx = spawn_deployment_context(
        deployment_id,
        server_compiled_linked.workers_linked,
        server_compiled_linked.component_registry_ro,
        &db_pool,
        &cancel_registry,
        &log_forwarder_sender,
    );

    info!(%deployment_id, "Switched to new deployment");
    Ok(SwitchOutcome::Switched)
}

// Create seed cron executions if they don't already exist (same config = same ContentDigest)
async fn create_missing_cron_seeds(
    db_pool: &Arc<dyn DbPool>,
    deployment_id: DeploymentId,
    cron_configs: impl Iterator<Item = &ScheduleWorkerConfig>,
) -> Result<(), anyhow::Error> {
    let conn = db_pool.external_api_conn().await?;
    for cron_config in cron_configs {
        let digest = &cron_config.component_id.component_digest;
        let execution_id = ExecutionId::deterministic_at_unix_epoch(&digest.0.0);

        if conn.get(&execution_id).await.is_ok() {
            info!(
                %execution_id,
                "Cron execution `{}` already found",
                cron_config.component_id.name
            );
        } else {
            info!(
                %execution_id,
                "Creating cron execution `{}`",
                cron_config.component_id.name
            );
            let source_ffqn = cron_worker::cron_ffqn(&cron_config.target_ffqn);
            let now = chrono::Utc::now();
            conn.create(CreateRequest {
                created_at: now,
                execution_id,
                ffqn: source_ffqn,
                params: Params::empty(),
                parent: None,
                scheduled_at: now,
                component_id: cron_config.component_id.clone(),
                deployment_id,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_by: None,
                paused: false,
            })
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to create seed execution for cron `{}`: {e}",
                    cron_config.component_id.name
                )
            })?;
        }
    }
    Ok(())
}

#[instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
async fn spawn_tasks_and_threads(
    server_verified: ServerVerified,
    deployment_id: DeploymentId,
    db_pool: Arc<dyn DbPool>,
    db_close: Pin<Box<dyn Future<Output = ()> + Send>>,
    server_compiled_linked: ServerCompiledLinked,
    global_webhook_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    timers_watcher: TimersWatcherTomlConfig,
    cancel_watcher: CancelWatcherTomlConfig,
    cancel_registry: &CancelRegistry,
    termination_watcher: &watch::Receiver<()>,
    prepared_dirs: PreparedDirs,
) -> Result<ServerInit, anyhow::Error> {
    upsert_backtrace_sources(
        db_pool.external_api_conn().await?.as_ref(),
        db_pool.cas_conn().await?.as_ref(),
        &server_compiled_linked,
    )
    .await;

    // Start components requiring a database
    let epoch_ticker = EpochTicker::spawn_new(
        server_compiled_linked.engines.weak_refs(),
        Duration::from_millis(EPOCH_MILLIS),
    );

    let timers_watcher = if timers_watcher.enabled {
        Some(expired_timers_watcher::spawn_new(
            db_pool.clone(),
            TimersWatcherConfig {
                tick_sleep: timers_watcher.tick_sleep.into(),
                clock_fn: Now.clone_box(),
                leeway: timers_watcher.leeway.into(),
            },
        ))
    } else {
        None
    };

    let cancellation_driver = wasm_workers::cancellation_driver::CancellationDriver::spawn(
        db_pool.clone(),
        cancel_registry.clone(),
        Now.clone_box(),
        TokioSleep,
        cancel_watcher.tick_sleep.into(),
        CANCELLATION_DRIVER_BATCH_SIZE,
    );
    let cancel_watcher = cancel_registry.spawn_cancel_watcher(cancel_watcher.tick_sleep.into());

    server_compiled_linked
        .create_missing_cron_seeds(&db_pool, deployment_id)
        .await?;

    // Spawn Log -> Db Forwarder
    let (log_forwarder_sender, log_db_forarder) = {
        let (log_forwarder_sender, receiver) = mpsc::channel(1000); // TODO: make configurable
        let log_db_forarder = log_db_forwarder::spawn_new(db_pool.clone(), receiver);
        (log_forwarder_sender, log_db_forarder)
    };

    let webhook_registry = WebhookRegistry::new(
        server_compiled_linked
            .http_servers_to_webhooks_and_state
            .iter()
            .map(|(http_server, (_instance, state))| (http_server.name.to_string(), state.clone())),
    );
    // Start webhook HTTP servers
    let http_servers = server_compiled_linked
        .http_servers_to_webhooks_and_state
        .iter()
        .map(|(http_server, _)| http_server);

    let http_servers_handles: Vec<AbortOnDropHandle> = start_http_servers(
        http_servers,
        &webhook_registry,
        &server_compiled_linked.engines,
        db_pool.clone(),
        global_webhook_instance_limiter.clone(),
        termination_watcher,
        &log_forwarder_sender,
    )
    .await?;
    let deployment_ctx: DeploymentContextHandle =
        Arc::new(tokio::sync::RwLock::new(spawn_deployment_context(
            deployment_id,
            server_compiled_linked.workers_linked,
            server_compiled_linked.component_registry_ro,
            &db_pool,
            cancel_registry,
            &log_forwarder_sender,
        )));
    let server_init = ServerInit {
        server_verified,
        deployment_ctx,
        // deployment_id,
        db_pool,
        db_close,
        // exec_join_handles,
        timers_watcher,
        cancel_watcher,
        cancellation_driver,
        http_servers_handles,
        epoch_ticker,
        log_db_forarder,
        engines: server_compiled_linked.engines,
        log_forwarder_sender,
        webhook_registry: Arc::new(webhook_registry),
        prepared_dirs,
        deployment_switch_manager: None,
    };
    Ok(server_init)
}

struct ServerInit {
    server_verified: ServerVerified,
    deployment_ctx: DeploymentContextHandle,
    db_pool: Arc<dyn DbPool>,
    db_close: Pin<Box<dyn Future<Output = ()> + Send>>,
    engines: Engines,
    timers_watcher: Option<AbortOnDropHandle>,
    cancel_watcher: AbortOnDropHandle,
    cancellation_driver: AbortOnDropHandle,
    http_servers_handles: Vec<AbortOnDropHandle>,
    epoch_ticker: EpochTicker,
    log_db_forarder: AbortOnDropHandle,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    webhook_registry: Arc<WebhookRegistry>,
    prepared_dirs: PreparedDirs,
    deployment_switch_manager: Option<DeploymentSwitchManagerHandle>,
}
impl ServerInit {
    async fn close(self) {
        info!("Server is shutting down");

        let ServerInit {
            server_verified: _,
            deployment_ctx,
            db_pool,
            db_close,
            engines,
            timers_watcher,
            cancel_watcher,
            cancellation_driver,
            http_servers_handles,
            epoch_ticker,
            log_db_forarder,
            log_forwarder_sender,
            webhook_registry,
            prepared_dirs: _,
            deployment_switch_manager,
        } = self;

        if let Some(deployment_switch_manager) = deployment_switch_manager {
            deployment_switch_manager.close().await;
        }

        debug!("Closing executors");
        let executors = {
            let mut deployment_lock = deployment_ctx.write().await;
            deployment_lock.closed = true;
            std::mem::take(&mut deployment_lock.exec_task_handles)
        };
        let worker_tasks_handles = futures_util::future::join_all(
            executors
                .into_iter()
                .map(ExecutorTaskHandle::close_outer_task),
        )
        .await;
        debug!("Waiting for workers");
        futures_util::future::join_all(
            worker_tasks_handles
                .into_iter()
                .map(WorkerTasksHandle::close),
        )
        .await;
        // Explicit drop to avoid the pattern match footgun.
        // Close everything that is a dependency of executors or workers.
        drop(db_pool);
        drop(timers_watcher);
        drop(cancel_watcher);
        drop(cancellation_driver);
        drop(http_servers_handles);
        drop(epoch_ticker);
        drop(engines);
        drop(webhook_registry);
        drop(log_forwarder_sender);
        drop(log_db_forarder); // Some activity messages might not be stored.
        debug!("Closing db");
        db_close.await;
    }
}

type WebhookInstancesAndRoutes = (WebhookEndpointInstanceLinked, Vec<WebhookRouteVerified>);

/// Build a `WebhookServerState` from a list of webhook instances and their routes.
/// Used both at startup and during hot-redeploy.
pub(crate) fn build_webhook_server_state(
    deployment_id: DeploymentId,
    webhooks: &[WebhookInstancesAndRoutes],
    fn_registry: Arc<dyn FunctionRegistry>,
) -> WebhookServerState {
    let mut router = MethodAwareRouter::default();
    for (webhook_instance_linked, routes) in webhooks {
        for route in routes {
            if route.methods.is_empty() {
                router.add(None, &route.route, webhook_instance_linked.clone());
            } else {
                for method in &route.methods {
                    router.add(
                        Some(method.clone()),
                        &route.route,
                        webhook_instance_linked.clone(),
                    );
                }
            }
        }
    }
    WebhookServerState {
        deployment_id,
        router: Arc::new(router),
        fn_registry,
    }
}

async fn start_http_servers(
    http_servers: impl Iterator<Item = &webhook::HttpServer>,
    webhook_registry: &WebhookRegistry,
    engines: &Engines,
    db_pool: Arc<dyn DbPool>,
    global_webhook_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    termination_watcher: &watch::Receiver<()>,
    log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
) -> Result<Vec<AbortOnDropHandle>, anyhow::Error> {
    let mut abort_handles = Vec::new();
    let engine = &engines.webhook_engine;
    for http_server in http_servers {
        let state_watcher = webhook_registry
            .get_watcher(http_server.name.as_ref())
            .expect("wh_server_state_watcher must exist for every http_server");
        let tcp_listener = TcpListener::bind(&http_server.listening_addr)
            .await
            .with_context(|| {
                format!(
                    "cannot bind socket {} for `http_server` named `{}`",
                    http_server.listening_addr, http_server.name
                )
            })?;
        let server_addr = tcp_listener.local_addr()?;
        info!(
            "HTTP server `{}` is listening on http://{server_addr}",
            http_server.name,
        );
        let server = AbortOnDropHandle::new(
            tokio::spawn(webhook_trigger::server(
                http_server.name.to_string(),
                tcp_listener,
                engine.clone(),
                state_watcher,
                log_forwarder_sender.clone(),
                db_pool.clone(),
                Now.clone_box(),
                Arc::new(TokioSleep),
                global_webhook_instance_limiter.clone(),
                termination_watcher.clone(),
            ))
            .abort_handle(),
        );
        abort_handles.push(server);
    }
    Ok(abort_handles)
}

#[derive(Debug)]
pub(crate) struct DeploymentVerified {
    activities_wasm: Vec<ActivityWasmConfigVerified>,
    activities_js: Vec<ActivityJsConfigVerified>,
    activities_exec: Vec<ActivityExecConfigVerified>,
    activities_stub_ext: Vec<ActivityStubExtConfigVerified>,
    activities_stub_ext_inline: Vec<ActivityStubExtInlineConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    workflows_js: Vec<WorkflowJsConfigVerified>,
    webhooks_wasm_by_names: IndexMap<ConfigName, WebhookWasmComponentConfigVerified>,
    webhooks_js_by_names: IndexMap<ConfigName, WebhookJsConfigVerified>,
    crons: Vec<CronConfigVerified>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<ConfigName>)>,
    global_backtrace_persist: bool,
    fuel: Option<u64>,
}

impl DeploymentVerified {
    fn validate_component_digests(&self) -> Result<(), anyhow::Error> {
        fn record_component_ids<'a>(
            component_ids_by_digest: &mut HashMap<ComponentDigest, ComponentId>,
            component_ids: impl IntoIterator<Item = &'a ComponentId>,
        ) -> Result<(), anyhow::Error> {
            for component_id in component_ids {
                if let Some(existing) = component_ids_by_digest
                    .insert(component_id.component_digest.clone(), component_id.clone())
                    && existing.component_type != component_id.component_type
                {
                    bail!(
                        "component digest `{}` is shared between component types `{}` ({}) and `{}` ({})",
                        component_id.component_digest,
                        existing.component_type,
                        existing.name,
                        component_id.component_type,
                        component_id.name,
                    );
                }
            }
            Ok(())
        }

        let mut component_ids_by_digest = HashMap::<ComponentDigest, ComponentId>::new();
        record_component_ids(
            &mut component_ids_by_digest,
            self.activities_wasm
                .iter()
                .map(ActivityWasmConfigVerified::component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.activities_js
                .iter()
                .map(ActivityJsConfigVerified::component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.activities_exec
                .iter()
                .map(ActivityExecConfigVerified::component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.activities_stub_ext
                .iter()
                .map(|activity| &activity.component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.activities_stub_ext_inline
                .iter()
                .map(|activity| &activity.component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.workflows
                .iter()
                .map(WorkflowConfigVerified::component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.workflows_js
                .iter()
                .map(WorkflowJsConfigVerified::component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.webhooks_wasm_by_names
                .values()
                .map(|webhook| &webhook.component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.webhooks_js_by_names
                .values()
                .map(|webhook| &webhook.component_id),
        )?;
        record_component_ids(
            &mut component_ids_by_digest,
            self.crons.iter().map(|cron| &cron.component_id),
        )?;
        Ok(())
    }

    fn verify_no_webui_server_bindings(
        deployment: &DeploymentResolved,
    ) -> Result<(), anyhow::Error> {
        let mut offending_webhooks = deployment
            .webhooks_wasm
            .iter()
            .filter(|webhook| &**webhook.http_server == HTTP_SERVER_NAME_WEBUI)
            .map(|webhook| &webhook.common.name)
            .chain(
                deployment
                    .webhooks_js
                    .iter()
                    .filter(|webhook| &**webhook.http_server == HTTP_SERVER_NAME_WEBUI)
                    .map(|webhook| &webhook.name),
            )
            .peekable();
        if offending_webhooks.peek().is_some() {
            bail!(
                "the `{HTTP_SERVER_NAME_WEBUI}` http_server is reserved for the web UI; \
                     the following webhook(s) must not attach to it: {:?}",
                offending_webhooks.collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[instrument(skip_all)]
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify_all(
        deployment: DeploymentRunnable,
        http_servers: Vec<webhook::HttpServer>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_backtrace_persist: bool,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        termination_watcher: &mut watch::Receiver<()>,
        subscription_interruption: Option<Duration>,
        api_addr_if_webui_enabled: Option<String>,
    ) -> Result<DeploymentVerified, anyhow::Error> {
        let mut deployment = deployment.into_canonical();
        trace!("Using deployment toml: {deployment:#?}");
        // Check uniqueness of http_server names.
        if http_servers.len()
            > http_servers
                .iter()
                .map(|it| &it.name)
                .collect::<hashbrown::HashSet<_>>()
                .len()
        {
            bail!("Each `http_server` must have a unique name");
        }
        Self::verify_no_webui_server_bindings(&deployment)?;

        if let Some(api_listening_addr) = api_addr_if_webui_enabled {
            let target_url = format!("http://{api_listening_addr}");
            deployment
                .webhooks_wasm
                .push(webhook::WebhookWasmComponentConfigResolved {
                    common: ComponentCommon {
                        name: ConfigName::new(StrVariant::Static("obelisk_webui")).unwrap(),
                        location: WEBUI_LOCATION
                            .parse()
                            .expect("hard-coded webui reference must be parsed"),
                    },
                    content_digest: None,
                    http_server: ConfigName::new(HTTP_SERVER_NAME_WEBUI.into()).unwrap(),
                    routes: vec![WebhookRoute::default()],
                    forward_stdout: ComponentStdOutputToml::default(),
                    forward_stderr: ComponentStdOutputToml::default(),
                    env_vars: vec![EnvVarConfig::KeyValue {
                        key: "TARGET_URL".to_string(),
                        value: target_url.clone(),
                    }],
                    backtrace: crate::config::toml::ComponentBacktraceConfigResolved::default(),
                    logs_store_min_level: LogLevelToml::Off,
                    allowed_hosts: vec![AllowedHostToml {
                        pattern: target_url,
                        methods: Some(MethodsInput::Star(MethodsInputStar::default())),
                        request_url_regex: None,
                        secrets: None,
                    }],
                });
        }

        let http_servers_to_webhook_names = {
            let mut remaining_server_names_to_webhook_names = {
                let mut map: hashbrown::HashMap<ConfigName, Vec<ConfigName>> =
                    hashbrown::HashMap::default();
                for webhook in &deployment.webhooks_wasm {
                    map.entry(webhook.http_server.clone())
                        .or_default()
                        .push(webhook.common.name.clone());
                }
                for webhook_js in &deployment.webhooks_js {
                    map.entry(webhook_js.http_server.clone())
                        .or_default()
                        .push(webhook_js.name.clone());
                }
                map
            };
            let http_servers_len = http_servers.len();
            let http_servers_to_webhook_names = {
                let mut vec = Vec::new();
                for http_server in http_servers {
                    let webhooks = remaining_server_names_to_webhook_names
                        .remove(&http_server.name)
                        .unwrap_or_default();
                    vec.push((http_server, webhooks));
                }
                vec
            };
            // Each webhook must be associated with an `http_server`.
            if !remaining_server_names_to_webhook_names.is_empty() {
                bail!(
                    "No matching `http_server` found for some `webhook` configurations: {:?}",
                    remaining_server_names_to_webhook_names
                        .keys()
                        .collect::<Vec<_>>()
                );
            }
            assert_eq!(
                http_servers_len,
                http_servers_to_webhook_names.len(),
                "http servers with empty names must not be removed"
            );
            http_servers_to_webhook_names
        };

        // Fetch and verify components, each in its own tokio task.
        let activities_wasm = deployment
            .activities_wasm
            .into_iter()
            .map(|activity_wasm| {
                tokio::spawn(
                    activity_wasm
                        .fetch_and_verify(
                            wasm_cache_dir.clone(),
                            metadata_dir.clone(),
                            ignore_missing_env_vars,
                            global_executor_instance_limiter.clone(),
                            fuel,
                        )
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();

        let activities_stub_tasks = deployment
            .activities_stub
            .into_iter()
            .map(|activity| {
                tokio::spawn(
                    activity
                        .fetch_and_verify(wasm_cache_dir.clone(), metadata_dir.clone())
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();
        let activities_external_tasks = deployment
            .activities_external
            .into_iter()
            .map(|activity| {
                tokio::spawn(
                    activity
                        .fetch_and_verify(wasm_cache_dir.clone(), metadata_dir.clone())
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();

        let workflows = deployment
            .workflows_wasm
            .into_iter()
            .map(|workflow| {
                tokio::spawn(
                    workflow
                        .fetch_and_verify(
                            wasm_cache_dir.clone(),
                            metadata_dir.clone(),
                            global_backtrace_persist,
                            global_executor_instance_limiter.clone(),
                            fuel,
                            subscription_interruption,
                        )
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();
        let webhooks_wasm_by_names = deployment
            .webhooks_wasm
            .into_iter()
            .map(|webhook| {
                tokio::spawn({
                    let wasm_cache_dir = wasm_cache_dir.clone();
                    let metadata_dir = metadata_dir.clone();
                    webhook
                        .fetch_and_verify(
                            wasm_cache_dir,
                            metadata_dir,
                            ignore_missing_env_vars,
                            subscription_interruption,
                        )
                        .in_current_span()
                })
            })
            .collect::<Vec<_>>();

        // Skip fetching when no JS activities are configured
        let activity_js_runtime_fetch: OptionFuture<_> = if deployment.activities_js.is_empty() {
            None
        } else {
            Some(fetch_activity_js_runtime(
                wasm_cache_dir.clone(),
                metadata_dir.clone(),
            ))
        }
        .into();

        // Skip fetching when no JS workflows are configured
        let workflow_js_runtime_fetch: OptionFuture<_> = if deployment.workflows_js.is_empty() {
            None
        } else {
            Some(fetch_workflow_js_runtime(
                wasm_cache_dir.clone(),
                metadata_dir.clone(),
            ))
        }
        .into();

        // Skip fetching when no JS webhooks are configured
        let webhook_js_runtime_fetch: OptionFuture<_> = if deployment.webhooks_js.is_empty() {
            None
        } else {
            Some(fetch_webhook_js_runtime(
                wasm_cache_dir.clone(),
                metadata_dir.clone(),
            ))
        }
        .into();

        // Abort/cancel safety:
        // If an error happens or Ctrl-C is pressed the whole process will shut down.
        // Downloading metadata and content must be robust enough to handle it.
        // We do not need to abort the tasks here.
        let all = futures_util::future::join3(
            futures_util::future::join5(
                futures_util::future::join_all(activities_wasm),
                futures_util::future::join(
                    futures_util::future::join_all(activities_stub_tasks),
                    futures_util::future::join_all(activities_external_tasks),
                ),
                futures_util::future::join_all(workflows),
                futures_util::future::join_all(webhooks_wasm_by_names),
                activity_js_runtime_fetch,
            ),
            workflow_js_runtime_fetch,
            webhook_js_runtime_fetch,
        );
        tokio::select! {
            ((activity_wasm_results, (activity_stub_task_results, activity_external_task_results), workflow_results, webhook_results, activity_js_runtime_result), workflow_js_runtime_result, webhook_js_runtime_result) = all => {
                let activities_wasm = activity_wasm_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let stub_results: Vec<ActivityStubConfigVerified> = activity_stub_task_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let external_results: Vec<ActivityExternalConfigVerified> = activity_external_task_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let mut activities_stub_ext: Vec<ActivityStubExtConfigVerified> = Vec::new();
                let mut activities_stub_ext_inline: Vec<ActivityStubExtInlineConfigVerified> = Vec::new();
                for result in stub_results {
                    match result {
                        ActivityStubConfigVerified::File(ext) => activities_stub_ext.push(ext),
                        ActivityStubConfigVerified::Inline(inline) => activities_stub_ext_inline.push(inline),
                    }
                }
                for result in external_results {
                    match result {
                        ActivityExternalConfigVerified::File(ext) => activities_stub_ext.push(ext),
                        ActivityExternalConfigVerified::Inline(inline) => activities_stub_ext_inline.push(inline),
                    }
                }
                let workflows = workflow_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let mut webhooks_wasm_by_names = IndexMap::new();
                for webhook in webhook_results {
                    let (k, v) = webhook??;
                    webhooks_wasm_by_names.insert(k, v);
                }

                let activities_js_verified = if !deployment.activities_js.is_empty() {
                    let activity_js_wasm_path = activity_js_runtime_result.transpose()?;
                    let activity_js_wasm_path: Arc<Path> = Arc::from(activity_js_wasm_path
                        .expect("None only if there are no JS activities, see `activity_js_runtime_fetch`"));
                    let mut activities_js_verified = Vec::with_capacity(deployment.activities_js.len());
                    for js in deployment.activities_js {
                        activities_js_verified.push(
                            js.fetch_and_verify(
                                activity_js_wasm_path.clone(),
                                wasm_cache_dir.clone(),
                                ignore_missing_env_vars,
                                global_executor_instance_limiter.clone(),
                                fuel,
                            ).await?
                        );
                    }
                    activities_js_verified
                } else {
                    Vec::new()
                };

                let workflows_js_verified = if !deployment.workflows_js.is_empty() {
                    let workflow_js_wasm_path = workflow_js_runtime_result.transpose()?;
                    let workflow_js_wasm_path: Arc<Path> = Arc::from(workflow_js_wasm_path
                        .expect("None only if there are no JS workflows, see `workflow_js_runtime_fetch`"));
                    let mut workflows_js_verified = Vec::with_capacity(deployment.workflows_js.len());
                    for workflow_js in deployment.workflows_js {
                        workflows_js_verified.push(
                            workflow_js.fetch_and_verify(
                                workflow_js_wasm_path.clone(),
                                wasm_cache_dir.clone(),
                                global_executor_instance_limiter.clone(),
                            ).await?
                        );
                    }
                    workflows_js_verified
                } else {
                    Vec::new()
                };

                let mut webhooks_js_by_names = IndexMap::new();
                if !deployment.webhooks_js.is_empty() {
                    let webhook_js_wasm_path = webhook_js_runtime_result.transpose()?
                        .expect("None only if there are no JS webhooks, see `webhook_js_runtime_fetch`");
                    let webhook_js_wasm_path: Arc<Path> = Arc::from(webhook_js_wasm_path);
                    for webhook_js in deployment.webhooks_js {
                        let (k, v) = webhook_js.fetch_and_verify(
                            webhook_js_wasm_path.clone(),
                            wasm_cache_dir.clone(),
                            ignore_missing_env_vars,
                        ).await?;
                        webhooks_js_by_names.insert(k, v);
                    }
                }

                let mut activities_exec_verified = Vec::with_capacity(deployment.activities_exec.len());
                for exec in deployment.activities_exec {
                    let resolved_program = exec.resolve(&wasm_cache_dir).await?;
                    activities_exec_verified.push(
                        exec.fetch_and_verify(
                            resolved_program,
                            ignore_missing_env_vars,
                            global_executor_instance_limiter.clone(),
                        )?
                    );
                }

                let mut crons = Vec::with_capacity(deployment.crons.len());
                for cron in deployment.crons {
                    let name = cron.name.clone();
                    crons.push(cron.verify().with_context(|| {
                        format!("failed to verify cron `{name}`")
                    })?);
                }

                let deployment_verified = DeploymentVerified {
                    activities_wasm,
                    activities_js: activities_js_verified,
                    activities_exec: activities_exec_verified,
                    activities_stub_ext,
                    activities_stub_ext_inline,
                    workflows,
                    workflows_js: workflows_js_verified,
                    webhooks_wasm_by_names,
                    webhooks_js_by_names,
                    crons,
                    http_servers_to_webhook_names,
                    global_backtrace_persist,
                    fuel
                };
                deployment_verified.validate_component_digests()?;
                Ok(deployment_verified)
            },
            _ = termination_watcher.changed() => {
                warn!("Received SIGINT, canceling while resolving the WASM files");
                anyhow::bail!("canceling while resolving the WASM files")
            }
        }
    }
}

struct Linked {
    workers: Vec<WorkerLinked>,
    webhooks_wasm_by_names: IndexMap<ConfigName, WebhookInstancesAndRoutes>,
    component_registry_ro: ComponentConfigRegistryRO,
    supressed_errors: Option<String>,
    all_frame_files: Vec<(ComponentDigest, FrameFilesToSourceContent)>,
}

#[expect(clippy::large_enum_variant)]
enum CompiledComponent {
    ActivityOrWorkflow {
        worker: WorkerCompiled,
        component_config: ComponentConfig,
        frame_files: FrameFilesToSourceContent,
    },
    Webhook {
        webhook_name: ConfigName,
        webhook_compiled: WebhookEndpointCompiled,
        routes: Vec<WebhookRouteVerified>,
        frame_files: FrameFilesToSourceContent,
    },
    ActivityStubOrExternal {
        component_config: ComponentConfig,
    },
}

#[instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
async fn compile_and_link(
    engines: &Engines,
    activities_wasm: Vec<ActivityWasmConfigVerified>,
    activities_js: Vec<ActivityJsConfigVerified>,
    activities_exec: Vec<ActivityExecConfigVerified>,
    activities_stub_ext: Vec<ActivityStubExtConfigVerified>,
    activities_stub_ext_inline: Vec<ActivityStubExtInlineConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    workflows_js: Vec<WorkflowJsConfigVerified>,
    webhooks_wasm_by_names: IndexMap<ConfigName, WebhookWasmComponentConfigVerified>,
    webhooks_js_by_names: IndexMap<ConfigName, WebhookJsConfigVerified>,
    crons: Vec<CronConfigVerified>,
    global_backtrace_persist: bool,
    fuel: Option<u64>,
    build_semaphore: Option<u64>,
    workflows_lock_extension_leeway: Duration,
    termination_watcher: &mut watch::Receiver<()>,
    suppress_linking_errors: bool,
) -> Result<Linked, anyhow::Error> {
    let build_semaphore = build_semaphore.map(|permits| {
        semaphore::Semaphore::new(permits.try_into().expect("u64 must fit into usize"))
    });
    let parent_span = Span::current();

    // JS runtimes are compiled in parallel, then all other WASM components.
    let activity_js_runnable = if let Some(first_activity_js) = activities_js.first() {
        let engine = engines.activity_engine.clone();
        let build_semaphore = build_semaphore.clone();
        let parent_span = parent_span.clone();
        let wasm_path = first_activity_js.wasm_path.clone();
        let component_type = first_activity_js.component_id().component_type;
        let runnable = tokio::task::spawn_blocking(move || {
            let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
            let span = info_span!(parent: parent_span, "activity_js_wasm_compile");
            span.in_scope(|| {
                debug!("Building activity-js-runtime");
                RunnableComponent::new(&wasm_path, &engine, component_type)
                    .context("cannot compile activity-js-runtime")
            })
        });
        Some(runnable)
    } else {
        None
    };
    let workflow_js_runnable = if let Some(first_workflow_js) = workflows_js.first() {
        let engine = engines.workflow_engine.clone();
        let build_semaphore = build_semaphore.clone();
        let parent_span = parent_span.clone();
        let wasm_path = first_workflow_js.wasm_path.clone();
        let runnable = tokio::task::spawn_blocking(move || {
            let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
            let span = info_span!(parent: parent_span, "workflow_js_wasm_compile");
            span.in_scope(|| {
                debug!("Building workflow-js-runtime");
                RunnableComponent::new(&wasm_path, &engine, ComponentType::Workflow)
                    .context("cannot compile workflow-js-runtime")
            })
        });
        Some(runnable)
    } else {
        None
    };
    let webhook_js_runnable = if let Some((_, first_webhook_js)) = webhooks_js_by_names.first() {
        let engine = engines.webhook_engine.clone();
        let build_semaphore = build_semaphore.clone();
        let parent_span = parent_span.clone();
        let wasm_path = first_webhook_js.wasm_path.clone();
        let runnable = tokio::task::spawn_blocking(move || {
            let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
            let span = info_span!(parent: parent_span, "webhook_js_wasm_compile");
            span.in_scope(|| {
                debug!("Building webhook-js-runtime");
                RunnableComponent::new(&wasm_path, &engine, ComponentType::WebhookEndpoint)
                    .context("cannot compile webhook-js-runtime")
            })
        });
        Some(runnable)
    } else {
        None
    };

    let activity_js_runnable = match activity_js_runnable {
        Some(wasm) => Some(wasm.await??),
        None => None,
    };
    let workflow_js_runnable = match workflow_js_runnable {
        Some(wasm) => Some(wasm.await??),
        None => None,
    };
    let webhook_js_runnable = match webhook_js_runnable {
        Some(wasm) => Some(wasm.await??),
        None => None,
    };

    let pre_spawns: Vec<tokio::task::JoinHandle<Result<CompiledComponent, anyhow::Error>>> = activities_wasm
        .into_iter()
        .map(|activity_wasm| {
            let engines = engines.clone();
            let build_semaphore = build_semaphore.clone();
            let parent_span = parent_span.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
                let span = info_span!(parent: parent_span, "activity_wasm_compile", component_id = %activity_wasm.component_id());
                span.in_scope(|| {
                    prespawn_activity_wasm(activity_wasm, &engines, suppress_linking_errors)
                })
            })
        })
        .chain(
            activities_js.into_iter().map(|activity_js| {
            // No build_semaphore as the WASM was already compiled.
            let engines = engines.clone();
            let parent_span = parent_span.clone();
            let activity_js_runnable = activity_js_runnable.clone().expect("must have been filled above");
            tokio::task::spawn_blocking(move || {
                let span = info_span!(parent: parent_span, "activity_js_compile", component_id = %activity_js.component_id());
                span.in_scope(|| {
                    prespawn_activity_js(activity_js, &engines, activity_js_runnable).map(|(worker, component_config, frame_files)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                            frame_files,
                        }
                    })
                })
            })
        }))
        .chain(activities_exec.into_iter().map(|activity_exec| {
            // No build_semaphore and no WASM compilation needed for exec activities.
            let parent_span = parent_span.clone();
            tokio::task::spawn_blocking(move || {
                let span = info_span!(parent: parent_span, "activity_exec_compile", component_id = %activity_exec.component_id());
                span.in_scope(|| {
                    prespawn_activity_exec(activity_exec).map(|(worker, component_config)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                            frame_files: FrameFilesToSourceContent::default(),
                        }
                    })
                })
            })
        }))
        .chain(activities_stub_ext.into_iter().map(|activity_stub_ext| {
            // No build_semaphore as there is no WASM compilation.
            let span = info_span!("activity_stub_ext_init", component_id = %activity_stub_ext.component_id); // automatically associated with parent
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let wasm_component = WasmComponent::new(
                        activity_stub_ext.wasm_path,
                        activity_stub_ext.component_id.component_type,
                    )?;
                    let wit = wasm_component.wit();
                    let exports_ext = wasm_component.exim.get_exports(true).to_vec();
                    let exports_hierarchy_ext =
                        wasm_component.exim.get_exports_hierarchy_ext().to_vec();
                    let component_config_importable = ComponentConfigImportable {
                        exports_ext,
                        exports_hierarchy_ext,
                    };
                    let component_config = ComponentConfig {
                        component_id: activity_stub_ext.component_id,
                        imports: vec![],
                        workflow_or_activity_config: Some(component_config_importable),
                        wit,
                        wit_origin: WitOrigin::Wasm,
                    };
                    Ok(CompiledComponent::ActivityStubOrExternal { component_config })
                })
            })
        }))
        .chain(activities_stub_ext_inline.into_iter().map(|stub| {
            // No build_semaphore needed (no WASM compilation).
            let span = info_span!("activity_inline_init", component_id = %stub.component_id);
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let component_config = compile_activity_inline(
                        stub.component_id,
                        &stub.ffqn,
                        &stub.params,
                        &stub.return_type,
                    )?;
                    Ok(CompiledComponent::ActivityStubOrExternal { component_config })
                })
            })
        }))
        .chain(workflows.into_iter().map(|workflow| {
            let engines = engines.clone();
            let build_semaphore = build_semaphore.clone();
            let parent_span = parent_span.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
                let span = info_span!(parent: parent_span, "workflow_compile", component_id = %workflow.component_id());
                span.in_scope(|| {
                    prespawn_workflow_wasm(workflow, &engines, workflows_lock_extension_leeway)
                    .map(|(worker, component_config, frame_files)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                            frame_files,
                        }
                    })
                })
            })
        }))
        .chain(workflows_js.into_iter().map(|workflow_js| {
            // No build_semaphore as the WASM was already compiled.
            let engines = engines.clone();
            let parent_span = parent_span.clone();
            let workflow_js_runnable = workflow_js_runnable.clone().expect("must have been filled above");
            tokio::task::spawn_blocking(move || {
                let span = info_span!(parent: parent_span, "workflow_js_compile", component_id = %workflow_js.component_id());
                span.in_scope(|| {
                    prespawn_workflow_js(workflow_js, &engines,workflow_js_runnable, workflows_lock_extension_leeway)
                        .map(|(worker, component_config, frame_files)| {
                            CompiledComponent::ActivityOrWorkflow {
                                worker,
                                component_config,
                                frame_files,
                            }
                        })
                })
            })
        }))
        .chain(
            webhooks_wasm_by_names
                .into_iter()
                .map(|(webhook_name, webhook)| {
                    let engines = engines.clone();
                    let build_semaphore = build_semaphore.clone();
                    let parent_span = parent_span.clone();
                    tokio::task::spawn_blocking(move || {
                        let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
                        let span = info_span!(parent: parent_span, "webhook_compile", component_id = %webhook.component_id);
                        span.in_scope(|| {
                            let component_id = webhook.component_id;
                            let config = WebhookEndpointConfig {
                                component_id,
                                forward_stdout: webhook.forward_stdout,
                                forward_stderr: webhook.forward_stderr,
                                env_vars: webhook.env_vars,
                                fuel,
                                backtrace_persist: global_backtrace_persist,
                                subscription_interruption: webhook.subscription_interruption,
                                logs_store_min_level: webhook.logs_store_min_level,
                                allowed_hosts: webhook.allowed_hosts,
                                js_config: None,
                                config_section_hint: webhook.config_section_hint,
                            };
                             let runnable_component =
                                RunnableComponent::new(webhook.wasm_path, &engines.webhook_engine, ComponentType::WebhookEndpoint)?;
                            let webhook_compiled = webhook_trigger::WebhookEndpointCompiled::new(
                                config,
                                runnable_component,
                            )?;
                            Ok(CompiledComponent::Webhook {
                                webhook_name,
                                webhook_compiled,
                                routes: webhook.routes,
                                frame_files: webhook.frame_files_to_sources,
                            })
                        })
                    })
                }),
        )
        .chain(
            webhooks_js_by_names
                .into_iter()
                .map(|(webhook_name, webhook_js)| {
                    let build_semaphore = build_semaphore.clone();
                    let parent_span = parent_span.clone();
                    let webhook_js_runnable = webhook_js_runnable.clone().expect("must have been filled above");
                    tokio::task::spawn_blocking(move || {
                        let _permit = build_semaphore.map(semaphore::Semaphore::acquire);
                        let span = info_span!(parent: parent_span, "webhook_js_compile", component_id = %webhook_js.component_id);
                        span.in_scope(|| {
                            let frame_files = webhook_js.as_frame_sources();
                            let config = WebhookEndpointConfig {
                                component_id: webhook_js.component_id,
                                forward_stdout: webhook_js.forward_stdout,
                                forward_stderr: webhook_js.forward_stderr,
                                env_vars: webhook_js.env_vars,
                                fuel,
                                backtrace_persist: false,
                                subscription_interruption: None,
                                logs_store_min_level: webhook_js.logs_store_min_level,
                                allowed_hosts: webhook_js.allowed_hosts,
                                js_config: Some(WebhookEndpointJsConfig {
                                    source: webhook_js.js_source,
                                    file_name: webhook_js.js_file_name.clone(),
                                }),
                                config_section_hint: webhook_js.config_section_hint,
                            };

                            let webhook_compiled = webhook_trigger::WebhookEndpointCompiled::new(
                                config,
                                webhook_js_runnable
                            )?;
                            Ok(CompiledComponent::Webhook {
                                webhook_name,
                                webhook_compiled,
                                routes: webhook_js.routes,
                                frame_files,
                            })
                        })
                    })
                }),
        )
        .collect();

    // Abort/cancel safety:
    // If an error happens or Ctrl-C is pressed the whole process will shut down.
    let pre_spawns = futures_util::future::join_all(pre_spawns);
    let results_of_results = tokio::select! {
        results_of_results = pre_spawns => results_of_results,
        _ = termination_watcher.changed() => {
            warn!("Received SIGINT, canceling while compiling the components");
            anyhow::bail!("canceling while compiling the components")
        }
    };

    let mut component_registry = ComponentConfigRegistry::default();
    let mut workers_compiled = Vec::with_capacity(results_of_results.len());
    let mut webhooks_compiled_by_names = hashbrown::HashMap::new();
    let mut all_frame_files: Vec<(ComponentDigest, FrameFilesToSourceContent)> = Vec::new();
    for handle in results_of_results {
        match handle?? {
            CompiledComponent::ActivityOrWorkflow {
                worker,
                component_config,
                frame_files,
            } => {
                if !frame_files.is_empty() {
                    all_frame_files.push((
                        component_config.component_id.component_digest.clone(),
                        frame_files,
                    ));
                }
                component_registry.insert(component_config)?;
                workers_compiled.push(worker);
            }
            CompiledComponent::Webhook {
                webhook_name,
                webhook_compiled,
                routes,
                frame_files,
            } => {
                if !frame_files.is_empty() {
                    all_frame_files.push((
                        webhook_compiled
                            .config
                            .component_id
                            .component_digest
                            .clone(),
                        frame_files,
                    ));
                }
                let component = ComponentConfig {
                    component_id: webhook_compiled.config.component_id.clone(),
                    imports: webhook_compiled.imports().to_vec(),
                    workflow_or_activity_config: None,
                    wit: webhook_compiled.runnable_component.wasm_component.wit(),
                    wit_origin: WitOrigin::Wasm,
                };
                component_registry.insert(component)?;
                let old =
                    webhooks_compiled_by_names.insert(webhook_name, (webhook_compiled, routes));
                assert!(old.is_none());
            }
            CompiledComponent::ActivityStubOrExternal { component_config } => {
                component_registry.insert(component_config)?;
            }
        }
    }
    // Register cron components in the registry (no WASM, no imports/exports)
    for cron in &crons {
        let component_config = ComponentConfig {
            component_id: cron.component_id.clone(),
            imports: vec![],
            workflow_or_activity_config: None,
            wit: String::new(), // does not matter, WIT is not exposed
            wit_origin: WitOrigin::Synthesized,
        };
        component_registry.insert(component_config)?; // Mostly just for name uniqueness checking
    }
    let (component_registry_ro, supressed_errors) = component_registry.verify_registry();
    let fn_registry: Arc<dyn FunctionRegistry> = Arc::from(component_registry_ro.clone());
    let mut workers_linked = workers_compiled
        .into_iter()
        .map(|worker| worker.link(&fn_registry))
        .collect::<Result<Vec<_>, _>>()?;
    // Resolve cron target FFQNs, type-check params, and create WorkerLinked entries
    for cron in crons {
        let (target_component_id, target_fn_metadata) = component_registry_ro
            .find_by_exported_ffqn(&cron.target_ffqn)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "cron `{}` targets function `{}` which is not exported by any component",
                    cron.component_id.name,
                    cron.target_ffqn,
                )
            })?;
        // Parse and type-check params against the target function's parameter types
        let params = Params::from_json_values(
            Arc::from(cron.params_json),
            target_fn_metadata
                .parameter_types
                .iter()
                .map(|pt| &pt.type_wrapper),
        )
        .with_context(|| {
            format!(
                "cron `{}`: params do not match target function `{}` parameter types",
                cron.component_id.name, cron.target_ffqn,
            )
        })?;
        workers_linked.push(WorkerLinked {
            worker: LinkedWorkerKind::Cron(Box::new(ScheduleWorkerConfig {
                component_id: cron.component_id,
                target_ffqn: cron.target_ffqn,
                target_component_id: target_component_id.clone(),
                params,
                cron_schedule: cron.cron_schedule,
            })),
            exec_config: cron.exec_config,
            logs_store_min_level: None,
        });
    }
    let webhooks_wasm_by_names = webhooks_compiled_by_names
        .into_iter()
        .map(|(name, (compiled, routes))| {
            let component_id = compiled.config.component_id.clone();
            compiled
                .link(&engines.webhook_engine, fn_registry.as_ref())
                .map(|instance| (name, (instance, routes)))
                .with_context(|| format!("cannot compile {component_id}"))
        })
        .collect::<Result<IndexMap<_, _>, _>>()?;
    Ok(Linked {
        workers: workers_linked,
        webhooks_wasm_by_names,
        component_registry_ro,
        supressed_errors,
        all_frame_files,
    })
}
/// Build a [`ComponentConfig`] for an inline-defined stub activity.
///
/// The function synthesizes a WIT string from `ffqn`, `params`, and `return_type`
/// (same approach as JS activities), then creates a virtual `WasmComponent` from
/// the WIT alone — no real WASM binary is required.
fn compile_activity_inline(
    component_id: ComponentId,
    ffqn: &FunctionFqn,
    params: &[ParameterType],
    return_type: &ReturnTypeExtendable,
) -> Result<ComponentConfig, utils::wasm_tools::DecodeError> {
    let wasm_component = WasmComponent::new_from_fn_signature(
        ffqn,
        params,
        return_type,
        ComponentType::ActivityStub,
        "stub-activity",
    )?;
    let wit_text_with_extensions = wasm_component.wit();
    let exports_ext = wasm_component.exim.get_exports(true).to_vec();
    let exports_hierarchy_ext = wasm_component.exim.get_exports_hierarchy_ext().to_vec();
    let component_config_importable = ComponentConfigImportable {
        exports_ext,
        exports_hierarchy_ext,
    };
    Ok(ComponentConfig {
        component_id,
        imports: vec![],
        workflow_or_activity_config: Some(component_config_importable),
        wit: wit_text_with_extensions,
        wit_origin: WitOrigin::Synthesized,
    })
}

mod semaphore {
    use std::sync::{Arc, Condvar, Mutex};

    pub(crate) struct Semaphore {
        mutex: Mutex<usize>,
        condvar: Condvar,
    }
    impl Semaphore {
        pub(crate) fn new(permits: usize) -> Arc<Semaphore> {
            Arc::new(Semaphore {
                mutex: Mutex::new(permits),
                condvar: Condvar::new(),
            })
        }
        pub(crate) fn acquire(self: Arc<Semaphore>) -> Permit {
            {
                let mut guard = self.mutex.lock().unwrap();
                while *guard == 0 {
                    guard = self.condvar.wait(guard).unwrap();
                }
                *guard -= 1;
            }
            Permit(self)
        }
    }

    pub(crate) struct Permit(Arc<Semaphore>);
    impl Drop for Permit {
        fn drop(&mut self) {
            {
                let mut guard = self.0.mutex.lock().unwrap();
                *guard += 1;
            }
            self.0.condvar.notify_one();
        }
    }
}

#[instrument(level = "debug", skip_all, fields(
    executor_id = %activity.exec_config.executor_id,
    component_id = %activity.exec_config.component_id,
    wasm_path = ?activity.wasm_path,
))]
fn prespawn_activity_wasm(
    activity: ActivityWasmConfigVerified,
    engines: &Engines,
    suppress_linking_errors: bool,
) -> Result<CompiledComponent, anyhow::Error> {
    let component_id = activity.component_id().clone();
    assert!(component_id.component_type == ComponentType::Activity);
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let engine = engines.activity_engine.clone();
    let runnable_component =
        RunnableComponent::new(activity.wasm_path, &engine, component_id.component_type)?;
    let wit = runnable_component.wasm_component.wit();
    // Pre-extract WIT info before consuming runnable_component, so it is available
    // as a fallback if linking fails and suppress_linking_errors is set.
    let exports_ext = runnable_component
        .wasm_component
        .exim
        .get_exports(true)
        .to_vec();
    let exports_hierarchy_ext = runnable_component
        .wasm_component
        .exim
        .get_exports_hierarchy_ext()
        .to_vec();
    let imports_flat = runnable_component.wasm_component.exim.imports_flat.clone();

    match ActivityWorkerCompiled::new_with_config(
        runnable_component,
        activity.activity_config,
        engine,
        Now.clone_box(),
        Arc::new(TokioSleep),
    ) {
        Ok(worker) => {
            let (worker, component_config) = WorkerCompiled::new_activity(
                worker,
                activity.exec_config,
                wit,
                activity.logs_store_min_level,
            );
            Ok(CompiledComponent::ActivityOrWorkflow {
                worker,
                component_config,
                frame_files: FrameFilesToSourceContent::new(),
            })
        }
        Err(err) if suppress_linking_errors => {
            warn!("Suppressing linking error for {component_id}: {err:#}");
            let component_config = ComponentConfig {
                component_id: activity.exec_config.component_id,
                workflow_or_activity_config: Some(ComponentConfigImportable {
                    exports_ext,
                    exports_hierarchy_ext,
                }),
                imports: imports_flat,
                wit,
                wit_origin: WitOrigin::Wasm,
            };
            Ok(CompiledComponent::ActivityStubOrExternal { component_config })
        }
        Err(err) => Err(err).with_context(|| format!("cannot compile {component_id}")),
    }
}

#[instrument(level = "debug", skip_all, fields(
    component_id = %activity_js.exec_config.component_id,
))]
fn prespawn_activity_js(
    activity_js: ActivityJsConfigVerified,
    engines: &Engines,
    runnable_component: RunnableComponent,
) -> Result<(WorkerCompiled, ComponentConfig, FrameFilesToSourceContent), anyhow::Error> {
    let component_id = activity_js.component_id().clone();
    assert!(component_id.component_type == ComponentType::Activity);
    let frame_files = activity_js.as_frame_sources();

    let inner = ActivityWorkerCompiled::new_with_config(
        runnable_component,
        activity_js.activity_config,
        engines.activity_engine.clone(),
        Now.clone_box(),
        Arc::new(TokioSleep),
    )
    .with_context(|| format!("cannot compile JS activity runtime for {component_id}"))?;

    let worker = ActivityJsWorkerCompiled::new(
        inner,
        activity_js.js_source,
        activity_js.ffqn,
        activity_js.params,
        activity_js.return_type,
    )
    .with_context(|| format!("cannot create JS activity worker for {component_id}"))?;
    let wit = worker.wit();

    Ok(WorkerCompiled::new_js_activity(
        worker,
        activity_js.exec_config,
        wit,
        activity_js.logs_store_min_level,
        frame_files,
    ))
}

fn prespawn_activity_exec(
    activity_exec: ActivityExecConfigVerified,
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    let component_id = activity_exec.component_id().clone();
    assert!(component_id.component_type == ComponentType::Activity);

    let program = activity_exec.program;

    // The worker nests these secrets under the `secrets` key of the stdin JSON document
    // at execution time.
    let secrets = activity_exec.secrets.map(|secrets| secrets.env_vars);

    let worker = ActivityExecWorkerCompiled::new(
        program,
        activity_exec.ffqn,
        activity_exec.params,
        activity_exec.return_type,
        activity_exec.env_vars,
        activity_exec.max_output_bytes,
        activity_exec.forward_stdout,
        activity_exec.forward_stderr,
        secrets,
        activity_exec.params_via_stdin,
    )
    .with_context(|| format!("cannot create exec activity worker for {component_id}"))?;
    let wit = worker.wit();

    Ok(WorkerCompiled::new_exec_activity(
        worker,
        activity_exec.exec_config,
        wit,
        activity_exec.logs_store_min_level,
    ))
}

/// Resolve the activity-js runtime WASM path from the local build.
#[cfg(feature = "activity-js-local")]
async fn fetch_activity_js_runtime(
    _wasm_cache_dir: Arc<Path>,
    _metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    Ok(PathBuf::from(
        activity_js_runtime_builder::ACTIVITY_JS_RUNTIME,
    ))
}

/// Fetch the activity-js runtime WASM from OCI.
#[cfg(not(feature = "activity-js-local"))]
async fn fetch_activity_js_runtime(
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    let location: crate::config::toml::ComponentLocationToml = ACTIVITY_JS_LOCATION
        .parse()
        .context("cannot parse built-in activity-js runtime location")?;
    let (_content_digest, wasm_path) = location
        .fetch(&wasm_cache_dir, &metadata_dir)
        .await
        .context("cannot fetch activity-js runtime")?;
    Ok(wasm_path)
}

/// Resolve the workflow-js runtime WASM path from the local build.
#[cfg(feature = "workflow-js-local")]
async fn fetch_workflow_js_runtime(
    _wasm_cache_dir: Arc<Path>,
    _metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    Ok(PathBuf::from(
        workflow_js_runtime_builder::WORKFLOW_JS_RUNTIME,
    ))
}

/// Fetch the workflow-js runtime WASM from OCI.
#[cfg(not(feature = "workflow-js-local"))]
async fn fetch_workflow_js_runtime(
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    let location: crate::config::toml::ComponentLocationToml = WORKFLOW_JS_LOCATION
        .parse()
        .context("cannot parse built-in workflow-js runtime location")?;
    let (_content_digest, wasm_path) = location
        .fetch(&wasm_cache_dir, &metadata_dir)
        .await
        .context("cannot fetch workflow-js runtime")?;
    Ok(wasm_path)
}

/// Resolve the webhook-js runtime WASM path from the local build.
#[cfg(feature = "webhook-js-local")]
async fn fetch_webhook_js_runtime(
    _wasm_cache_dir: Arc<Path>,
    _metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    Ok(PathBuf::from(
        webhook_js_runtime_builder::WEBHOOK_JS_RUNTIME,
    ))
}

/// Fetch the webhook-js runtime WASM from OCI.
#[cfg(not(feature = "webhook-js-local"))]
#[instrument(skip_all)]
async fn fetch_webhook_js_runtime(
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
) -> Result<PathBuf, anyhow::Error> {
    let location: crate::config::toml::ComponentLocationToml = WEBHOOK_JS_LOCATION
        .parse()
        .context("cannot parse built-in webhook-js runtime location")?;
    let (_content_digest, wasm_path) = location
        .fetch(&wasm_cache_dir, &metadata_dir)
        .await
        .context("cannot fetch webhook-js runtime")?;
    Ok(wasm_path)
}

#[instrument(level = "debug", skip_all, fields(
    executor_id = %workflow.exec_config.executor_id,
    component_id = %workflow.exec_config.component_id,
    wasm_path = ?workflow.wasm_path,
))]
fn prespawn_workflow_wasm(
    workflow: WorkflowConfigVerified,
    engines: &Engines,
    workflows_lock_extension_leeway: Duration,
) -> Result<(WorkerCompiled, ComponentConfig, FrameFilesToSourceContent), anyhow::Error> {
    let component_id = workflow.component_id().clone();
    assert!(component_id.component_type == ComponentType::Workflow);
    debug!("Instantiating workflow");
    trace!(?workflow, "Full configuration");
    let engine = engines.workflow_engine.clone();
    let runnable_component =
        RunnableComponent::new(&workflow.wasm_path, &engine, component_id.component_type)?;
    let wit = runnable_component.wasm_component.wit();

    WorkerCompiled::new_workflow(
        runnable_component,
        engine,
        workflow,
        wit,
        workflows_lock_extension_leeway,
    )
    .with_context(|| format!("cannot compile {component_id}"))
}

#[instrument(level = "debug", skip_all, fields(
    executor_id = %workflow_js.exec_config.executor_id,
    component_id = %workflow_js.exec_config.component_id,
    wasm_path = ?workflow_js.wasm_path,
))]
fn prespawn_workflow_js(
    workflow_js: WorkflowJsConfigVerified,
    engines: &Engines,
    runnable_component: RunnableComponent,
    workflows_lock_extension_leeway: Duration,
) -> Result<(WorkerCompiled, ComponentConfig, FrameFilesToSourceContent), anyhow::Error> {
    let component_id = workflow_js.component_id().clone();
    assert!(component_id.component_type == ComponentType::Workflow);
    let engine = engines.workflow_engine.clone();

    let replay_inner = WorkflowWorkerCompiled::new_with_config(
        runnable_component.clone(),
        replay_workflow_config(&component_id),
        engine.clone(),
        Now.clone_box(),
    )
    .with_context(|| format!("cannot compile replay JS workflow runtime for {component_id}"))?;
    let replay_compiled = WorkflowJsWorkerCompiled::new(
        replay_inner,
        workflow_js.js_source.clone(),
        workflow_js.js_file_name.clone(),
        &workflow_js.ffqn,
        workflow_js.params.clone(),
        workflow_js.return_type.clone(),
    )
    .with_context(|| format!("cannot create replay JS workflow worker for {component_id}"))?;

    let inner = WorkflowWorkerCompiled::new_with_config(
        runnable_component,
        workflow_js.workflow_config,
        engine,
        Now.clone_box(),
    )
    .with_context(|| format!("cannot compile JS workflow runtime for {component_id}"))?;

    let worker = WorkflowJsWorkerCompiled::new(
        inner,
        workflow_js.js_source.clone(),
        workflow_js.js_file_name.clone(),
        &workflow_js.ffqn,
        workflow_js.params,
        workflow_js.return_type,
    )
    .with_context(|| format!("cannot create JS workflow worker for {component_id}"))?;
    let wit = worker.wit();
    Ok(WorkerCompiled::new_js_workflow(
        worker,
        replay_compiled,
        workflow_js.exec_config,
        workflow_js.logs_store_min_level,
        workflows_lock_extension_leeway,
        wit,
        workflow_js.js_source,
        workflow_js.js_file_name,
    ))
}

/// Replay-purposed [`WorkflowConfig`]: Interrupt strategy, persisted backtraces, stubbed WASI,
/// no lock extension, no subscription interruption, no fuel limit.
fn replay_workflow_config(component_id: &ComponentId) -> WorkflowConfig {
    WorkflowConfig {
        component_id: component_id.clone(),
        join_next_blocking_strategy: JoinNextBlockingStrategy::Interrupt,
        backtrace_persist: true,
        stub_wasi: true,
        fuel: None,
        lock_extension: None, // does not matter for the `Interrupt` strategy.
        subscription_interruption: None, // does not matter for the `Interrupt` strategy.
    }
}

struct WorkflowWorkerCompiledWithConfig {
    worker: WorkflowWorkerCompiled,
    workflows_lock_extension_leeway: Duration,
    /// Replay-purposed compiled worker (Interrupt strategy, stubbed WASI, persisted backtraces).
    /// Linked alongside the production worker; spawned into a long-lived [`ReplayWorker`].
    replay_compiled: WorkflowWorkerCompiled,
}

struct WorkflowJsWorkerCompiledWithConfig {
    worker: WorkflowJsWorkerCompiled,
    workflows_lock_extension_leeway: Duration,
    replay_compiled: WorkflowJsWorkerCompiled,
}

enum CompiledWorkerKind {
    ActivityWasm(Box<ActivityWorkerCompiled>),
    ActivityJs(Box<ActivityJsWorkerCompiled>),
    ActivityExec(Box<ActivityExecWorkerCompiled>),
    WorkflowWasm(Box<WorkflowWorkerCompiledWithConfig>),
    WorkflowJs(Box<WorkflowJsWorkerCompiledWithConfig>),
}

struct WorkerCompiled {
    worker: CompiledWorkerKind,
    exec_config: ExecConfig,
    logs_store_min_level: Option<LogLevel>,
}

impl WorkerCompiled {
    fn new_activity(
        worker: ActivityWorkerCompiled,
        exec_config: ExecConfig,
        wit: String,
        logs_store_min_level: Option<LogLevel>,
    ) -> (WorkerCompiled, ComponentConfig) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            wit_origin: WitOrigin::Wasm,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::ActivityWasm(Box::new(worker)),
                exec_config,
                logs_store_min_level,
            },
            component,
        )
    }

    fn new_js_activity(
        worker: ActivityJsWorkerCompiled,
        exec_config: ExecConfig,
        wit: String,
        logs_store_min_level: Option<LogLevel>,
        frame_files: FrameFilesToSourceContent, // to be served by GetBacktraceSource
    ) -> (WorkerCompiled, ComponentConfig, FrameFilesToSourceContent) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            wit_origin: WitOrigin::Synthesized,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::ActivityJs(Box::new(worker)),
                exec_config,
                logs_store_min_level,
            },
            component,
            frame_files,
        )
    }

    fn new_exec_activity(
        worker: ActivityExecWorkerCompiled,
        exec_config: ExecConfig,
        wit: String,
        logs_store_min_level: Option<LogLevel>,
    ) -> (WorkerCompiled, ComponentConfig) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: vec![],
            wit,
            wit_origin: WitOrigin::Synthesized,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::ActivityExec(Box::new(worker)),
                exec_config,
                logs_store_min_level,
            },
            component,
        )
    }

    fn new_workflow(
        runnable_component: RunnableComponent,
        engine: Arc<Engine>,
        workflow: WorkflowConfigVerified,
        wit: String,
        workflows_lock_extension_leeway: Duration,
    ) -> Result<
        (WorkerCompiled, ComponentConfig, FrameFilesToSourceContent),
        utils::wasm_tools::DecodeError,
    > {
        let replay_compiled = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
            replay_workflow_config(&workflow.workflow_config.component_id),
            engine.clone(),
            Now.clone_box(),
        )?;
        let worker = WorkflowWorkerCompiled::new_with_config(
            runnable_component,
            workflow.workflow_config,
            engine,
            Now.clone_box(),
        )?;
        let component = ComponentConfig {
            component_id: workflow.exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            wit_origin: WitOrigin::Wasm,
        };
        Ok((
            WorkerCompiled {
                worker: CompiledWorkerKind::WorkflowWasm(Box::new(
                    WorkflowWorkerCompiledWithConfig {
                        worker,
                        workflows_lock_extension_leeway,
                        replay_compiled,
                    },
                )),
                exec_config: workflow.exec_config,
                logs_store_min_level: workflow.logs_store_min_level,
            },
            component,
            workflow.frame_files_to_sources,
        ))
    }

    #[expect(clippy::too_many_arguments)]
    fn new_js_workflow(
        worker: WorkflowJsWorkerCompiled,
        replay_compiled: WorkflowJsWorkerCompiled,
        exec_config: ExecConfig,
        logs_store_min_level: Option<LogLevel>,
        workflows_lock_extension_leeway: Duration,
        wit: String,
        js_source: String,
        js_file_name: String,
    ) -> (WorkerCompiled, ComponentConfig, FrameFilesToSourceContent) {
        let frame_files = WorkflowJsConfigVerified::frame_sources(js_file_name, js_source);
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            wit_origin: WitOrigin::Synthesized,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::WorkflowJs(Box::new(
                    WorkflowJsWorkerCompiledWithConfig {
                        worker,
                        workflows_lock_extension_leeway,
                        replay_compiled,
                    },
                )),
                exec_config,
                logs_store_min_level,
            },
            component,
            frame_files,
        )
    }

    #[instrument(skip_all, fields(component_id = %self.exec_config.component_id))]
    fn link(self, fn_registry: &Arc<dyn FunctionRegistry>) -> Result<WorkerLinked, anyhow::Error> {
        Ok(WorkerLinked {
            worker: match self.worker {
                CompiledWorkerKind::ActivityWasm(activity) => {
                    LinkedWorkerKind::ActivityWasm(activity)
                }
                CompiledWorkerKind::ActivityJs(js_activity) => {
                    LinkedWorkerKind::ActivityJs(js_activity)
                }
                CompiledWorkerKind::ActivityExec(exec_activity) => {
                    LinkedWorkerKind::ActivityExec(exec_activity)
                }
                CompiledWorkerKind::WorkflowWasm(workflow_compiled) => {
                    LinkedWorkerKind::WorkflowWasm(Box::new(WorkflowWorkerLinkedWithConfig {
                        worker: workflow_compiled.worker.link(fn_registry.clone())?,
                        workflows_lock_extension_leeway: workflow_compiled
                            .workflows_lock_extension_leeway,
                        replay_linked: workflow_compiled
                            .replay_compiled
                            .link(fn_registry.clone())?,
                    }))
                }
                CompiledWorkerKind::WorkflowJs(workflow_js_compiled) => {
                    LinkedWorkerKind::WorkflowJs(Box::new(WorkflowJsWorkerLinkedWithConfig {
                        worker: workflow_js_compiled.worker.link(fn_registry.clone())?,
                        workflows_lock_extension_leeway: workflow_js_compiled
                            .workflows_lock_extension_leeway,
                        replay_linked: workflow_js_compiled
                            .replay_compiled
                            .link(fn_registry.clone())?,
                    }))
                }
            },
            exec_config: self.exec_config,
            logs_store_min_level: self.logs_store_min_level,
        })
    }
}

struct WorkflowWorkerLinkedWithConfig {
    worker: WorkflowWorkerLinked,
    workflows_lock_extension_leeway: Duration,
    replay_linked: WorkflowWorkerLinked,
}

struct WorkflowJsWorkerLinkedWithConfig {
    worker: WorkflowJsWorkerLinked,
    workflows_lock_extension_leeway: Duration,
    replay_linked: WorkflowJsWorkerLinked,
}

enum LinkedWorkerKind {
    ActivityWasm(Box<ActivityWorkerCompiled>),
    ActivityJs(Box<ActivityJsWorkerCompiled>),
    ActivityExec(Box<ActivityExecWorkerCompiled>),
    WorkflowWasm(Box<WorkflowWorkerLinkedWithConfig>),
    WorkflowJs(Box<WorkflowJsWorkerLinkedWithConfig>),
    Cron(Box<ScheduleWorkerConfig>),
}

/// Configuration carried through the pipeline to construct a [`ScheduleWorker`] at spawn time.
struct ScheduleWorkerConfig {
    component_id: ComponentId,
    target_ffqn: FunctionFqn,
    target_component_id: ComponentId,
    params: Params,
    cron_schedule: CronOrOnce,
}

pub(crate) struct WorkerLinked {
    worker: LinkedWorkerKind,
    exec_config: ExecConfig,
    logs_store_min_level: Option<LogLevel>,
}
impl WorkerLinked {
    pub(crate) fn spawn(
        self,
        deployment_id: DeploymentId,
        db_pool: &Arc<dyn DbPool>,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
    ) -> (ExecutorTaskHandle, Option<(ComponentId, ReplayWorker)>) {
        let logs_storage_config = self.logs_store_min_level.map(|min_level| LogStrageConfig {
            min_level,
            log_sender: log_forwarder_sender.clone(),
        });
        let mut replay_entry: Option<(ComponentId, ReplayWorker)> = None;
        let worker: Arc<dyn Worker> = match self.worker {
            LinkedWorkerKind::ActivityWasm(activity_compiled) => {
                Arc::from(activity_compiled.into_worker(
                    cancel_registry,
                    log_forwarder_sender,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::ActivityJs(js_activity_compiled) => {
                Arc::from(js_activity_compiled.into_worker(
                    cancel_registry,
                    log_forwarder_sender,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::ActivityExec(exec_activity_compiled) => {
                Arc::from(exec_activity_compiled.into_worker(
                    cancel_registry,
                    log_forwarder_sender,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::WorkflowWasm(workflow_linked) => {
                let factory = DeadlineTrackerFactoryTokio::new(
                    workflow_linked.workflows_lock_extension_leeway,
                    Now.clone_box(),
                );
                let replay_worker = Arc::new(workflow_linked.replay_linked.into_worker(
                    deployment_id,
                    db_pool.clone(),
                    Arc::new(DeadlineTrackerFactoryForReplay {}),
                    CancelRegistry::new(),
                    logs_storage_config.clone(),
                ));
                replay_entry = Some((
                    self.exec_config.component_id.clone(),
                    ReplayWorker::Wasm(replay_worker),
                ));
                Arc::from(workflow_linked.worker.into_worker(
                    deployment_id,
                    db_pool.clone(),
                    Arc::new(factory),
                    cancel_registry,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::WorkflowJs(workflow_js_linked) => {
                let factory = DeadlineTrackerFactoryTokio::new(
                    workflow_js_linked.workflows_lock_extension_leeway,
                    Now.clone_box(),
                );
                let replay_worker = Arc::new(workflow_js_linked.replay_linked.into_worker(
                    deployment_id,
                    db_pool.clone(),
                    Arc::new(DeadlineTrackerFactoryForReplay {}),
                    CancelRegistry::new(),
                    logs_storage_config.clone(),
                ));
                replay_entry = Some((
                    self.exec_config.component_id.clone(),
                    ReplayWorker::Js(replay_worker),
                ));
                Arc::from(workflow_js_linked.worker.into_worker(
                    deployment_id,
                    db_pool.clone(),
                    Arc::new(factory),
                    cancel_registry,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::Cron(config) => {
                let config = *config;
                Arc::from(CronWorker::new(
                    config.component_id,
                    config.target_ffqn,
                    config.target_component_id,
                    config.params,
                    config.cron_schedule,
                    deployment_id,
                    db_pool.clone(),
                    Now.clone_box(),
                ))
            }
        };
        let handle = ExecTask::spawn_new(
            deployment_id,
            worker,
            self.exec_config,
            Now.clone_box(),
            db_pool.clone(),
            TokioSleep,
        );
        (handle, replay_entry)
    }
}

pub(crate) fn gen_trace_id() -> String {
    use rand::SeedableRng;
    let mut rng = rand::rngs::SmallRng::from_os_rng();
    (0..5)
        .map(|_| rand::Rng::random_range(&mut rng, b'a'..=b'z') as char)
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use crate::{
        command::server::{
            DeploymentRunnable, DeploymentVerified, PrepareDirsParams, ServerCompiledLinked,
            ServerVerified, VerifyParams, compile_activity_inline, create_engines,
            deployment_verify_config, prepare_dirs,
        },
        config::config_holder::{ConfigHolder, load_deployment_canonical},
    };
    use concepts::{ComponentId, FunctionFqn, prefixed_ulid::DeploymentId};
    use concepts::{
        ComponentType, ParameterType, ReturnType, StrVariant,
        component_id::{ComponentDigest, Digest},
    };
    use directories::BaseDirs;
    use rstest::rstest;
    use std::path::PathBuf;
    use tokio::sync::watch;

    fn get_workspace_dir() -> PathBuf {
        PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
    }

    #[test]
    fn wit_includes_obelisk_extension_packages() {
        let component_id = ComponentId::new(
            ComponentType::ActivityStub,
            StrVariant::Static("test_stub"),
            ComponentDigest(Digest([0u8; 32])),
        )
        .unwrap();
        let ffqn = FunctionFqn::new_static("ns:pkg/ifc", "my-fn");
        let params = vec![ParameterType {
            name: StrVariant::Static("id"),
            type_wrapper: val_json::type_wrapper::TypeWrapper::U64,
            wit_type: StrVariant::Static("u64"),
        }];

        let ret_type = {
            let tw = val_json::type_wrapper::parse_wit_type("result<string, string>").unwrap();
            let ReturnType::Extendable(rt) =
                ReturnType::detect(tw, StrVariant::Static("result<string, string>"))
            else {
                unreachable!()
            };
            rt
        };

        let config = compile_activity_inline(component_id, &ffqn, &params, &ret_type)
            .expect("compile must succeed");

        // The synthesized WIT must re-parse: the world lives in a dedicated `root:component`
        // package so the extension packages do not form a package dependency cycle.
        let group =
            wit_parser::UnresolvedPackageGroup::parse(std::path::PathBuf::new(), &config.wit)
                .expect("synthesized WIT must parse");
        wit_parser::Resolve::new()
            .push_group(group)
            .expect("synthesized WIT must not contain a package dependency cycle");

        // The rebuilt WIT includes extension packages absent from the raw synthesized string.
        insta::assert_snapshot!(config.wit);
    }

    #[rstest]
    #[tokio::test]
    async fn server_verify(
        #[values("server-sqlite.toml", "server-postgres.toml")] server_toml: &'static str,
        #[values("obelisk-testing-wasm-local.toml", "obelisk-testing-wasm-oci.toml")]
        deployment_toml: &'static str,
    ) -> Result<(), anyhow::Error> {
        test_utils::set_up();

        let workspace = get_workspace_dir();
        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder =
            ConfigHolder::new(project_dirs, base_dirs, Some(workspace.join(server_toml)))?;
        let config = config_holder.load_config().await?;

        let fixture = crate::command::test_support::target_aware_deployment_fixture(
            &workspace,
            deployment_toml,
        )
        .await?;
        let deployment = load_deployment_canonical(fixture.path()).await?;

        let prepared_dirs = prepare_dirs(
            &config,
            &PrepareDirsParams::default(),
            &config_holder.path_prefixes,
        )
        .await?;

        let (_termination_sender, mut termination_watcher) = watch::channel(());
        let engines = create_engines(&config, &prepared_dirs)?;
        let server_verified = Box::pin(ServerVerified::new(engines, config)).await?;
        let params = VerifyParams::default();
        let webui_enabled = None;

        // Verify deployment. The current disk-authored canonical holds internal absolute
        // paths, so it resolves to itself without a CAS.
        let deployment =
            DeploymentRunnable::resolve(deployment, None, &prepared_dirs.wasm_cache_dir).await?;
        let deployment_verified = Box::pin(DeploymentVerified::fetch_and_verify_all(
            deployment,
            server_verified.http_servers,
            prepared_dirs.wasm_cache_dir.clone(),
            prepared_dirs.metadata_dir.clone(),
            params.ignore_missing_env_vars,
            server_verified.global_backtrace_persist,
            server_verified.global_executor_instance_limiter,
            server_verified.fuel,
            &mut termination_watcher,
            server_verified.database_subscription_interruption,
            webui_enabled,
        ))
        .await?;

        let _compiled_and_linked = ServerCompiledLinked::new(
            DeploymentId::generate(),
            deployment_verified,
            server_verified.launch,
            &mut termination_watcher,
            params.suppress_type_checking_errors,
            params.suppress_linking_errors,
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn deployment_verify_rejects_digest_shared_across_component_types()
    -> Result<(), anyhow::Error> {
        test_utils::set_up();

        let workspace = get_workspace_dir();
        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(
            project_dirs,
            base_dirs,
            Some(workspace.join("server-sqlite.toml")),
        )?;
        let config = config_holder.load_config().await?;

        let fixture = crate::command::test_support::target_aware_deployment_fixture(
            &workspace,
            "obelisk-testing-wasm-local.toml",
        )
        .await?;
        let mut deployment = load_deployment_canonical(fixture.path()).await?;
        let shared_digest: ComponentDigest =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap();
        deployment.activities_wasm[0].component_digest = Some(shared_digest.clone());
        deployment.workflows_wasm[0].component_digest = Some(shared_digest);

        let prepared_dirs = prepare_dirs(
            &config,
            &PrepareDirsParams::default(),
            &config_holder.path_prefixes,
        )
        .await?;

        let (_termination_sender, mut termination_watcher) = watch::channel(());
        let engines = create_engines(&config, &prepared_dirs)?;
        let server_verified = Box::pin(ServerVerified::new(engines, config)).await?;
        let err = deployment_verify_config(
            &server_verified,
            &prepared_dirs,
            deployment,
            None,
            VerifyParams::default(),
            &mut termination_watcher,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("shared between component types"));
        Ok(())
    }

    mod submit_package {
        use crate::command::server::{
            MAX_DEPLOYMENT_FILE_BYTES, SuppliedFile, compute_content_digest,
            validate_submit_package,
        };
        use crate::config::manifest::{DeploymentFileRef, ManifestFieldRef};
        use concepts::ContentDigest;
        use hashbrown::HashSet;

        fn file_ref(path: &str, content: &[u8]) -> (DeploymentFileRef, ContentDigest) {
            let digest = compute_content_digest(content);
            (
                DeploymentFileRef {
                    path: path.to_string(),
                    digest: digest.clone(),
                    field: ManifestFieldRef {
                        section: "activity_wasm".to_string(),
                        component_name: Some("c".to_string()),
                        field_path: "activity_wasm[name=c].location".to_string(),
                    },
                },
                digest,
            )
        }

        #[test]
        fn attached_blob_is_scheduled_for_write() {
            let (expected, _digest) = file_ref("a.wasm", b"\0asm-bytes");
            let supplied = vec![SuppliedFile {
                path: "a.wasm".to_string(),
                supplied_digest: None,
                content: b"\0asm-bytes".to_vec(),
            }];
            let to_write = validate_submit_package(
                &[expected],
                supplied,
                &HashSet::new(),
                MAX_DEPLOYMENT_FILE_BYTES,
            )
            .expect("complete package");
            assert_eq!(to_write.len(), 1);
            assert_eq!(to_write[0].path, "a.wasm");
        }

        #[test]
        fn cas_hit_needs_no_attached_blob_and_no_write() {
            let (expected, digest) = file_ref("a.wasm", b"\0asm-bytes");
            let cas_present = HashSet::from_iter([digest]);
            let to_write = validate_submit_package(
                &[expected],
                Vec::new(),
                &cas_present,
                MAX_DEPLOYMENT_FILE_BYTES,
            )
            .expect("CAS hit is complete");
            assert!(to_write.is_empty());
        }

        #[test]
        fn missing_blob_reports_field_context() {
            let (expected, digest) = file_ref("a.wasm", b"\0asm-bytes");
            let err = validate_submit_package(
                &[expected],
                Vec::new(),
                &HashSet::new(),
                MAX_DEPLOYMENT_FILE_BYTES,
            )
            .expect_err("missing blob");
            assert_eq!(err.missing_files.len(), 1);
            assert_eq!(err.missing_files[0].section, "activity_wasm");
            assert_eq!(
                err.missing_files[0].digest.as_deref(),
                Some(digest.to_string().as_str())
            );
        }

        #[test]
        fn unexpected_blob_is_rejected() {
            let (expected, _digest) = file_ref("a.wasm", b"\0asm-bytes");
            let supplied = vec![SuppliedFile {
                path: "stray.wasm".to_string(),
                supplied_digest: None,
                content: b"unexpected".to_vec(),
            }];
            // The expected file is missing too, but the unexpected blob is still reported.
            let err = validate_submit_package(
                &[expected],
                supplied,
                &HashSet::new(),
                MAX_DEPLOYMENT_FILE_BYTES,
            )
            .expect_err("unexpected blob");
            assert_eq!(err.unexpected_files.len(), 1);
            assert_eq!(err.unexpected_files[0].path.as_deref(), Some("stray.wasm"));
        }

        #[test]
        fn digest_mismatch_is_rejected() {
            let (expected, _digest) = file_ref("a.wasm", b"\0asm-bytes");
            let supplied = vec![SuppliedFile {
                path: "a.wasm".to_string(),
                supplied_digest: Some(
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                        .to_string(),
                ),
                content: b"\0asm-bytes".to_vec(),
            }];
            let err = validate_submit_package(
                &[expected],
                supplied,
                &HashSet::new(),
                MAX_DEPLOYMENT_FILE_BYTES,
            )
            .expect_err("digest mismatch");
            assert_eq!(err.digest_mismatches.len(), 1);
            assert_eq!(
                err.digest_mismatches[0].actual_digest,
                compute_content_digest(b"\0asm-bytes").to_string()
            );
        }

        #[test]
        fn oversized_blob_is_rejected() {
            let (expected, _digest) = file_ref("a.wasm", b"\0asm-bytes");
            let supplied = vec![SuppliedFile {
                path: "a.wasm".to_string(),
                supplied_digest: None,
                content: b"\0asm-bytes".to_vec(),
            }];
            let err = validate_submit_package(&[expected], supplied, &HashSet::new(), 1)
                .expect_err("oversized blob");
            assert_eq!(err.oversized_files.len(), 1);
        }
    }
}
