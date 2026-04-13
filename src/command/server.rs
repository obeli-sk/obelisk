use crate::args::Server;
use crate::args::shadow::PKG_VERSION;
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::PathPrefixes;
use crate::config::config_holder::load_deployment_toml;
use crate::config::env_var::EnvVarConfig;
use crate::config::toml::ActivitiesDirectoriesCleanupConfigToml;
use crate::config::toml::ActivitiesDirectoriesGlobalConfigToml;
use crate::config::toml::ActivityExternalConfigVerified;
use crate::config::toml::ActivityJsConfigVerified;
use crate::config::toml::ActivityStubConfigVerified;
use crate::config::toml::ActivityStubExtConfigVerified;
use crate::config::toml::ActivityStubExtInlineConfigVerified;
use crate::config::toml::ActivityWasmConfigVerified;
use crate::config::toml::CancelWatcherTomlConfig;
use crate::config::toml::ComponentCommon;
use crate::config::toml::ComponentStdOutputToml;
use crate::config::toml::ConfigName;
use crate::config::toml::DatabaseConfigToml;
use crate::config::toml::DeploymentCanonical;
use crate::config::toml::DeploymentTomlValidated;
use crate::config::toml::LogLevelToml;
use crate::config::toml::SQLITE_FILE_NAME;
use crate::config::toml::ServerConfigToml;
use crate::config::toml::TimersWatcherTomlConfig;
use crate::config::toml::WasmtimeAllocatorConfig;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::WorkflowJsConfigVerified;
use crate::config::toml::cron::CronConfigVerified;
use crate::config::toml::webhook;
use crate::config::toml::webhook::HttpServer;
use crate::config::toml::webhook::WebhookJsConfigVerified;
use crate::config::toml::webhook::WebhookRoute;
use crate::config::toml::webhook::WebhookRouteVerified;
use crate::config::toml::webhook::WebhookWasmComponentConfigVerified;
use crate::config::toml::{AllowedHostToml, MethodsInput, MethodsInputStar};
use crate::init;
use crate::init::Guard;
use crate::project_dirs;
use crate::server::grpc_server::GrpcServer;
use crate::server::web_api_server::WebApiState;
use crate::server::web_api_server::app_router;
use anyhow::Context;
use anyhow::bail;
use concepts::ComponentId;
use concepts::ComponentType;
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
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::CreateRequest;
use concepts::storage::DbErrorWrite;
use concepts::storage::DbErrorWriteNonRetriable;
use concepts::storage::DbExternalApi;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use concepts::storage::LogInfoAppendRow;
use concepts::storage::LogLevel;
use concepts::storage::{DeploymentRecord, DeploymentStatus};
use concepts::time::ClockFn;
use concepts::time::Now;
use concepts::time::TokioSleep;
use db_sqlite::sqlite_dao::SqlitePool;
use directories::BaseDirs;
use directories::ProjectDirs;
use executor::AbortOnDropHandle;
use executor::executor::ExecutorTaskHandle;
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
use std::fmt::Debug;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
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
use wasm_workers::preopens_cleaner::PreopensCleaner;
use wasm_workers::registry::ComponentConfig;
use wasm_workers::registry::ComponentConfigImportable;
use wasm_workers::registry::ComponentConfigRegistry;
use wasm_workers::registry::ComponentConfigRegistryRO;
use wasm_workers::registry::JsWorkflowReplayInfo;
use wasm_workers::registry::WitOrigin;
use wasm_workers::registry::WorkflowReplayInfo;
use wasm_workers::webhook::webhook_registry::WebhookRegistry;
use wasm_workers::webhook::webhook_trigger;
use wasm_workers::webhook::webhook_trigger::MethodAwareRouter;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointCompiled;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointConfig;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointInstanceLinked;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointJsConfig;
use wasm_workers::webhook::webhook_trigger::WebhookServerState;
use wasm_workers::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
use wasm_workers::workflow::host_exports::history_event_schedule_at_from_wast_val;
use wasm_workers::workflow::workflow_js_worker::WorkflowJsWorkerCompiled;
use wasm_workers::workflow::workflow_js_worker::WorkflowJsWorkerLinked;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerCompiled;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerLinked;
use wasmtime::Engine;

pub(crate) struct DeploymentContext {
    pub(crate) deployment_id: DeploymentId,
    pub(crate) component_registry_ro: wasm_workers::registry::ComponentConfigRegistryRO,
    pub(crate) exec_task_handles: Vec<ExecutorTaskHandle>,
    pub(crate) closed: bool,
}

pub(crate) type DeploymentContextHandle = Arc<tokio::sync::RwLock<DeploymentContext>>;

const EPOCH_MILLIS: u64 = 10;
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
                suppress_type_checking_errors,
            } => {
                Box::pin(run(
                    project_dirs(),
                    BaseDirs::new(),
                    server_config,
                    deployment,
                    deployment_empty,
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
            "value": schedule_at,
            "type": schedule_at_type_wrapper
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
    params: RunParams,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, server_config)?;
    let config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&config)?;
    let (deployment_toml, path_prefixes) = if let Some(deployment_path) = deployment {
        let toml = load_deployment_toml(deployment_path).await?;
        (Some(toml), config_holder.path_prefixes)
    } else if deployment_empty {
        (
            Some(DeploymentTomlValidated::default()),
            config_holder.path_prefixes,
        )
    } else {
        (None, config_holder.path_prefixes)
    };

    let (termination_sender, termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });

    let prepared_dirs = prepare_dirs(&config, &params.dir_params, &path_prefixes).await?;
    Box::pin(run_internal(
        config,
        deployment_toml,
        Arc::new(path_prefixes),
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
    let path_prefixes = config_holder.path_prefixes;
    let deployment_toml_opt = if let Some(deployment_path) = deployment {
        Some(load_deployment_toml(deployment_path).await?)
    } else {
        None
    };
    let path_prefixes = Arc::new(path_prefixes);
    let deployment = if let Some(toml) = deployment_toml_opt {
        crate::config::toml::resolve_local_refs_to_canonical(&toml).await?
    } else {
        get_deployment_canonical_from_db(&config.database, &path_prefixes).await?
    };
    let deployment_id = DeploymentId::generate();
    let (termination_sender, mut termination_watcher) = watch::channel(());
    let prepared_dirs = prepare_dirs(&config, &verify_params.dir_params, &path_prefixes).await?;
    let engines = create_engines(&config, &prepared_dirs)?;
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    if !skip_db {
        verify_db_schema(&config.database, &path_prefixes).await?;
    }
    let server_verified = Box::pin(server_verify(config, engines, path_prefixes)).await?;
    deployment_verify_config_compile_link(
        server_verified,
        &prepared_dirs,
        deployment,
        deployment_id,
        verify_params,
        &mut termination_watcher,
    )
    .await?;
    Ok(())
}

fn ignore_not_found(err: std::io::Error) -> Result<(), std::io::Error> {
    if err.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(err)
    }
}

async fn verify_db_schema(
    db_config_toml: &DatabaseConfigToml,
    path_prefixes: &PathPrefixes,
) -> Result<(), anyhow::Error> {
    match db_config_toml {
        DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
            let db_dir = sqlite_config_toml.get_sqlite_dir(path_prefixes).await?;
            let sqlite_config = sqlite_config_toml.as_sqlite_config();
            let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
            if sqlite_file.exists() {
                let db_pool = SqlitePool::new(&sqlite_file, sqlite_config)
                    .await
                    .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?;
                db_pool.close().await;
                info!("SQLite database schema verified");
            } else {
                info!("SQLite database does not exist yet, skipping schema verification");
            }
        }
        DatabaseConfigToml::Postgres(postgres_config_toml) => {
            let db_pool = db_postgres::postgres_dao::PostgresPool::new(
                postgres_config_toml.as_config()?,
                postgres_config_toml.as_provision_policy(),
            )
            .await
            .context("cannot initialize postgres connection pool")?;
            db_pool.close().await;
            info!("PostgreSQL database schema verified");
        }
    }
    Ok(())
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
    let metadata_dir = wasm_cache_dir.join("metadata");
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
    path_prefixes: Arc<PathPrefixes>,
) -> Result<ServerVerified, anyhow::Error> {
    info!("Verifying server configuration");
    // Check obelisk-version compatibility if specified
    if let Some(version_req_str) = &config.obelisk_version {
        let version_req = semver::VersionReq::parse(version_req_str)
            .with_context(|| format!("Invalid obelisk-version requirement: {version_req_str}"))?;
        let current_version = semver::Version::parse(PKG_VERSION)
            .with_context(|| format!("Invalid current version: {PKG_VERSION}",))?;
        if !version_req.matches(&current_version) {
            bail!(
                "Obelisk version mismatch: config requires {version_req_str}, but running version is {PKG_VERSION}",
            );
        }
        info!("Obelisk version {PKG_VERSION} matches requirement {version_req_str}",);
    }
    // Verify server
    Box::pin(ServerVerified::new(engines, config, path_prefixes)).await
}

/// Verifies configuration without database schema check.
#[instrument(skip_all, fields(%deployment_id))]
pub(crate) async fn deployment_verify_config_compile_link(
    server_verified: ServerVerified,
    prepared_dirs: &PreparedDirs,
    deployment: DeploymentCanonical,
    deployment_id: DeploymentId,
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<ServerCompiledLinked, anyhow::Error> {
    info!("Verifying deployment configuration, compiling WASM components");
    // Verify deployment
    let config = Box::pin(ConfigVerified::fetch_and_verify_all(
        deployment,
        server_verified.http_servers,
        prepared_dirs.wasm_cache_dir.clone(),
        prepared_dirs.metadata_dir.clone(),
        params.ignore_missing_env_vars,
        server_verified.global_backtrace_persist,
        server_verified.launch.parent_preopen_dir.clone(),
        server_verified.global_executor_instance_limiter,
        server_verified.fuel,
        termination_watcher,
        server_verified.database_subscription_interruption,
        server_verified.api_addr_if_webui_enabled,
    ))
    .await?;
    debug!("Verified config: {config:#?}");

    let compiled_and_linked = ServerCompiledLinked::new(
        deployment_id,
        config,
        server_verified.launch,
        termination_watcher,
        params.suppress_type_checking_errors,
    )
    .await?;
    if compiled_and_linked.supressed_errors.is_none() {
        info!("Obelisk configuration was verified");
    } else {
        warn!("Obelisk configuration was verified with supressed errors");
    }
    Ok(compiled_and_linked)
}

/// Look up the current deployment from the database.
/// Prefers Enqueued over Active; errors if neither exists.
async fn get_deployment_canonical_from_db(
    database: &DatabaseConfigToml,
    path_prefixes: &PathPrefixes,
) -> anyhow::Result<DeploymentCanonical> {
    let record = match database {
        DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
            let db_dir = sqlite_config_toml.get_sqlite_dir(path_prefixes).await?;
            let sqlite_config = sqlite_config_toml.as_sqlite_config();
            let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
            let pool = SqlitePool::new(&sqlite_file, sqlite_config)
                .await
                .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?;
            let conn = pool
                .external_api_conn()
                .await
                .context("cannot get db connection for deployment lookup")?;
            let record = conn
                .get_current_deployment()
                .await
                .context("cannot query current deployment")?;
            pool.close().await;
            record
        }
        DatabaseConfigToml::Postgres(postgres_config_toml) => {
            let pool = db_postgres::postgres_dao::PostgresPool::new(
                postgres_config_toml.as_config()?,
                postgres_config_toml.as_provision_policy(),
            )
            .await
            .context("cannot initialize postgres connection pool")?;
            let conn = pool
                .external_api_conn()
                .await
                .context("cannot get db connection for deployment lookup")?;
            let record = conn
                .get_current_deployment()
                .await
                .context("cannot query current deployment")?;
            pool.close().await;
            record
        }
    };

    let record = record
        .context("no Enqueued or Active deployment found in database; provide --deployment")?;

    serde_json::from_str(&record.config_json).with_context(|| {
        format!(
            "cannot parse deployment config_json for {:?}",
            record.deployment_id
        )
    })
}

async fn insert_and_activate_deployment(
    db_pool: &dyn concepts::storage::DbPool,
    deployment_id: DeploymentId,
    config_json: String,
) -> anyhow::Result<()> {
    use chrono::Utc;
    use concepts::storage::{DeploymentRecord, DeploymentStatus};

    let api_conn = db_pool
        .external_api_conn()
        .await
        .context("cannot get external api connection for deployment activation")?;

    let now = Utc::now();
    api_conn
        .insert_deployment(DeploymentRecord {
            deployment_id,
            created_at: now,
            last_active_at: None,
            status: DeploymentStatus::Inactive,
            config_json,
            obelisk_version: PKG_VERSION.to_string(),
            created_by: Some("server".to_string()),
        })
        .await
        .context("cannot insert deployment")?;
    api_conn
        .activate_deployment(deployment_id, Utc::now())
        .await
        .context("cannot activate deployment")?;
    Ok(())
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
    deployment: Option<DeploymentTomlValidated>,
    path_prefixes: Arc<PathPrefixes>,
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
                db_postgres::postgres_dao::PostgresPool::new(
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
    // Determine the deployment to compile and the active deployment_id.
    let (active_deployment_id, deployment_canonical) = if let Some(deployment_toml) = deployment {
        // --deployment or `--deployment-empty` provided: resolve, insert, and activate.
        let new_deployment_id = DeploymentId::generate();
        span.record("deployment_id", tracing::field::display(&new_deployment_id));
        let canonical = crate::config::toml::resolve_local_refs_to_canonical(&deployment_toml)
            .await
            .context("cannot resolve deployment to canonical form")?;
        let config_json = crate::config::toml::compute_config_json(&canonical);
        insert_and_activate_deployment(&*db_pool, new_deployment_id, config_json).await?;
        info!("Activated new deployment");
        (new_deployment_id, canonical)
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
            let canonical: DeploymentCanonical = serde_json::from_str(&record.config_json)
                .context("cannot parse deployment config_json")?;
            span.record(
                "deployment_id",
                tracing::field::display(&record.deployment_id),
            );
            if record.status == concepts::storage::DeploymentStatus::Enqueued {
                info!("Activated enqueued deployment");
            } else {
                info!("Using the currently active deployment");
            }

            (record.deployment_id, canonical)
        } else {
            let new_deployment_id = DeploymentId::generate();
            span.record("deployment_id", tracing::field::display(&new_deployment_id));

            info!("No deployment found in DB; starting with empty deployment");
            let config_json =
                crate::config::toml::compute_config_json(&DeploymentCanonical::default());
            insert_and_activate_deployment(&*db_pool, new_deployment_id, config_json).await?;

            (new_deployment_id, DeploymentCanonical::default())
        }
    };
    let engines = create_engines(&config, &prepared_dirs)?;
    let server_verified = server_verify(config, engines, path_prefixes).await?;
    let compiled_and_linked = Box::pin(deployment_verify_config_compile_link(
        server_verified.clone(),
        &prepared_dirs,
        deployment_canonical,
        active_deployment_id,
        VerifyParams {
            dir_params: PrepareDirsParams {
                clean_cache: params.dir_params.clean_cache,
                clean_codegen_cache: params.dir_params.clean_codegen_cache,
            },
            ignore_missing_env_vars: false,
            suppress_type_checking_errors: params.suppress_type_checking_errors,
        },
        &mut termination_watcher,
    ))
    .instrument(span.clone())
    .await?;

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

    let grpc_server = Arc::new(GrpcServer::new(
        server_init.server_verified.clone(),
        server_init.db_pool.clone(),
        termination_watcher.clone(),
        cancel_registry.clone(),
        server_init.engines.clone(),
        prepared_dirs.clone(),
        server_init.log_forwarder_sender.clone(),
        server_init.deployment_ctx.clone(),
        server_init.webhook_registry.clone(),
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
        .accept_compressed(CompressionEncoding::Gzip),
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
        engines: server_init.engines.clone(),
        log_forwarder_sender: server_init.log_forwarder_sender.clone(),
        prepared_dirs: server_init.prepared_dirs.clone(),
        webhook_registry: server_init.webhook_registry.clone(),
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
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
    build_semaphore: Option<u64>,
    workflows_lock_extension_leeway: Duration,
}

impl ServerVerified {
    #[instrument(name = "ServerVerified::new", skip_all)]
    async fn new(
        engines: Engines,
        config: ServerConfigToml,
        path_prefixes: Arc<PathPrefixes>,
    ) -> Result<ServerVerified, anyhow::Error> {
        debug!("Using server toml: {config:#?}");
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
        let parent_preopen_dir = OptionFuture::from(
            config
                .activities_global_config
                .get_directories()
                .map(|dirs| dirs.get_parent_directory(&path_prefixes)),
        )
        .await
        .transpose()
        .context("error resolving `activities.directories.parent_directory`")?;
        let activities_cleanup = config
            .activities_global_config
            .get_directories()
            .and_then(ActivitiesDirectoriesGlobalConfigToml::get_cleanup);
        let build_semaphore = config.wasm_global_config.build_semaphore.into();
        let global_executor_instance_limiter = config
            .wasm_global_config
            .global_executor_instance_limiter
            .as_semaphore();
        let database_subscription_interruption = config.database.get_subscription_interruption();

        Ok(Self {
            launch: ServerVerifiedLaunch {
                engines,
                parent_preopen_dir,
                activities_cleanup,
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
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
    pub(crate) http_servers_to_webhooks_and_state: HttpServersToWebhooksAndState,
    supressed_errors: Option<String>,
    frame_files: Vec<(ComponentDigest, FrameFilesToSourceContent)>,
}

impl ServerCompiledLinked {
    async fn new(
        deployment_id: DeploymentId,
        config: ConfigVerified,
        server_verified: ServerVerifiedLaunch,
        termination_watcher: &mut watch::Receiver<()>,
        suppress_type_checking_errors: bool,
    ) -> Result<Self, anyhow::Error> {
        let linked = compile_and_link(
            &server_verified.engines,
            config.activities_wasm,
            config.activities_js,
            config.activities_stub_ext,
            config.activities_stub_ext_inline,
            config.workflows,
            config.workflows_js,
            config.webhooks_wasm_by_names,
            config.webhooks_js_by_names,
            config.crons,
            config.global_backtrace_persist,
            config.fuel,
            server_verified.build_semaphore,
            server_verified.workflows_lock_extension_leeway,
            termination_watcher,
        )
        .await?;
        if !suppress_type_checking_errors && linked.supressed_errors.is_some() {
            bail!("type checking errors detected");
        }
        let http_server_len = config.http_servers_to_webhook_names.len();
        let http_servers_to_webhooks = Self::connect_http_servers_to_webhooks(
            &config.http_servers_to_webhook_names,
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
            parent_preopen_dir: server_verified.parent_preopen_dir,
            activities_cleanup: server_verified.activities_cleanup,
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
                    Some(cron_config)
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
    server_compiled_linked: &ServerCompiledLinked,
) {
    // Persist source files to the DB so GetBacktraceSource can serve them without filesystem access.
    {
        for (component_digest, frame_files) in &server_compiled_linked.frame_files {
            for (config_key, source) in frame_files {
                // Keys starting with ".../" become suffix keys (strip "...", prepend "/").
                let (frame_key, is_suffix) = if let Some(stripped) = config_key.strip_prefix(".../")
                {
                    (format!("/{stripped}"), true)
                } else {
                    (config_key.clone(), false)
                };
                let res = conn
                    .upsert_source_file(component_digest, &frame_key, is_suffix, source)
                    .await;
                if let Err(err) = res {
                    warn!("Cannot store backtrace source {config_key:?} in DB: {err:?}");
                }
            }
        }
    }
}

/// Shared logic for submitting a deployment (used by both gRPC and web API).
/// Parses, verifies, and inserts the deployment record.
#[instrument(skip_all)]
pub(crate) async fn submit_deployment(
    server_verified: ServerVerified,
    config_json: &str,
    verify: bool,
    created_by: Option<String>,
    prepared_dirs: &PreparedDirs,
    db_pool: Arc<dyn DbPool>,
    termination_watcher: &mut watch::Receiver<()>,
) -> anyhow::Result<DeploymentId> {
    let deployment: DeploymentCanonical =
        serde_json::from_str(config_json).with_context(|| "cannot parse config_json")?;

    let canonical_config = crate::config::toml::compute_config_json(&deployment);

    let verify_deployment_id = DeploymentId::generate();
    let server_compiled = deployment_verify_config_compile_link(
        server_verified,
        prepared_dirs,
        deployment,
        verify_deployment_id,
        VerifyParams {
            dir_params: PrepareDirsParams {
                clean_cache: false,
                clean_codegen_cache: false,
            },
            ignore_missing_env_vars: !verify,
            suppress_type_checking_errors: false,
        },
        termination_watcher,
    )
    .await?;

    let deployment_id = DeploymentId::generate();
    let conn = db_pool.external_api_conn().await?;
    let now = chrono::Utc::now();

    conn.insert_deployment(DeploymentRecord {
        deployment_id,
        created_at: now,
        last_active_at: None,
        status: DeploymentStatus::Inactive,
        obelisk_version: crate::args::shadow::PKG_VERSION.to_string(),
        created_by,
        config_json: canonical_config,
    })
    .await?;

    upsert_backtrace_sources(conn.as_ref(), &server_compiled).await;

    info!(%deployment_id, "Deployment submitted");
    Ok(deployment_id)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SwitchDeploymentAction {
    HotRedeploy,
    VerifyAndStore,
    Store,
}
impl SwitchDeploymentAction {
    pub(crate) fn new(hot_redeploy: bool, verify: bool) -> SwitchDeploymentAction {
        if hot_redeploy {
            SwitchDeploymentAction::HotRedeploy
        } else if verify {
            SwitchDeploymentAction::VerifyAndStore
        } else {
            SwitchDeploymentAction::Store
        }
    }
}

#[expect(clippy::too_many_arguments)]
#[instrument(skip_all, fields(%deployment_id))]
pub(crate) async fn switch_deployment(
    server_verified: ServerVerified,
    deployment_id: DeploymentId,
    action: SwitchDeploymentAction,
    prepared_dirs: &PreparedDirs,
    db_pool: Arc<dyn DbPool>,
    termination_watcher: &mut watch::Receiver<()>,
    deployment_ctx: &DeploymentContextHandle,
    webhook_registry: &WebhookRegistry,
    cancel_registry: CancelRegistry,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
) -> Result<SwitchOutcome, SwitchError> {
    let db_conn = db_pool
        .external_api_conn()
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;

    let deployment_record = db_conn
        .get_deployment(deployment_id)
        .await
        .map_err(|e| SwitchError::Other(e.into()))?
        .ok_or(SwitchError::NotFound)?;

    let target_deployment: DeploymentCanonical =
        serde_json::from_str(&deployment_record.config_json)
            .with_context(|| "cannot parse stored deployment config")?;

    if action == SwitchDeploymentAction::HotRedeploy {
        let server_compiled_linked = deployment_verify_config_compile_link(
            server_verified,
            prepared_dirs,
            target_deployment,
            DeploymentId::generate(),
            VerifyParams {
                dir_params: PrepareDirsParams {
                    clean_cache: false,
                    clean_codegen_cache: false,
                },
                ignore_missing_env_vars: false,
                suppress_type_checking_errors: false,
            },
            termination_watcher,
        )
        .await?;
        switch_hot_redeploy(
            server_compiled_linked,
            db_conn.as_ref(),
            deployment_id,
            &db_pool,
            deployment_ctx,
            webhook_registry,
            cancel_registry,
            &log_forwarder_sender,
        )
        .await
    } else {
        if action == SwitchDeploymentAction::VerifyAndStore {
            deployment_verify_config_compile_link(
                server_verified,
                prepared_dirs,
                target_deployment,
                DeploymentId::generate(),
                VerifyParams {
                    dir_params: PrepareDirsParams {
                        clean_cache: false,
                        clean_codegen_cache: false,
                    },
                    ignore_missing_env_vars: false,
                    suppress_type_checking_errors: false,
                },
                termination_watcher,
            )
            .await?;
        }
        db_conn
            .enqueue_deployment(deployment_id)
            .await
            .map_err(|e| SwitchError::Other(e.into()))?;

        info!(%deployment_id, "Deployment enqueued for next restart");
        Ok(SwitchOutcome::RestartRequired)
    }
}

/// Write lock pretected switch to the new deployment
#[instrument(skip_all, fields(%deployment_id))]
#[expect(clippy::too_many_arguments)]
async fn switch_hot_redeploy(
    server_compiled_linked: ServerCompiledLinked,
    db_conn: &dyn DbExternalApi,
    deployment_id: DeploymentId,
    db_pool: &Arc<dyn DbPool>,
    deployment_ctx: &DeploymentContextHandle,
    webhook_registry: &WebhookRegistry,
    cancel_registry: CancelRegistry,
    log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
) -> Result<SwitchOutcome, SwitchError> {
    let mut write_guard_ctx = deployment_ctx.write().await;
    if write_guard_ctx.closed {
        return Err(SwitchError::Other(anyhow::anyhow!(
            "server is being shut down"
        )));
    }

    let old = std::mem::take(&mut write_guard_ctx.exec_task_handles);
    futures_util::future::join_all(
        old.iter()
            .map(executor::executor::ExecutorTaskHandle::close),
    )
    .await;

    webhook_registry.swap(
        server_compiled_linked
            .http_servers_to_webhooks_and_state
            .iter()
            .map(|(http_server, (_instance, state))| (http_server.name.to_string(), state.clone())),
    );

    server_compiled_linked
        .create_missing_cron_seeds(db_pool, deployment_id)
        .await?;

    let new_handles: Vec<ExecutorTaskHandle> = server_compiled_linked
        .workers_linked
        .into_iter()
        .map(|pre_spawn| {
            pre_spawn.spawn(
                deployment_id,
                db_pool,
                cancel_registry.clone(),
                log_forwarder_sender,
            )
        })
        .collect();

    *write_guard_ctx = DeploymentContext {
        deployment_id,
        component_registry_ro: server_compiled_linked.component_registry_ro.clone(),
        exec_task_handles: new_handles,
        closed: false,
    };

    db_conn
        .activate_deployment(deployment_id, chrono::Utc::now())
        .await
        .map_err(|e| SwitchError::Other(e.into()))?;

    info!(%deployment_id, "Switched to new  deployment");
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
        &server_compiled_linked,
    )
    .await;
    {
        let conn = db_pool.external_api_conn().await?;
        for (component_digest, frame_files) in &server_compiled_linked.frame_files {
            for (config_key, source) in frame_files {
                // Keys starting with ".../" become suffix keys (strip "...", prepend "/").
                let (frame_key, is_suffix) = if let Some(stripped) = config_key.strip_prefix(".../")
                {
                    (format!("/{stripped}"), true)
                } else {
                    (config_key.clone(), false)
                };
                let res = conn
                    .upsert_source_file(component_digest, &frame_key, is_suffix, source)
                    .await;
                if let Err(err) = res {
                    warn!("Cannot store backtrace source {config_key:?} in DB: {err:?}");
                }
            }
        }
    }

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

    let cancel_watcher = if cancel_watcher.enabled {
        Some(
            cancel_registry.spawn_cancel_watcher(db_pool.clone(), cancel_watcher.tick_sleep.into()),
        )
    } else {
        None
    };

    server_compiled_linked
        .create_missing_cron_seeds(&db_pool, deployment_id)
        .await?;

    let preopens_cleaner = if let (Some(parent_preopen_dir), Some(activities_cleanup)) = (
        server_compiled_linked.parent_preopen_dir,
        server_compiled_linked.activities_cleanup,
    ) {
        Some(PreopensCleaner::spawn_task(
            activities_cleanup.older_than.into(),
            parent_preopen_dir,
            activities_cleanup.run_every.into(),
            Arc::new(TokioSleep),
            Now.clone_box(),
            db_pool.clone(),
        ))
    } else {
        None
    };

    // Spawn Log -> Db Forwarder
    let (log_forwarder_sender, log_db_forarder) = {
        let (log_forwarder_sender, receiver) = mpsc::channel(1000); // TODO: make configurable
        let log_db_forarder = log_db_forwarder::spawn_new(db_pool.clone(), receiver);
        (log_forwarder_sender, log_db_forarder)
    };

    // Spawn executors
    let exec_join_handles: Vec<ExecutorTaskHandle> = server_compiled_linked
        .workers_linked
        .into_iter()
        .map(|pre_spawn| {
            pre_spawn.spawn(
                deployment_id,
                &db_pool,
                cancel_registry.clone(),
                &log_forwarder_sender,
            )
        })
        .collect();

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
        Arc::new(tokio::sync::RwLock::new(DeploymentContext {
            deployment_id,
            component_registry_ro: server_compiled_linked.component_registry_ro,
            exec_task_handles: exec_join_handles,
            closed: false,
        }));
    let server_init = ServerInit {
        server_verified,
        deployment_ctx,
        // deployment_id,
        db_pool,
        db_close,
        // exec_join_handles,
        timers_watcher,
        cancel_watcher,
        http_servers_handles,
        epoch_ticker,
        preopens_cleaner,
        log_db_forarder,
        engines: server_compiled_linked.engines,
        log_forwarder_sender,
        webhook_registry: Arc::new(webhook_registry),
        prepared_dirs,
    };
    Ok(server_init)
}

struct ServerInit {
    server_verified: ServerVerified,
    deployment_ctx: DeploymentContextHandle,
    // deployment_id: DeploymentId,
    db_pool: Arc<dyn DbPool>,
    db_close: Pin<Box<dyn Future<Output = ()> + Send>>,
    engines: Engines,
    // exec_join_handles: Vec<ExecutorTaskHandle>,
    timers_watcher: Option<AbortOnDropHandle>,
    cancel_watcher: Option<AbortOnDropHandle>,
    http_servers_handles: Vec<AbortOnDropHandle>,
    epoch_ticker: EpochTicker,
    preopens_cleaner: Option<AbortOnDropHandle>,
    log_db_forarder: AbortOnDropHandle,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    webhook_registry: Arc<WebhookRegistry>,
    prepared_dirs: PreparedDirs,
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
            http_servers_handles,
            epoch_ticker,
            preopens_cleaner,
            log_db_forarder,
            log_forwarder_sender,
            webhook_registry,
            prepared_dirs: _,
        } = self;

        debug!("Closing executors");
        let executors = {
            let mut deployment_lock = deployment_ctx.write().await;
            deployment_lock.closed = true;
            std::mem::take(&mut deployment_lock.exec_task_handles)
        };
        futures_util::future::join_all(executors.iter().map(ExecutorTaskHandle::close)).await;
        // Explicit drop to avoid the pattern match footgun.
        // Close everything that is a dependency of executors or workers.
        drop(db_pool);
        drop(timers_watcher);
        drop(cancel_watcher);
        drop(http_servers_handles);
        drop(epoch_ticker);
        drop(preopens_cleaner);
        drop(engines);
        drop(webhook_registry);
        drop(log_forwarder_sender);
        drop(log_db_forarder); // Some activity messages might not be stored.
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
struct ConfigVerified {
    activities_wasm: Vec<ActivityWasmConfigVerified>,
    activities_js: Vec<ActivityJsConfigVerified>,
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

impl ConfigVerified {
    #[instrument(skip_all)]
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify_all(
        mut deployment: DeploymentCanonical,
        http_servers: Vec<webhook::HttpServer>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        global_backtrace_persist: bool,
        parent_preopen_dir: Option<Arc<Path>>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        termination_watcher: &mut watch::Receiver<()>,
        subscription_interruption: Option<Duration>,
        api_addr_if_webui_enabled: Option<String>,
    ) -> Result<ConfigVerified, anyhow::Error> {
        debug!("Using deployment toml: {deployment:#?}");
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
        if let Some(api_listening_addr) = api_addr_if_webui_enabled {
            let target_url = format!("http://{api_listening_addr}");
            deployment
                .webhooks
                .push(webhook::WebhookWasmComponentConfigCanonical {
                    common: ComponentCommon {
                        name: ConfigName::new(StrVariant::Static("obelisk_webui")).unwrap(),
                        location: WEBUI_LOCATION
                            .parse()
                            .expect("hard-coded webui reference must be parsed"),
                    },
                    http_server: ConfigName::new(HTTP_SERVER_NAME_WEBUI.into()).unwrap(),
                    routes: vec![WebhookRoute::default()],
                    forward_stdout: ComponentStdOutputToml::default(),
                    forward_stderr: ComponentStdOutputToml::default(),
                    env_vars: vec![EnvVarConfig::KeyValue {
                        key: "TARGET_URL".to_string(),
                        value: target_url.clone(),
                    }],
                    backtrace: crate::config::toml::ComponentBacktraceConfigCanonical::default(),
                    logs_store_min_level: LogLevelToml::Off,
                    allowed_hosts: vec![AllowedHostToml {
                        pattern: target_url,
                        methods: Some(MethodsInput::Star(MethodsInputStar::default())),
                        secrets: None,
                    }],
                });
        }

        let http_servers_to_webhook_names = {
            let mut remaining_server_names_to_webhook_names = {
                let mut map: hashbrown::HashMap<ConfigName, Vec<ConfigName>> =
                    hashbrown::HashMap::default();
                for webhook in &deployment.webhooks {
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
                            parent_preopen_dir.clone(),
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
            .workflows
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
            .webhooks
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

                let mut crons = Vec::with_capacity(deployment.crons.len());
                for cron in deployment.crons {
                    let name = cron.name.clone();
                    crons.push(cron.verify().with_context(|| {
                        format!("failed to verify cron `{name}`")
                    })?);
                }

                Ok(ConfigVerified {
                    activities_wasm,
                    activities_js: activities_js_verified,
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
                })
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
) -> Result<Linked, anyhow::Error> {
    let build_semaphore = build_semaphore.map(|permits| {
        semaphore::Semaphore::new(permits.try_into().expect("u64 must fit into usize"))
    });
    let parent_span = Span::current();

    // TODO: other components are not compiled in parallel.
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
        })
        .await
        .context("panic while compiling activity-js-runtime")?;
        Some(runnable)
    } else {
        None
    }
    .transpose()?;

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
        })
        .await
        .context("panic while compiling workflow-js-runtime")?;
        Some(runnable)
    } else {
        None
    }
    .transpose()?;

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
        })
        .await
        .context("panic while compiling webhook-js-runtime")?;
        Some(runnable)
    } else {
        None
    }
    .transpose()?;

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
                    prespawn_activity(activity_wasm, &engines).map(|(worker, component_config)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                            frame_files: FrameFilesToSourceContent::new(),
                        }
                    })
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
                    prespawn_js_activity(activity_js, &engines, activity_js_runnable).map(|(worker, component_config)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                            frame_files: FrameFilesToSourceContent::new(),
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
                        workflow_replay_info: None,
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
                    prespawn_workflow(workflow, &engines, workflows_lock_extension_leeway)
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
                    prespawn_js_workflow(workflow_js, &engines,workflow_js_runnable, workflows_lock_extension_leeway)
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
    tokio::select! {
        results_of_results = pre_spawns => {
            let mut component_registry = ComponentConfigRegistry::default();
            let mut workers_compiled = Vec::with_capacity(results_of_results.len());
            let mut webhooks_compiled_by_names = hashbrown::HashMap::new();
            let mut all_frame_files: Vec<(ComponentDigest, FrameFilesToSourceContent)> = Vec::new();
            for handle in results_of_results {
                match handle?? {
                    CompiledComponent::ActivityOrWorkflow { worker, component_config, frame_files } => {
                        if !frame_files.is_empty() {
                            all_frame_files.push((component_config.component_id.component_digest.clone(), frame_files));
                        }
                        component_registry.insert(component_config)?;
                        workers_compiled.push(worker);
                    },
                    CompiledComponent::Webhook{ webhook_name, webhook_compiled, routes, frame_files } => {
                        if !frame_files.is_empty() {
                            all_frame_files.push((webhook_compiled.config.component_id.component_digest.clone(), frame_files));
                        }
                        let component = ComponentConfig {
                            component_id: webhook_compiled.config.component_id.clone(),
                            imports: webhook_compiled.imports().to_vec(),
                            workflow_or_activity_config: None,
                            wit: webhook_compiled.runnable_component.wasm_component.wit(),
                            workflow_replay_info: None,
                            wit_origin: WitOrigin::Wasm,
                        };
                        component_registry.insert(component)?;
                        let old = webhooks_compiled_by_names.insert(webhook_name, (webhook_compiled, routes));
                        assert!(old.is_none());
                    },
                    CompiledComponent::ActivityStubOrExternal { component_config } => {
                        component_registry.insert(component_config)?;
                    },
                }
            }
            // Register cron components in the registry (no WASM, no imports/exports)
            for cron in &crons {
                let component_config = ComponentConfig {
                    component_id: cron.component_id.clone(),
                    imports: vec![],
                    workflow_or_activity_config: None,
                    wit: String::new(), // does not matter, WIT is not exposed
                    workflow_replay_info: None,
                    wit_origin: WitOrigin::Synthesized,
                };
                component_registry.insert(component_config)?; // Mostly just for name uniqueness checking
            }
            let (component_registry_ro, supressed_errors) = component_registry.verify_registry();
            let fn_registry: Arc<dyn FunctionRegistry> = Arc::from(component_registry_ro.clone());
            let mut workers_linked = workers_compiled
                .into_iter()
                .map(|worker| worker.link(&fn_registry))
                .collect::<Result<Vec<_>,_>>()?;
            // Resolve cron target FFQNs, type-check params, and create WorkerLinked entries
            for cron in crons {
                let (target_component_id, target_fn_metadata) = component_registry_ro
                    .find_by_exported_ffqn(&cron.target_ffqn)
                    .ok_or_else(|| anyhow::anyhow!(
                        "cron `{}` targets function `{}` which is not exported by any component",
                        cron.component_id.name,
                        cron.target_ffqn,
                    ))?;
                // Parse and type-check params against the target function's parameter types
                let params = Params::from_json_values(
                    Arc::from(cron.params_json),
                    target_fn_metadata.parameter_types.iter().map(|pt| &pt.type_wrapper),
                ).with_context(|| format!(
                    "cron `{}`: params do not match target function `{}` parameter types",
                    cron.component_id.name, cron.target_ffqn,
                ))?;
                workers_linked.push(WorkerLinked {
                    worker: LinkedWorkerKind::Cron(ScheduleWorkerConfig {
                        component_id: cron.component_id,
                        target_ffqn: cron.target_ffqn,
                        target_component_id: target_component_id.clone(),
                        params,
                        cron_schedule: cron.cron_schedule,
                    }),
                    exec_config: cron.exec_config,
                    logs_store_min_level: None,
                });
            }
            let webhooks_wasm_by_names = webhooks_compiled_by_names
                .into_iter()
                .map(|(name, (compiled, routes))|{
                    let component_id = compiled.config.component_id.clone();
                    compiled.link(&engines.webhook_engine, fn_registry.as_ref())
                        .map(|instance| (name, (instance, routes)))
                        .with_context(||format!("cannot compile {component_id}"))
                })
                .collect::<Result<IndexMap<_,_>,_>>()?;
            Ok(Linked {
                workers: workers_linked,
                webhooks_wasm_by_names,
             component_registry_ro,
              supressed_errors,
               all_frame_files})
        },
        _ = termination_watcher.changed() => {
            warn!("Received SIGINT, canceling while compiling the components");
            anyhow::bail!("canceling while compiling the components")
        }
    }
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
        workflow_replay_info: None,
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
fn prespawn_activity(
    activity: ActivityWasmConfigVerified,
    engines: &Engines,
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    let component_id = activity.component_id().clone();
    assert!(component_id.component_type == ComponentType::Activity);
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let engine = engines.activity_engine.clone();
    let runnable_component =
        RunnableComponent::new(activity.wasm_path, &engine, component_id.component_type)?;
    let wit = runnable_component.wasm_component.wit();

    let worker = ActivityWorkerCompiled::new_with_config(
        runnable_component,
        activity.activity_config,
        engine,
        Now.clone_box(),
        Arc::new(TokioSleep),
    )
    .with_context(|| format!("cannot compile {component_id}"))?;
    let (worker, component_config) = WorkerCompiled::new_activity(
        worker,
        activity.exec_config,
        wit,
        activity.logs_store_min_level,
    );
    Ok((worker, component_config))
}

#[instrument(level = "debug", skip_all, fields(
    component_id = %activity_js.exec_config.component_id,
))]
fn prespawn_js_activity(
    activity_js: ActivityJsConfigVerified,
    engines: &Engines,
    runnable_component: RunnableComponent,
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    let component_id = activity_js.component_id().clone();
    assert!(component_id.component_type == ComponentType::Activity);

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
fn prespawn_workflow(
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
fn prespawn_js_workflow(
    workflow_js: WorkflowJsConfigVerified,
    engines: &Engines,
    runnable_component: RunnableComponent,
    workflows_lock_extension_leeway: Duration,
) -> Result<(WorkerCompiled, ComponentConfig, FrameFilesToSourceContent), anyhow::Error> {
    let component_id = workflow_js.component_id().clone();
    assert!(component_id.component_type == ComponentType::Workflow);
    let frame_sources = workflow_js.as_frame_sources();
    let engine = engines.workflow_engine.clone();

    let inner = WorkflowWorkerCompiled::new_with_config(
        runnable_component.clone(),
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
        workflow_js.params.clone(),
        workflow_js.return_type.clone(),
    )
    .with_context(|| format!("cannot create JS workflow worker for {component_id}"))?;
    let wit = worker.wit();
    Ok(WorkerCompiled::new_js_workflow(
        worker,
        runnable_component,
        workflow_js.exec_config,
        workflow_js.logs_store_min_level,
        workflows_lock_extension_leeway,
        wit,
        workflow_js.js_source,
        frame_sources,
        workflow_js.params,
    ))
}

struct WorkflowWorkerCompiledWithConfig {
    worker: WorkflowWorkerCompiled,
    workflows_lock_extension_leeway: Duration,
}

struct WorkflowJsWorkerCompiledWithConfig {
    worker: WorkflowJsWorkerCompiled,
    workflows_lock_extension_leeway: Duration,
}

enum CompiledWorkerKind {
    ActivityWasm(ActivityWorkerCompiled),
    ActivityJs(Box<ActivityJsWorkerCompiled>),
    WorkflowWasm(WorkflowWorkerCompiledWithConfig),
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
            workflow_replay_info: None,
            wit_origin: WitOrigin::Wasm,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::ActivityWasm(worker),
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
    ) -> (WorkerCompiled, ComponentConfig) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            workflow_replay_info: None,
            wit_origin: WitOrigin::Synthesized,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::ActivityJs(Box::new(worker)),
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
        let worker = WorkflowWorkerCompiled::new_with_config(
            runnable_component.clone(),
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
            workflow_replay_info: Some(WorkflowReplayInfo {
                runnable_component,
                logs_store_min_level: workflow.logs_store_min_level,
                js_workflow_info: None,
            }),
            wit_origin: WitOrigin::Wasm,
        };
        Ok((
            WorkerCompiled {
                worker: CompiledWorkerKind::WorkflowWasm(WorkflowWorkerCompiledWithConfig {
                    worker,
                    workflows_lock_extension_leeway,
                }),
                exec_config: workflow.exec_config,
                logs_store_min_level: workflow.logs_store_min_level,
            },
            component,
            workflow.frame_files_to_sources,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn new_js_workflow(
        worker: WorkflowJsWorkerCompiled,
        runnable_component: RunnableComponent,
        exec_config: ExecConfig,
        logs_store_min_level: Option<LogLevel>,
        workflows_lock_extension_leeway: Duration,
        wit: String,
        js_source: String,
        frame_files: FrameFilesToSourceContent, // to be served by GetBacktraceSource
        user_params: Vec<concepts::ParameterType>,
    ) -> (WorkerCompiled, ComponentConfig, FrameFilesToSourceContent) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
            workflow_replay_info: Some(WorkflowReplayInfo {
                runnable_component,
                logs_store_min_level,
                js_workflow_info: Some(JsWorkflowReplayInfo {
                    js_source,
                    user_params,
                }),
            }),
            wit_origin: WitOrigin::Synthesized,
        };
        (
            WorkerCompiled {
                worker: CompiledWorkerKind::WorkflowJs(Box::new(
                    WorkflowJsWorkerCompiledWithConfig {
                        worker,
                        workflows_lock_extension_leeway,
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
                CompiledWorkerKind::WorkflowWasm(workflow_compiled) => {
                    LinkedWorkerKind::WorkflowWasm(WorkflowWorkerLinkedWithConfig {
                        worker: workflow_compiled.worker.link(fn_registry.clone())?,
                        workflows_lock_extension_leeway: workflow_compiled
                            .workflows_lock_extension_leeway,
                    })
                }
                CompiledWorkerKind::WorkflowJs(workflow_js_compiled) => {
                    LinkedWorkerKind::WorkflowJs(WorkflowJsWorkerLinkedWithConfig {
                        worker: workflow_js_compiled.worker.link(fn_registry.clone())?,
                        workflows_lock_extension_leeway: workflow_js_compiled
                            .workflows_lock_extension_leeway,
                    })
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
}

struct WorkflowJsWorkerLinkedWithConfig {
    worker: WorkflowJsWorkerLinked,
    workflows_lock_extension_leeway: Duration,
}

enum LinkedWorkerKind {
    ActivityWasm(ActivityWorkerCompiled),
    ActivityJs(Box<ActivityJsWorkerCompiled>),
    WorkflowWasm(WorkflowWorkerLinkedWithConfig),
    WorkflowJs(WorkflowJsWorkerLinkedWithConfig),
    Cron(ScheduleWorkerConfig),
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
    ) -> ExecutorTaskHandle {
        let logs_storage_config = self.logs_store_min_level.map(|min_level| LogStrageConfig {
            min_level,
            log_sender: log_forwarder_sender.clone(),
        });
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
            LinkedWorkerKind::WorkflowWasm(workflow_linked) => {
                let factory = DeadlineTrackerFactoryTokio::new(
                    workflow_linked.workflows_lock_extension_leeway,
                    Now.clone_box(),
                );
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
                Arc::from(workflow_js_linked.worker.into_worker(
                    deployment_id,
                    db_pool.clone(),
                    Arc::new(factory),
                    cancel_registry,
                    logs_storage_config,
                ))
            }
            LinkedWorkerKind::Cron(config) => Arc::from(CronWorker::new(
                config.component_id,
                config.target_ffqn,
                config.target_component_id,
                config.params,
                config.cron_schedule,
                deployment_id,
                db_pool.clone(),
                Now.clone_box(),
            )),
        };
        ExecTask::spawn_new(
            deployment_id,
            worker,
            self.exec_config,
            Now.clone_box(),
            db_pool.clone(),
            TokioSleep,
        )
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
            ConfigVerified, PrepareDirsParams, ServerCompiledLinked, ServerVerified, VerifyParams,
            compile_activity_inline, create_engines, prepare_dirs,
        },
        config::config_holder::{ConfigHolder, load_deployment_toml},
    };
    use concepts::{ComponentId, FunctionFqn, prefixed_ulid::DeploymentId};
    use concepts::{
        ComponentType, ParameterType, ReturnType, StrVariant,
        component_id::{ComponentDigest, Digest},
    };
    use directories::BaseDirs;
    use rstest::rstest;
    use std::{path::PathBuf, sync::Arc};
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

        let deployment_toml = load_deployment_toml(workspace.join(deployment_toml)).await?;
        let path_prefixes = Arc::new(config_holder.path_prefixes);

        let deployment =
            crate::config::toml::resolve_local_refs_to_canonical(&deployment_toml).await?;

        let prepared_dirs =
            prepare_dirs(&config, &PrepareDirsParams::default(), &path_prefixes).await?;

        let (_termination_sender, mut termination_watcher) = watch::channel(());
        let engines = create_engines(&config, &prepared_dirs)?;
        let server_verified = Box::pin(ServerVerified::new(engines, config, path_prefixes)).await?;
        let params = VerifyParams::default();
        let webui_enabled = None;

        // Verify deployment
        let config = Box::pin(ConfigVerified::fetch_and_verify_all(
            deployment,
            server_verified.http_servers,
            prepared_dirs.wasm_cache_dir.clone(),
            prepared_dirs.metadata_dir.clone(),
            params.ignore_missing_env_vars,
            server_verified.global_backtrace_persist,
            server_verified.launch.parent_preopen_dir.clone(),
            server_verified.global_executor_instance_limiter,
            server_verified.fuel,
            &mut termination_watcher,
            server_verified.database_subscription_interruption,
            webui_enabled,
        ))
        .await?;

        let _compiled_and_linked = ServerCompiledLinked::new(
            DeploymentId::generate(),
            config,
            server_verified.launch,
            &mut termination_watcher,
            params.suppress_type_checking_errors,
        )
        .await?;

        Ok(())
    }
}
