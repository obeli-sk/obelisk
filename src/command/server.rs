use crate::args::Server;
use crate::args::shadow::PKG_VERSION;
use crate::command::termination_notifier::termination_notifier;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::ConfigSource;
use crate::config::config_holder::PathPrefixes;
use crate::config::env_var::EnvVarConfig;
use crate::config::toml::ActivitiesDirectoriesCleanupConfigToml;
use crate::config::toml::ActivitiesDirectoriesGlobalConfigToml;
use crate::config::toml::ActivityStubExtComponentConfigToml;
use crate::config::toml::ActivityStubExtConfigVerified;
use crate::config::toml::ActivityWasmComponentConfigToml;
use crate::config::toml::ActivityWasmConfigVerified;
use crate::config::toml::CancelWatcherTomlConfig;
use crate::config::toml::ComponentBacktraceConfig;
use crate::config::toml::ComponentCommon;
use crate::config::toml::ComponentStdOutputToml;
use crate::config::toml::ConfigName;
use crate::config::toml::ConfigToml;
use crate::config::toml::DatabaseConfigToml;
use crate::config::toml::LogLevelToml;
use crate::config::toml::SQLITE_FILE_NAME;
use crate::config::toml::TimersWatcherTomlConfig;
use crate::config::toml::WasmtimeAllocatorConfig;
use crate::config::toml::WorkflowComponentConfigToml;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::webhook;
use crate::config::toml::webhook::WebhookComponentConfigVerified;
use crate::config::toml::webhook::WebhookRoute;
use crate::config::toml::webhook::WebhookRouteVerified;
use crate::init;
use crate::init::Guard;
use crate::project_dirs;
use crate::server::grpc_server::GrpcServer;
use crate::server::grpc_server::IGNORING_COMPONENT_DIGEST;
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
use concepts::SUFFIX_FN_SCHEDULE;
use concepts::StrVariant;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::CreateRequest;
use concepts::storage::DbErrorWrite;
use concepts::storage::DbErrorWriteNonRetriable;
use concepts::storage::DbExternalApi;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use concepts::storage::LogInfoAppendRow;
use concepts::storage::LogLevel;
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
use itertools::Either;
use serde_json::json;
use std::fmt::Debug;
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
use wasm_workers::activity::activity_worker::ActivityWorkerCompiled;
use wasm_workers::activity::cancel_registry::CancelRegistry;
use wasm_workers::component_logger::LogStrageConfig;
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
use wasm_workers::registry::WorkflowReplayInfo;
use wasm_workers::webhook::webhook_trigger;
use wasm_workers::webhook::webhook_trigger::MethodAwareRouter;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointCompiled;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointConfig;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointInstanceLinked;
use wasm_workers::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
use wasm_workers::workflow::host_exports::history_event_schedule_at_from_wast_val;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerCompiled;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerLinked;
use wasmtime::Engine;

const EPOCH_MILLIS: u64 = 10;
const WEBUI_LOCATION: &str = include_str!("../../assets/webui-version.txt");

pub(crate) type ComponentSourceMap = hashbrown::HashMap<ComponentId, MatchableSourceMap>;

impl Server {
    pub(crate) async fn run(self) -> Result<(), anyhow::Error> {
        match self {
            Server::Run {
                clean_sqlite_directory,
                clean_cache,
                clean_codegen_cache,
                config,
                suppress_type_checking_errors,
            } => {
                Box::pin(run(
                    project_dirs(),
                    BaseDirs::new(),
                    config,
                    RunParams {
                        clean_cache,
                        clean_codegen_cache,
                        clean_sqlite_directory,
                        suppress_type_checking_errors,
                    },
                ))
                .await
            }
            Server::Verify {
                clean_cache,
                clean_codegen_cache,
                config,
                ignore_missing_env_vars,
                suppress_type_checking_errors,
                skip_db,
            } => {
                verify(
                    project_dirs(),
                    BaseDirs::new(),
                    config,
                    VerifyParams {
                        clean_cache,
                        clean_codegen_cache,
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

#[expect(clippy::struct_excessive_bools)]
pub(crate) struct RunParams {
    pub(crate) clean_cache: bool,
    pub(crate) clean_codegen_cache: bool,
    pub(crate) clean_sqlite_directory: bool,
    pub(crate) suppress_type_checking_errors: bool,
}

pub(crate) async fn run(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    config: Option<ConfigSource>,
    params: RunParams,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, config, false)?;
    let mut config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&mut config)?;
    let (termination_sender, termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });

    Box::pin(run_internal(
        config,
        config_holder,
        params,
        termination_watcher,
    ))
    .await?;
    Ok(())
}

#[derive(Debug, Default, Clone)]
#[expect(clippy::struct_excessive_bools)]
pub(crate) struct VerifyParams {
    pub(crate) clean_cache: bool,
    pub(crate) clean_codegen_cache: bool,
    pub(crate) ignore_missing_env_vars: bool,
    pub(crate) suppress_type_checking_errors: bool,
}

pub(crate) async fn verify(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    config: Option<ConfigSource>,
    verify_params: VerifyParams,
    skip_db: bool,
) -> Result<(), anyhow::Error> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, config, false)?;
    let mut config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&mut config)?;
    let deployment_id = config.get_deployment_id();
    let (termination_sender, mut termination_watcher) = watch::channel(());
    tokio::spawn(async move { termination_notifier(termination_sender).await });
    if skip_db {
        Box::pin(verify_config_compile_link(
            config,
            Arc::new(config_holder.path_prefixes),
            deployment_id,
            verify_params,
            &mut termination_watcher,
        ))
        .await?;
    } else {
        Box::pin(verify_with_db_schema(
            config,
            Arc::new(config_holder.path_prefixes),
            deployment_id,
            verify_params,
            &mut termination_watcher,
        ))
        .await?;
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

/// Verifies configuration including database schema version.
/// Called by `verify` command.
async fn verify_with_db_schema(
    config: ConfigToml,
    path_prefixes: Arc<PathPrefixes>,
    deployment_id: DeploymentId,
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<(ServerCompiledLinked, ComponentSourceMap), anyhow::Error> {
    let database = config.database.clone();
    let ok = Box::pin(verify_config_compile_link(
        config,
        path_prefixes.clone(),
        deployment_id,
        params,
        termination_watcher,
    ))
    .await?;
    // Verify database schema version
    match &database {
        DatabaseConfigToml::Sqlite(sqlite_config_toml) => {
            let db_dir = sqlite_config_toml.get_sqlite_dir(&path_prefixes).await?;
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
    Ok(ok)
}

/// Verifies configuration without database schema check.
#[instrument(skip_all, name = "verify")]
pub(crate) async fn verify_config_compile_link(
    config: ConfigToml,
    path_prefixes: Arc<PathPrefixes>,
    deployment_id: DeploymentId, // only to distinguish when writing to the same log file as running server
    params: VerifyParams,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<(ServerCompiledLinked, ComponentSourceMap), anyhow::Error> {
    Span::current().record("deployment_id", tracing::field::display(&deployment_id));
    info!("Verifying configuration, compiling WASM components");
    debug!("Using toml config: {config:#?}");

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

    let wasm_cache_dir = config
        .wasm_global_config
        .get_wasm_cache_directory(&path_prefixes)
        .await?;
    let codegen_cache = config
        .wasm_global_config
        .codegen_cache
        .get_directory(&path_prefixes)
        .await?;
    debug!("Using codegen cache? {codegen_cache:?}");
    if params.clean_cache {
        tokio::fs::remove_dir_all(&wasm_cache_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete wasm cache directory {wasm_cache_dir:?}"))?;
    }
    if (params.clean_cache || params.clean_codegen_cache)
        && let Some(codegen_cache) = &codegen_cache
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

    let (server_verified, component_source_map) = ServerVerified::new(
        config,
        codegen_cache,
        Arc::from(wasm_cache_dir),
        Arc::from(metadata_dir),
        params.ignore_missing_env_vars,
        path_prefixes,
        termination_watcher,
    )
    .await?;
    let compiled_and_linked = ServerCompiledLinked::new(
        server_verified,
        termination_watcher,
        params.suppress_type_checking_errors,
    )
    .await?;
    if compiled_and_linked.supressed_errors.is_none() {
        info!("Obelisk configuration was verified");
    } else {
        warn!("Obelisk configuration was verified with supressed errors");
    }
    Ok((compiled_and_linked, component_source_map))
}

async fn run_internal(
    config: ConfigToml,
    config_holder: ConfigHolder,
    params: RunParams,
    mut termination_watcher: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let deployment_id = config.get_deployment_id();
    let span = info_span!("init", %deployment_id);
    let api_listening_addr = config.api.listening_addr;

    let global_webhook_instance_limiter = config
        .wasm_global_config
        .global_webhook_instance_limiter
        .as_semaphore();
    let timers_watcher = config.timers_watcher;
    let cancel_watcher = config.cancel_watcher;
    let path_prefixes = Arc::new(config_holder.path_prefixes);
    let database = config.database.clone();
    let (compiled_and_linked, component_source_map) = Box::pin(verify_config_compile_link(
        config,
        path_prefixes.clone(),
        deployment_id,
        VerifyParams {
            clean_cache: params.clean_cache,
            clean_codegen_cache: params.clean_codegen_cache,
            ignore_missing_env_vars: false,
            suppress_type_checking_errors: params.suppress_type_checking_errors,
        },
        &mut termination_watcher,
    ))
    .instrument(span.clone())
    .await?;

    let cancel_registry = CancelRegistry::new();
    let subscription_interruption = database.get_subscription_interruption();

    let (server_init, component_registry_ro) = match database {
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
                async move {
                    db_pool.close().await;
                }
            });
            spawn_tasks_and_threads(
                deployment_id,
                db_pool,
                db_close,
                compiled_and_linked,
                global_webhook_instance_limiter,
                timers_watcher,
                cancel_watcher,
                &cancel_registry,
                &termination_watcher,
            )
            .instrument(span)
            .await?
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
                async move {
                    db_pool.close().await;
                }
            });
            spawn_tasks_and_threads(
                deployment_id,
                db_pool,
                db_close,
                compiled_and_linked,
                global_webhook_instance_limiter,
                timers_watcher,
                cancel_watcher,
                &cancel_registry,
                &termination_watcher,
            )
            .instrument(span)
            .await?
        }
    };

    let grpc_server = Arc::new(GrpcServer::new(
        server_init.deployment_id,
        server_init.db_pool.clone(),
        termination_watcher.clone(),
        component_registry_ro.clone(),
        component_source_map,
        cancel_registry.clone(),
        server_init.engines.clone(),
        server_init.log_forwarder_sender.clone(),
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
        deployment_id: server_init.deployment_id,
        db_pool: server_init.db_pool.clone(),
        component_registry_ro,
        cancel_registry,
        termination_watcher: termination_watcher.clone(),
        subscription_interruption,
        engines: server_init.engines.clone(),
        log_forwarder_sender: server_init.log_forwarder_sender.clone(),
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

struct ServerVerified {
    config: ConfigVerified,
    engines: Engines,
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
    build_semaphore: Option<u64>,
    workflows_lock_extension_leeway: Duration,
}

impl ServerVerified {
    #[instrument(name = "ServerVerified::new", skip_all)]
    async fn new(
        config: ConfigToml,
        codegen_cache_dir: Option<PathBuf>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        path_prefixes: Arc<PathPrefixes>,
        termination_watcher: &mut watch::Receiver<()>,
    ) -> Result<(Self, ComponentSourceMap), anyhow::Error> {
        let fuel: Option<u64> = config.wasm_global_config.fuel.into();
        let workflows_lock_extension_leeway =
            config.workflows_global_config.lock_extension_leeway.into();
        let engines = {
            let consume_fuel = fuel.is_some();
            let engine_config = EngineConfig {
                codegen_cache_dir,
                consume_fuel,
                parallel_compilation: config.wasm_global_config.parallel_compilation,
                debug: config.wasm_global_config.debug,
                pooling_config: match config.wasm_global_config.allocator_config {
                    WasmtimeAllocatorConfig::OnDemand => PoolingConfig::OnDemand,
                    WasmtimeAllocatorConfig::Pooling => PoolingConfig::Pooling(
                        config.wasm_global_config.wasmtime_pooling_config.into(),
                    ),
                    WasmtimeAllocatorConfig::Auto => PoolingConfig::PoolingWithFallback(
                        config.wasm_global_config.wasmtime_pooling_config.into(),
                    ),
                },
            };
            Engines::new(engine_config)?
        };
        let mut http_servers = config.http_servers;
        let mut webhooks = config.webhooks;
        if let Some(webui_listening_addr) = config.webui.listening_addr {
            let http_server_name = ConfigName::new(StrVariant::Static("webui")).unwrap();
            http_servers.push(webhook::HttpServer {
                name: http_server_name.clone(),
                listening_addr: webui_listening_addr
                    .parse()
                    .context("error converting `webui.listening_addr` to a socket address")?,
            });
            webhooks.push(webhook::WebhookComponentConfigToml {
                common: ComponentCommon {
                    name: ConfigName::new(StrVariant::Static("obelisk_webui")).unwrap(),
                    location: WEBUI_LOCATION
                        .parse()
                        .expect("hard-coded webui reference must be parsed"),
                    content_digest: None,
                },
                http_server: http_server_name,
                routes: vec![WebhookRoute::default()],
                forward_stdout: ComponentStdOutputToml::default(),
                forward_stderr: ComponentStdOutputToml::default(),
                env_vars: vec![EnvVarConfig {
                    key: "TARGET_URL".to_string(),
                    val: Some(format!(
                        "http://{}",
                        config.api.listening_addr.context(
                            "cannot expose webui without configuring `api.listening_addr`"
                        )?
                    )),
                }],
                backtrace: ComponentBacktraceConfig::default(),
                logs_store_min_level: LogLevelToml::Off,
                allowed_hosts: None,
                secrets: vec![],
            });
        }
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

        let mut config = ConfigVerified::fetch_and_verify_all(
            config.activities_wasm,
            config.activities_stub,
            config.activities_external,
            config.workflows,
            http_servers,
            webhooks,
            wasm_cache_dir,
            metadata_dir,
            ignore_missing_env_vars,
            path_prefixes,
            global_backtrace_persist,
            parent_preopen_dir.clone(),
            config
                .wasm_global_config
                .global_executor_instance_limiter
                .as_semaphore(),
            fuel,
            termination_watcher,
            config.database.get_subscription_interruption(),
        )
        .await?;
        debug!("Verified config: {config:#?}");
        let component_source_map = {
            let mut map = hashbrown::HashMap::new();
            for workflow in &config.workflows {
                let inner_map = workflow.backtrace_config.frame_files_to_sources.clone();
                let matchable_source_map = MatchableSourceMap::new(inner_map);
                let mut component_id = workflow.component_id().clone();
                if workflow.backtrace_config.ignore_component_digest {
                    component_id.input_digest = IGNORING_COMPONENT_DIGEST;
                }
                map.insert(component_id, matchable_source_map);
            }
            for webhook in config.webhooks_by_names.values_mut() {
                let inner_map = webhook.backtrace_config.frame_files_to_sources.clone();
                let matchable_source_map = MatchableSourceMap::new(inner_map);
                let mut component_id = webhook.component_id.clone();
                if webhook.backtrace_config.ignore_component_digest {
                    component_id.input_digest = IGNORING_COMPONENT_DIGEST;
                }
                map.insert(component_id, matchable_source_map);
            }
            map
        };

        Ok((
            Self {
                config,
                engines,
                parent_preopen_dir,
                activities_cleanup,
                build_semaphore,
                workflows_lock_extension_leeway,
            },
            component_source_map,
        ))
    }
}

pub(crate) struct ServerCompiledLinked {
    engines: Engines,
    pub(crate) component_registry_ro: ComponentConfigRegistryRO,
    compiled_components: LinkedComponents,
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<ConfigName>)>,
    supressed_errors: Option<String>,
}

impl ServerCompiledLinked {
    async fn new(
        server_verified: ServerVerified,
        termination_watcher: &mut watch::Receiver<()>,
        suppress_type_checking_errors: bool,
    ) -> Result<Self, anyhow::Error> {
        let (compiled_components, component_registry_ro, supressed_errors) = compile_and_verify(
            &server_verified.engines,
            server_verified.config.activities_wasm,
            server_verified.config.activities_stub_ext,
            server_verified.config.workflows,
            server_verified.config.webhooks_by_names,
            server_verified.config.global_backtrace_persist,
            server_verified.config.fuel,
            server_verified.build_semaphore,
            server_verified.workflows_lock_extension_leeway,
            termination_watcher,
        )
        .await?;
        if !suppress_type_checking_errors && supressed_errors.is_some() {
            bail!("type checking errors detected");
        }
        Ok(Self {
            compiled_components,
            component_registry_ro,
            engines: server_verified.engines,
            parent_preopen_dir: server_verified.parent_preopen_dir,
            activities_cleanup: server_verified.activities_cleanup,
            http_servers_to_webhook_names: server_verified.config.http_servers_to_webhook_names,
            supressed_errors,
        })
    }
}

#[instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
async fn spawn_tasks_and_threads(
    deployment_id: DeploymentId,
    db_pool: Arc<dyn DbPool>,
    db_close: Pin<Box<dyn Future<Output = ()> + Send>>,
    mut server_compiled_linked: ServerCompiledLinked,
    global_webhook_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    timers_watcher: TimersWatcherTomlConfig,
    cancel_watcher: CancelWatcherTomlConfig,
    cancel_registry: &CancelRegistry,
    termination_watcher: &watch::Receiver<()>,
) -> Result<(ServerInit, ComponentConfigRegistryRO), anyhow::Error> {
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

    let preopens_cleaner = if let (Some(parent_preopen_dir), Some(activities_cleanup)) = (
        server_compiled_linked.parent_preopen_dir,
        server_compiled_linked.activities_cleanup,
    ) {
        Some(PreopensCleaner::spawn_task(
            activities_cleanup.older_than.into(),
            parent_preopen_dir,
            activities_cleanup.run_every.into(),
            TokioSleep,
            Now.clone_box(),
            db_pool.clone(),
        ))
    } else {
        None
    };

    // Associate webhooks with http servers
    let http_servers_to_webhooks = {
        server_compiled_linked
            .http_servers_to_webhook_names
            .into_iter()
            .map(|(http_server, webhook_names)| {
                let instances_and_routes = webhook_names
                    .into_iter()
                    .map(|name| {
                        server_compiled_linked
                            .compiled_components
                            .webhooks_by_names
                            .remove(&name)
                            .expect("all webhooks must be verified")
                    })
                    .collect::<Vec<_>>();
                (http_server, instances_and_routes)
            })
            .collect()
    };

    // Spawn Log -> Db Forwarder
    let (log_forwarder_sender, log_db_forarder) = {
        let (log_forwarder_sender, receiver) = mpsc::channel(1000); // TODO: make configurable
        let log_db_forarder = log_db_forwarder::spawn_new(db_pool.clone(), receiver);
        (log_forwarder_sender, log_db_forarder)
    };

    // Spawn executors
    let exec_join_handles = server_compiled_linked
        .compiled_components
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

    // Start webhook HTTP servers
    let http_servers_handles: Vec<AbortOnDropHandle> = start_http_servers(
        deployment_id,
        http_servers_to_webhooks,
        &server_compiled_linked.engines,
        db_pool.clone(),
        Arc::from(server_compiled_linked.component_registry_ro.clone()),
        global_webhook_instance_limiter.clone(),
        termination_watcher,
        &log_forwarder_sender,
    )
    .await?;

    let server_init = ServerInit {
        deployment_id,
        db_pool,
        db_close,
        exec_join_handles,
        timers_watcher,
        cancel_watcher,
        http_servers_handles,
        epoch_ticker,
        preopens_cleaner,
        log_db_forarder,
        engines: server_compiled_linked.engines,
        log_forwarder_sender,
    };
    Ok((server_init, server_compiled_linked.component_registry_ro))
}

struct ServerInit {
    deployment_id: DeploymentId,
    db_pool: Arc<dyn DbPool>,
    db_close: Pin<Box<dyn Future<Output = ()> + Send>>,
    engines: Engines,
    exec_join_handles: Vec<ExecutorTaskHandle>,
    timers_watcher: Option<AbortOnDropHandle>,
    cancel_watcher: Option<AbortOnDropHandle>,
    http_servers_handles: Vec<AbortOnDropHandle>,
    epoch_ticker: EpochTicker,
    preopens_cleaner: Option<AbortOnDropHandle>,
    log_db_forarder: AbortOnDropHandle,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
}
impl ServerInit {
    async fn close(self) {
        info!("Server is shutting down");

        let ServerInit {
            deployment_id: _,
            db_pool,
            db_close,
            engines,
            exec_join_handles,
            timers_watcher,
            cancel_watcher,
            http_servers_handles,
            epoch_ticker,
            preopens_cleaner,
            log_db_forarder,
            log_forwarder_sender,
        } = self;
        // Explicit drop to avoid the pattern match footgun.
        drop(db_pool);
        drop(timers_watcher);
        drop(cancel_watcher);
        drop(http_servers_handles);
        drop(epoch_ticker);
        drop(preopens_cleaner);
        drop(engines);
        drop(log_forwarder_sender);
        debug!("Closing executors");
        // Close most of the services. Worker tasks might still be running.
        for exec_join_handle in exec_join_handles {
            exec_join_handle.close().await;
        }
        drop(log_db_forarder); // Some incoming messages might not be stored.
        db_close.await;
    }
}

type WebhookInstancesAndRoutes = (
    WebhookEndpointInstanceLinked<TokioSleep>,
    Vec<WebhookRouteVerified>,
);

#[expect(clippy::too_many_arguments)]
async fn start_http_servers(
    deployment_id: DeploymentId,
    http_servers_to_webhooks: Vec<(webhook::HttpServer, Vec<WebhookInstancesAndRoutes>)>,
    engines: &Engines,
    db_pool: Arc<dyn DbPool>,
    fn_registry: Arc<dyn FunctionRegistry>,
    global_webhook_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
    termination_watcher: &watch::Receiver<()>,
    log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
) -> Result<Vec<AbortOnDropHandle>, anyhow::Error> {
    let mut abort_handles = Vec::with_capacity(http_servers_to_webhooks.len());
    let engine = &engines.webhook_engine;
    for (http_server, webhooks) in http_servers_to_webhooks {
        let mut router = MethodAwareRouter::default();
        for (webhook_instance_linked, routes) in webhooks {
            for route in routes {
                if route.methods.is_empty() {
                    router.add(
                        None,
                        &route.route,
                        webhook_instance_linked.clone().build(log_forwarder_sender),
                    );
                } else {
                    for method in route.methods {
                        router.add(
                            Some(method),
                            &route.route,
                            webhook_instance_linked.clone().build(log_forwarder_sender),
                        );
                    }
                }
            }
        }
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
                deployment_id,
                StrVariant::from(http_server.name),
                tcp_listener,
                engine.clone(),
                router,
                db_pool.clone(),
                Now.clone_box(),
                TokioSleep,
                fn_registry.clone(),
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
    activities_stub_ext: Vec<ActivityStubExtConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<ConfigName, WebhookComponentConfigVerified>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<ConfigName>)>,
    global_backtrace_persist: bool,
    fuel: Option<u64>,
}

impl ConfigVerified {
    #[instrument(skip_all)]
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify_all(
        activities_wasm: Vec<ActivityWasmComponentConfigToml>,
        activities_stub: Vec<ActivityStubExtComponentConfigToml>,
        activities_external: Vec<ActivityStubExtComponentConfigToml>,
        workflows: Vec<WorkflowComponentConfigToml>,
        http_servers: Vec<webhook::HttpServer>,
        webhooks: Vec<webhook::WebhookComponentConfigToml>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        path_prefixes: Arc<PathPrefixes>,
        global_backtrace_persist: bool,
        parent_preopen_dir: Option<Arc<Path>>,
        global_executor_instance_limiter: Option<Arc<tokio::sync::Semaphore>>,
        fuel: Option<u64>,
        termination_watcher: &mut watch::Receiver<()>,
        subscription_interruption: Option<Duration>,
    ) -> Result<ConfigVerified, anyhow::Error> {
        // Check uniqueness of server and webhook names.
        {
            if http_servers.len()
                > http_servers
                    .iter()
                    .map(|it| &it.name)
                    .collect::<hashbrown::HashSet<_>>()
                    .len()
            {
                bail!("Each `http_server` must have a unique name");
            }
            if webhooks.len()
                > webhooks
                    .iter()
                    .map(|it| &it.common.name)
                    .collect::<hashbrown::HashSet<_>>()
                    .len()
            {
                bail!("Each `webhook` must have a unique name");
            }
        }
        let http_servers_to_webhook_names = {
            let mut remaining_server_names_to_webhook_names = {
                let mut map: hashbrown::HashMap<ConfigName, Vec<ConfigName>> =
                    hashbrown::HashMap::default();
                for webhook in &webhooks {
                    map.entry(webhook.http_server.clone())
                        .or_default()
                        .push(webhook.common.name.clone());
                }
                map
            };
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
            http_servers_to_webhook_names
        };

        // Fetch and verify components, each in its own tokio task.
        let activities_wasm = activities_wasm
            .into_iter()
            .map(|activity_wasm| {
                tokio::spawn(
                    activity_wasm
                        .fetch_and_verify(
                            wasm_cache_dir.clone(),
                            metadata_dir.clone(),
                            path_prefixes.clone(),
                            ignore_missing_env_vars,
                            parent_preopen_dir.clone(),
                            global_executor_instance_limiter.clone(),
                            fuel,
                        )
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();

        let stub_ext_fetch_verify =
            |activities: Vec<ActivityStubExtComponentConfigToml>, component_type: ComponentType| {
                activities
                    .into_iter()
                    .zip(std::iter::repeat(component_type))
                    .map(|(activity, component_type)| {
                        tokio::spawn(
                            activity
                                .fetch_and_verify(
                                    component_type,
                                    wasm_cache_dir.clone(),
                                    metadata_dir.clone(),
                                    path_prefixes.clone(),
                                )
                                .in_current_span(),
                        )
                    })
            };
        let mut activities_stub_ext =
            stub_ext_fetch_verify(activities_stub, ComponentType::ActivityStub).collect::<Vec<_>>();
        activities_stub_ext.extend(stub_ext_fetch_verify(
            activities_external,
            ComponentType::ActivityExternal,
        ));

        let workflows = workflows
            .into_iter()
            .map(|workflow| {
                tokio::spawn(
                    workflow
                        .fetch_and_verify(
                            wasm_cache_dir.clone(),
                            metadata_dir.clone(),
                            path_prefixes.clone(),
                            global_backtrace_persist,
                            global_executor_instance_limiter.clone(),
                            fuel,
                            subscription_interruption,
                        )
                        .in_current_span(),
                )
            })
            .collect::<Vec<_>>();
        let webhooks_by_names = webhooks
            .into_iter()
            .map(|webhook| {
                tokio::spawn({
                    let wasm_cache_dir = wasm_cache_dir.clone();
                    let metadata_dir = metadata_dir.clone();
                    let path_prefixes = path_prefixes.clone();
                    webhook
                        .fetch_and_verify(
                            wasm_cache_dir,
                            metadata_dir,
                            ignore_missing_env_vars,
                            path_prefixes,
                            subscription_interruption,
                        )
                        .in_current_span()
                })
            })
            .collect::<Vec<_>>();

        // Abort/cancel safety:
        // If an error happens or Ctrl-C is pressed the whole process will shut down.
        // Downloading metadata and content must be robust enough to handle it.
        // We do not need to abort the tasks here.
        let all = futures_util::future::join4(
            futures_util::future::join_all(activities_wasm),
            futures_util::future::join_all(activities_stub_ext),
            futures_util::future::join_all(workflows),
            futures_util::future::join_all(webhooks_by_names),
        );
        tokio::select! {
            (activity_wasm_results, activity_stub_ext_results, workflow_results, webhook_results) = all => {
                let activities_wasm = activity_wasm_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let activities_stub_ext = activity_stub_ext_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let workflows = workflow_results.into_iter().collect::<Result<Result<Vec<_>, _>, _>>()??;
                let mut webhooks_by_names = hashbrown::HashMap::new();
                for webhook in webhook_results {
                    let (k, v) = webhook??;
                    webhooks_by_names.insert(k, v);
                }
                Ok(ConfigVerified {
                    activities_wasm,
                    activities_stub_ext,
                    workflows,
                    webhooks_by_names,
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

/// Holds all the work that does not require a database connection.
struct LinkedComponents {
    workers_linked: Vec<WorkerLinked>,
    webhooks_by_names: hashbrown::HashMap<ConfigName, WebhookInstancesAndRoutes>,
}

#[expect(clippy::large_enum_variant)]
enum CompiledComponent {
    ActivityOrWorkflow {
        worker: WorkerCompiled,
        component_config: ComponentConfig,
    },
    Webhook {
        webhook_name: ConfigName,
        webhook_compiled: WebhookEndpointCompiled,
        routes: Vec<WebhookRouteVerified>,
    },
    ActivityStubOrExternal {
        component_config: ComponentConfig,
    },
}

#[instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
async fn compile_and_verify(
    engines: &Engines,
    activities_wasm: Vec<ActivityWasmConfigVerified>,
    activities_stub_ext: Vec<ActivityStubExtConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<ConfigName, WebhookComponentConfigVerified>,
    global_backtrace_persist: bool,
    fuel: Option<u64>,
    build_semaphore: Option<u64>,
    workflows_lock_extension_leeway: Duration,
    termination_watcher: &mut watch::Receiver<()>,
) -> Result<
    (
        LinkedComponents,
        ComponentConfigRegistryRO,
        Option<String>, /* supressed_errors */
    ),
    anyhow::Error,
> {
    let build_semaphore = build_semaphore.map(|permits| {
        semaphore::Semaphore::new(permits.try_into().expect("u64 must fit into usize"))
    });
    let parent_span = Span::current();
    let pre_spawns: Vec<tokio::task::JoinHandle<Result<_, anyhow::Error>>> = activities_wasm
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
                        }
                    })
                })
            })
        })
        .chain(activities_stub_ext.into_iter().map(|activity_stub_ext| {
            // No build_semaphore as there is no WASM compilation.
            let span = info_span!("activity_stub_ext_init", component_id = %activity_stub_ext.component_id); // automatically associated with parent
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let wasm_component = WasmComponent::new(
                        activity_stub_ext.wasm_path,
                        activity_stub_ext.component_id.component_type,
                    )?;
                    let wit = wasm_component
                        .wit()
                        .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
                        .ok();
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
                    };
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
                    .map(|(worker, component_config)| {
                        CompiledComponent::ActivityOrWorkflow {
                            worker,
                            component_config,
                        }
                    })
                })
            })
        }))
        .chain(
            webhooks_by_names
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
                                secrets: webhook.secrets,
                                allowed_hosts: webhook.allowed_hosts,
                            };
                            let webhook_compiled = webhook_trigger::WebhookEndpointCompiled::new(
                                config,
                                webhook.wasm_path,
                                &engines.webhook_engine,
                            )?;
                            Ok(CompiledComponent::Webhook {
                                webhook_name,
                                webhook_compiled,
                                routes: webhook.routes,
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
            for handle in results_of_results {
                match handle?? {
                    CompiledComponent::ActivityOrWorkflow { worker, component_config } => {
                        // Activity or Workflow
                        component_registry.insert(component_config)?;
                        workers_compiled.push(worker);
                    },
                    CompiledComponent::Webhook{ webhook_name, webhook_compiled, routes } => {
                        let component = ComponentConfig {
                            component_id: webhook_compiled.config.component_id.clone(),
                            imports: webhook_compiled.imports().to_vec(),
                            workflow_or_activity_config: None,
                            wit: webhook_compiled.runnable_component.wasm_component.wit()
                                .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
                                .ok(),
                            workflow_replay_info: None,
                        };
                        component_registry.insert(component)?;
                        let old = webhooks_compiled_by_names.insert(webhook_name, (webhook_compiled, routes));
                        assert!(old.is_none());
                    },
                    CompiledComponent::ActivityStubOrExternal {  component_config } => {
                        component_registry.insert(component_config)?;
                    },
                }
            }
            let (component_registry_ro, supressed_errors) = component_registry.verify_registry();
            let fn_registry: Arc<dyn FunctionRegistry> = Arc::from(component_registry_ro.clone());
            let workers_linked = workers_compiled.into_iter().map(|worker| worker.link(&fn_registry)).collect::<Result<Vec<_>,_>>()?;
            let webhooks_by_names = webhooks_compiled_by_names
                .into_iter()
                .map(|(name, (compiled, routes))|{
                    let component_id = compiled.config.component_id.clone();
                    compiled.link(&engines.webhook_engine, fn_registry.as_ref())
                        .map(|instance| (name, (instance, routes)))
                        .with_context(||format!("cannot compile {component_id}"))
                })
                .collect::<Result<hashbrown::HashMap<_,_>,_>>()?;
            Ok((LinkedComponents {
                workers_linked,
                webhooks_by_names,
            }, component_registry_ro, supressed_errors))
        },
        _ = termination_watcher.changed() => {
            warn!("Received SIGINT, canceling while compiling the components");
            anyhow::bail!("canceling while compiling the components")
        }
    }
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
    assert!(component_id.component_type == ComponentType::ActivityWasm);
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let engine = engines.activity_engine.clone();
    let runnable_component =
        RunnableComponent::new(activity.wasm_path, &engine, component_id.component_type)?;
    let wit = runnable_component
        .wasm_component
        .wit()
        .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
        .ok();

    let worker = ActivityWorkerCompiled::new_with_config(
        runnable_component,
        activity.activity_config,
        engine,
        Now.clone_box(),
        TokioSleep,
    )
    .with_context(|| format!("cannot compile {component_id}"))?;
    Ok(WorkerCompiled::new_activity(
        worker,
        activity.exec_config,
        wit,
        activity.logs_store_min_level,
    ))
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
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    let component_id = workflow.component_id().clone();
    assert!(component_id.component_type == ComponentType::Workflow);
    debug!("Instantiating workflow");
    trace!(?workflow, "Full configuration");
    let engine = engines.workflow_engine.clone();
    let runnable_component =
        RunnableComponent::new(&workflow.wasm_path, &engine, component_id.component_type)?;
    let wit = runnable_component
        .wasm_component
        .wit()
        .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
        .ok();

    WorkerCompiled::new_workflow(
        runnable_component,
        engine,
        workflow,
        wit,
        workflows_lock_extension_leeway,
    )
    .with_context(|| format!("cannot compile {component_id}"))
}

struct WorkflowWorkerCompiledWithConfig {
    worker: WorkflowWorkerCompiled,
    workflows_lock_extension_leeway: Duration,
}

struct WorkerCompiled {
    worker: Either<ActivityWorkerCompiled<TokioSleep>, WorkflowWorkerCompiledWithConfig>,
    exec_config: ExecConfig,
    logs_store_min_level: Option<LogLevel>,
}

impl WorkerCompiled {
    fn new_activity(
        worker: ActivityWorkerCompiled<TokioSleep>,
        exec_config: ExecConfig,
        wit: Option<String>,
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
        };
        (
            WorkerCompiled {
                worker: Either::Left(worker),
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
        wit: Option<String>,
        workflows_lock_extension_leeway: Duration,
    ) -> Result<(WorkerCompiled, ComponentConfig), utils::wasm_tools::DecodeError> {
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
            }),
        };
        Ok((
            WorkerCompiled {
                worker: Either::Right(WorkflowWorkerCompiledWithConfig {
                    worker,
                    workflows_lock_extension_leeway,
                }),
                exec_config: workflow.exec_config,
                logs_store_min_level: workflow.logs_store_min_level,
            },
            component,
        ))
    }

    #[instrument(skip_all, fields(component_id = %self.exec_config.component_id))]
    fn link(self, fn_registry: &Arc<dyn FunctionRegistry>) -> Result<WorkerLinked, anyhow::Error> {
        Ok(WorkerLinked {
            worker: match self.worker {
                Either::Left(activity) => Either::Left(activity),
                Either::Right(workflow_compiled) => Either::Right(WorkflowWorkerLinkedWithConfig {
                    worker: workflow_compiled.worker.link(fn_registry.clone())?,
                    workflows_lock_extension_leeway: workflow_compiled
                        .workflows_lock_extension_leeway,
                }),
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

struct WorkerLinked {
    worker: Either<ActivityWorkerCompiled<TokioSleep>, WorkflowWorkerLinkedWithConfig>,
    exec_config: ExecConfig,
    logs_store_min_level: Option<LogLevel>,
}
impl WorkerLinked {
    fn spawn(
        self,
        deployment_id: DeploymentId,
        db_pool: &Arc<dyn DbPool>,
        cancel_registry: CancelRegistry,
        log_forwarder_sender: &mpsc::Sender<LogInfoAppendRow>,
    ) -> ExecutorTaskHandle {
        let worker: Arc<dyn Worker> = match self.worker {
            Either::Left(activity_compiled) => Arc::from(activity_compiled.into_worker(
                cancel_registry,
                log_forwarder_sender,
                self.logs_store_min_level.map(|min_level| LogStrageConfig {
                    min_level,
                    log_sender: log_forwarder_sender.clone(),
                }),
            )),
            Either::Right(workflow_linked) => Arc::from(workflow_linked.worker.into_worker(
                deployment_id,
                db_pool.clone(),
                Arc::new(DeadlineTrackerFactoryTokio {
                    leeway: workflow_linked.workflows_lock_extension_leeway,
                    clock_fn: Now.clone_box(),
                }),
                cancel_registry,
                self.logs_store_min_level.map(|min_level| LogStrageConfig {
                    min_level,
                    log_sender: log_forwarder_sender.clone(),
                }),
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

#[derive(Debug)]
pub(crate) struct MatchableSourceMap {
    exact_matches: HashMap<String, PathBuf>,
    suffix_matches: HashMap<String, PathBuf>,
}
impl MatchableSourceMap {
    fn new(config_map: HashMap<String, PathBuf>) -> Self {
        let mut exact_matches = HashMap::new();
        let mut suffix_matches = HashMap::new();

        for (k, v) in config_map {
            if let Some(stripped) = k.strip_prefix(".../") {
                // Ensure that all suffixes start with a slash, so the `ends_with` below will only matches full path segments.
                suffix_matches.insert(format!("/{stripped}"), v);
            } else {
                exact_matches.insert(k, v);
            }
        }

        Self {
            exact_matches,
            suffix_matches,
        }
    }

    pub(crate) fn find_matching(&self, frame_symbol_path: &str) -> Option<&PathBuf> {
        if let Some(v) = self.exact_matches.get(frame_symbol_path) {
            return Some(v);
        }

        let mut matches = vec![];

        for (suffix, v) in &self.suffix_matches {
            if frame_symbol_path.ends_with(suffix) {
                matches.push(v);
            }
        }

        match matches.len() {
            0 => None,
            1 => Some(matches[0]),
            _ => {
                warn!("Multiple suffix matches for '{frame_symbol_path}', returning None",);
                None
            }
        }
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
        command::server::{ServerCompiledLinked, ServerVerified, VerifyParams},
        config::config_holder::ConfigHolder,
    };
    use directories::BaseDirs;
    use rstest::rstest;
    use std::{path::PathBuf, sync::Arc};
    use tokio::sync::watch;

    fn get_workspace_dir() -> PathBuf {
        PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")] // for more performant WASM component compilation
    async fn server_verify(
        #[values(
            "obelisk-testing-sqlite-local.toml",
            "obelisk-testing-sqlite-oci.toml",
            "obelisk-testing-sqlite-oci-compat-4.0.0.toml"
        )]
        obelisk_toml: &'static str,
    ) -> Result<(), anyhow::Error> {
        test_utils::set_up();

        let obelisk_toml = get_workspace_dir().join(obelisk_toml);
        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(
            project_dirs,
            base_dirs,
            Some(crate::config::config_holder::ConfigSource::LocalFile(
                obelisk_toml,
            )),
            false,
        )?;
        let config = config_holder.load_config().await?;

        let wasm_cache_dir = config
            .wasm_global_config
            .get_wasm_cache_directory(&config_holder.path_prefixes)
            .await?;
        let codegen_cache = config
            .wasm_global_config
            .codegen_cache
            .get_directory(&config_holder.path_prefixes)
            .await?;
        tokio::fs::create_dir_all(&wasm_cache_dir).await?;
        let metadata_dir = wasm_cache_dir.join("metadata");
        tokio::fs::create_dir_all(&metadata_dir).await?;

        let (_termination_sender, mut termination_watcher) = watch::channel(());

        let (server_verified, _component_source_map) = ServerVerified::new(
            config,
            codegen_cache,
            Arc::from(wasm_cache_dir),
            Arc::from(metadata_dir),
            VerifyParams::default().ignore_missing_env_vars,
            Arc::new(config_holder.path_prefixes),
            &mut termination_watcher,
        )
        .await?;
        ServerCompiledLinked::new(server_verified, &mut termination_watcher, false).await?;
        Ok(())
    }
}
