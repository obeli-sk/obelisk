use super::grpc;
use crate::config::config_holder::ConfigHolder;
use crate::config::toml::webhook;
use crate::config::toml::webhook::WebhookComponentVerified;
use crate::config::toml::webhook::WebhookRouteVerified;
use crate::config::toml::ActivityConfigVerified;
use crate::config::toml::ObeliskConfig;
use crate::config::toml::WasmActivityToml;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::WorkflowToml;
use crate::config::Component;
use crate::config::ConfigStore;
use crate::grpc_util::extractor::accept_trace;
use crate::grpc_util::grpc_mapping::PendingStatusExt as _;
use crate::grpc_util::grpc_mapping::TonicServerOptionExt;
use crate::grpc_util::grpc_mapping::TonicServerResultExt;
use crate::grpc_util::TonicRespResult;
use crate::grpc_util::TonicResult;
use crate::init;
use anyhow::bail;
use anyhow::Context;
use assert_matches::assert_matches;
use concepts::prefixed_ulid::ExecutorId;
use concepts::storage::CreateRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEvent;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionLog;
use concepts::storage::PendingState;
use concepts::ComponentRetryConfig;
use concepts::ConfigId;
use concepts::ConfigIdType;
use concepts::ExecutionId;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::ImportType;
use concepts::PackageIfcFns;
use concepts::ParameterType;
use concepts::Params;
use concepts::ReturnType;
use db_sqlite::sqlite_dao::SqliteConfig;
use db_sqlite::sqlite_dao::SqlitePool;
use directories::ProjectDirs;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher;
use executor::expired_timers_watcher::TimersWatcherConfig;
use executor::worker::Worker;
use hashbrown::HashSet;
use itertools::Either;
use serde::Deserialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tracing::error;
use tracing::info_span;
use tracing::instrument;
use tracing::warn;
use tracing::Instrument;
use tracing::Span;
use tracing::{debug, info, trace};
use utils::time::ClockFn;
use utils::time::Now;
use utils::wasm_tools::SUFFIX_PKG_EXT;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::webhook_trigger;
use wasm_workers::webhook_trigger::MethodAwareRouter;
use wasm_workers::webhook_trigger::WebhookInstance;
use wasm_workers::workflow_worker::WorkflowWorkerCompiled;
use wasm_workers::workflow_worker::WorkflowWorkerLinked;

const EPOCH_MILLIS: u64 = 10;

#[derive(Debug)]
struct GrpcServer<DB: DbConnection, P: DbPool<DB>> {
    db_pool: P,
    component_registry_ro: ComponentConfigRegistryRO,
    phantom_data: PhantomData<DB>,
}

impl<DB: DbConnection, P: DbPool<DB>> GrpcServer<DB, P> {
    fn new(db_pool: P, component_registry_ro: ComponentConfigRegistryRO) -> Self {
        Self {
            db_pool,
            component_registry_ro,
            phantom_data: PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<DB: DbConnection + 'static, P: DbPool<DB> + 'static> grpc::scheduler_server::Scheduler
    for GrpcServer<DB, P>
{
    #[instrument(skip_all, fields(execution_id, ffqn, params, config_id))]
    async fn submit(
        &self,
        request: tonic::Request<grpc::SubmitRequest>,
    ) -> TonicRespResult<grpc::SubmitResponse> {
        let request = request.into_inner();
        let grpc::FunctionName {
            interface_name,
            function_name,
        } = request.function.argument_must_exist("function")?;
        let execution_id = request
            .execution_id
            .map_or_else(|| Ok(ExecutionId::generate()), ExecutionId::try_from)?;
        let span = Span::current();
        span.record("execution_id", tracing::field::display(execution_id));
        let ffqn =
            concepts::FunctionFqn::new_arc(Arc::from(interface_name), Arc::from(function_name));
        span.record("ffqn", tracing::field::display(&ffqn));
        // Deserialize params JSON into `Params`
        let params = {
            let params = request.params.argument_must_exist("params")?;
            let params = String::from_utf8(params.value).map_err(|_err| {
                tonic::Status::invalid_argument("argument `params` must be UTF-8 encoded")
            })?;
            span.record("params", &params);
            Params::deserialize(&mut serde_json::Deserializer::from_str(&params)).map_err(
                |serde_err| {
                    tonic::Status::invalid_argument(format!(
                        "argument `params` must be encoded as JSON array - {serde_err}"
                    ))
                },
            )?
        };

        // Check that ffqn exists
        let Some((component, fn_meta)) = self.component_registry_ro.find_by_exported_ffqn(&ffqn)
        else {
            return Err(tonic::Status::not_found("function not found"));
        };
        // Type check `params`
        if let Err(err) = params.typecheck(
            fn_meta
                .parameter_types
                .iter()
                .map(|ParameterType { type_wrapper, .. }| type_wrapper),
        ) {
            return Err(tonic::Status::invalid_argument(format!(
                "argument `params` invalid - {err}"
            )));
        }
        let db_connection = self.db_pool.connection();
        let created_at = Now.now();
        span.record("config_id", tracing::field::display(&component.config_id));
        // Associate the (root) request execution with the request span. Makes possible to find the trace by execution id.
        let metadata = concepts::ExecutionMetadata::from_parent_span(&span);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                metadata,
                ffqn,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: component.config_store.default_retry_exp_backoff(),
                max_retries: component.config_store.default_max_retries(),
                config_id: component.config_id.clone(),
                return_type: fn_meta
                    .return_type
                    .as_ref()
                    .map(|rt| rt.type_wrapper.clone()),
                topmost_parent: execution_id,
            })
            .await
            .to_status()?;
        let resp = grpc::SubmitResponse {
            execution_id: Some(grpc::ExecutionId {
                id: execution_id.to_string(),
            }),
        };
        Ok(tonic::Response::new(resp))
    }

    type GetStatusStream =
        Pin<Box<dyn Stream<Item = Result<grpc::GetStatusResponse, tonic::Status>> + Send>>;

    #[instrument(skip_all, fields(execution_id))]
    async fn get_status(
        &self,
        request: tonic::Request<grpc::GetStatusRequest>,
    ) -> TonicRespResult<Self::GetStatusStream> {
        use grpc::get_status_response::{ExecutionSummary, Message};
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(execution_id));
        let db_connection = self.db_pool.connection();
        let execution_log = db_connection.get(execution_id).await.to_status()?;
        let current_pending_state = execution_log.pending_state;
        let grpc_pending_status = convert_execution_status(&execution_log);
        let is_finished = grpc_pending_status.is_finished();
        let summary = grpc::GetStatusResponse {
            message: Some(Message::Summary(ExecutionSummary {
                function_name: Some(execution_log.ffqn().into()),
                current_status: Some(grpc_pending_status),
            })),
        };
        if is_finished || !request.follow {
            let output = tokio_stream::once(Ok(summary));
            Ok(tonic::Response::new(
                Box::pin(output) as Self::GetStatusStream
            ))
        } else {
            let (tx, rx) = mpsc::channel(1);
            // send current pending status
            tx.send(TonicResult::Ok(summary))
                .await
                .expect("mpsc bounded channel requires buffer > 0");
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await; // TODO: Switch to subscription-based approach
                    // FIXME: pagination
                    match db_connection.get(execution_id).await {
                        Ok(execution_log) => {
                            if execution_log.pending_state != current_pending_state {
                                let grpc_pending_status = convert_execution_status(&execution_log);
                                let is_finished = grpc_pending_status.is_finished();
                                let message = grpc::GetStatusResponse {
                                    message: Some(Message::CurrentStatus(grpc_pending_status)),
                                };
                                if tx.send(TonicResult::Ok(message)).await.is_err() || is_finished {
                                    return;
                                }
                            }
                        }
                        Err(db_err) => {
                            error!("Database error while streaming status of {execution_id} - {db_err:?}");
                            return;
                        }
                    }
                }
            }.in_current_span()
            );
            let output = ReceiverStream::new(rx);
            Ok(tonic::Response::new(
                Box::pin(output) as Self::GetStatusStream
            ))
        }
    }
}

#[tonic::async_trait]
impl<DB: DbConnection + 'static, P: DbPool<DB> + 'static>
    grpc::function_repository_server::FunctionRepository for GrpcServer<DB, P>
{
    #[instrument(skip_all)]
    async fn list_components(
        &self,
        request: tonic::Request<grpc::ListComponentsRequest>,
    ) -> TonicRespResult<grpc::ListComponentsResponse> {
        let request = request.into_inner();
        let components = self.component_registry_ro.list(request.extensions);
        let mut res_components = Vec::with_capacity(components.len());
        for component in components {
            let res_component = grpc::Component {
                name: component.config_store.name().to_string(),
                r#type: component.config_id.config_id_type.to_string(),
                config_id: Some(component.config_id.into()),
                digest: component.config_store.common().content_digest.to_string(),
                exports: inspect_fns(component.exports, true),
                imports: inspect_fns(component.imports, true),
            };
            res_components.push(res_component);
        }
        Ok(tonic::Response::new(grpc::ListComponentsResponse {
            components: res_components,
        }))
    }
}

fn inspect_fns(functions: Vec<FunctionMetadata>, show_params: bool) -> Vec<grpc::FunctionDetails> {
    let mut vec = Vec::with_capacity(functions.len());
    for FunctionMetadata {
        ffqn,
        parameter_types,
        return_type,
    } in functions
    {
        let fun = grpc::FunctionDetails {
            params: if show_params {
                parameter_types
                    .0
                    .into_iter()
                    .map(|p| grpc::FunctionParameter {
                        name: p.name.map(|s| s.to_string()),
                        r#type: Some(grpc::WitType {
                            wit_type: p.wit_type.map(|s| s.to_string()),
                            internal: to_any(
                                &p.type_wrapper,
                                format!("urn:obelisk:json:params:{ffqn}"),
                            ),
                        }),
                    })
                    .collect()
            } else {
                Vec::default()
            },
            return_type: return_type.map(
                |ReturnType {
                     type_wrapper,
                     wit_type,
                 }| grpc::WitType {
                    wit_type: wit_type.map(|s| s.to_string()),
                    internal: to_any(&type_wrapper, format!("urn:obelisk:json:ret:{ffqn}")),
                },
            ),
            function: Some(ffqn.into()),
        };
        vec.push(fun);
    }
    vec
}

fn to_any<T: serde::Serialize>(serializable: T, uri: String) -> Option<prost_wkt_types::Any> {
    serde_json::to_string(&serializable)
        .inspect_err(|ser_err| {
            error!(
                "Cannot serialize {:?} - {ser_err:?}",
                std::any::type_name::<T>()
            );
        })
        .ok()
        .map(|res| prost_wkt_types::Any {
            type_url: uri,
            value: res.into_bytes(),
        })
}

fn convert_execution_status(execution_log: &ExecutionLog) -> grpc::ExecutionStatus {
    use grpc::execution_status::{BlockedByJoinSet, Finished, Locked, PendingAt, Status};
    grpc::ExecutionStatus {
        status: Some(match execution_log.pending_state {
            PendingState::Locked {
                executor_id: _,
                run_id,
                lock_expires_at,
            } => Status::Locked(Locked {
                run_id: Some(run_id.into()),
                lock_expires_at: Some(lock_expires_at.into()),
            }),
            PendingState::PendingAt { scheduled_at } => Status::PendingAt(PendingAt {
                scheduled_at: Some(scheduled_at.into()),
            }),
            PendingState::BlockedByJoinSet {
                join_set_id,
                lock_expires_at,
            } => Status::BlockedByJoinSet(BlockedByJoinSet {
                join_set_id: Some(join_set_id.into()),
                lock_expires_at: Some(lock_expires_at.into()),
            }),
            PendingState::Finished => {
                let finished = execution_log.last_event();
                let finished_at = finished.created_at;
                let finished = assert_matches!(finished, ExecutionEvent {
                    event: ExecutionEventInner::Finished { result },
                    ..
                } => result);
                Status::Finished(Finished {
                    result: to_any(
                        finished,
                        format!(
                            "urn:obelisk:json:params:{ffqn}",
                            ffqn = execution_log.ffqn()
                        ),
                    ),
                    created_at: execution_log
                        .events
                        .first()
                        .map(|event| event.created_at.into()),
                    finished_at: Some(finished_at.into()),
                })
            }
        }),
    }
}

pub(crate) async fn run(
    project_dirs: Option<ProjectDirs>,
    config: Option<PathBuf>,
    clean_db: bool,
    clean_all_cache: bool,
    clean_codegen_cache: bool,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs, config);
    let mut config = config_holder.load_config().await?;
    let _guard = init::init(&mut config);

    Box::pin(run_internal(
        config,
        clean_db,
        clean_all_cache,
        clean_codegen_cache,
        config_holder,
    ))
    .await?;
    Ok(())
}

pub(crate) async fn verify(
    project_dirs: Option<ProjectDirs>,
    config: Option<PathBuf>,
    clean_db: bool,
    clean_cache: bool,
    clean_codegen_cache: bool,
) -> Result<(), anyhow::Error> {
    let config_holder = ConfigHolder::new(project_dirs, config);
    let mut config = config_holder.load_config().await?;
    let _guard = init::init(&mut config);
    Box::pin(verify_internal(
        config,
        clean_db,
        clean_cache,
        clean_codegen_cache,
        config_holder,
    ))
    .await?;
    Ok(())
}

async fn verify_internal(
    config: ObeliskConfig,
    clean_db: bool,
    clean_cache: bool,
    clean_codegen_cache: bool,
    config_holder: ConfigHolder,
) -> Result<ServerVerified, anyhow::Error> {
    debug!("Using toml config: {config:#?}");
    let db_file = config
        .sqlite
        .get_sqlite_file(config_holder.project_dirs.as_ref())
        .await?;
    let wasm_cache_dir = config
        .oci
        .get_wasm_directory(config_holder.project_dirs.as_ref())
        .await?;
    let codegen_cache = if let Some(codegen_cache) = &config.codegen_cache {
        Some(
            codegen_cache
                .get_directory(config_holder.project_dirs.as_ref())
                .await?,
        )
    } else {
        None
    };
    debug!("Using codegen cache? {codegen_cache:?}");
    let ignore_not_found = |err: std::io::Error| {
        if err.kind() == std::io::ErrorKind::NotFound {
            Ok(())
        } else {
            Err(err)
        }
    };
    if clean_db {
        tokio::fs::remove_file(&db_file)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database file `{db_file:?}`"))?;
        let with_suffix = |suffix: &str| {
            let db_file_name = db_file.file_name().expect("db_file must be a file");
            let mut wal_file_name = db_file_name.to_owned();
            wal_file_name.push(suffix);
            if let Some(parent) = db_file.parent() {
                parent.join(wal_file_name)
            } else {
                PathBuf::from(wal_file_name)
            }
        };
        let wal_file = with_suffix("-wal");
        tokio::fs::remove_file(&wal_file)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database file `{wal_file:?}`"))?;
        let shm_file = with_suffix("-shm");
        tokio::fs::remove_file(&shm_file)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database file `{shm_file:?}`"))?;
    }
    if clean_cache {
        tokio::fs::remove_dir_all(&wasm_cache_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete wasm cache directory {wasm_cache_dir:?}"))?;
    }
    if clean_cache || clean_codegen_cache {
        if let Some(codegen_cache) = &codegen_cache {
            tokio::fs::remove_dir_all(codegen_cache)
                .await
                .or_else(ignore_not_found)
                .with_context(|| {
                    format!("cannot delete codegen cache directory {wasm_cache_dir:?}")
                })?;
            tokio::fs::create_dir_all(codegen_cache)
                .await
                .with_context(|| {
                    format!("cannot create codegen cache directory {codegen_cache:?}")
                })?;
        }
    }
    tokio::fs::create_dir_all(&wasm_cache_dir)
        .await
        .with_context(|| format!("cannot create wasm cache directory {wasm_cache_dir:?}"))?;
    let metadata_dir = wasm_cache_dir.join("metadata");
    tokio::fs::create_dir_all(&metadata_dir)
        .await
        .with_context(|| format!("cannot create wasm metadata directory {metadata_dir:?}"))?;

    let server_verified = ServerVerified::new(
        config,
        codegen_cache.as_deref(),
        Arc::from(wasm_cache_dir),
        Arc::from(metadata_dir),
        db_file,
    )
    .await?;
    info!("Server configuration was verified");
    Ok(server_verified)
}

async fn run_internal(
    config: ObeliskConfig,
    clean_db: bool,
    clean_cache: bool,
    clean_codegen_cache: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
    let api_listening_addr = config.api_listening_addr;
    let verified = Box::pin(verify_internal(
        config,
        clean_db,
        clean_cache,
        clean_codegen_cache,
        config_holder,
    ))
    .await?;
    let (init, component_registry_ro) = ServerInit::new(verified).await?;
    let grpc_server = Arc::new(GrpcServer::new(init.db_pool.clone(), component_registry_ro));
    let grpc_server_res = tonic::transport::Server::builder()
        .layer(
            tower::ServiceBuilder::new()
                .layer(tower_http::trace::TraceLayer::new_for_grpc().make_span_with(make_span))
                .map_request(accept_trace),
        )
        .add_service(
            grpc::scheduler_server::SchedulerServer::from_arc(grpc_server.clone())
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip),
        )
        .add_service(
            grpc::function_repository_server::FunctionRepositoryServer::from_arc(
                grpc_server.clone(),
            )
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip),
        )
        .serve_with_shutdown(api_listening_addr, async move {
            info!("Serving gRPC requests at {api_listening_addr}");
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for SIGINT event");
            warn!("Received SIGINT");
        })
        .await
        .with_context(|| format!("grpc server error listening on {api_listening_addr}"));
    // ^ Will await until the gRPC server shuts down.
    init.close().await?;
    grpc_server_res
}

fn make_span<B>(request: &axum::http::Request<B>) -> Span {
    let headers = request.headers();
    info_span!("incoming gRPC request", ?headers)
}

struct ServerVerified {
    component_registry_ro: ComponentConfigRegistryRO,
    compiled_components: LinkedComponents,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<String>)>,
    engines: Engines,
    sqlite_config: SqliteConfig,
    db_file: PathBuf,
}

impl ServerVerified {
    #[instrument(name = "verify", skip_all)]
    async fn new(
        config: ObeliskConfig,
        codegen_cache: Option<&Path>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        db_file: PathBuf,
    ) -> Result<Self, anyhow::Error> {
        let engines = {
            let codegen_cache_config_file_holder = Engines::write_codegen_config(codegen_cache)
                .await
                .context("error configuring codegen cache")?;

            Engines::auto_detect_allocator(
                &config.wasmtime_pooling_config.into(),
                codegen_cache_config_file_holder,
            )?
        };
        let sqlite_config = config.sqlite.as_config();
        let config = fetch_and_verify_all(
            config.wasm_activities,
            config.workflows,
            config.http_servers,
            config.webhook_components,
            wasm_cache_dir,
            metadata_dir,
        )
        .await?;
        debug!("Verified config: {config:#?}");
        let (compiled_components, component_registry_ro) = compile_and_verify(
            &engines,
            config.wasm_activities,
            config.workflows,
            config.webhooks_by_names,
        )
        .await?;
        Ok(Self {
            compiled_components,
            component_registry_ro,
            http_servers_to_webhook_names: config.http_servers_to_webhook_names,
            engines,
            sqlite_config,
            db_file,
        })
    }
}

struct ServerInit {
    db_pool: SqlitePool,
    exec_join_handles: Vec<ExecutorTaskHandle>,
    timers_watcher: expired_timers_watcher::TaskHandle,
    #[expect(dead_code)] // http servers will be aborted automatically
    http_servers_handles: Vec<AbortOnDropHandle>,
}

impl ServerInit {
    async fn new(
        mut verified: ServerVerified,
    ) -> Result<(ServerInit, ComponentConfigRegistryRO), anyhow::Error> {
        // Start components requiring a database
        let _epoch_ticker = EpochTicker::spawn_new(
            verified.engines.weak_refs(),
            Duration::from_millis(EPOCH_MILLIS),
        );
        let db_pool = SqlitePool::new(&verified.db_file, verified.sqlite_config)
            .await
            .with_context(|| format!("cannot open sqlite file `{:?}`", verified.db_file))?;

        let timers_watcher = expired_timers_watcher::spawn_new(
            db_pool.clone(),
            TimersWatcherConfig {
                tick_sleep: Duration::from_millis(100),
                clock_fn: Now,
            },
        );

        // Associate webhooks with http servers
        let http_servers_to_webhooks = {
            verified
                .http_servers_to_webhook_names
                .into_iter()
                .map(|(http_server, webhook_names)| {
                    let instances_and_routes = webhook_names
                        .into_iter()
                        .map(|name| {
                            verified
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

        // Spawn executors
        let exec_join_handles = verified
            .compiled_components
            .workers_linked
            .into_iter()
            .map(|pre_spawn| pre_spawn.spawn(db_pool.clone()))
            .collect();

        // Start TCP listeners
        let http_servers_handles: Vec<AbortOnDropHandle> = start_webhooks(
            http_servers_to_webhooks,
            &verified.engines,
            db_pool.clone(),
            Arc::from(verified.component_registry_ro.clone()),
        )
        .await?;
        Ok((
            ServerInit {
                db_pool,
                exec_join_handles,
                timers_watcher,
                http_servers_handles,
            },
            verified.component_registry_ro,
        ))
    }

    async fn close(self) -> Result<(), anyhow::Error> {
        self.timers_watcher.close().await;
        for exec_join_handle in self.exec_join_handles {
            exec_join_handle.close().await;
        }
        self.db_pool
            .close()
            .await
            .context("cannot close the database")
    }
}

type WebhookInstancesAndRoutes = (
    WebhookInstance<Now, SqlitePool, SqlitePool>,
    Vec<WebhookRouteVerified>,
);

async fn start_webhooks(
    http_servers_to_webhooks: Vec<(webhook::HttpServer, Vec<WebhookInstancesAndRoutes>)>,
    engines: &Engines,
    db_pool: SqlitePool,
    fn_registry: Arc<dyn FunctionRegistry>,
) -> Result<Vec<AbortOnDropHandle>, anyhow::Error> {
    let mut abort_handles = Vec::with_capacity(http_servers_to_webhooks.len());
    let engine = &engines.webhook_engine;
    for (http_server, webhooks) in http_servers_to_webhooks {
        let mut router = MethodAwareRouter::default();
        for (webhook_instance, routes) in webhooks {
            for route in routes {
                if route.methods.is_empty() {
                    router.add(None, &route.route, webhook_instance.clone());
                } else {
                    for method in route.methods {
                        router.add(Some(method), &route.route, webhook_instance.clone());
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
            "HTTP server `{}` is listening on {server_addr}",
            http_server.name,
        );
        let server = AbortOnDropHandle(
            tokio::spawn(webhook_trigger::server(
                tcp_listener,
                engine.clone(),
                router,
                db_pool.clone(),
                Now,
                fn_registry.clone(),
                http_server.request_timeout.into(),
            ))
            .abort_handle(),
        );
        abort_handles.push(server);
    }
    Ok(abort_handles)
}

#[derive(Debug)]
struct ConfigVerified {
    wasm_activities: Vec<ActivityConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<String, WebhookComponentVerified>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<String>)>,
}

#[instrument(skip_all)]
async fn fetch_and_verify_all(
    wasm_activities: Vec<WasmActivityToml>,
    workflows: Vec<WorkflowToml>,
    http_servers: Vec<webhook::HttpServer>,
    webhooks: Vec<webhook::WebhookComponent>,
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
) -> Result<ConfigVerified, anyhow::Error> {
    // Check for name clashes which might make for confusing traces.
    {
        let mut seen = HashSet::new();
        for name in wasm_activities
            .iter()
            .map(|a| &a.common.name)
            .chain(workflows.iter().map(|w| &w.common.name))
        {
            if !seen.insert(name) {
                warn!("Component with the same name already exists - {name}");
            }
        }
    }
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
            let mut map: hashbrown::HashMap<String, Vec<String>> = hashbrown::HashMap::default();
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
    // Download WASM files from OCI registries if needed.
    // TODO: Switch to `JoinSet` when madsim supports it.
    let activities = wasm_activities
        .into_iter()
        .map(|activity| {
            tokio::spawn({
                let wasm_cache_dir = wasm_cache_dir.clone();
                let metadata_dir = metadata_dir.clone();
                async move {
                    activity
                        .fetch_and_verify(wasm_cache_dir.clone(), metadata_dir.clone())
                        .await
                }
                .in_current_span()
            })
        })
        .collect::<Vec<_>>();
    let workflows = workflows
        .into_iter()
        .map(|workflow| {
            tokio::spawn(
                workflow
                    .fetch_and_verify(wasm_cache_dir.clone(), metadata_dir.clone())
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
                async move {
                    let name = webhook.common.name.clone();
                    let webhook = webhook
                        .fetch_and_verify(wasm_cache_dir.clone(), metadata_dir.clone())
                        .await?;
                    Ok::<_, anyhow::Error>((name, webhook))
                }
                .in_current_span()
            })
        })
        .collect::<Vec<_>>();

    // Abort/cancel safety:
    // If an error happens or Ctrl-C is pressed the whole process will shut down.
    // Downloading metadata and content must be robust enough to handle it.
    // We do not need to abort the tasks here.
    let all = futures_util::future::join3(
        futures_util::future::join_all(activities),
        futures_util::future::join_all(workflows),
        futures_util::future::join_all(webhooks_by_names),
    );
    tokio::select! {
        (activity_results, workflow_results, webhook_results) = all => {
            let mut wasm_activities = Vec::with_capacity(activity_results.len());
            for a in activity_results {
                wasm_activities.push(a??);
            }
            let mut workflows = Vec::with_capacity(workflow_results.len());
            for w in workflow_results {
                workflows.push(w??);
            }
            let mut webhooks_by_names = hashbrown::HashMap::new();
            for webhook in webhook_results {
                let (k, v) = webhook??;
                webhooks_by_names.insert(k, v);
            }
            Ok(ConfigVerified {wasm_activities, workflows, webhooks_by_names, http_servers_to_webhook_names})
        },
        sigint = tokio::signal::ctrl_c() => {
            sigint.expect("failed to listen for SIGINT event");
            warn!("Received SIGINT");
            anyhow::bail!("cancelled while resolving the WASM files")
        }
    }
}

/// Holds all the work that does not require a database connection.
struct LinkedComponents {
    workers_linked: Vec<WorkerLinked>,
    webhooks_by_names: hashbrown::HashMap<String, WebhookInstancesAndRoutes>,
}

#[instrument(skip_all)]
async fn compile_and_verify(
    engines: &Engines,
    wasm_activities: Vec<ActivityConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<String, WebhookComponentVerified>,
) -> Result<(LinkedComponents, ComponentConfigRegistryRO), anyhow::Error> {
    let mut component_registry = ComponentConfigRegistry::default();
    let pre_spawns: Vec<tokio::task::JoinHandle<Result<_, anyhow::Error>>> = wasm_activities
        .into_iter()
        .map(|activity| {
            let engines = engines.clone();
            let span = tracing::Span::current();
            #[cfg_attr(madsim, allow(deprecated))]
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let executor_id = ExecutorId::generate();
                    prespawn_activity(activity, &engines, executor_id).map(Either::Left)
                })
            })
        })
        .chain(workflows.into_iter().map(|workflow| {
            let engines = engines.clone();
            let span = tracing::Span::current();
            #[cfg_attr(madsim, allow(deprecated))]
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let executor_id = ExecutorId::generate();
                    prespawn_workflow(workflow, &engines, executor_id).map(Either::Left)
                })
            })
        }))
        .chain(webhooks_by_names.into_iter().map(|(name, webhook)| {
            let engines = engines.clone();
            let span = tracing::Span::current();
            #[cfg_attr(madsim, allow(deprecated))]
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let config_id = webhook.config_id;
                    let webhook_compiled = webhook_trigger::WebhookCompiled::new(
                        webhook.wasm_path,
                        &engines.webhook_engine,
                        config_id.clone(),
                        webhook.forward_stdout,
                        webhook.forward_stderr,
                        Arc::from(webhook.env_vars),
                        webhook_trigger::RetryConfigOverride::default(), // TODO make configurable
                    )?;
                    Ok(Either::Right((name, (webhook_compiled, webhook.routes))))
                })
            })
        }))
        .collect();

    // Abort/cancel safety:
    // If an error happens or Ctrl-C is pressed the whole process will shut down.
    let pre_spawns = futures_util::future::join_all(pre_spawns);
    tokio::select! {
        results_of_results = pre_spawns => {
            let mut webhook_imports: hashbrown::HashMap<ConfigId, Vec<FunctionMetadata>> = hashbrown::HashMap::new();
            let mut workers_compiled = Vec::with_capacity(results_of_results.len());
            let mut webhooks_compiled_by_names = hashbrown::HashMap::new();
            for handle in results_of_results {
                match handle?? {
                    Either::Left((worker_compiled, component)) => {
                        component_registry.insert(component)?;
                        workers_compiled.push(worker_compiled);
                    },
                    Either::Right((webhook_name, (webhook_compiled, routes))) => {
                        webhook_imports.insert(webhook_compiled.config_id.clone(), webhook_compiled.imports().to_vec());
                        let old = webhooks_compiled_by_names.insert(webhook_name, (webhook_compiled, routes));

                        assert!(old.is_none());
                    },
                }
            }
            let component_registry_ro = component_registry.verify_imports(webhook_imports)?;
            let fn_registry: Arc<dyn FunctionRegistry> = Arc::from(component_registry_ro.clone());
            let workers_linked = workers_compiled.into_iter().map(|worker| worker.link(&fn_registry)).collect::<Result<Vec<_>,_>>()?;
            let webhooks_by_names = webhooks_compiled_by_names
                .into_iter()
                .map(|(name, (compiled, routes))| compiled.link(&engines.webhook_engine, fn_registry.as_ref()).map(|instance| (name, (instance, routes))))
                .collect::<Result<hashbrown::HashMap<_,_>,_>>()?;
            Ok((LinkedComponents {
                workers_linked,
                webhooks_by_names,
            }, component_registry_ro))
        },
        sigint = tokio::signal::ctrl_c() => {
            sigint.expect("failed to listen for SIGINT event");
            warn!("Received SIGINT");
            anyhow::bail!("cancelled while compiling the components")
        }
    }
}

#[instrument(skip_all, fields(
    %executor_id,
    config_id = %activity.exec_config.config_id,
    name = activity.config_store.name(),
    wasm_path = ?activity.wasm_path,
))]
fn prespawn_activity(
    activity: ActivityConfigVerified,
    engines: &Engines,
    executor_id: ExecutorId,
) -> Result<(WorkerCompiled, Component), anyhow::Error> {
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let worker = ActivityWorker::new_with_config(
        activity.wasm_path,
        activity.activity_config,
        engines.activity_engine.clone(),
        Now,
    )?;
    Ok(WorkerCompiled::new_activity(
        worker,
        activity.config_store,
        activity.exec_config,
        executor_id,
    ))
}

#[instrument(skip_all, fields(
    %executor_id,
    config_id = %workflow.exec_config.config_id,
    name = workflow.config_store.name(),
    wasm_path = ?workflow.wasm_path,
))]
fn prespawn_workflow(
    workflow: WorkflowConfigVerified,
    engines: &Engines,
    executor_id: ExecutorId,
) -> Result<(WorkerCompiled, Component), anyhow::Error> {
    debug!("Instantiating workflow");
    trace!(?workflow, "Full configuration");
    let worker = WorkflowWorkerCompiled::new_with_config(
        workflow.wasm_path,
        workflow.workflow_config,
        engines.workflow_engine.clone(),
        Now,
    )?;
    Ok(WorkerCompiled::new_workflow(
        worker,
        workflow.config_store,
        workflow.exec_config,
        executor_id,
    ))
}

struct WorkerCompiled {
    worker: Either<Arc<dyn Worker>, WorkflowWorkerCompiled<Now>>,
    exec_config: ExecConfig,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_id: ExecutorId,
}

impl WorkerCompiled {
    fn new_activity(
        worker: ActivityWorker<Now>,
        config_store: ConfigStore,
        exec_config: ExecConfig,
        executor_id: ExecutorId,
    ) -> (WorkerCompiled, Component) {
        let component = Component {
            config_id: exec_config.config_id.clone(),
            config_store,
            exports: worker.exported_functions().to_vec(),
            exports_hierarchy: worker.exports_hierarchy().to_vec(),
            imports: worker.imported_functions().to_vec(),
            import_type: ImportType::ActivityWasm,
        };
        (
            WorkerCompiled {
                worker: Either::Left(Arc::from(worker)),
                exec_config,
                task_limiter: None,
                executor_id,
            },
            component,
        )
    }

    fn new_workflow(
        worker: WorkflowWorkerCompiled<Now>,
        config_store: ConfigStore,
        exec_config: ExecConfig,
        executor_id: ExecutorId,
    ) -> (WorkerCompiled, Component) {
        let component = Component {
            config_id: exec_config.config_id.clone(),
            config_store,
            exports: worker.exported_functions().to_vec(),
            exports_hierarchy: worker.exports_hierarchy().to_vec(),
            imports: worker.imported_functions().to_vec(),
            import_type: ImportType::Workflow,
        };
        (
            WorkerCompiled {
                worker: Either::Right(worker),
                exec_config,
                task_limiter: None,
                executor_id,
            },
            component,
        )
    }

    #[instrument(skip_all, fields(config_id = %self.exec_config.config_id), err)]
    fn link(self, fn_registry: &Arc<dyn FunctionRegistry>) -> Result<WorkerLinked, anyhow::Error> {
        Ok(WorkerLinked {
            worker: match self.worker {
                Either::Left(activity) => Either::Left(activity),
                Either::Right(workflow_compiled) => {
                    Either::Right(workflow_compiled.link(fn_registry.clone())?)
                }
            },
            exec_config: self.exec_config,
            task_limiter: self.task_limiter,
            executor_id: self.executor_id,
        })
    }
}

struct WorkerLinked {
    worker: Either<Arc<dyn Worker>, WorkflowWorkerLinked<Now, SqlitePool, SqlitePool>>,
    exec_config: ExecConfig,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_id: ExecutorId,
}
impl WorkerLinked {
    fn spawn(self, db_pool: SqlitePool) -> ExecutorTaskHandle {
        let worker = match self.worker {
            Either::Left(activity) => activity,
            Either::Right(workflow_linked) => {
                Arc::from(workflow_linked.into_worker(db_pool.clone()))
            }
        };
        ExecTask::spawn_new(
            worker,
            self.exec_config,
            Now,
            db_pool,
            self.task_limiter,
            self.executor_id,
        )
    }
}

#[derive(Default, Debug)]
struct ComponentConfigRegistry {
    inner: ComponentConfigRegistryInner,
}

#[derive(Default, Debug)]
struct ComponentConfigRegistryInner {
    exported_ffqns: hashbrown::HashMap<
        FunctionFqn,
        (ConfigId, FunctionMetadata, ComponentRetryConfig, ImportType),
    >,
    export_hierarchy: Vec<PackageIfcFns>,
    ids_to_components: hashbrown::HashMap<ConfigId, Component>,
}

impl ComponentConfigRegistry {
    fn insert(&mut self, component: Component) -> Result<(), anyhow::Error> {
        // verify that the component or its exports are not already present
        if self
            .inner
            .ids_to_components
            .contains_key(&component.config_id)
        {
            bail!("component {} is already inserted", component.config_id);
        }
        for exported_ffqn in component.exports.iter().map(|f| &f.ffqn) {
            if let Some((offending_id, _, _, _)) = self.inner.exported_ffqns.get(exported_ffqn) {
                bail!("function {exported_ffqn} is already exported by component {offending_id}, cannot insert {}", component.config_id);
            }
        }

        // insert to `exported_ffqns`
        for exported_fn_metadata in &component.exports {
            let old = self.inner.exported_ffqns.insert(
                exported_fn_metadata.ffqn.clone(),
                (
                    component.config_id.clone(),
                    exported_fn_metadata.clone(),
                    ComponentRetryConfig {
                        max_retries: component.config_store.default_max_retries(),
                        retry_exp_backoff: component.config_store.default_retry_exp_backoff(),
                    },
                    component.import_type,
                ),
            );
            assert!(old.is_none());
        }
        // insert to `export_hierarchy`
        self.inner
            .export_hierarchy
            .extend_from_slice(&component.exports_hierarchy);
        // insert to `ids_to_components`
        let old = self
            .inner
            .ids_to_components
            .insert(component.config_id.clone(), component);
        assert!(old.is_none());

        Ok(())
    }

    /// Verify that each imported function can be matched by looking at the available exports.
    /// This is a best effort to give function-level error messages.
    /// WASI imports and host functions are not validated at the moment, those errors
    /// are caught by wasmtime while pre-instantiation with a message containing the missing interface.
    fn verify_imports(
        self,
        webhook_imports: hashbrown::HashMap<ConfigId, Vec<FunctionMetadata>>,
    ) -> Result<ComponentConfigRegistryRO, anyhow::Error> {
        let mut errors = Vec::new();
        for (config_id, examined_component) in &self.inner.ids_to_components {
            self.verify_imports_component(config_id, &examined_component.imports, &mut errors);
        }
        for (config_id, imports) in webhook_imports {
            self.verify_imports_component(&config_id, &imports, &mut errors);
        }
        if errors.is_empty() {
            Ok(ComponentConfigRegistryRO {
                inner: Arc::new(self.inner),
            })
        } else {
            let errors = errors.join("\n");
            bail!("component resolution error: \n{errors}")
        }
    }

    fn additional_import_whitelist(import: &FunctionMetadata, config_id: &ConfigId) -> bool {
        match config_id.config_id_type {
            ConfigIdType::ActivityWasm => {
                // wasi + log
                import.ffqn.ifc_fqn.namespace() == "wasi"
                    || import.ffqn.ifc_fqn.deref() == "obelisk:log/log"
            }
            ConfigIdType::Workflow => {
                // host activities + log
                import.ffqn.ifc_fqn.deref() == "obelisk:log/log"
                    || import.ffqn.ifc_fqn.deref() == "obelisk:workflow/host-activities"
            }
            ConfigIdType::WebhookWasm => {
                // wasi + host activities + log
                import.ffqn.ifc_fqn.namespace() == "wasi"
                    || import.ffqn.ifc_fqn.deref() == "obelisk:log/log"
                    || import.ffqn.ifc_fqn.deref() == "obelisk:workflow/host-activities"
            }
        }
    }

    fn verify_imports_component(
        &self,
        config_id: &ConfigId,
        imports: &[FunctionMetadata],
        errors: &mut Vec<String>,
    ) {
        for imported_fn_metadata in imports {
            if let Some((exported_config_id, exported_fn_metadata, _, _)) =
                self.inner.exported_ffqns.get(&imported_fn_metadata.ffqn)
            {
                // check parameters
                if imported_fn_metadata.parameter_types != exported_fn_metadata.parameter_types {
                    error!("Parameter types do not match: {config_id} imports {import} , {exported_config_id} exports {export}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("parameter types do not match: {config_id} imports {imported_fn_metadata} , {exported_config_id} exports {exported_fn_metadata}"));
                }
                if imported_fn_metadata.return_type != exported_fn_metadata.return_type {
                    error!("Return types do not match: {config_id} imports {import} , {exported_config_id} exports {export}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("return types do not match: {config_id} imports {imported_fn_metadata} , {exported_config_id} exports {exported_fn_metadata}"));
                }
            } else if !Self::additional_import_whitelist(imported_fn_metadata, config_id) {
                errors.push(format!(
                    "function imported by {config_id} not found: {imported_fn_metadata}"
                ));
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
struct ComponentConfigRegistryRO {
    inner: Arc<ComponentConfigRegistryInner>,
}

impl ComponentConfigRegistryRO {
    fn find_by_exported_ffqn(&self, ffqn: &FunctionFqn) -> Option<(Component, FunctionMetadata)> {
        self.inner.exported_ffqns.get(ffqn).map(|(id, meta, ..)| {
            (
                self.inner.ids_to_components.get(id).unwrap().clone(),
                meta.clone(),
            )
        })
    }

    fn list(&self, extensions: bool) -> Vec<Component> {
        self.inner
            .ids_to_components
            .values()
            .cloned()
            .map(|mut component| {
                if !extensions {
                    component.exports.retain(|fn_metadata| {
                        !fn_metadata
                            .ffqn
                            .ifc_fqn
                            .package_name()
                            .ends_with(SUFFIX_PKG_EXT)
                    });
                }
                component
            })
            .collect()
    }
}

#[async_trait]
impl FunctionRegistry for ComponentConfigRegistryRO {
    async fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ConfigId, ComponentRetryConfig, ImportType)> {
        self.inner
            .exported_ffqns
            .get(ffqn)
            .map(|(id, metadata, retry, import_type)| {
                (metadata.clone(), id.clone(), *retry, *import_type)
            })
    }

    fn all_exports(&self) -> &[PackageIfcFns] {
        &self.inner.export_hierarchy
    }
}

struct AbortOnDropHandle(AbortHandle);
impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
