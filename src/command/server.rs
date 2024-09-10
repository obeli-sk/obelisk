use super::grpc;
use crate::config::config_holder::ConfigHolder;
use crate::config::toml::webhook;
use crate::config::toml::ActivityConfigVerified;
use crate::config::toml::ObeliskConfig;
use crate::config::toml::WasmActivityToml;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::WorkflowToml;
use crate::config::Component;
use crate::config::ConfigStore;
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
use concepts::ExecutionId;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::ParameterType;
use concepts::Params;
use concepts::ReturnType;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use hashbrown::HashSet;
use serde::Deserialize;
use std::fmt::Debug;
use std::marker::PhantomData;
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
use tracing::{debug, info, trace};
use utils::time::now;
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::webhook_trigger;
use wasm_workers::webhook_trigger::MethodAwareRouter;
use wasm_workers::workflow_worker::WorkflowWorker;

#[derive(Debug)]
struct GrpcServer<DB: DbConnection, P: DbPool<DB>> {
    db_pool: P,
    component_registry: ComponentConfigRegistry,
    phantom_data: PhantomData<DB>,
}

impl<DB: DbConnection, P: DbPool<DB>> GrpcServer<DB, P> {
    fn new(db_pool: P, component_registry: ComponentConfigRegistry) -> Self {
        Self {
            db_pool,
            component_registry,
            phantom_data: PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<DB: DbConnection + 'static, P: DbPool<DB> + 'static> grpc::scheduler_server::Scheduler
    for GrpcServer<DB, P>
{
    #[instrument(skip_all, fields(execution_id))]
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
        tracing::Span::current().record("execution_id", tracing::field::display(execution_id));
        let ffqn =
            concepts::FunctionFqn::new_arc(Arc::from(interface_name), Arc::from(function_name));
        // Deserialize params JSON into `Params`
        let params = {
            let params = request.params.argument_must_exist("params")?;
            let params = String::from_utf8(params.value).map_err(|_err| {
                tonic::Status::invalid_argument("argument `params` must be UTF-8 encoded")
            })?;
            Params::deserialize(&mut serde_json::Deserializer::from_str(&params)).map_err(
                |serde_err| {
                    tonic::Status::invalid_argument(format!(
                        "argument `params` must be encoded as JSON array - {serde_err}"
                    ))
                },
            )?
        };

        // Check that ffqn exists
        let Some((component, fn_meta)) = self.component_registry.find_by_exported_ffqn(&ffqn)
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
        let created_at = now();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                topmost_parent_id: None,
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
        _request: tonic::Request<grpc::ListComponentsRequest>,
    ) -> TonicRespResult<grpc::ListComponentsResponse> {
        let components = self.component_registry.list();
        let mut res_components = Vec::with_capacity(components.len());
        for component in components {
            let res_component = grpc::Component {
                name: component.config_store.name().to_string(),
                r#type: component.config_id.component_type.to_string(),
                config_id: Some(component.config_id.into()),
                digest: component.config_store.common().content_digest.to_string(),
                exports: match component.exports {
                    Some(exports) => inspect_fns(exports, true),
                    None => Vec::new(),
                },
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
                        name: p.name,
                        r#type: Some(grpc::WitType {
                            wit_type: p.wit_type,
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
                    wit_type,
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
    mut config: ObeliskConfig,
    clean_db: bool,
    clean_cache: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
    let _guard = init::init(&mut config);
    Box::pin(run_internal(config, clean_db, clean_cache, config_holder)).await?;
    Ok(())
}

#[expect(clippy::too_many_lines)]
async fn run_internal(
    config: ObeliskConfig,
    clean_db: bool,
    clean_cache: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
    debug!("Using toml configuration {config:#?}");
    let db_file = &config
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
        tokio::fs::remove_file(db_file)
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

    let api_listening_addr = config.api_listening_addr;
    let init_span = info_span!("init");
    let db_pool = SqlitePool::new(db_file)
        .instrument(init_span.clone())
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;

    let mut component_registry = ComponentConfigRegistry::default();
    let engines = {
        let codegen_cache_config_file_holder =
            Engines::write_codegen_config(codegen_cache.as_deref())
                .context("error configuring codegen cache")?;
        Engines::auto_detect_allocator(
            &config.wasmtime_pooling_config.into(),
            codegen_cache_config_file_holder,
        )?
    };
    let _epoch_ticker = EpochTicker::spawn_new(engines.weak_refs(), Duration::from_millis(10));
    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );
    let VerifiedConfig {
        wasm_activities,
        workflows,
        http_servers_to_webhooks,
    } = fetch_and_verify_all(
        config.wasm_activities,
        config.workflows,
        config.http_servers,
        config.webhooks,
        Arc::from(wasm_cache_dir),
        Arc::from(metadata_dir),
    )
    .await?;
    let exec_join_handles = spawn_tasks(
        &engines,
        wasm_activities,
        workflows,
        &db_pool,
        &mut component_registry,
    )
    .instrument(init_span)
    .await?;
    let _http_servers_handles = start_webhooks(
        http_servers_to_webhooks,
        &engines,
        db_pool.clone(),
        &mut component_registry,
    )
    .await;
    let grpc_server = Arc::new(GrpcServer::new(db_pool.clone(), component_registry));
    let grpc_server_res = tonic::transport::Server::builder()
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
            if let Err(err) = tokio::signal::ctrl_c().await {
                error!("Error while listening to ctrl-c - {err:?}");
            }
        })
        .await
        .with_context(|| format!("grpc server error listening on {api_listening_addr}"));
    // ^ Will await until the gRPC server shuts down.
    timers_watcher.close().await;
    for exec_join_handle in exec_join_handles {
        exec_join_handle.close().await;
    }
    db_pool.close().await.context("cannot close database")?;
    grpc_server_res
}

async fn start_webhooks<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    http_servers_to_webhooks: Vec<(webhook::HttpServer, Vec<webhook::WebhookVerified>)>,
    engines: &Engines,
    db_pool: P,
    component_registry: &mut ComponentConfigRegistry,
) -> Result<Vec<AbortOnDropHandle>, anyhow::Error> {
    let mut abort_handles = Vec::with_capacity(http_servers_to_webhooks.len());
    let engine = &engines.webhook_engine;
    for (http_server, webhooks) in http_servers_to_webhooks {
        let mut router = MethodAwareRouter::default();
        for webhook in webhooks {
            let wasm_component = WasmComponent::new(webhook.wasm_path, engine)?;
            let imports = wasm_component.imported_functions().to_vec();
            let instance = webhook_trigger::component_to_instance(
                &wasm_component,
                engine,
                webhook.config_store.config_id(),
            )?;
            component_registry.insert(Component {
                config_id: webhook.config_store.config_id(),
                config_store: webhook.config_store,
                exports: None,
                imports,
            })?;
            for route in webhook.routes {
                if route.methods.is_empty() {
                    router.add(None, &route.route, instance.clone());
                } else {
                    for method in route.methods {
                        router.add(Some(method), &route.route, instance.clone());
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
        let fn_registry = component_registry.get_fn_registry();
        let server = AbortOnDropHandle(
            tokio::spawn(webhook_trigger::server(
                tcp_listener,
                engine.clone(),
                router,
                db_pool.clone(),
                now,
                fn_registry.clone(),
                webhook_trigger::RetryConfigOverride::default(), // TODO make configurable
            ))
            .abort_handle(),
        );
        abort_handles.push(server);
    }
    Ok(abort_handles)
}

struct VerifiedConfig {
    wasm_activities: Vec<ActivityConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    http_servers_to_webhooks: Vec<(webhook::HttpServer, Vec<webhook::WebhookVerified>)>,
}

#[instrument(skip_all)]
async fn fetch_and_verify_all(
    wasm_activities: Vec<WasmActivityToml>,
    workflows: Vec<WorkflowToml>,
    http_servers: Vec<webhook::HttpServer>,
    webhooks: Vec<webhook::Webhook>,
    wasm_cache_dir: Arc<Path>,
    metadata_dir: Arc<Path>,
) -> Result<VerifiedConfig, anyhow::Error> {
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
        let mut server_names_to_webhook_names = {
            let mut map: hashbrown::HashMap<std::string::String, Vec<String>> =
                hashbrown::HashMap::default();
            for webhook in &webhooks {
                map.entry(webhook.http_server.clone())
                    .or_default()
                    .push(webhook.common.name.clone());
            }
            map
        };
        let http_servers = {
            let mut vec = Vec::new();
            for http_server in http_servers {
                let webhooks = server_names_to_webhook_names
                    .remove(&http_server.name)
                    .unwrap_or_default();
                vec.push((http_server, webhooks));
            }
            vec
        };
        // Each webhook must be associated with an `http_server`.
        if !server_names_to_webhook_names.is_empty() {
            bail!(
                "No matching `http_server` found for some `webhook` configurations: {:?}",
                server_names_to_webhook_names.keys().collect::<Vec<_>>()
            );
        }
        http_servers
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
                        .in_current_span()
                        .await?;
                    Ok::<_, anyhow::Error>((name, webhook))
                }
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
            let http_servers_to_webhooks = {
                http_servers_to_webhook_names.into_iter().map(|(http_server, webhook_names) | {
                    let verified_webhooks = webhook_names.into_iter()
                        .map(|name| webhooks_by_names.remove(&name).expect("all webhooks must be verified")).collect::<Vec<_>>();
                    (http_server, verified_webhooks)
                }).collect()
            };
            Ok(VerifiedConfig {wasm_activities, workflows, http_servers_to_webhooks})
        },
        _ = tokio::signal::ctrl_c() => {
            anyhow::bail!("cancelled downloading images from OCI registries")
        }
    }
}

#[instrument(skip_all)]
async fn spawn_tasks<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    engines: &Engines,
    wasm_activities: Vec<ActivityConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    db_pool: &P,
    component_registry: &mut ComponentConfigRegistry,
) -> Result<Vec<ExecutorTaskHandle>, anyhow::Error> {
    let pre_spawns = prespawn_all(
        wasm_activities,
        workflows,
        component_registry,
        engines,
        db_pool,
    );

    // Abort/cancel safety:
    // If an error happens or Ctrl-C is pressed the whole process will shut down.
    let pre_spawns = futures_util::future::join_all(pre_spawns);
    let pre_spawns = tokio::select! {
        results_of_results = pre_spawns => {
            let mut pre_spawns = Vec::with_capacity(results_of_results.len());
            for handle in results_of_results {
                pre_spawns.push(handle??);
            }
            pre_spawns
        },
        _ = tokio::signal::ctrl_c() => {
            anyhow::bail!("cancelled downloading images from OCI registries")
        }
    };
    // Start all executors
    Ok(pre_spawns
        .into_iter()
        .map(|pre_spawn| {
            ExecTask::spawn_new(
                pre_spawn.worker,
                pre_spawn.exec_config,
                now,
                db_pool.clone(),
                pre_spawn.task_limiter,
                pre_spawn.executor_id,
                pre_spawn.component_name,
            )
        })
        .collect())
}

/// Compile WASM components in parallel.
#[instrument(skip_all)]
fn prespawn_all<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    activities: Vec<ActivityConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    component_registry: &mut ComponentConfigRegistry,
    engines: &Engines,
    db_pool: &P,
) -> Vec<tokio::task::JoinHandle<Result<ExecutorPreSpawn, anyhow::Error>>> {
    activities
        .into_iter()
        .map(|activity| {
            let mut component_registry = component_registry.clone();
            let engines = engines.clone();
            let span = tracing::Span::current();
            #[cfg_attr(madsim, allow(deprecated))]
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let executor_id = ExecutorId::generate();
                    prespawn_activity(activity, &mut component_registry, &engines, executor_id)
                })
            })
        })
        .chain(workflows.into_iter().map(|workflow| {
            let mut component_registry = component_registry.clone();
            let engines = engines.clone();
            let db_pool = db_pool.clone();
            let span = tracing::Span::current();
            #[cfg_attr(madsim, allow(deprecated))]
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    let executor_id = ExecutorId::generate();
                    prespawn_workflow(
                        workflow,
                        db_pool,
                        &mut component_registry,
                        &engines,
                        executor_id,
                    )
                })
            })
        }))
        .collect()
}

#[instrument(skip_all, fields(
    %executor_id,
    config_id = %activity.exec_config.config_id,
    name = activity.config_store.name(),
    wasm_path = ?activity.wasm_path,
))]
fn prespawn_activity(
    activity: ActivityConfigVerified,
    component_registry: &mut ComponentConfigRegistry,
    engines: &Engines,
    executor_id: ExecutorId,
) -> Result<ExecutorPreSpawn, anyhow::Error> {
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let worker = Arc::new(ActivityWorker::new_with_config(
        activity.wasm_path,
        activity.activity_config,
        engines.activity_engine.clone(),
        now,
    )?);
    register_worker_and_prespawn(
        worker,
        activity.config_store,
        activity.exec_config,
        component_registry,
        executor_id,
    )
}

#[instrument(skip_all, fields(
    %executor_id,
    config_id = %workflow.exec_config.config_id,
    name = workflow.config_store.name(),
    wasm_path = ?workflow.wasm_path,
))]
fn prespawn_workflow<DB: DbConnection + 'static>(
    workflow: WorkflowConfigVerified,
    db_pool: impl DbPool<DB> + 'static,
    component_registry: &mut ComponentConfigRegistry,
    engines: &Engines,
    executor_id: ExecutorId,
) -> Result<ExecutorPreSpawn, anyhow::Error> {
    debug!("Instantiating workflow");
    trace!(?workflow, "Full configuration");
    let worker = Arc::new(WorkflowWorker::new_with_config(
        workflow.wasm_path,
        workflow.workflow_config,
        engines.workflow_engine.clone(),
        db_pool.clone(),
        now,
        component_registry.get_fn_registry(),
    )?);
    register_worker_and_prespawn(
        worker,
        workflow.config_store,
        workflow.exec_config,
        component_registry,
        executor_id,
    )
}

// Wrap the activity or workflow worker with type erased `ExecutorPreSpawn`.
fn register_worker_and_prespawn(
    worker: Arc<dyn Worker>,
    config_store: ConfigStore,
    exec_config: ExecConfig,
    component_registry: &mut ComponentConfigRegistry,
    executor_id: ExecutorId,
) -> Result<ExecutorPreSpawn, anyhow::Error> {
    let component_name = config_store.name().to_string();
    let component = Component {
        config_id: config_store.config_id(),
        config_store,
        exports: Some(worker.exported_functions().to_vec()),
        imports: worker.imported_functions().to_vec(),
    };
    component_registry.insert(component)?;
    Ok(ExecutorPreSpawn {
        worker,
        exec_config,
        task_limiter: None,
        executor_id,
        component_name,
    })
}

struct ExecutorPreSpawn {
    worker: Arc<dyn Worker>,
    exec_config: ExecConfig,
    task_limiter: Option<Arc<tokio::sync::Semaphore>>,
    executor_id: ExecutorId,
    component_name: String,
}

#[derive(Default, Debug, Clone)]
struct ComponentConfigRegistry {
    inner: Arc<std::sync::RwLock<ComponentConfigRegistryInner>>,
}

#[derive(Default, Debug)]
struct ComponentConfigRegistryInner {
    exported_ffqns:
        hashbrown::HashMap<FunctionFqn, (ConfigId, FunctionMetadata, ComponentRetryConfig)>,
    ids_to_components: hashbrown::HashMap<ConfigId, Component>,
}

impl ComponentConfigRegistry {
    fn insert(&self, component: Component) -> Result<(), anyhow::Error> {
        let mut write_guad = self.inner.write().unwrap();
        // check for conflicts
        if write_guad
            .ids_to_components
            .contains_key(&component.config_id)
        {
            bail!("component {} is already inserted", component.config_id);
        }
        for exported_ffqn in component.exports.iter().flatten().map(|f| &f.ffqn) {
            if let Some((offending_id, _, _)) = write_guad.exported_ffqns.get(exported_ffqn) {
                bail!("function {exported_ffqn} is already exported by component {offending_id}, cannot insert {}", component.config_id);
            }
        }
        // insert
        for exported_fn in component.exports.iter().flatten() {
            assert!(write_guad
                .exported_ffqns
                .insert(
                    exported_fn.ffqn.clone(),
                    (
                        component.config_id.clone(),
                        exported_fn.clone(),
                        ComponentRetryConfig {
                            max_retries: component.config_store.default_max_retries(),
                            retry_exp_backoff: component.config_store.default_retry_exp_backoff()
                        }
                    ),
                )
                .is_none());
        }
        assert!(write_guad
            .ids_to_components
            .insert(component.config_id.clone(), component)
            .is_none());
        Ok(())
    }

    fn find_by_exported_ffqn(&self, ffqn: &FunctionFqn) -> Option<(Component, FunctionMetadata)> {
        let read_guard = self.inner.read().unwrap();
        read_guard.exported_ffqns.get(ffqn).map(|(id, meta, ..)| {
            (
                read_guard.ids_to_components.get(id).unwrap().clone(),
                meta.clone(),
            )
        })
    }

    fn list(&self) -> Vec<Component> {
        self.inner
            .read()
            .unwrap()
            .ids_to_components
            .values()
            .cloned()
            .collect()
    }

    fn get_fn_registry(&self) -> Arc<dyn FunctionRegistry> {
        Arc::from(self.clone())
    }
}

#[async_trait]
impl FunctionRegistry for ComponentConfigRegistry {
    async fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ConfigId, ComponentRetryConfig)> {
        self.inner
            .read()
            .unwrap()
            .exported_ffqns
            .get(ffqn)
            .map(|(id, metadata, retry)| (metadata.clone(), id.clone(), *retry))
    }
}

struct AbortOnDropHandle(AbortHandle);
impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
