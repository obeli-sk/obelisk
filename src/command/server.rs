use super::grpc;
use crate::config::toml::ConfigHolder;
use crate::config::toml::ObeliskConfig;
use crate::config::toml::VerifiedActivityConfig;
use crate::config::toml::VerifiedWorkflowConfig;
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
use concepts::ComponentConfigHash;
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
use serde::Deserialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::async_trait;
use tonic::codec::CompressionEncoding;
use tracing::error;
use tracing::info_span;
use tracing::instrument;
use tracing::Instrument;
use tracing::{debug, info};
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
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
    #[instrument(skip_all)]
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
                ffqn,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: component.config_store.common().default_retry_exp_backoff,
                max_retries: component.config_store.common().default_max_retries,
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

    #[instrument(skip_all)]
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
                    tokio::time::sleep(Duration::from_millis(500)).await; // TODO: Switch to subscription-based approach
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
                file_path: Some("TODO - location?".to_string()),
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
    config: ObeliskConfig,
    clean: bool,
    config_holder: ConfigHolder,
    machine_readable_logs: bool,
) -> anyhow::Result<()> {
    let _guard = init::init("obelisk-server", machine_readable_logs);
    run_internal(config, clean, config_holder).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_internal(
    config: ObeliskConfig,
    clean: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
    let db_file = &config
        .get_sqlite_file(config_holder.project_dirs.as_ref())
        .await?;
    let wasm_cache_dir = config
        .oci
        .get_wasm_directory(config_holder.project_dirs.as_ref())
        .await?;
    let codegen_cache = config
        .codegen_cache
        .get_directory_if_enabled(config_holder.project_dirs.as_ref())
        .await?;
    debug!("Using codegen cache? {codegen_cache:?}");
    if clean {
        let ignore_not_found = |err: std::io::Error| {
            if err.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(err)
            }
        };
        tokio::fs::remove_file(db_file)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database file `{db_file:?}`"))?;
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

    let api_listening_addr = config.api_listening_addr;
    let init_span = info_span!("init");
    let db_pool = SqlitePool::new(db_file)
        .instrument(init_span.clone())
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;

    let mut component_registry = ComponentConfigRegistry::default();
    let (exec_join_handles, timers_watcher) = spawn_tasks(
        config,
        &db_pool,
        &mut component_registry,
        codegen_cache.as_deref(),
        &wasm_cache_dir,
    )
    .instrument(init_span)
    .await?;
    let grpc_server = Arc::new(GrpcServer::new(db_pool.clone(), component_registry));
    let res = tonic::transport::Server::builder()
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
            if let Err(err) = tokio::signal::ctrl_c().await {
                error!("Error while listening to ctrl-c - {err:?}");
            }
        })
        .await
        .context("grpc server error");
    // ^ Will await until the gRPC server shuts down.
    timers_watcher.close().await;
    for exec_join_handle in exec_join_handles {
        exec_join_handle.close().await;
    }
    db_pool.close().await.context("cannot close database")?;
    res
}

#[instrument(skip_all)]
async fn spawn_tasks<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    config: ObeliskConfig,
    db_pool: &P,
    component_registry: &mut ComponentConfigRegistry,
    codegen_cache: Option<&Path>,
    wasm_cache_dir: &Path,
) -> Result<
    (
        Vec<ExecutorTaskHandle>,
        executor::expired_timers_watcher::TaskHandle,
    ),
    anyhow::Error,
> {
    debug!("Using toml configuration: {config:?}");
    let codegen_cache_config_file_holder =
        Engines::write_codegen_config(codegen_cache).context("error configuring codegen cache")?;
    let engines = Engines::auto_detect_allocator(
        &config.wasmtime_pooling_config.into(),
        codegen_cache_config_file_holder,
    )?;

    let _epoch_ticker = EpochTicker::spawn_new(
        vec![
            engines.activity_engine.weak(),
            engines.workflow_engine.weak(),
        ],
        Duration::from_millis(10),
    );

    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );

    let mut exec_join_handles = Vec::new();

    // FIXME: Ctrl-C here is ignored.
    for activity in config.activity.into_iter().filter(|it| it.common.enabled) {
        let activity = activity.verify_content_digest(&wasm_cache_dir).await?;
        let executor_id = ExecutorId::generate();
        if activity.enabled {
            let exec_task_handle = instantiate_activity(
                activity,
                db_pool.clone(),
                component_registry,
                &engines,
                executor_id,
            )?;
            exec_join_handles.push(exec_task_handle);
        }
    }
    for workflow in config.workflow.into_iter().filter(|it| it.common.enabled) {
        let workflow = workflow.verify_content_digest(&wasm_cache_dir).await?;
        let executor_id = ExecutorId::generate();
        if workflow.enabled {
            let exec_task_handle = instantiate_workflow(
                workflow,
                db_pool.clone(),
                &engines,
                component_registry,
                executor_id,
            )?;
            exec_join_handles.push(exec_task_handle);
        }
    }
    Ok((exec_join_handles, timers_watcher))
}

#[instrument(skip_all, fields(
    %executor_id,
    config_id = %activity.exec_config.config_id,
    name = activity.config_store.name(),
    wasm_path = ?activity.wasm_path,
))]
fn instantiate_activity<DB: DbConnection + 'static>(
    activity: VerifiedActivityConfig,
    db_pool: impl DbPool<DB> + 'static,
    component_registry: &mut ComponentConfigRegistry,
    engines: &Engines,
    executor_id: ExecutorId,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!("Instantiating activity");
    debug!("Full configuration: {activity:?}");
    let worker = Arc::new(ActivityWorker::new_with_config(
        activity.wasm_path,
        activity.activity_config,
        engines.activity_engine.clone(),
        now,
    )?);
    register_and_spawn(
        worker,
        db_pool,
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
fn instantiate_workflow<DB: DbConnection + 'static>(
    workflow: VerifiedWorkflowConfig,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
    component_registry: &mut ComponentConfigRegistry,
    executor_id: ExecutorId,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!("Instantiating workflow");
    debug!("Full configuration: {workflow:?}");
    let worker = Arc::new(WorkflowWorker::new_with_config(
        workflow.wasm_path,
        workflow.workflow_config,
        engines.workflow_engine.clone(),
        db_pool.clone(),
        now,
        component_registry.get_fn_registry(),
    )?);
    register_and_spawn(
        worker,
        db_pool,
        workflow.config_store,
        workflow.exec_config,
        component_registry,
        executor_id,
    )
}

fn register_and_spawn<W: Worker, DB: DbConnection + 'static>(
    worker: Arc<W>,
    db_pool: impl DbPool<DB> + 'static,
    config_store: ConfigStore,
    exec_config: ExecConfig,
    component_registry: &mut ComponentConfigRegistry,
    executor_id: ExecutorId,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let root_span = info_span!(parent: None, "executor",
        %executor_id,
        config_id = %exec_config.config_id,
        name = config_store.name()
    );
    let component = Component {
        config_id: config_store.as_hash(),
        config_store,
        exports: worker.exported_functions().collect(),
        imports: worker.imported_functions().collect(),
    };
    component_registry.insert(component)?;

    Ok(ExecTask::spawn_new(
        worker,
        exec_config,
        now,
        db_pool,
        None,
        executor_id,
        root_span,
    ))
}

// TODO: If dynamic executor is not needed, split registry into Writer that is turned into a Reader. Reader is passed to the Executor when spawing.
#[derive(Default, Debug, Clone)]
struct ComponentConfigRegistry {
    inner: Arc<std::sync::RwLock<ComponentConfigRegistryInner>>,
}

#[derive(Default, Debug)]
struct ComponentConfigRegistryInner {
    exported_ffqns: hashbrown::HashMap<FunctionFqn, (ComponentConfigHash, FunctionMetadata)>,
    ids_to_components: hashbrown::HashMap<ComponentConfigHash, Component>,
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
        for exported_ffqn in component.exports.iter().map(|f| &f.ffqn) {
            if let Some((offending_id, _)) = write_guad.exported_ffqns.get(exported_ffqn) {
                bail!("function {exported_ffqn} is already exported by component {offending_id}, cannot insert {}", component.config_id);
            }
        }
        // insert
        for exported_fn in &component.exports {
            assert!(write_guad
                .exported_ffqns
                .insert(
                    exported_fn.ffqn.clone(),
                    (component.config_id.clone(), exported_fn.clone()),
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
        read_guard.exported_ffqns.get(ffqn).map(|(id, meta)| {
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
    ) -> Option<(FunctionMetadata, ComponentConfigHash)> {
        self.inner
            .read()
            .unwrap()
            .exported_ffqns
            .get(ffqn)
            .map(|(id, metadata)| (metadata.clone(), id.clone()))
    }
}
