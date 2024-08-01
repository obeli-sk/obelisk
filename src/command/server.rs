use super::grpc;
use crate::config::store::ConfigStore;
use crate::config::store::ConfigStoreCommon;
use crate::config::toml::ConfigHolder;
use crate::config::toml::ObeliskConfig;
use crate::config::toml::VerifiedActivityConfig;
use crate::config::toml::VerifiedWorkflowConfig;
use crate::grpc_util::grpc_mapping::OptionExt;
use crate::grpc_util::TonicRespResult;
use crate::grpc_util::TonicResult;
use anyhow::Context;
use concepts::storage::Component;
use concepts::storage::ComponentToggle;
use concepts::storage::ComponentWithMetadata;
use concepts::storage::CreateRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbError;
use concepts::storage::DbPool;
use concepts::storage::ExecutionLog;
use concepts::storage::PendingState;
use concepts::storage::SpecificError;
use concepts::ExecutionId;
use concepts::FinishedExecutionResult;
use concepts::FunctionFqn;
use concepts::FunctionRegistry;
use concepts::Params;
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::ExecutorTaskHandle;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use serde::Deserialize;
use std::fmt::Debug;
use std::fmt::Display;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::codec::CompressionEncoding;
use tracing::error;
use tracing::{debug, info};
use utils::time::now;
use wasm_workers::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::WorkflowWorker;

#[derive(Debug)]
struct GrpcServer<DB: DbConnection, P: DbPool<DB>> {
    db_pool: P,
    phantom_data: PhantomData<DB>,
}

impl<DB: DbConnection, P: DbPool<DB>> GrpcServer<DB, P> {
    fn new(db_pool: P) -> Self {
        Self {
            db_pool,
            phantom_data: PhantomData,
        }
    }

    async fn close(self) -> Result<(), DbError> {
        self.db_pool.close().await
    }
}

#[tonic::async_trait]
impl<DB: DbConnection + 'static, P: DbPool<DB> + 'static> grpc::scheduler_server::Scheduler
    for GrpcServer<DB, P>
{
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
            .map(ExecutionId::try_from)
            .unwrap_or_else(|| Ok(ExecutionId::generate()))?;
        let ffqn =
            concepts::FunctionFqn::new_arc(Arc::from(interface_name), Arc::from(function_name));
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

        let db_connection = self.db_pool.connection();
        // Check that ffqn exists

        let (config_id, param_types, return_type) = {
            let (config_id, (_, param_types, return_type)) = db_connection
                .component_enabled_get_exported_function(&ffqn)
                .await
                .map_err(|db_err| {
                    if matches!(db_err, DbError::Specific(SpecificError::NotFound)) {
                        tonic::Status::not_found("function not found")
                    } else {
                        error!("Cannot submit execution - {db_err:?}");
                        tonic::Status::internal(format!("database error: {db_err}"))
                    }
                })?;
            (config_id, param_types, return_type)
        };
        // Type check `params`
        if let Err(err) = params.typecheck(param_types.iter().map(|(_, type_wrapper)| type_wrapper))
        {
            return Err(tonic::Status::invalid_argument(format!(
                "argument `params` invalid - {err}"
            )));
        }
        let component = db_connection
            .component_get_metadata(&config_id)
            .await
            .map_err(|db_err| {
                error!("Cannot submit execution - {db_err:?}");
                tonic::Status::internal(format!("database error: {db_err}"))
            })?;
        let config_store: ConfigStore = serde_json::from_value(component.component.config)
            .map_err(|serde_err| {
                error!("Cannot deserialize configuration {config_id} - {serde_err:?}");
                tonic::Status::internal(format!("configuration deserialization error: {serde_err}"))
            })?;
        let ConfigStoreCommon {
            default_retry_exp_backoff,
            default_max_retries,
            ..
        } = config_store.common();
        let created_at = now();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn,
                params,
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: *default_retry_exp_backoff,
                max_retries: *default_max_retries,
                config_id,
                return_type,
            })
            .await
            .unwrap();
        let resp = grpc::SubmitResponse {
            execution_id: Some(grpc::ExecutionId {
                id: execution_id.to_string(),
            }),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn get_status(
        &self,
        request: tonic::Request<grpc::GetStatusRequest>,
    ) -> TonicRespResult<grpc::GetStatusResponse> {
        let execution_id: ExecutionId = request
            .into_inner()
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        let db_connection = self.db_pool.connection();
        let execution_log = db_connection.get(execution_id).await.map_err(|db_err| {
            if matches!(db_err, DbError::Specific(SpecificError::NotFound)) {
                tonic::Status::not_found("execution not found")
            } else {
                error!("Cannot get execution status - {db_err:?}");
                tonic::Status::internal(format!("database error: {db_err}"))
            }
        })?;
        let resp = grpc::GetStatusResponse {
            status: Some(convert_execution_status(execution_log)),
        };
        Ok(tonic::Response::new(resp))
    }

    type StreamStatusStream =
        Pin<Box<dyn Stream<Item = Result<grpc::ExecutionStatus, tonic::Status>> + Send>>;

    async fn stream_status(
        &self,
        request: tonic::Request<grpc::StreamStatusRequest>,
    ) -> TonicRespResult<Self::StreamStatusStream> {
        let execution_id: ExecutionId = request
            .into_inner()
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        let db_connection = self.db_pool.connection();
        let execution_log = db_connection.get(execution_id).await.map_err(|db_err| {
            if matches!(db_err, DbError::Specific(SpecificError::NotFound)) {
                tonic::Status::not_found("execution not found")
            } else {
                error!("Cannot get execution status - {db_err:?}");
                tonic::Status::internal(format!("database error: {db_err}"))
            }
        })?;
        if execution_log.finished_result().is_some() {
            let converted = convert_execution_status(execution_log);
            let output = tokio_stream::once(Ok(converted));
            Ok(tonic::Response::new(
                Box::pin(output) as Self::StreamStatusStream
            ))
        } else {
            let (tx, rx) = mpsc::channel(1);
            let current_pending_state = execution_log.pending_state.clone();
            let converted = convert_execution_status(execution_log);
            // send current pending status
            tx.send(TonicResult::Ok(converted))
                .await
                .expect("mpsc bounded channel requires buffer > 0");
            tokio::spawn(async move {
                loop {
                    match db_connection.get(execution_id).await {
                        Ok(execution_log) => {
                            if execution_log.pending_state != current_pending_state {
                                let converted = convert_execution_status(execution_log);
                                let is_finished = is_finished(&converted);
                                if tx.send(TonicResult::Ok(converted)).await.is_err() || is_finished
                                {
                                    return;
                                }
                            }
                        }
                        Err(db_err) => {
                            error!("Database error while streaming status of {execution_id} - {db_err:?}");
                            return;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await; // TODO: Switch to subscription-based approach
                }
            });
            let output = ReceiverStream::new(rx);
            Ok(tonic::Response::new(
                Box::pin(output) as Self::StreamStatusStream
            ))
        }
    }
}

fn is_finished(status: &grpc::ExecutionStatus) -> bool {
    use grpc::execution_status::*;
    matches!(
        status,
        grpc::ExecutionStatus {
            status: Some(Status::Finished(..))
        }
    )
}

fn convert_execution_status(execution_log: ExecutionLog) -> grpc::ExecutionStatus {
    use grpc::execution_status::*;
    grpc::ExecutionStatus {
        status: Some(match execution_log.pending_state {
            PendingState::Locked {
                executor_id,
                run_id,
                lock_expires_at,
            } => Status::Locked(Locked {
                executor_id: Some(executor_id.into()),
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
            PendingState::Finished => Status::Finished(Finished {
                result: {
                    let finished = execution_log.finished_result().expect("must be finished");
                    serde_json::to_string(finished)
                        .inspect_err(|ser_err| {
                            error!(
                                "Cannot serialize result of {execution_id} - {ser_err:?}",
                                execution_id = execution_log.execution_id
                            )
                        })
                        .ok()
                        .map(|res| prost_wkt_types::Any {
                            type_url: format!(
                                "urn:obelisk:json:params:{ffqn}",
                                ffqn = execution_log.ffqn()
                            ),
                            value: res.into_bytes(),
                        })
                },
            }),
        }),
    }
}

pub(crate) async fn run(
    config: ObeliskConfig,
    db_file: &PathBuf,
    clean: bool,
    config_holder: ConfigHolder,
) -> anyhow::Result<()> {
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

    // Set up codegen cache
    let codegen_cache_config_file_holder = Engines::write_codegen_config(codegen_cache.as_deref())
        .context("error configuring codegen cache")?;
    let engines =
        Engines::auto_detect_allocator(&get_opts_from_env(), codegen_cache_config_file_holder)?;

    let _epoch_ticker = EpochTicker::spawn_new(
        vec![
            engines.activity_engine.weak(),
            engines.workflow_engine.weak(),
        ],
        Duration::from_millis(10),
    );
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;

    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    );
    disable_all_components(db_pool.connection()).await?;

    debug!("Loading components: {config:?}");
    let mut exec_join_handles = Vec::new();

    for activity in config.activity.into_iter().filter(|it| it.common.enabled) {
        let activity = activity.verify_content_digest(&wasm_cache_dir).await?;

        if activity.enabled.into() {
            let exec_task_handle =
                instantiate_activity(activity, db_pool.clone(), &engines).await?;
            exec_join_handles.push(exec_task_handle);
        }
    }
    for workflow in config.workflow.into_iter().filter(|it| it.common.enabled) {
        let workflow = workflow.verify_content_digest(&wasm_cache_dir).await?;
        if workflow.enabled.into() {
            let exec_task_handle =
                instantiate_workflow(workflow, db_pool.clone(), &engines).await?;
            exec_join_handles.push(exec_task_handle);
        }
    }
    let addr = "127.0.0.1:50055".parse()?;
    let grpc_server = Arc::new(GrpcServer::new(db_pool));
    let grpc_service = grpc::scheduler_server::SchedulerServer::from_arc(grpc_server.clone())
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let res = tonic::transport::Server::builder()
        .add_service(grpc_service)
        .serve_with_shutdown(addr, async move {
            if let Err(err) = tokio::signal::ctrl_c().await {
                error!("Error while listening to ctrl-c - {err:?}");
            }
        })
        .await
        .context("grpc server error");
    timers_watcher.close().await;
    for exec_join_handle in exec_join_handles {
        exec_join_handle.close().await;
    }
    Arc::into_inner(grpc_server)
        .expect("must be the last reference")
        .close()
        .await
        .context("cannot close database")?;
    res
}

fn get_opts_from_env() -> wasm_workers::engines::PoolingOptions {
    fn get_env<T: FromStr + Display>(key: &str, into: &mut Option<T>)
    where
        <T as FromStr>::Err: Debug,
    {
        if let Ok(val) = std::env::var(key) {
            let val = val.parse().unwrap();
            info!("Setting {key}={val}");
            *into = Some(val);
        }
    }
    let mut opts = wasm_workers::engines::PoolingOptions::default();
    get_env(
        "WASMTIME_POOLING_MEMORY_KEEP_RESIDENT",
        &mut opts.pooling_memory_keep_resident,
    );
    get_env(
        "WASMTIME_POOLING_TABLE_KEEP_RESIDENT",
        &mut opts.pooling_table_keep_resident,
    );
    get_env(
        "WASMTIME_MEMORY_PROTECTION_KEYS",
        &mut opts.memory_protection_keys,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_CORE_INSTANCES",
        &mut opts.pooling_total_core_instances,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_COMPONENT_INSTANCES",
        &mut opts.pooling_total_component_instances,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_MEMORIES",
        &mut opts.pooling_total_memories,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_TABLES",
        &mut opts.pooling_total_tables,
    );
    get_env(
        "WASMTIME_POOLING_TOTAL_STACKS",
        &mut opts.pooling_total_stacks,
    );
    get_env(
        "WASMTIME_POOLING_MAX_MEMORY_SIZE",
        &mut opts.pooling_max_memory_size,
    );
    opts
}

async fn instantiate_activity<DB: DbConnection + 'static>(
    activity: VerifiedActivityConfig,
    db_pool: impl DbPool<DB> + 'static,
    engines: &Engines,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!(
        "Instantiating activity {name} with id {config_id} from {wasm_path:?}",
        name = activity.config_store.name(),
        config_id = activity.exec_config.config_id,
        wasm_path = activity.wasm_path,
    );
    debug!("Full configuration: {activity:?}");
    let worker = Arc::new(ActivityWorker::new_with_config(
        activity.wasm_path,
        activity.activity_config,
        engines.activity_engine.clone(),
        now,
    )?);
    register_and_spawn(
        worker,
        &activity.config_store,
        activity.exec_config,
        db_pool,
    )
    .await
}

async fn disable_all_components(conn: impl DbConnection) -> Result<(), anyhow::Error> {
    // TODO: should be in a tx together with enabling the current components
    for component in conn.component_list(ComponentToggle::Enabled).await? {
        conn.component_toggle(&component.config_id, ComponentToggle::Disabled, now())
            .await?;
    }
    Ok(())
}

async fn instantiate_workflow<DB: DbConnection + 'static>(
    workflow: VerifiedWorkflowConfig,
    db_pool: impl DbPool<DB> + FunctionRegistry + 'static,
    engines: &Engines,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    info!(
        "Instantiating workflow {name} with id {config_id} from {wasm_path:?}",
        name = workflow.config_store.name(),
        config_id = workflow.exec_config.config_id,
        wasm_path = workflow.wasm_path,
    );
    debug!("Full configuration: {workflow:?}");
    let fn_registry = Arc::from(db_pool.clone());
    let worker = Arc::new(WorkflowWorker::new_with_config(
        workflow.wasm_path,
        workflow.workflow_config,
        engines.workflow_engine.clone(),
        db_pool.clone(),
        now,
        fn_registry,
    )?);
    register_and_spawn(
        worker,
        &workflow.config_store,
        workflow.exec_config,
        db_pool,
    )
    .await
}

async fn register_and_spawn<W: Worker, DB: DbConnection + 'static>(
    worker: Arc<W>,
    config: &ConfigStore,
    exec_config: ExecConfig,
    db_pool: impl DbPool<DB> + 'static,
) -> Result<ExecutorTaskHandle, anyhow::Error> {
    let config_id = exec_config.config_id.clone();
    let connection = db_pool.connection();
    // If the component exists, just enable it
    let found = match connection
        .component_toggle(&config_id, ComponentToggle::Enabled, now())
        .await
    {
        Ok(()) => {
            debug!("Enabled component {config_id}");
            true
        }
        Err(DbError::Specific(concepts::storage::SpecificError::NotFound)) => false,
        Err(other) => Err(other)?,
    };
    if !found {
        let component = ComponentWithMetadata {
            component: Component {
                config_id,
                config: serde_json::to_value(config)
                    .expect("ConfigStore must be serializable to JSON"),
                enabled: ComponentToggle::Enabled,
            },
            exports: worker.exported_functions().collect(),
            imports: worker.imported_functions().collect(),
        };
        connection
            .component_add(now(), component, ComponentToggle::Enabled)
            .await?;
    }
    Ok(ExecTask::spawn_new(worker, exec_config, now, db_pool, None))
}
