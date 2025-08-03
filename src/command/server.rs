use crate::config::ComponentConfig;
use crate::config::ComponentConfigImportable;
use crate::config::ComponentLocation;
use crate::config::config_holder::ConfigHolder;
use crate::config::config_holder::PathPrefixes;
use crate::config::env_var::EnvVarConfig;
use crate::config::toml::ActivitiesDirectoriesCleanupConfigToml;
use crate::config::toml::ActivitiesDirectoriesGlobalConfigToml;
use crate::config::toml::ActivityComponentConfigToml;
use crate::config::toml::ActivityWasmConfigVerified;
use crate::config::toml::ComponentCommon;
use crate::config::toml::ConfigName;
use crate::config::toml::ConfigToml;
use crate::config::toml::InflightSemaphore;
use crate::config::toml::SQLITE_FILE_NAME;
use crate::config::toml::StdOutput;
use crate::config::toml::WasmtimeAllocatorConfig;
use crate::config::toml::WorkflowComponentBacktraceConfig;
use crate::config::toml::WorkflowComponentConfigToml;
use crate::config::toml::WorkflowConfigVerified;
use crate::config::toml::webhook;
use crate::config::toml::webhook::WebhookComponentVerified;
use crate::config::toml::webhook::WebhookRoute;
use crate::config::toml::webhook::WebhookRouteVerified;
use crate::grpc_util::TonicRespResult;
use crate::grpc_util::TonicResult;
use crate::grpc_util::extractor::accept_trace;
use crate::grpc_util::grpc_gen;
use crate::grpc_util::grpc_gen::GenerateExecutionIdResponse;
use crate::grpc_util::grpc_mapping::TonicServerOptionExt;
use crate::grpc_util::grpc_mapping::TonicServerResultExt;
use crate::grpc_util::grpc_mapping::db_error_to_status;
use crate::grpc_util::grpc_mapping::from_execution_event_to_grpc;
use crate::init;
use crate::init::Guard;
use anyhow::Context;
use anyhow::bail;
use assert_matches::assert_matches;
use chrono::DateTime;
use chrono::Utc;
use concepts::ComponentId;
use concepts::ComponentRetryConfig;
use concepts::ComponentType;
use concepts::ContentDigest;
use concepts::ExecutionId;
use concepts::FinishedExecutionResult;
use concepts::FnName;
use concepts::FunctionExtension;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::IfcFqnName;
use concepts::PackageIfcFns;
use concepts::ParameterType;
use concepts::Params;
use concepts::ReturnType;
use concepts::StrVariant;
use concepts::SupportedFunctionReturnValue;
use concepts::storage::AppendRequest;
use concepts::storage::BacktraceFilter;
use concepts::storage::CreateRequest;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionListPagination;
use concepts::storage::ExecutionWithState;
use concepts::storage::JoinSetResponse;
use concepts::storage::JoinSetResponseEvent;
use concepts::storage::JoinSetResponseEventOuter;
use concepts::storage::PendingState;
use concepts::storage::Version;
use concepts::storage::VersionType;
use concepts::time::ClockFn;
use concepts::time::Now;
use concepts::time::TokioSleep;
use concepts::time::now_tokio_instant;
use db_sqlite::sqlite_dao::SqliteConfig;
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
use itertools::Either;
use serde::Deserialize;
use serde_json::json;
use std::fmt::Debug;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::codec::CompressionEncoding;
use tonic_web::GrpcWebLayer;
use tower_http::trace::DefaultOnFailure;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;
use tracing::error;
use tracing::info_span;
use tracing::instrument;
use tracing::warn;
use tracing::{debug, info, trace};
use utils::wasm_tools::EXTENSION_FN_SUFFIX_SCHEDULE;
use utils::wasm_tools::WasmComponent;
use val_json::wast_val::WastValWithType;
use val_json::wast_val_ser::deserialize_slice;
use wasm_workers::activity::activity_worker::ActivityWorker;
use wasm_workers::engines::Engines;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::preopens_cleaner::PreopensCleaner;
use wasm_workers::webhook::webhook_trigger;
use wasm_workers::webhook::webhook_trigger::MethodAwareRouter;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointCompiled;
use wasm_workers::webhook::webhook_trigger::WebhookEndpointInstance;
use wasm_workers::workflow::host_exports::history_event_schedule_at_from_wast_val;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerCompiled;
use wasm_workers::workflow::workflow_worker::WorkflowWorkerLinked;

const EPOCH_MILLIS: u64 = 10;
const WEBUI_OCI_REFERENCE: &str = include_str!("../../assets/webui-version.txt");
const GET_STATUS_POLLING_SLEEP: Duration = Duration::from_secs(1);

type ComponentSourceMap = hashbrown::HashMap<ComponentId, hashbrown::HashMap<String, PathBuf>>;

#[derive(derive_more::Debug)]
struct GrpcServer {
    #[debug(skip)]
    db_pool: Arc<dyn DbPool>,
    component_registry_ro: ComponentConfigRegistryRO,
    component_source_map: ComponentSourceMap,
}

impl GrpcServer {
    fn new(
        db_pool: Arc<dyn DbPool>,
        component_registry_ro: ComponentConfigRegistryRO,
        component_source_map: ComponentSourceMap,
    ) -> Self {
        Self {
            db_pool,
            component_registry_ro,
            component_source_map,
        }
    }
}

#[tonic::async_trait]
impl grpc_gen::execution_repository_server::ExecutionRepository for GrpcServer {
    async fn generate_execution_id(
        &self,
        _request: tonic::Request<grpc_gen::GenerateExecutionIdRequest>,
    ) -> TonicRespResult<grpc_gen::GenerateExecutionIdResponse> {
        let execution_id = ExecutionId::generate();
        Ok(tonic::Response::new(GenerateExecutionIdResponse {
            execution_id: Some(execution_id.into()),
        }))
    }

    #[instrument(skip_all, fields(execution_id, ffqn, params, component_id))]
    async fn submit(
        &self,
        request: tonic::Request<grpc_gen::SubmitRequest>,
    ) -> TonicRespResult<grpc_gen::SubmitResponse> {
        struct JsonVals {
            vec: Vec<serde_json::Value>,
        }

        impl<'de> Deserialize<'de> for JsonVals {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let vec: Vec<serde_json::Value> =
                    deserializer.deserialize_seq(concepts::serde_params::VecVisitor)?;
                Ok(Self { vec })
            }
        }

        let request = request.into_inner();
        let grpc_gen::FunctionName {
            interface_name,
            function_name,
        } = request.function_name.argument_must_exist("function")?;
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        let span = Span::current();
        span.record("execution_id", tracing::field::display(&execution_id));
        if !execution_id.is_top_level() {
            return Err(tonic::Status::invalid_argument(
                "argument `execution_id` must be a top-level Execution ID",
            ));
        }

        // Deserialize params JSON into `Params`
        let mut params = {
            let params = request.params.argument_must_exist("params")?;
            let params = String::from_utf8(params.value).map_err(|_err| {
                tonic::Status::invalid_argument("argument `params` must be UTF-8 encoded")
            })?;
            JsonVals::deserialize(&mut serde_json::Deserializer::from_str(&params))
                .map_err(|serde_err| {
                    tonic::Status::invalid_argument(format!(
                        "argument `params` must be encoded as JSON array - {serde_err}"
                    ))
                })?
                .vec
        };

        // Check that ffqn exists
        let Some((component_id, retry_config, fn_metadata)) = self
            .component_registry_ro
            .find_by_exported_ffqn_submittable(&concepts::FunctionFqn::new_arc(
                Arc::from(interface_name),
                Arc::from(function_name),
            ))
        else {
            return Err(tonic::Status::not_found("function not found"));
        };
        span.record("component_id", tracing::field::display(component_id));

        // Extract `scheduled_at`
        let created_at = Now.now();
        let (scheduled_at, params, fn_metadata) = if fn_metadata.extension
            == Some(FunctionExtension::Schedule)
        {
            // First parameter must be `schedule-at`
            let Some(schedule_at) = params.drain(0..1).next() else {
                return Err(tonic::Status::invalid_argument(
                    "argument `params` must be an array with first value of type `schedule-at`",
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
            let wast_val_with_type: WastValWithType =
                serde_json::from_value(wast_val_with_type).map_err(|serde_err|
                    tonic::Status::invalid_argument(
                        format!("argument `params` must be an array with first value of type `schedule-at` - {serde_err}")
                    ))?;
            let schedule_at = history_event_schedule_at_from_wast_val(&wast_val_with_type.value)
                .map_err(|serde_err| {
                    tonic::Status::invalid_argument(format!(
                        "cannot convert `schedule-at` - {serde_err}"
                    ))
                })?;
            // Find the original fn_metadata. No need to change the `component_id` or `retry_config` as they belong to the same component.
            let ffqn = FunctionFqn {
                ifc_fqn: IfcFqnName::from_parts(
                    fn_metadata.ffqn.ifc_fqn.namespace(),
                    fn_metadata
                        .ffqn
                        .ifc_fqn
                        .package_strip_obelisk_ext_suffix()
                        .expect("checked that the ifc is ext"),
                    fn_metadata.ffqn.ifc_fqn.ifc_name(),
                    fn_metadata.ffqn.ifc_fqn.version(),
                ),
                function_name: FnName::from(
                    fn_metadata
                        .ffqn
                        .function_name
                        .to_string()
                        .strip_suffix(EXTENSION_FN_SUFFIX_SCHEDULE)
                        .expect("checked that the function is FunctionExtension::Schedule")
                        .to_string(),
                ),
            };
            let fn_metadata = self
                .component_registry_ro
                .find_by_exported_ffqn_submittable(&ffqn)
                .expect("-schedule must have the original counterpart in the component registry")
                .2;

            (
                schedule_at
                    .as_date_time(created_at)
                    .map_err(|_| tonic::Status::invalid_argument("schedule-at conversion error"))?,
                Params::from_json_values(params),
                fn_metadata,
            )
        } else {
            assert!(fn_metadata.extension.is_none());
            (created_at, Params::from_json_values(params), fn_metadata)
        };

        let ffqn = &fn_metadata.ffqn;
        span.record("ffqn", tracing::field::display(ffqn));
        // Type check `params`
        if let Err(err) = params.typecheck(
            fn_metadata
                .parameter_types
                .iter()
                .map(|ParameterType { type_wrapper, .. }| type_wrapper),
        ) {
            return Err(tonic::Status::invalid_argument(format!(
                "argument `params` invalid - {err}"
            )));
        }

        let db_connection = self.db_pool.connection();

        if !execution_id.is_top_level() {
            return Err(tonic::Status::invalid_argument(
                "argument `execution_id` must be a top-level Execution ID",
            ));
        }
        // Associate the (root) request execution with the request span. Makes possible to find the trace by execution id.
        let metadata = concepts::ExecutionMetadata::from_parent_span(&span);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                metadata,
                ffqn: ffqn.clone(),
                params,
                parent: None,
                scheduled_at,
                retry_exp_backoff: retry_config.retry_exp_backoff,
                max_retries: retry_config.max_retries,
                component_id: component_id.clone(),
                scheduled_by: None,
            })
            .await
            .to_status()?;

        let resp = grpc_gen::SubmitResponse {
            execution_id: Some(grpc_gen::ExecutionId {
                id: execution_id.to_string(),
            }),
        };
        Ok(tonic::Response::new(resp))
    }

    #[instrument(skip_all, fields(execution_id, ffqn, params, component_id))]
    async fn stub(
        &self,
        request: tonic::Request<grpc_gen::StubRequest>,
    ) -> TonicRespResult<grpc_gen::StubResponse> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        let span = Span::current();
        let execution_id = match execution_id {
            ExecutionId::TopLevel(_) => {
                return Err(tonic::Status::invalid_argument(
                    "execution ID value must be a derived ExecutionId",
                ));
            }
            ExecutionId::Derived(derived) => derived,
        };
        let Ok((parent_id, join_set_id)) = execution_id.split_to_parts() else {
            return Err(tonic::Status::invalid_argument(
                "execution ID cannot be parsed",
            ));
        };
        span.record("execution_id", tracing::field::display(&execution_id));
        // Get FFQN

        let db_connection = self.db_pool.connection();
        let ffqn = db_connection
            .get_create_request(&ExecutionId::Derived(execution_id.clone()))
            .await
            .to_status()?
            .ffqn;

        // Check that ffqn exists
        let Some((component_id, fn_metadata)) =
            self.component_registry_ro.find_by_exported_ffqn_stub(&ffqn)
        else {
            return Err(tonic::Status::not_found("function not found"));
        };
        span.record("component_id", tracing::field::display(component_id));

        let created_at = Now.now();

        let ffqn = &fn_metadata.ffqn;
        span.record("ffqn", tracing::field::display(ffqn));
        if ffqn.ifc_fqn.is_extension() {
            return Err(tonic::Status::invalid_argument(
                "argument `ffqn` must be the target, not the extension".to_string(),
            ));
        }
        // Type check `return_value`
        let return_value = match (request.return_value, fn_metadata.return_type.clone()) {
            (Some(return_value), Some(return_type)) => {
                match deserialize_slice(&return_value.value, return_type.type_wrapper) {
                    Ok(return_value) => SupportedFunctionReturnValue::from(return_value),
                    Err(err) => {
                        return Err(tonic::Status::invalid_argument(format!(
                            "cannot deserialize return value according to its type - {err}"
                        )));
                    }
                }
            }
            (None, None) => SupportedFunctionReturnValue::None,
            (_return_value, _ty) => {
                return Err(tonic::Status::invalid_argument(
                    "mismatch between return value and its type",
                ));
            }
        };

        let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
        // Attempt to write to `execution_id` and its parent, ignoring the possible conflict error on this tx
        let write_attempt = {
            let finished_req = AppendRequest {
                created_at,
                event: ExecutionEventInner::Finished {
                    result: Ok(return_value.clone()),
                    http_client_traces: None,
                },
            };
            db_connection
                .append_batch_respond_to_parent(
                    execution_id.clone(),
                    created_at,
                    vec![finished_req],
                    stub_finished_version.clone(),
                    parent_id,
                    JoinSetResponseEventOuter {
                        created_at,
                        event: JoinSetResponseEvent {
                            join_set_id,
                            event: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: execution_id.clone(),
                                finished_version: stub_finished_version.clone(),
                                result: Ok(return_value.clone()),
                            },
                        },
                    },
                )
                .await
        };
        if let Err(write_attempt) = write_attempt {
            // Check that the expected value is in the database
            debug!("Stub write attempt failed - {write_attempt:?}");

            let found = db_connection
                .get_execution_event(&ExecutionId::Derived(execution_id), &stub_finished_version)
                .await
                .to_status()?; // Not found at this point should not happen, unless the previous write failed. Will be retried.
            match found.event {
                ExecutionEventInner::Finished {
                    result: Ok(result), ..
                } if result == return_value => {}
                ExecutionEventInner::Finished { .. } => {
                    return Err(tonic::Status::already_exists(
                        "different value found in stubbed execution's finished event",
                    ));
                }
                _other => {
                    return Err(tonic::Status::internal(
                        "unexpected execution event at stubbed execution",
                    ));
                }
            }
        }
        let resp = grpc_gen::StubResponse {};
        Ok(tonic::Response::new(resp))
    }

    type GetStatusStream =
        Pin<Box<dyn Stream<Item = Result<grpc_gen::GetStatusResponse, tonic::Status>> + Send>>;

    #[instrument(skip_all, fields(execution_id))]
    async fn get_status(
        &self,
        request: tonic::Request<grpc_gen::GetStatusRequest>,
    ) -> TonicRespResult<Self::GetStatusStream> {
        use grpc_gen::ExecutionSummary;
        use grpc_gen::get_status_response::Message;

        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let conn = self.db_pool.connection();
        let current_pending_state = conn.get_pending_state(&execution_id).await.to_status()?;
        let (create_request, mut current_pending_state, grpc_pending_status) = {
            let create_request = conn.get_create_request(&execution_id).await.to_status()?;
            let grpc_pending_status =
                grpc_gen::ExecutionStatus::from(current_pending_state.clone());
            (create_request, current_pending_state, grpc_pending_status)
        };
        let summary = grpc_gen::GetStatusResponse {
            message: Some(Message::Summary(ExecutionSummary {
                created_at: Some(create_request.created_at.into()),
                scheduled_at: Some(create_request.scheduled_at.into()),
                execution_id: Some(grpc_gen::ExecutionId::from(&execution_id)),
                function_name: Some(create_request.ffqn.clone().into()),
                current_status: Some(grpc_pending_status),
            })),
        };
        if current_pending_state.is_finished() || !request.follow {
            let output: Self::GetStatusStream = if request.send_finished_status {
                let pending_state_finished = assert_matches!(current_pending_state, PendingState::Finished { finished } => finished);
                let finished_result = conn
                    .get_finished_result(&execution_id, pending_state_finished)
                    .await
                    .to_status()?
                    .expect("checked using `current_pending_state.is_finished()` that the execution is finished");
                let finished_message = grpc_gen::GetStatusResponse {
                    message: Some(Message::FinishedStatus(to_finished_status(
                        finished_result,
                        &create_request,
                        pending_state_finished.finished_at,
                    ))),
                };
                Box::pin(tokio_stream::iter([Ok(summary), Ok(finished_message)]))
            } else {
                Box::pin(tokio_stream::iter([Ok(summary)]))
            };
            Ok(tonic::Response::new(output))
        } else {
            let (tx, rx) = mpsc::channel(1);
            // send current pending status
            tx.send(TonicResult::Ok(summary))
                .await
                .expect("mpsc bounded channel requires buffer > 0");
            let db_pool = self.db_pool.clone();

            tokio::spawn(
                async move {
                    loop {
                        let sleep_until = now_tokio_instant() + GET_STATUS_POLLING_SLEEP;
                        loop {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            if db_pool.is_closing() {
                                debug!("Exitting get_status early, database is closing");
                                let _ = tx
                                    .send(TonicResult::Err(tonic::Status::aborted(
                                        "server is shutting down",
                                    )))
                                    .await;
                                return;
                            }
                            if now_tokio_instant() >= sleep_until {
                                break;
                            }
                        }
                        let conn = db_pool.connection();
                        match conn.get_pending_state(&execution_id).await {
                            Ok(pending_state) => {
                                if pending_state != current_pending_state {
                                    let grpc_pending_status =
                                        grpc_gen::ExecutionStatus::from(pending_state.clone());

                                    let message = grpc_gen::GetStatusResponse {
                                        message: Some(Message::CurrentStatus(grpc_pending_status)),
                                    };
                                    let send_res = tx.send(TonicResult::Ok(message)).await;
                                    if let Err(err) = send_res {
                                        info!("Cannot send the message - {err:?}");
                                        return;
                                    }
                                    if let PendingState::Finished { finished: pending_state_finished } = pending_state {
                                        if !request.send_finished_status {
                                            return;
                                        }
                                        // Send the last message and close the RPC.
                                        let finished_result = match conn
                                            .get_finished_result(&execution_id, pending_state_finished)
                                            .await
                                        {
                                            Ok(ok) => ok.expect("checked using `if let PendingState::Finished` that the execution is finished"),
                                            Err(db_err) => {
                                                error!("Cannot obtain finished result: {db_err:?}");
                                                let _ =
                                                    tx.send(Err(db_error_to_status(&db_err))).await;
                                                return;
                                            }
                                        };
                                        let message = grpc_gen::GetStatusResponse {
                                            message: Some(Message::FinishedStatus(to_finished_status(
                                                finished_result,
                                                &create_request,
                                                pending_state_finished.finished_at,
                                            ))),
                                        };
                                        let send_res = tx.send(TonicResult::Ok(message)).await;
                                        if let Err(err) = send_res {
                                            error!("Cannot send the final message - {err:?}");
                                        }
                                        return;
                                    }
                                    current_pending_state = pending_state;
                                }
                            }
                            Err(db_err) => {
                                error!("Database error while streaming status - {db_err:?}");
                                let _ = tx.send(Err(db_error_to_status(&db_err))).await;
                                return;
                            }
                        }
                    }
                }
                .in_current_span(),
            );
            let output = ReceiverStream::new(rx);
            Ok(tonic::Response::new(
                Box::pin(output) as Self::GetStatusStream
            ))
        }
    }

    #[instrument(skip_all, fields(ffqn))]
    async fn list_executions(
        &self,
        request: tonic::Request<grpc_gen::ListExecutionsRequest>,
    ) -> TonicRespResult<grpc_gen::ListExecutionsResponse> {
        let request = request.into_inner();
        let ffqn = request
            .function_name
            .map(FunctionFqn::try_from)
            .transpose()?;
        tracing::Span::current().record("ffqn", tracing::field::debug(&ffqn));
        let pagination =
            request
                .pagination
                .unwrap_or(grpc_gen::list_executions_request::Pagination::OlderThan(
                    grpc_gen::list_executions_request::OlderThan {
                        length: 20,
                        cursor: None,
                        including_cursor: false,
                    },
                ));
        let conn = self.db_pool.connection();
        let executions: Vec<_> = conn
            .list_executions(
                ffqn,
                request.top_level_only,
                ExecutionListPagination::try_from(pagination)?,
            )
            .await
            .to_status()?
            .into_iter()
            .map(
                |ExecutionWithState {
                     execution_id,
                     ffqn,
                     pending_state,
                     created_at,
                     scheduled_at,
                 }| grpc_gen::ExecutionSummary {
                    execution_id: Some(grpc_gen::ExecutionId::from(execution_id)),
                    function_name: Some(ffqn.into()),
                    current_status: Some(grpc_gen::ExecutionStatus::from(pending_state)),
                    created_at: Some(created_at.into()),
                    scheduled_at: Some(scheduled_at.into()),
                },
            )
            .collect();
        Ok(tonic::Response::new(grpc_gen::ListExecutionsResponse {
            executions,
        }))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn list_execution_events(
        &self,
        request: tonic::Request<grpc_gen::ListExecutionEventsRequest>,
    ) -> std::result::Result<tonic::Response<grpc_gen::ListExecutionEventsResponse>, tonic::Status>
    {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));

        let conn = self.db_pool.connection();
        let events = conn
            .list_execution_events(
                &execution_id,
                &Version(request.version_from),
                request.length,
                request.include_backtrace_id,
            )
            .await
            .to_status()?;

        let events = events
            .into_iter()
            .enumerate()
            .map(|(idx, execution_event)| {
                from_execution_event_to_grpc(
                    execution_event,
                    request.version_from
                        + VersionType::try_from(idx).expect("both from and to are VersionType"),
                )
            })
            .collect();
        Ok(tonic::Response::new(
            grpc_gen::ListExecutionEventsResponse { events },
        ))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn list_responses(
        &self,
        request: tonic::Request<grpc_gen::ListResponsesRequest>,
    ) -> std::result::Result<tonic::Response<grpc_gen::ListResponsesResponse>, tonic::Status> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let conn = self.db_pool.connection();
        let responses = conn
            .list_responses(
                &execution_id,
                concepts::storage::Pagination::NewerThan {
                    length: request.length,
                    cursor: request.cursor_from,
                    including_cursor: request.including_cursor,
                },
            )
            .await
            .to_status()?
            .into_iter()
            .map(grpc_gen::ResponseWithCursor::from)
            .collect();

        Ok(tonic::Response::new(grpc_gen::ListResponsesResponse {
            responses,
        }))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn list_execution_events_and_responses(
        &self,
        request: tonic::Request<grpc_gen::ListExecutionEventsAndResponsesRequest>,
    ) -> std::result::Result<
        tonic::Response<grpc_gen::ListExecutionEventsAndResponsesResponse>,
        tonic::Status,
    > {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));

        let conn = self.db_pool.connection();
        let events = conn
            .list_execution_events(
                &execution_id,
                &Version(request.version_from),
                request.events_length,
                request.include_backtrace_id,
            )
            .await
            .to_status()?
            .into_iter()
            .enumerate()
            .map(|(idx, execution_event)| {
                from_execution_event_to_grpc(
                    execution_event,
                    request.version_from
                        + VersionType::try_from(idx).expect("both from and to are VersionType"),
                )
            })
            .collect();

        let responses = conn
            .list_responses(
                &execution_id,
                concepts::storage::Pagination::NewerThan {
                    length: request.responses_length,
                    cursor: request.responses_cursor_from,
                    including_cursor: request.responses_including_cursor,
                },
            )
            .await
            .to_status()?
            .into_iter()
            .map(grpc_gen::ResponseWithCursor::from)
            .collect();

        Ok(tonic::Response::new(
            grpc_gen::ListExecutionEventsAndResponsesResponse { events, responses },
        ))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn get_backtrace(
        &self,
        request: tonic::Request<grpc_gen::GetBacktraceRequest>,
    ) -> Result<tonic::Response<grpc_gen::GetBacktraceResponse>, tonic::Status> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;

        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));

        let conn = self.db_pool.connection();
        let filter = match request.filter {
            Some(grpc_gen::get_backtrace_request::Filter::Specific(
                grpc_gen::get_backtrace_request::Specific { version },
            )) => BacktraceFilter::Specific(Version::new(version)),
            Some(grpc_gen::get_backtrace_request::Filter::Last(
                grpc_gen::get_backtrace_request::Last {},
            )) => BacktraceFilter::Last,
            Some(grpc_gen::get_backtrace_request::Filter::First(
                grpc_gen::get_backtrace_request::First {},
            ))
            | None => BacktraceFilter::First,
        };
        let backtrace_info = conn
            .get_backtrace(&execution_id, filter)
            .await
            .to_status()?;

        Ok(tonic::Response::new(grpc_gen::GetBacktraceResponse {
            wasm_backtrace: Some(grpc_gen::WasmBacktrace {
                version_min_including: backtrace_info.version_min_including.into(),
                version_max_excluding: backtrace_info.version_max_excluding.into(),
                frames: backtrace_info
                    .wasm_backtrace
                    .frames
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }),
            component_id: Some(backtrace_info.component_id.into()),
        }))
    }

    #[instrument(skip_all)]
    async fn get_backtrace_source(
        &self,
        request: tonic::Request<grpc_gen::GetBacktraceSourceRequest>,
    ) -> Result<tonic::Response<grpc_gen::GetBacktraceSourceResponse>, tonic::Status> {
        let request = request.into_inner();
        let component_id =
            ComponentId::try_from(request.component_id.argument_must_exist("component_id")?)?;
        if let Some(inner_map) = self.component_source_map.get(&component_id) {
            if let Some(actual_path) = inner_map.get(&request.file) {
                match tokio::fs::read_to_string(actual_path).await {
                    Ok(content) => Ok(tonic::Response::new(grpc_gen::GetBacktraceSourceResponse {
                        content,
                    })),
                    Err(err) => {
                        error!(%component_id, "Cannot read backtrace source {actual_path:?} - {err:?}");
                        Err(tonic::Status::internal("cannot read source file"))
                    }
                }
            } else {
                warn!(
                    "Backtrace file mapping not found for {component_id}, src {}",
                    request.file
                );
                Err(tonic::Status::not_found("backtrace file mapping not found"))
            }
        } else {
            warn!("Component {component_id} not found");
            Err(tonic::Status::not_found("component not found"))
        }
    }
}

fn to_finished_status(
    finished_result: FinishedExecutionResult,
    create_request: &CreateRequest,
    finished_at: DateTime<Utc>,
) -> grpc_gen::FinishedStatus {
    let result_detail = finished_result.into();
    grpc_gen::FinishedStatus {
        result_detail: Some(result_detail),
        created_at: Some(create_request.created_at.into()),
        scheduled_at: Some(create_request.scheduled_at.into()),
        finished_at: Some(finished_at.into()),
    }
}

#[tonic::async_trait]
impl grpc_gen::function_repository_server::FunctionRepository for GrpcServer {
    #[instrument(skip_all)]
    async fn list_components(
        &self,
        request: tonic::Request<grpc_gen::ListComponentsRequest>,
    ) -> TonicRespResult<grpc_gen::ListComponentsResponse> {
        let request = request.into_inner();
        let components = self.component_registry_ro.list(request.extensions);
        let mut res_components = Vec::with_capacity(components.len());
        for component in components {
            let res_component = grpc_gen::Component {
                name: component.component_id.name.to_string(),
                r#type: grpc_gen::ComponentType::from(component.component_id.component_type).into(),
                component_id: Some(component.component_id.into()),
                digest: component.content_digest.to_string(),
                exports: component
                    .workflow_or_activity_config
                    .map(|workflow_or_activity_config| {
                        list_fns(workflow_or_activity_config.exports_ext)
                    })
                    .unwrap_or_default(),
                imports: list_fns(component.imports),
            };
            res_components.push(res_component);
        }
        Ok(tonic::Response::new(grpc_gen::ListComponentsResponse {
            components: res_components,
        }))
    }

    async fn get_wit(
        &self,
        request: tonic::Request<grpc_gen::GetWitRequest>,
    ) -> TonicRespResult<grpc_gen::GetWitResponse> {
        let request = request.into_inner();
        let component_id =
            ComponentId::try_from(request.component_id.argument_must_exist("component_id")?)?;
        let wit = self
            .component_registry_ro
            .get_wit(&component_id)
            .entity_must_exist()?;
        Ok(tonic::Response::new(grpc_gen::GetWitResponse {
            content: wit.to_string(),
        }))
    }
}

fn list_fns(functions: Vec<FunctionMetadata>) -> Vec<grpc_gen::FunctionDetail> {
    let mut vec = Vec::with_capacity(functions.len());
    for FunctionMetadata {
        ffqn,
        parameter_types,
        return_type,
        extension,
        submittable,
    } in functions
    {
        let fun = grpc_gen::FunctionDetail {
            params: parameter_types
                .0
                .into_iter()
                .map(|param| grpc_gen::FunctionParameter {
                    name: param.name.to_string(),
                    r#type: Some(grpc_gen::WitType {
                        wit_type: param.wit_type.to_string(),
                        type_wrapper: serde_json::to_string(&param.type_wrapper)
                            .expect("`TypeWrapper` must be serializable"),
                    }),
                })
                .collect(),
            return_type: return_type.map(
                |ReturnType {
                     type_wrapper,
                     wit_type,
                 }| grpc_gen::WitType {
                    wit_type: wit_type.to_string(),
                    type_wrapper: serde_json::to_string(&type_wrapper)
                        .expect("`TypeWrapper` must be serializable"),
                },
            ),
            function_name: Some(ffqn.into()),
            extension: extension.map(|it| {
                match it {
                    FunctionExtension::Submit => grpc_gen::FunctionExtension::Submit,
                    FunctionExtension::AwaitNext => grpc_gen::FunctionExtension::AwaitNext,
                    FunctionExtension::Schedule => grpc_gen::FunctionExtension::Schedule,
                    FunctionExtension::Stub => grpc_gen::FunctionExtension::Stub,
                }
                .into()
            }),
            submittable,
        };
        vec.push(fun);
    }
    vec
}

pub(crate) struct RunParams {
    pub(crate) clean_db: bool,
    pub(crate) clean_cache: bool,
    pub(crate) clean_codegen_cache: bool,
}

pub(crate) async fn run(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    config: Option<PathBuf>,
    params: RunParams,
) -> anyhow::Result<()> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, config)?;
    let mut config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&mut config)?;

    Box::pin(run_internal(config, config_holder, params)).await?;
    Ok(())
}

#[derive(Debug, Default, Clone)]
pub(crate) struct VerifyParams {
    pub(crate) clean_cache: bool,
    pub(crate) clean_codegen_cache: bool,
    pub(crate) ignore_missing_env_vars: bool,
}

pub(crate) async fn verify(
    project_dirs: Option<ProjectDirs>,
    base_dirs: Option<BaseDirs>,
    config: Option<PathBuf>,
    verify_params: VerifyParams,
) -> Result<(), anyhow::Error> {
    let config_holder = ConfigHolder::new(project_dirs, base_dirs, config)?;
    let mut config = config_holder.load_config().await?;
    let _guard: Guard = init::init(&mut config)?;
    Box::pin(verify_internal(config, config_holder, verify_params)).await?;
    Ok(())
}

fn ignore_not_found(err: std::io::Error) -> Result<(), std::io::Error> {
    if err.kind() == std::io::ErrorKind::NotFound {
        Ok(())
    } else {
        Err(err)
    }
}

#[instrument(skip_all, name = "verify")]
async fn verify_internal(
    config: ConfigToml,
    config_holder: ConfigHolder,
    params: VerifyParams,
) -> Result<(ServerCompiledLinked, ComponentSourceMap), anyhow::Error> {
    info!("Verifying server configuration, compiling WASM components");
    debug!("Using toml config: {config:#?}");

    let wasm_cache_dir = config
        .wasm_global_config
        .get_wasm_cache_directory(&config_holder.path_prefixes)
        .await?;
    let codegen_cache = config
        .codegen_cache
        .get_directory(&config_holder.path_prefixes)
        .await?;
    debug!("Using codegen cache? {codegen_cache:?}");
    if params.clean_cache {
        tokio::fs::remove_dir_all(&wasm_cache_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete wasm cache directory {wasm_cache_dir:?}"))?;
    }
    if params.clean_cache || params.clean_codegen_cache {
        if let Some(codegen_cache) = &codegen_cache {
            // delete codegen_cache
            tokio::fs::remove_dir_all(codegen_cache)
                .await
                .or_else(ignore_not_found)
                .with_context(|| {
                    format!("cannot delete codegen cache directory {codegen_cache:?}")
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

    let (server_verified, component_source_map) = ServerVerified::new(
        config,
        codegen_cache,
        Arc::from(wasm_cache_dir),
        Arc::from(metadata_dir),
        params.ignore_missing_env_vars,
        config_holder.path_prefixes,
    )
    .await?;
    let compiled_and_linked = ServerCompiledLinked::new(server_verified).await?;
    info!(
        "Server configuration was verified{}",
        if compiled_and_linked.supressed_errors.is_some() {
            " with supressed errors"
        } else {
            ""
        }
    );
    Ok((compiled_and_linked, component_source_map))
}

async fn run_internal(
    config: ConfigToml,
    config_holder: ConfigHolder,
    params: RunParams,
) -> anyhow::Result<()> {
    let span = info_span!("init");
    let api_listening_addr = config.api.listening_addr;

    let db_dir = config
        .sqlite
        .get_sqlite_dir(&config_holder.path_prefixes)
        .await?;
    let sqlite_config = config.sqlite.as_sqlite_config();
    let sqlite_file = db_dir.join(SQLITE_FILE_NAME);
    if params.clean_db {
        tokio::fs::remove_dir_all(&db_dir)
            .await
            .or_else(ignore_not_found)
            .with_context(|| format!("cannot delete database directory `{db_dir:?}`"))?;
        tokio::fs::create_dir_all(&db_dir)
            .await
            .with_context(|| format!("cannot create database directory {db_dir:?}"))?;
    }

    let (compiled_and_linked, component_source_map) = Box::pin(verify_internal(
        config,
        config_holder,
        VerifyParams {
            clean_cache: params.clean_cache,
            clean_codegen_cache: params.clean_codegen_cache,
            ignore_missing_env_vars: false,
        },
    ))
    .instrument(span.clone())
    .await?;

    let (init, component_registry_ro) =
        ServerInit::spawn_tasks_and_threads(compiled_and_linked, &sqlite_file, sqlite_config)
            .instrument(span)
            .await?;
    let grpc_server = Arc::new(GrpcServer::new(
        init.db_pool.clone(),
        component_registry_ro,
        component_source_map,
    ));
    tonic::transport::Server::builder()
        .accept_http1(true)
        .layer(
            tower::ServiceBuilder::new()
                .layer(
                    // Enable logging with `tower_http::trace=debug`
                    tower_http::trace::TraceLayer::new_for_grpc()
                        .on_failure(DefaultOnFailure::new().level(Level::DEBUG))
                        .make_span_with(make_span),
                )
                .layer(GrpcWebLayer::new())
                .map_request(accept_trace),
        )
        .add_service(
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
        .serve_with_shutdown(api_listening_addr, async move {
            info!("Serving gRPC requests at {api_listening_addr}");
            info!("Server is ready");
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for SIGINT event");
            warn!("Received SIGINT, waiting for gRPC server to shut down");
            init.close().await;
        })
        .await
        .with_context(|| format!("grpc server error listening on {api_listening_addr}"))
}

fn make_span<B>(request: &axum::http::Request<B>) -> Span {
    let headers = request.headers();
    info_span!("incoming gRPC request", ?headers)
}

struct ServerVerified {
    config: ConfigVerified,
    engines: Engines,
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
}

impl ServerVerified {
    #[instrument(name = "ServerVerified::new", skip_all)]
    async fn new(
        config: ConfigToml,
        codegen_cache_dir: Option<PathBuf>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        path_prefixes: PathPrefixes,
    ) -> Result<(Self, ComponentSourceMap), anyhow::Error> {
        let engines = {
            match config.wasm_global_config.allocator_config {
                WasmtimeAllocatorConfig::Auto => Engines::auto_detect_allocator(
                    config.wasmtime_pooling_config.into(),
                    codegen_cache_dir,
                )?,
                WasmtimeAllocatorConfig::OnDemand => Engines::on_demand(codegen_cache_dir)?,
                WasmtimeAllocatorConfig::Pooling => {
                    Engines::pooling(config.wasmtime_pooling_config.into(), codegen_cache_dir)?
                }
            }
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
                max_inflight_requests: InflightSemaphore::default(),
            });
            webhooks.push(webhook::WebhookComponentConfigToml {
                common: ComponentCommon {
                    name: ConfigName::new(StrVariant::Static("obelisk_webui")).unwrap(),
                    location: ComponentLocation::Oci(
                        WEBUI_OCI_REFERENCE
                            .parse()
                            .expect("hard-coded webui reference must be parsed"),
                    ),
                },
                http_server: http_server_name,
                routes: vec![WebhookRoute::default()],
                forward_stdout: StdOutput::default(),
                forward_stderr: StdOutput::default(),
                env_vars: vec![EnvVarConfig {
                    key: "TARGET_URL".to_string(),
                    val: Some(format!("http://{}", config.api.listening_addr)),
                }],
                backtrace: WorkflowComponentBacktraceConfig::default(),
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
        let mut config = ConfigVerified::fetch_and_verify_all(
            config.wasm_activities,
            config.workflows,
            http_servers,
            webhooks,
            wasm_cache_dir,
            metadata_dir,
            ignore_missing_env_vars,
            path_prefixes,
            global_backtrace_persist,
            parent_preopen_dir.clone(),
        )
        .await?;
        debug!("Verified config: {config:#?}");
        let component_source_map = {
            let mut map = hashbrown::HashMap::new();
            for workflow in &mut config.workflows {
                let inner_map = std::mem::take(&mut workflow.frame_files_to_sources);
                map.insert(workflow.component_id().clone(), inner_map);
            }
            for webhook in &mut config.webhooks_by_names.values_mut() {
                let inner_map = std::mem::take(&mut webhook.frame_files_to_sources);
                map.insert(webhook.component_id.clone(), inner_map);
            }
            map
        };

        Ok((
            Self {
                config,
                engines,
                parent_preopen_dir,
                activities_cleanup,
            },
            component_source_map,
        ))
    }
}

struct ServerCompiledLinked {
    engines: Engines,
    component_registry_ro: ComponentConfigRegistryRO,
    compiled_components: LinkedComponents,
    parent_preopen_dir: Option<Arc<Path>>,
    activities_cleanup: Option<ActivitiesDirectoriesCleanupConfigToml>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<ConfigName>)>,
    supressed_errors: Option<String>,
}

impl ServerCompiledLinked {
    async fn new(server_verified: ServerVerified) -> Result<Self, anyhow::Error> {
        let (compiled_components, component_registry_ro, supressed_errors) = compile_and_verify(
            &server_verified.engines,
            server_verified.config.wasm_activities,
            server_verified.config.workflows,
            server_verified.config.webhooks_by_names,
            server_verified.config.global_backtrace_persist,
        )
        .await?;
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

struct ServerInit {
    db_pool: Arc<dyn DbPool>,
    exec_join_handles: Vec<ExecutorTaskHandle>,
    #[expect(dead_code)]
    timers_watcher: AbortOnDropHandle,
    #[expect(dead_code)] // http servers will be aborted automatically
    http_servers_handles: Vec<AbortOnDropHandle>,
    #[expect(dead_code)] // Shuts itself down in drop
    epoch_ticker: EpochTicker,
    #[expect(dead_code)]
    preopens_cleaner: Option<AbortOnDropHandle>,
}

impl ServerInit {
    #[instrument(skip_all)]
    async fn spawn_tasks_and_threads(
        mut server_compiled_linked: ServerCompiledLinked,
        sqlite_file: &Path,
        sqlite_config: SqliteConfig,
    ) -> Result<(ServerInit, ComponentConfigRegistryRO), anyhow::Error> {
        // Start components requiring a database
        let epoch_ticker = EpochTicker::spawn_new(
            server_compiled_linked.engines.weak_refs(),
            Duration::from_millis(EPOCH_MILLIS),
        );
        let db_pool = Arc::new(
            SqlitePool::new(sqlite_file, sqlite_config, TokioSleep)
                .await
                .with_context(|| format!("cannot open sqlite file {sqlite_file:?}"))?,
        );

        let timers_watcher = expired_timers_watcher::spawn_new(
            db_pool.clone(),
            TimersWatcherConfig {
                tick_sleep: Duration::from_millis(100),
                clock_fn: Now,
                leeway: Duration::from_millis(100), // TODO: Make configurable
            },
        );

        let preopens_cleaner = if let (Some(parent_preopen_dir), Some(activities_cleanup)) = (
            server_compiled_linked.parent_preopen_dir,
            server_compiled_linked.activities_cleanup,
        ) {
            Some(PreopensCleaner::spawn_task(
                activities_cleanup.older_than.into(),
                parent_preopen_dir,
                activities_cleanup.run_every.into(),
                TokioSleep,
                Now,
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

        // Spawn executors
        let exec_join_handles = server_compiled_linked
            .compiled_components
            .workers_linked
            .into_iter()
            .map(|pre_spawn| pre_spawn.spawn(db_pool.clone()))
            .collect();

        // Start TCP listeners
        let http_servers_handles: Vec<AbortOnDropHandle> = start_webhooks(
            http_servers_to_webhooks,
            &server_compiled_linked.engines,
            db_pool.clone(),
            Arc::from(server_compiled_linked.component_registry_ro.clone()),
        )
        .await?;
        Ok((
            ServerInit {
                db_pool,
                exec_join_handles,
                timers_watcher,
                http_servers_handles,
                epoch_ticker,
                preopens_cleaner,
            },
            server_compiled_linked.component_registry_ro,
        ))
    }

    async fn close(self) {
        info!("Server is shutting down");
        let (db_pool, exec_join_handles) = {
            let ServerInit {
                db_pool,
                exec_join_handles,
                timers_watcher: _,
                http_servers_handles: _,
                epoch_ticker: _,
                preopens_cleaner: _,
            } = self;
            // drop AbortOnDropHandles
            (db_pool, exec_join_handles)
        };
        for exec_join_handle in exec_join_handles {
            exec_join_handle.close().await;
        }
        // Close most of the services. Note that a task responding to --follow might be still running.
        let res = db_pool.close().await;
        if let Err(err) = res {
            error!("Cannot close the database - {err:?}");
        }
    }
}

type WebhookInstancesAndRoutes = (WebhookEndpointInstance<Now>, Vec<WebhookRouteVerified>);

async fn start_webhooks(
    http_servers_to_webhooks: Vec<(webhook::HttpServer, Vec<WebhookInstancesAndRoutes>)>,
    engines: &Engines,
    db_pool: Arc<dyn DbPool>,
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
            "HTTP server `{}` is listening on http://{server_addr}",
            http_server.name,
        );
        let server = AbortOnDropHandle::new(
            tokio::spawn(webhook_trigger::server(
                StrVariant::from(http_server.name),
                tcp_listener,
                engine.clone(),
                router,
                db_pool.clone(),
                Now,
                fn_registry.clone(),
                http_server.max_inflight_requests.into(),
            ))
            .abort_handle(),
        );
        abort_handles.push(server);
    }
    Ok(abort_handles)
}

#[derive(Debug)]
struct ConfigVerified {
    wasm_activities: Vec<ActivityWasmConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<ConfigName, WebhookComponentVerified>,
    http_servers_to_webhook_names: Vec<(webhook::HttpServer, Vec<ConfigName>)>,
    global_backtrace_persist: bool,
}

impl ConfigVerified {
    #[instrument(skip_all)]
    #[expect(clippy::too_many_arguments)]
    async fn fetch_and_verify_all(
        wasm_activities: Vec<ActivityComponentConfigToml>,
        workflows: Vec<WorkflowComponentConfigToml>,
        http_servers: Vec<webhook::HttpServer>,
        webhooks: Vec<webhook::WebhookComponentConfigToml>,
        wasm_cache_dir: Arc<Path>,
        metadata_dir: Arc<Path>,
        ignore_missing_env_vars: bool,
        path_prefixes: PathPrefixes,
        global_backtrace_persist: bool,
        parent_preopen_dir: Option<Arc<Path>>,
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
        // Download WASM files from OCI registries if needed.
        // TODO!: Switch to `JoinSet` when madsim supports it.
        let activities = wasm_activities
            .into_iter()
            .map(|activity| {
                tokio::spawn({
                    let wasm_cache_dir = wasm_cache_dir.clone();
                    let metadata_dir = metadata_dir.clone();
                    let parent_preopen_dir = parent_preopen_dir.clone();
                    async move {
                        activity
                            .fetch_and_verify(
                                wasm_cache_dir.clone(),
                                metadata_dir.clone(),
                                ignore_missing_env_vars,
                                parent_preopen_dir,
                            )
                            .await
                    }
                    .in_current_span()
                })
            })
            .collect::<Vec<_>>();
        let path_prefixes = Arc::new(path_prefixes);
        let workflows = workflows
            .into_iter()
            .map(|workflow| {
                let path_prefixes = path_prefixes.clone();
                tokio::spawn(
                    workflow
                        .fetch_and_verify(
                            wasm_cache_dir.clone(),
                            metadata_dir.clone(),
                            path_prefixes,
                            global_backtrace_persist,
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
                    async move {
                        let name = webhook.common.name.clone();
                        let webhook = webhook
                            .fetch_and_verify(
                                wasm_cache_dir.clone(),
                                metadata_dir.clone(),
                                ignore_missing_env_vars,
                                path_prefixes,
                            )
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
                Ok(ConfigVerified {wasm_activities, workflows, webhooks_by_names, http_servers_to_webhook_names, global_backtrace_persist})
            },
            sigint = tokio::signal::ctrl_c() => {
                sigint.expect("failed to listen for SIGINT event");
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

enum CompiledComponent {
    ActivityOrWorkflow {
        worker: WorkerCompiled,
        component_config: ComponentConfig,
    },
    Webhook {
        webhook_name: ConfigName,
        webhook_compiled: WebhookEndpointCompiled,
        routes: Vec<WebhookRouteVerified>,
        content_digest: ContentDigest,
    },
    ActivityStub {
        component_config: ComponentConfig,
    },
}

#[instrument(skip_all)]
async fn compile_and_verify(
    engines: &Engines,
    wasm_activities: Vec<ActivityWasmConfigVerified>,
    workflows: Vec<WorkflowConfigVerified>,
    webhooks_by_names: hashbrown::HashMap<ConfigName, WebhookComponentVerified>,
    global_backtrace_persist: bool,
) -> Result<
    (
        LinkedComponents,
        ComponentConfigRegistryRO,
        Option<String>, /* supressed_errors */
    ),
    anyhow::Error,
> {
    let pre_spawns: Vec<tokio::task::JoinHandle<Result<_, anyhow::Error>>> = wasm_activities
        .into_iter()
        .map(|activity| {
            let engines = engines.clone();
            let span = info_span!("activity_compile", component_id = %activity.component_id());
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    if activity.component_id().component_type == ComponentType::ActivityStub {
                        let engine = engines.activity_engine.clone();
                        let wasm_component = WasmComponent::new(
                            activity.wasm_path,
                            &engine,
                            activity.activity_config.component_id.component_type,
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
                            retry_config: ComponentRetryConfig::ZERO,
                        };
                        let component_config = ComponentConfig {
                            component_id: activity.activity_config.component_id,
                            imports: vec![],
                            workflow_or_activity_config: Some(component_config_importable),
                            content_digest: activity.content_digest,
                            wit,
                        };
                        Ok(CompiledComponent::ActivityStub { component_config })
                    } else {
                        prespawn_activity(activity, &engines).map(|(worker, component_config)| {
                            CompiledComponent::ActivityOrWorkflow {
                                worker,
                                component_config,
                            }
                        })
                    }
                })
            })
        })
        .chain(workflows.into_iter().map(|workflow| {
            let engines = engines.clone();
            let span = info_span!("workflow_compile", component_id = %workflow.component_id());
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| {
                    prespawn_workflow(workflow, &engines).map(|(worker, component_config)| {
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
                    let span = info_span!("webhook_compile", component_id = %webhook.component_id);
                    tokio::task::spawn_blocking(move || {
                        span.in_scope(|| {
                            let component_id = webhook.component_id;
                            let webhook_compiled = webhook_trigger::WebhookEndpointCompiled::new(
                                webhook.wasm_path,
                                &engines.webhook_engine,
                                component_id.clone(),
                                webhook.forward_stdout,
                                webhook.forward_stderr,
                                webhook.env_vars,
                                global_backtrace_persist,
                            )?;
                            Ok(CompiledComponent::Webhook {
                                webhook_name,
                                webhook_compiled,
                                routes: webhook.routes,
                                content_digest: webhook.content_digest,
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
                    CompiledComponent::Webhook{webhook_name, webhook_compiled, routes, content_digest} => {
                        let component = ComponentConfig {
                            component_id: webhook_compiled.component_id.clone(),
                            imports: webhook_compiled.imports().to_vec(),
                            content_digest, workflow_or_activity_config: None,
                            wit: webhook_compiled.wasm_component.wit()
                                .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
                                .ok()
                        };
                        component_registry.insert(component)?;
                        let old = webhooks_compiled_by_names.insert(webhook_name, (webhook_compiled, routes));
                        assert!(old.is_none());
                    },
                    CompiledComponent::ActivityStub {  component_config } => {
                        component_registry.insert(component_config)?;
                    },
                }
            }
            let (component_registry_ro, supressed_errors) = component_registry.verify_imports();
            let fn_registry: Arc<dyn FunctionRegistry> = Arc::from(component_registry_ro.clone());
            let workers_linked = workers_compiled.into_iter().map(|worker| worker.link(&fn_registry)).collect::<Result<Vec<_>,_>>()?;
            let webhooks_by_names = webhooks_compiled_by_names
                .into_iter()
                .map(|(name, (compiled, routes))| compiled.link(&engines.webhook_engine, fn_registry.as_ref()).map(|instance| (name, (instance, routes))))
                .collect::<Result<hashbrown::HashMap<_,_>,_>>()?;
            Ok((LinkedComponents {
                workers_linked,
                webhooks_by_names,
            }, component_registry_ro, supressed_errors))
        },
        sigint = tokio::signal::ctrl_c() => {
            sigint.expect("failed to listen for SIGINT event");
            warn!("Received SIGINT, canceling while compiling the components");
            anyhow::bail!("canceling while compiling the components")
        }
    }
}

#[instrument(skip_all, fields(
    executor_id = %activity.exec_config.executor_id,
    component_id = %activity.exec_config.component_id,
    wasm_path = ?activity.wasm_path,
))]
fn prespawn_activity(
    activity: ActivityWasmConfigVerified,
    engines: &Engines,
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    assert!(activity.component_id().component_type == ComponentType::ActivityWasm);
    debug!("Instantiating activity");
    trace!(?activity, "Full configuration");
    let engine = engines.activity_engine.clone();
    let component_type = activity.component_id().component_type;
    let wasm_component = WasmComponent::new(activity.wasm_path, &engine, component_type)?;
    let wit = wasm_component
        .wit()
        .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
        .ok();
    let worker = ActivityWorker::new_with_config(
        wasm_component,
        activity.activity_config,
        engine,
        Now,
        TokioSleep,
    )?;
    Ok(WorkerCompiled::new_activity(
        worker,
        activity.content_digest,
        activity.exec_config,
        activity.retry_config,
        wit,
    ))
}

#[instrument(skip_all, fields(
    executor_id = %workflow.exec_config.executor_id,
    component_id = %workflow.exec_config.component_id,
    wasm_path = ?workflow.wasm_path,
))]
fn prespawn_workflow(
    workflow: WorkflowConfigVerified,
    engines: &Engines,
) -> Result<(WorkerCompiled, ComponentConfig), anyhow::Error> {
    assert!(workflow.component_id().component_type == ComponentType::Workflow);
    debug!("Instantiating workflow");
    trace!(?workflow, "Full configuration");
    let engine = engines.workflow_engine.clone();
    let component_type = workflow.component_id().component_type;
    let wasm_component = WasmComponent::new(&workflow.wasm_path, &engine, component_type)?;
    let wit = wasm_component
        .wit()
        .inspect_err(|err| warn!("Cannot get wit - {err:?}"))
        .ok();
    let worker = WorkflowWorkerCompiled::new_with_config(
        wasm_component,
        workflow.workflow_config,
        engine,
        Now,
        TokioSleep,
    )?;
    Ok(WorkerCompiled::new_workflow(
        worker,
        workflow.content_digest,
        workflow.exec_config,
        workflow.retry_config,
        wit,
    ))
}

struct WorkerCompiled {
    worker: Either<Arc<dyn Worker>, WorkflowWorkerCompiled<Now, TokioSleep>>,
    exec_config: ExecConfig,
}

impl WorkerCompiled {
    fn new_activity(
        worker: ActivityWorker<Now, TokioSleep>,
        content_digest: ContentDigest,
        exec_config: ExecConfig,
        retry_config: ComponentRetryConfig,
        wit: Option<String>,
    ) -> (WorkerCompiled, ComponentConfig) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            content_digest,
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
                retry_config,
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
        };
        (
            WorkerCompiled {
                worker: Either::Left(Arc::from(worker)),
                exec_config,
            },
            component,
        )
    }

    fn new_workflow(
        worker: WorkflowWorkerCompiled<Now, TokioSleep>,
        content_digest: ContentDigest,
        exec_config: ExecConfig,
        retry_config: ComponentRetryConfig,
        wit: Option<String>,
    ) -> (WorkerCompiled, ComponentConfig) {
        let component = ComponentConfig {
            component_id: exec_config.component_id.clone(),
            content_digest,
            workflow_or_activity_config: Some(ComponentConfigImportable {
                exports_ext: worker.exported_functions_ext().to_vec(),
                exports_hierarchy_ext: worker.exports_hierarchy_ext().to_vec(),
                retry_config,
            }),
            imports: worker.imported_functions().to_vec(),
            wit,
        };
        (
            WorkerCompiled {
                worker: Either::Right(worker),
                exec_config,
            },
            component,
        )
    }

    #[instrument(skip_all, fields(component_id = %self.exec_config.component_id), err)]
    fn link(self, fn_registry: &Arc<dyn FunctionRegistry>) -> Result<WorkerLinked, anyhow::Error> {
        Ok(WorkerLinked {
            worker: match self.worker {
                Either::Left(activity) => Either::Left(activity),
                Either::Right(workflow_compiled) => {
                    Either::Right(workflow_compiled.link(fn_registry)?)
                }
            },
            exec_config: self.exec_config,
        })
    }
}

struct WorkerLinked {
    worker: Either<Arc<dyn Worker>, WorkflowWorkerLinked<Now, TokioSleep>>,
    exec_config: ExecConfig,
}
impl WorkerLinked {
    fn spawn(self, db_pool: Arc<dyn DbPool>) -> ExecutorTaskHandle {
        let worker = match self.worker {
            Either::Left(activity) => activity,
            Either::Right(workflow_linked) => {
                Arc::from(workflow_linked.into_worker(db_pool.clone()))
            }
        };
        ExecTask::spawn_new(worker, self.exec_config, Now, db_pool)
    }
}

// TODO: Move to wasm-workers
#[derive(Default, Debug)]
pub struct ComponentConfigRegistry {
    inner: ComponentConfigRegistryInner,
}

#[derive(Default, Debug)]
struct ComponentConfigRegistryInner {
    exported_ffqns_ext:
        hashbrown::HashMap<FunctionFqn, (ComponentId, FunctionMetadata, ComponentRetryConfig)>,
    export_hierarchy: Vec<PackageIfcFns>,
    ids_to_components: hashbrown::HashMap<ComponentId, ComponentConfig>,
}

impl ComponentConfigRegistry {
    pub fn insert(&mut self, component: ComponentConfig) -> Result<(), anyhow::Error> {
        // verify that the component or its exports are not already present
        if self
            .inner
            .ids_to_components
            .contains_key(&component.component_id)
        {
            bail!("component {} is already inserted", component.component_id);
        }
        if let Some(workflow_or_activity_config) = &component.workflow_or_activity_config {
            for exported_ffqn in workflow_or_activity_config
                .exports_ext
                .iter()
                .map(|f| &f.ffqn)
            {
                if let Some((offending_id, _, _)) = self.inner.exported_ffqns_ext.get(exported_ffqn)
                {
                    bail!(
                        "function {exported_ffqn} is already exported by component {offending_id}, cannot insert {}",
                        component.component_id
                    );
                }
            }
            // insert to `exported_ffqns_ext`
            for exported_fn_metadata in &workflow_or_activity_config.exports_ext {
                let old = self.inner.exported_ffqns_ext.insert(
                    exported_fn_metadata.ffqn.clone(),
                    (
                        component.component_id.clone(),
                        exported_fn_metadata.clone(),
                        workflow_or_activity_config.retry_config,
                    ),
                );
                assert!(old.is_none());
            }
            // insert to `export_hierarchy`
            self.inner
                .export_hierarchy
                .extend_from_slice(&workflow_or_activity_config.exports_hierarchy_ext);
        }
        // insert to `ids_to_components`
        let old = self
            .inner
            .ids_to_components
            .insert(component.component_id.clone(), component);
        assert!(old.is_none());

        Ok(())
    }

    /// Verify that each imported function can be matched by looking at the available exports.
    /// This is a best effort to give function-level error messages.
    /// WASI imports and host functions are not validated at the moment, those errors
    /// are caught by wasmtime while pre-instantiation with a message containing the missing interface.
    pub fn verify_imports(
        self,
    ) -> (
        ComponentConfigRegistryRO,
        Option<String>, /* supressed_errors */
    ) {
        let mut errors = Vec::new();
        for (component_id, examined_component) in &self.inner.ids_to_components {
            self.verify_imports_component(component_id, &examined_component.imports, &mut errors);
        }
        let errors = if !errors.is_empty() {
            let errors = errors.join("\n");
            // TODO: Promote to an error when version resolution is implemented
            // https://github.com/bytecodealliance/wasmtime/blob/8dbd5db30d05e96594e2516cdbd7cc213f1c2fa4/crates/environ/src/component/names.rs#L102-L104
            tracing::warn!("component resolution error: \n{errors}");
            Some(errors)
        } else {
            None
        };
        (
            ComponentConfigRegistryRO {
                inner: Arc::new(self.inner),
            },
            errors,
        )
    }

    fn additional_import_allowlist(import: &FunctionMetadata, component_id: &ComponentId) -> bool {
        match component_id.component_type {
            ComponentType::ActivityWasm => {
                // wasi + log
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => {
                        import.ffqn.ifc_fqn.deref() == "obelisk:log/log@1.0.0"
                            || import.ffqn.ifc_fqn.deref() == "obelisk:activity/process@1.0.0"
                    }
                    _ => false,
                }
            }
            ComponentType::Workflow => {
                // log + workflow(-support) + types
                matches!(
                    import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                    "obelisk:log@1.0.0"
                        | "obelisk:workflow@1.1.0"
                        | "obelisk:workflow@2.0.0"
                        | "obelisk:types@1.1.0"
                )
            }
            ComponentType::WebhookEndpoint => {
                // wasi + log + types (needed for scheduling)
                match import.ffqn.ifc_fqn.namespace() {
                    "wasi" => true,
                    "obelisk" => matches!(
                        import.ffqn.ifc_fqn.pkg_fqn_name().to_string().as_str(),
                        "obelisk:log@1.0.0" | "obelisk:types@1.1.0"
                    ),
                    _ => false,
                }
            }
            ComponentType::ActivityStub => false,
        }
    }

    fn verify_imports_component(
        &self,
        component_id: &ComponentId,
        imports: &[FunctionMetadata],
        errors: &mut Vec<String>,
    ) {
        for imported_fn_metadata in imports {
            if let Some((exported_component_id, exported_fn_metadata, _)) = self
                .inner
                .exported_ffqns_ext
                .get(&imported_fn_metadata.ffqn)
            {
                // check parameters
                if imported_fn_metadata.parameter_types != exported_fn_metadata.parameter_types {
                    error!(
                        "Parameter types do not match: {ffqn} imported by {component_id} , exported by {exported_component_id}",
                        ffqn = imported_fn_metadata.ffqn
                    );
                    error!(
                        "Import {import}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                    );
                    error!(
                        "Export {export}",
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("parameter types do not match: {component_id} imports {imported_fn_metadata} , {exported_component_id} exports {exported_fn_metadata}"));
                }
                if imported_fn_metadata.return_type != exported_fn_metadata.return_type {
                    error!(
                        "Return types do not match: {ffqn} imported by {component_id} , exported by {exported_component_id}",
                        ffqn = imported_fn_metadata.ffqn
                    );
                    error!(
                        "Import {import}",
                        import = serde_json::to_string(imported_fn_metadata).unwrap(), // TODO: print in WIT format
                    );
                    error!(
                        "Export {export}",
                        export = serde_json::to_string(exported_fn_metadata).unwrap(),
                    );
                    errors.push(format!("return types do not match: {component_id} imports {imported_fn_metadata} , {exported_component_id} exports {exported_fn_metadata}"));
                }
            } else if !Self::additional_import_allowlist(imported_fn_metadata, component_id) {
                errors.push(format!(
                    "function imported by {component_id} not found: {imported_fn_metadata}"
                ));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComponentConfigRegistryRO {
    inner: Arc<ComponentConfigRegistryInner>,
}

impl ComponentConfigRegistryRO {
    pub fn get_wit(&self, id: &ComponentId) -> Option<&str> {
        self.inner
            .ids_to_components
            .get(id)
            .and_then(|component_config| component_config.wit.as_deref())
    }

    pub fn find_by_exported_ffqn_submittable(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(&ComponentId, ComponentRetryConfig, &FunctionMetadata)> {
        self.inner.exported_ffqns_ext.get(ffqn).and_then(
            |(component_id, fn_metadata, retry_config)| {
                if fn_metadata.submittable {
                    Some((component_id, *retry_config, fn_metadata))
                } else {
                    None
                }
            },
        )
    }

    pub fn find_by_exported_ffqn_stub(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(&ComponentId, &FunctionMetadata)> {
        self.inner.exported_ffqns_ext.get(ffqn).and_then(
            |(component_id, fn_metadata, _retry_config)| {
                if component_id.component_type == ComponentType::ActivityStub {
                    Some((component_id, fn_metadata))
                } else {
                    None
                }
            },
        )
    }

    pub fn list(&self, extensions: bool) -> Vec<ComponentConfig> {
        self.inner
            .ids_to_components
            .values()
            .cloned()
            .map(|mut component| {
                // If no extensions are requested, retain those that are !ext
                if !extensions && let Some(importable) = &mut component.workflow_or_activity_config
                {
                    importable
                        .exports_ext
                        .retain(|fn_metadata| !fn_metadata.ffqn.ifc_fqn.is_extension());
                }
                component
            })
            .collect()
    }
}

impl FunctionRegistry for ComponentConfigRegistryRO {
    fn get_by_exported_function(
        &self,
        ffqn: &FunctionFqn,
    ) -> Option<(FunctionMetadata, ComponentId, ComponentRetryConfig)> {
        if ffqn.ifc_fqn.is_extension() {
            None
        } else {
            self.inner
                .exported_ffqns_ext
                .get(ffqn)
                .map(|(id, metadata, retry)| (metadata.clone(), id.clone(), *retry))
        }
    }

    fn all_exports(&self) -> &[PackageIfcFns] {
        &self.inner.export_hierarchy
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ConfigHolder,
        command::server::{ServerCompiledLinked, ServerVerified, VerifyParams},
    };
    use directories::BaseDirs;
    use parameterized::parameterized;
    use std::{path::PathBuf, sync::Arc};

    fn get_workspace_dir() -> PathBuf {
        PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
    }

    #[parameterized(obelisk_toml = {
    "obelisk-local.toml", "obelisk.toml"
})]
    #[parameterized_macro(tokio::test(flavor = "multi_thread"))] // for WASM component compilation
    async fn server_verify(obelisk_toml: &str) -> Result<(), anyhow::Error> {
        let obelisk_toml = get_workspace_dir().join(obelisk_toml);
        let project_dirs = crate::project_dirs();
        let base_dirs = BaseDirs::new();
        let config_holder = ConfigHolder::new(project_dirs, base_dirs, Some(obelisk_toml))?;
        let config = config_holder.load_config().await?;

        let wasm_cache_dir = config
            .wasm_global_config
            .get_wasm_cache_directory(&config_holder.path_prefixes)
            .await?;
        let codegen_cache = config
            .codegen_cache
            .get_directory(&config_holder.path_prefixes)
            .await?;
        tokio::fs::create_dir_all(&wasm_cache_dir).await?;
        let metadata_dir = wasm_cache_dir.join("metadata");
        tokio::fs::create_dir_all(&metadata_dir).await?;
        let (server_verified, _component_source_map) = ServerVerified::new(
            config,
            codegen_cache,
            Arc::from(wasm_cache_dir),
            Arc::from(metadata_dir),
            VerifyParams::default().ignore_missing_env_vars,
            config_holder.path_prefixes,
        )
        .await?;
        let compiled_and_linked = ServerCompiledLinked::new(server_verified).await?;
        assert_eq!(compiled_and_linked.supressed_errors, None);
        Ok(())
    }
}
