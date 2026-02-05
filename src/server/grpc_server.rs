use crate::command::server;
use crate::command::server::ComponentSourceMap;
use crate::command::server::MatchableSourceMap;
use crate::command::server::SubmitError;
use base64::Engine as _;
use base64::prelude::BASE64_STANDARD;
use chrono::DateTime;
use chrono::Utc;
use concepts::ComponentId;
use concepts::ExecutionId;
use concepts::FunctionExtension;
use concepts::FunctionFqn;
use concepts::FunctionMetadata;
use concepts::SupportedFunctionReturnValue;
use concepts::component_id::CONTENT_DIGEST_DUMMY;
use concepts::component_id::InputContentDigest;
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage;
use concepts::storage::BacktraceFilter;
use concepts::storage::CreateRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbErrorGeneric;
use concepts::storage::DbPool;
use concepts::storage::DeploymentState;
use concepts::storage::ExecutionListPagination;
use concepts::storage::ExecutionRequest;
use concepts::storage::LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH;
use concepts::storage::LIST_DEPLOYMENT_STATES_DEFAULT_PAGINATION;
use concepts::storage::ListExecutionsFilter;
use concepts::storage::LogFilter;
use concepts::storage::LogInfoAppendRow;
use concepts::storage::LogLevel;
use concepts::storage::LogStreamType;
use concepts::storage::Pagination;
use concepts::storage::PendingState;
use concepts::storage::Version;
use concepts::storage::VersionType;
use concepts::time::ClockFn;
use concepts::time::Now;
use grpc::TonicRespResult;
use grpc::TonicResult;
use grpc::grpc_gen;
use grpc::grpc_gen::GenerateExecutionIdResponse;
use grpc::grpc_gen::GetStatusResponse;
use grpc::grpc_gen::get_status_response::Message;
use grpc::grpc_mapping::TonicServerOptionExt;
use grpc::grpc_mapping::TonicServerResultExt;
use grpc::grpc_mapping::convert_length;
use grpc::grpc_mapping::db_error_read_to_status;
use grpc::grpc_mapping::db_error_write_to_status;
use grpc::grpc_mapping::from_execution_event_to_grpc;
use grpc_gen::ExecutionSummary;
use serde::Deserialize;
use serde::Serialize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tracing::Instrument;
use tracing::Span;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::instrument;
use val_json::wast_val_ser::deserialize_slice;
use wasm_workers::activity::cancel_registry::CancelRegistry;
use wasm_workers::component_logger::LogStrageConfig;
use wasm_workers::engines::Engines;
use wasm_workers::registry::ComponentConfigRegistryRO;
use wasm_workers::workflow::workflow_worker::WorkflowWorker;

pub(crate) const IGNORING_COMPONENT_DIGEST: InputContentDigest =
    InputContentDigest(CONTENT_DIGEST_DUMMY);

#[derive(derive_more::Debug)]
pub(crate) struct GrpcServer {
    deployment_id: DeploymentId,
    #[debug(skip)]
    db_pool: Arc<dyn DbPool>,
    termination_watcher: watch::Receiver<()>,
    component_registry_ro: ComponentConfigRegistryRO,
    component_source_map: ComponentSourceMap,
    #[debug(skip)]
    cancel_registry: CancelRegistry,
    #[debug(skip)]
    engines: Engines,
    #[debug(skip)]
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
}

impl GrpcServer {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        deployment_id: DeploymentId,
        db_pool: Arc<dyn DbPool>,
        termination_watcher: watch::Receiver<()>,
        component_registry_ro: ComponentConfigRegistryRO,
        component_source_map: ComponentSourceMap,
        cancel_registry: CancelRegistry,
        engines: Engines,
        log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    ) -> Self {
        Self {
            deployment_id,
            db_pool,
            termination_watcher,
            component_registry_ro,
            component_source_map,
            cancel_registry,
            engines,
            log_forwarder_sender,
        }
    }
}

/// Convert gRPC `ListDeploymentStatesRequest` pagination to internal Pagination type.
fn convert_deployment_pagination(
    request: &grpc_gen::ListDeploymentStatesRequest,
) -> Result<Pagination<Option<DeploymentId>>, tonic::Status> {
    use grpc_gen::list_deployment_states_request;

    match request.pagination.as_ref() {
        Some(list_deployment_states_request::Pagination::NewerThan(p)) => {
            Ok(Pagination::NewerThan {
                length: u16::try_from(p.length)
                    .ok()
                    .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
                cursor: p
                    .cursor
                    .as_ref()
                    .map(|c| DeploymentId::try_from(c.clone()))
                    .transpose()?,
                including_cursor: p.including_cursor,
            })
        }
        Some(list_deployment_states_request::Pagination::OlderThan(p)) => {
            Ok(Pagination::OlderThan {
                length: u16::try_from(p.length)
                    .ok()
                    .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
                cursor: p
                    .cursor
                    .as_ref()
                    .map(|c| DeploymentId::try_from(c.clone()))
                    .transpose()?,
                including_cursor: p.including_cursor,
            })
        }
        None => Ok(LIST_DEPLOYMENT_STATES_DEFAULT_PAGINATION),
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
        let ffqn = FunctionFqn::try_from(request.function_name.argument_must_exist("function")?)?;
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;

        // Deserialize params JSON into `Params`
        let params = {
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

        let outcome = server::submit(
            self.deployment_id,
            self.db_pool
                .external_api_conn()
                .await
                .map_err(map_to_status)?
                .as_ref(),
            execution_id,
            ffqn,
            params,
            &self.component_registry_ro,
        )
        .await?;

        let resp = grpc_gen::SubmitResponse {
            outcome: match outcome {
                server::SubmitOutcome::Created => grpc_gen::submit_response::Outcome::Created,
                server::SubmitOutcome::ExistsWithSameParameters => {
                    grpc_gen::submit_response::Outcome::ExistsWithSameParameters
                }
            }
            .into(),
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
        let (parent_execution_id, join_set_id) = execution_id.split_to_parts();
        span.record("execution_id", tracing::field::display(&execution_id));
        // Get FFQN
        let db_connection = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
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

        let return_value = request.return_value.argument_must_exist("return_value")?;
        // Type check `return_value`
        let return_value = {
            let type_wrapper = fn_metadata.return_type.type_wrapper();
            let return_value = match deserialize_slice(&return_value.value, type_wrapper) {
                Ok(wast_val_with_type) => wast_val_with_type,
                Err(err) => {
                    return Err(tonic::Status::invalid_argument(format!(
                        "cannot deserialize return value according to its type - {err}"
                    )));
                }
            };
            SupportedFunctionReturnValue::from_wast_val_with_type(return_value)
                .expect("checked that ffqn is no-ext, return type must be Compatible")
        };
        storage::stub_execution(
            db_connection.as_ref(),
            execution_id,
            parent_execution_id,
            join_set_id,
            created_at,
            return_value,
        )
        .await
        .to_status()?;

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
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
        let current_execution_with_state =
            conn.get_pending_state(&execution_id).await.to_status()?;
        let create_request = conn.get_create_request(&execution_id).await.to_status()?;
        let summary = grpc_gen::GetStatusResponse {
            message: Some(Message::Summary(ExecutionSummary::from(
                current_execution_with_state.clone(),
            ))),
        };
        if current_execution_with_state.pending_state.is_finished() || !request.follow {
            // No waiting in this case
            let output: Self::GetStatusStream = if let PendingState::Finished(finished) =
                current_execution_with_state.pending_state
                && request.send_finished_status
            {
                // Send summary + finished status only if the execution is finished && request.send_finished_status
                let finished_result = conn
                    .get_execution_event(&execution_id, &Version(finished.version))
                    .await
                    .to_status()?;
                let ExecutionRequest::Finished {
                    result: finished_result,
                    ..
                } = finished_result.event
                else {
                    return Err(tonic::Status::internal(
                        "pending state finished implies `Finished` event",
                    ));
                };
                // .expect("checked using `current_pending_state.is_finished()` that the execution is finished");
                let finished_message = grpc_gen::GetStatusResponse {
                    message: Some(Message::FinishedStatus(to_finished_status(
                        finished_result,
                        &create_request,
                        finished.finished_at,
                    ))),
                };
                Box::pin(tokio_stream::iter([Ok(summary), Ok(finished_message)]))
            } else {
                Box::pin(tokio_stream::iter([Ok(summary)]))
            };
            Ok(tonic::Response::new(output))
        } else {
            let (status_stream_sender, remote_client_recv) = mpsc::channel(1);
            // send current pending status
            status_stream_sender
                .send(TonicResult::Ok(summary))
                .await
                .expect("mpsc bounded channel requires buffer > 0");
            let db_pool = self.db_pool.clone();
            let termination_watcher = self.termination_watcher.clone();
            let trace_id = server::gen_trace_id();
            let span = info_span!("poll_status", trace_id);
            tokio::spawn(
                async move {
                    debug!("poll_status started");
                    poll_status(
                        db_pool,
                        termination_watcher,
                        execution_id,
                        status_stream_sender,
                        current_execution_with_state.pending_state,
                        create_request,
                        request.send_finished_status,
                    )
                    .await;
                    debug!("poll_status finished");
                }
                .instrument(span),
            );
            Ok(tonic::Response::new(
                Box::pin(ReceiverStream::new(remote_client_recv)) as Self::GetStatusStream,
            ))
        }
    }

    #[instrument(skip_all, fields(ffqn))]
    async fn list_executions(
        &self,
        request: tonic::Request<grpc_gen::ListExecutionsRequest>,
    ) -> TonicRespResult<grpc_gen::ListExecutionsResponse> {
        let request = request.into_inner();

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

        let filter = ListExecutionsFilter {
            show_derived: !request.top_level_only,
            hide_finished: request.hide_finished,
            execution_id_prefix: request.execution_id_prefix,
            ffqn_prefix: request.function_name_prefix,
            component_digest: request
                .component_digest
                .map(InputContentDigest::try_from)
                .transpose()?,
            deployment_id: request
                .deployment_id
                .map(DeploymentId::try_from)
                .transpose()?,
        };
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
        let executions: Vec<_> = conn
            .list_executions(filter, ExecutionListPagination::try_from(pagination)?)
            .await
            .to_status()?
            .into_iter()
            .map(grpc_gen::ExecutionSummary::from)
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

        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
        let result = conn
            .list_execution_events(
                &execution_id,
                &Version::new(request.version_from),
                VersionType::try_from(request.length).map_err(|_| {
                    tonic::Status::invalid_argument("`length` must be u16".to_string())
                })?,
                request.include_backtrace_id,
            )
            .await
            .to_status()?;

        let events = result
            .events
            .into_iter()
            .map(from_execution_event_to_grpc)
            .collect();
        Ok(tonic::Response::new(
            grpc_gen::ListExecutionEventsResponse {
                events,
                max_version: result.max_version.into(),
            },
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
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
        let result = conn
            .list_responses(
                &execution_id,
                concepts::storage::Pagination::NewerThan {
                    length: convert_length(request.length)?,
                    cursor: request.cursor_from,
                    including_cursor: request.including_cursor,
                },
            )
            .await
            .to_status()?;

        let responses = result
            .responses
            .into_iter()
            .map(grpc_gen::ResponseWithCursor::from)
            .collect();

        Ok(tonic::Response::new(grpc_gen::ListResponsesResponse {
            responses,
            max_cursor: result.max_cursor.into(),
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

        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;

        let res = conn
            .list_execution_events_responses(
                &execution_id,
                &Version::new(request.version_from),
                VersionType::try_from(request.events_length).map_err(|_| {
                    tonic::Status::invalid_argument("`events_length` must be u16".to_string())
                })?,
                request.include_backtrace_id,
                concepts::storage::Pagination::NewerThan {
                    length: convert_length(request.responses_length)?,
                    cursor: request.responses_cursor_from,
                    including_cursor: request.responses_including_cursor,
                },
            )
            .await
            .to_status()?;

        let events = res
            .events
            .into_iter()
            .map(from_execution_event_to_grpc)
            .collect();

        let responses = res
            .responses
            .into_iter()
            .map(grpc_gen::ResponseWithCursor::from)
            .collect();

        let current_status = grpc_gen::ExecutionStatus::from(&res.execution_with_state);

        Ok(tonic::Response::new(
            grpc_gen::ListExecutionEventsAndResponsesResponse {
                events,
                responses,
                current_status: Some(current_status),
                max_version: res.max_version.0,
                max_cursor: res.max_cursor.0,
            },
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

        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
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
                version_min_including: backtrace_info.version_min_including.0,
                version_max_excluding: backtrace_info.version_max_excluding.0,
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
        async fn find_in_source_map(
            matchable_source_map: &MatchableSourceMap,
            component_id: &ComponentId,
            file: &str,
        ) -> Result<tonic::Response<grpc_gen::GetBacktraceSourceResponse>, tonic::Status> {
            if let Some(actual_path) = matchable_source_map.find_matching(file) {
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
                debug!("Backtrace file mapping not found for {component_id}, src {file}");
                Err(tonic::Status::not_found("backtrace file mapping not found"))
            }
        }

        let request = request.into_inner();
        let component_id =
            ComponentId::try_from(request.component_id.argument_must_exist("component_id")?)?;
        if let Some(matchable_source_map) = self.component_source_map.get(&component_id) {
            find_in_source_map(matchable_source_map, &component_id, &request.file).await
        } else {
            // Disregard input digest
            let mut component_id = component_id;
            component_id.input_digest = IGNORING_COMPONENT_DIGEST;
            if let Some(matchable_source_map) = self.component_source_map.get(&component_id) {
                find_in_source_map(matchable_source_map, &component_id, &request.file).await
            } else {
                debug!("Component {component_id} not found");
                Err(tonic::Status::not_found(format!(
                    "component {component_id} not found in source map"
                )))
            }
        }
    }

    #[instrument(skip_all, fields(execution_id, delay_id))]
    async fn cancel(
        &self,
        request: tonic::Request<grpc_gen::CancelRequest>,
    ) -> std::result::Result<tonic::Response<grpc_gen::CancelResponse>, tonic::Status> {
        let request = request.into_inner();
        let executed_at = Now.now();
        let response_id = request.request.argument_must_exist("request")?;
        let outcome = match response_id {
            grpc_gen::cancel_request::Request::Activity(activity_req) => {
                let child_execution_id = activity_req
                    .execution_id
                    .argument_must_exist("execution_id")?;
                let execution_id = ExecutionId::try_from(child_execution_id)?;
                tracing::Span::current()
                    .record("execution_id", tracing::field::display(&execution_id));

                let conn = self
                    .db_pool
                    .external_api_conn()
                    .await
                    .map_err(map_to_status)?;
                let child_create_req = conn.get_create_request(&execution_id).await.to_status()?;
                if !child_create_req.component_id.component_type.is_activity() {
                    return Err(tonic::Status::invalid_argument(
                        "cancelled execution must be an activity",
                    ));
                }
                self.cancel_registry
                    .cancel(conn.as_ref(), &execution_id, executed_at)
                    .await
                    .to_status()?
            }
            grpc_gen::cancel_request::Request::Delay(delay_req) => {
                let delay_id = delay_req.delay_id.argument_must_exist("delay_id")?;
                let delay_id = DelayId::try_from(delay_id)?;
                tracing::Span::current().record("delay_id", tracing::field::display(&delay_id));

                let conn = self
                    .db_pool
                    .external_api_conn()
                    .await
                    .map_err(map_to_status)?;
                storage::cancel_delay(conn.as_ref(), delay_id, executed_at)
                    .await
                    .to_status()?
            }
        };

        Ok(tonic::Response::new(grpc_gen::CancelResponse {
            outcome: grpc_gen::cancel_response::CancelOutcome::from(outcome).into(),
        }))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn replay_execution(
        &self,
        request: tonic::Request<grpc_gen::ReplayExecutionRequest>,
    ) -> Result<tonic::Response<grpc_gen::ReplayExecutionResponse>, tonic::Status> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let conn = self.db_pool.connection().await.map_err(map_to_status)?;
        // Find the execution's ffqn.
        let create_req = conn.get_create_request(&execution_id).await.to_status()?;

        // Check that ffqn exists
        let (component_id, _fn_metadata) = self
            .component_registry_ro
            .find_by_exported_ffqn_submittable(&create_req.ffqn)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "component for function '{}' not found",
                    create_req.ffqn
                ))
            })?;

        Span::current().record("component_id", tracing::field::display(component_id));

        let (component_id, replay_info) = self
            .component_registry_ro
            .get_workflow_replay_info(&component_id.input_digest)
            .expect("digest taken from found component id");

        let replay_res = WorkflowWorker::replay(
            self.deployment_id,
            component_id.clone(),
            replay_info.runnable_component.wasmtime_component.clone(),
            &replay_info.runnable_component.wasm_component.exim,
            self.engines.workflow_engine.clone(),
            Arc::new(self.component_registry_ro.clone()),
            conn.as_ref(),
            execution_id.clone(),
            replay_info
                .logs_store_min_level
                .map(|min_level| LogStrageConfig {
                    min_level,
                    log_sender: self.log_forwarder_sender.clone(),
                }),
        )
        .await;
        if let Err(err) = replay_res {
            debug!("Replay failed: {err:?}");
            return Err(tonic::Status::internal(format!("replay failed: {err}")));
        }
        Ok(tonic::Response::new(grpc_gen::ReplayExecutionResponse {}))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn upgrade_execution_component(
        &self,
        request: tonic::Request<grpc_gen::UpgradeExecutionComponentRequest>,
    ) -> Result<tonic::Response<grpc_gen::UpgradeExecutionComponentResponse>, tonic::Status> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let old = request
            .expected_component_digest
            .argument_must_exist("expected_component_digest")?
            .try_into()?;
        let new = request
            .new_component_digest
            .argument_must_exist("new_component_digest")?
            .try_into()?;
        if !request.skip_determinism_check {
            let (component_id, replay_info) = self
                .component_registry_ro
                .get_workflow_replay_info(&new)
                .ok_or_else(|| {
                    tonic::Status::not_found(format!("new component '{new}' not found in registry"))
                })?;
            let replay_res = WorkflowWorker::replay(
                self.deployment_id,
                component_id.clone(),
                replay_info.runnable_component.wasmtime_component.clone(),
                &replay_info.runnable_component.wasm_component.exim,
                self.engines.workflow_engine.clone(),
                Arc::new(self.component_registry_ro.clone()),
                self.db_pool
                    .connection()
                    .await
                    .map_err(map_to_status)?
                    .as_ref(),
                execution_id.clone(),
                replay_info
                    .logs_store_min_level
                    .map(|min_level| LogStrageConfig {
                        min_level,
                        log_sender: self.log_forwarder_sender.clone(),
                    }),
            )
            .await;
            if let Err(err) = replay_res {
                debug!("Replay failed: {err:?}");
                return Err(tonic::Status::internal(format!("replay failed: {err}")));
            }
        }

        self.db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?
            .upgrade_execution_component(&execution_id, &old, &new)
            .await
            .to_status()?;
        Ok(tonic::Response::new(
            grpc_gen::UpgradeExecutionComponentResponse {},
        ))
    }

    #[instrument(skip_all, fields(execution_id))]
    async fn list_logs(
        &self,
        request: tonic::Request<grpc_gen::ListLogsRequest>,
    ) -> Result<tonic::Response<grpc_gen::ListLogsResponse>, tonic::Status> {
        let request = request.into_inner();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        let filter = {
            let levels = request
                .levels
                .into_iter()
                .map(|lvl| {
                    grpc_gen::LogLevel::try_from(lvl)
                        .map_err(|err| {
                            debug!("Cannot convert level {err:?}");
                            tonic::Status::invalid_argument(format!("unknown level filter: {lvl}"))
                        })
                        .and_then(|lvl| {
                            LogLevel::try_from(lvl).map_err(|err| {
                                debug!("Cannot convert level {err:?}");
                                tonic::Status::invalid_argument(
                                    "unspecified level filter cannot be used here",
                                )
                            })
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let stream_types = request
                .stream_types
                .into_iter()
                .map(|st| {
                    grpc_gen::LogStreamType::try_from(st)
                        .map_err(|err| {
                            debug!("Cannot convert stream type {err:?}");
                            tonic::Status::invalid_argument(format!(
                                "unknown stream type filter: {st}"
                            ))
                        })
                        .and_then(|st| {
                            LogStreamType::try_from(st).map_err(|err| {
                                debug!("Cannot convert stream type {err:?}");
                                tonic::Status::invalid_argument(
                                    "unspecified stream type filter cannot be used here",
                                )
                            })
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            match (request.show_logs, request.show_streams) {
                (true, true) => LogFilter::show_combined(levels, stream_types),
                (true, false) => LogFilter::show_logs(levels),
                (false, true) => LogFilter::show_streams(stream_types),
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "at least one of `show_logs`, `show_streams` must be set",
                    ));
                }
            }
        };

        let length = if let Ok(page_size) = u16::try_from(request.page_size)
            && page_size > 0
            && page_size <= 200
        {
            page_size
        } else {
            20
        };
        let pagination = if let Ok(decoded) = BASE64_STANDARD.decode(&request.page_token)
            && let Ok(decoded) = serde_json::from_slice::<ListLogsPagination>(&decoded)
        {
            match decoded {
                ListLogsPagination::NewerThan { cursor } => Pagination::NewerThan {
                    length,
                    cursor,
                    including_cursor: false,
                },
                ListLogsPagination::OlderThan { cursor } => Pagination::OlderThan {
                    length,
                    cursor,
                    including_cursor: false,
                },
            }
        } else {
            Pagination::NewerThan {
                length,
                cursor: 0,
                including_cursor: false,
            }
        };

        let resp = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?
            .list_logs(&execution_id, filter, pagination)
            .await
            .to_status()?;

        let to_base64 = |pagination| {
            BASE64_STANDARD.encode(
                serde_json::to_vec(&ListLogsPagination::from(pagination))
                    .expect("no NaNs or custom serialization"),
            )
        };

        Ok(tonic::Response::new(grpc_gen::ListLogsResponse {
            logs: resp
                .items
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
            next_page_token: to_base64(resp.next_page),
            prev_page_token: resp.prev_page.map(to_base64),
        }))
    }

    #[instrument(skip_all, fields(execution_id, delay_id))]
    async fn pause_execution(
        &self,
        request: tonic::Request<grpc_gen::PauseExecutionRequest>,
    ) -> std::result::Result<tonic::Response<grpc_gen::PauseExecutionResponse>, tonic::Status> {
        let request = request.into_inner();
        let executed_at = Now.now();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        self.db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?
            .pause_execution(&execution_id, executed_at)
            .await
            .to_status()?;
        Ok(tonic::Response::new(grpc_gen::PauseExecutionResponse {}))
    }

    #[instrument(skip_all, fields(execution_id, delay_id))]
    async fn unpause_execution(
        &self,
        request: tonic::Request<grpc_gen::UnpauseExecutionRequest>,
    ) -> std::result::Result<tonic::Response<grpc_gen::UnpauseExecutionResponse>, tonic::Status>
    {
        let request = request.into_inner();
        let executed_at = Now.now();
        let execution_id: ExecutionId = request
            .execution_id
            .argument_must_exist("execution_id")?
            .try_into()?;
        tracing::Span::current().record("execution_id", tracing::field::display(&execution_id));
        self.db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?
            .unpause_execution(&execution_id, executed_at)
            .await
            .to_status()?;
        Ok(tonic::Response::new(grpc_gen::UnpauseExecutionResponse {}))
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum ListLogsPagination {
    NewerThan { cursor: u32 },
    OlderThan { cursor: u32 },
}
impl From<Pagination<u32>> for ListLogsPagination {
    fn from(value: Pagination<u32>) -> Self {
        match value {
            Pagination::NewerThan { cursor, .. } => ListLogsPagination::NewerThan { cursor },
            Pagination::OlderThan { cursor, .. } => ListLogsPagination::OlderThan { cursor },
        }
    }
}

const GET_STATUS_POLLING_SLEEP: Duration = Duration::from_millis(200);

pub(crate) async fn poll_status(
    db_pool: Arc<dyn DbPool>,
    mut termination_watcher: watch::Receiver<()>,
    execution_id: ExecutionId,
    status_stream_sender: mpsc::Sender<TonicResult<GetStatusResponse>>,
    mut old_pending_state: PendingState,
    create_request: CreateRequest,
    send_finished_status: bool,
) {
    let conn = match db_pool.connection().await {
        Ok(conn) => conn,
        Err(err) => {
            debug!("Failed to acquire db connection for poll_status: {err:?}");
            return;
        }
    };
    loop {
        select! {
            res = async {
                tokio::time::sleep(GET_STATUS_POLLING_SLEEP).await;
                notify_status(conn.as_ref(), &execution_id, &status_stream_sender, old_pending_state, &create_request, send_finished_status).await
            } => {
                match res {
                    Ok(new_state) => {
                        old_pending_state = new_state;
                    }
                    Err(()) => return
                }
            }
            _ = termination_watcher.changed() => {
                debug!("Shutdown requested");
                let _ = status_stream_sender
                    .send(TonicResult::Err(tonic::Status::aborted(
                        "server is shutting down",
                    )))
                    .await;
                return;
            }
        }
        if status_stream_sender.is_closed() {
            debug!("Connection was closed by the client");
            return;
        }
    }
}
async fn notify_status(
    conn: &dyn DbConnection,
    execution_id: &ExecutionId,
    status_stream_sender: &mpsc::Sender<TonicResult<GetStatusResponse>>,
    old_pending_state: PendingState,
    create_request: &CreateRequest,
    send_finished_status: bool,
) -> Result<PendingState, ()> {
    let pending_state = conn.get_pending_state(execution_id).await;
    match pending_state {
        Ok(execution_with_state) => {
            if execution_with_state.pending_state != old_pending_state {
                let grpc_pending_status = grpc_gen::ExecutionStatus::from(&execution_with_state);

                let message = grpc_gen::GetStatusResponse {
                    message: Some(Message::CurrentStatus(grpc_pending_status)),
                };
                let send_res = status_stream_sender.send(TonicResult::Ok(message)).await;
                if let Err(err) = send_res {
                    info!("Cannot send the message - {err:?}");
                    return Err(());
                }
                if let PendingState::Finished(pending_state_finished) =
                    execution_with_state.pending_state
                {
                    if send_finished_status {
                        // Send the last message and close the RPC.
                        let finished_result = conn
                            .get_execution_event(
                                execution_id,
                                &Version(pending_state_finished.version),
                            )
                            .await
                            .to_status()
                            .and_then(|event| match event.event {
                                ExecutionRequest::Finished { result, .. } => Ok(result),
                                _ => Err(tonic::Status::internal(
                                    "pending state finished implies `Finished` event",
                                )),
                            });

                        let finished_result = match finished_result {
                            Ok(finished_result) => finished_result,
                            Err(err) => {
                                let _ = status_stream_sender.send(Err(err)).await;
                                return Err(());
                            }
                        };
                        let message = grpc_gen::GetStatusResponse {
                            message: Some(Message::FinishedStatus(to_finished_status(
                                finished_result,
                                create_request,
                                pending_state_finished.finished_at,
                            ))),
                        };
                        let send_res = status_stream_sender.send(TonicResult::Ok(message)).await;
                        if let Err(err) = send_res {
                            error!("Cannot send the final message - {err:?}");
                        }
                    }
                    return Err(());
                }
            }
            Ok(execution_with_state.pending_state)
        }
        Err(db_err) => {
            let _ = status_stream_sender
                .send(Err(db_error_read_to_status(&db_err)))
                .await;
            Err(())
        }
    }
}

#[expect(clippy::needless_pass_by_value)]
fn map_to_status(err: DbErrorGeneric) -> tonic::Status {
    tonic::Status::internal(err.to_string())
}

pub(crate) fn to_finished_status(
    finished_result: SupportedFunctionReturnValue,
    create_request: &CreateRequest,
    finished_at: DateTime<Utc>,
) -> grpc_gen::FinishedStatus {
    let result_detail = finished_result.into();
    grpc_gen::FinishedStatus {
        value: Some(result_detail),
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
        let all_components = self.component_registry_ro.list(request.extensions);
        let component_digest = request
            .component_digest
            .map(InputContentDigest::try_from)
            .transpose()?;
        let mut res_components = Vec::with_capacity(all_components.len());
        for component in all_components
            .into_iter()
            .filter(|component| match &component_digest {
                None => true,
                Some(filter) if *filter == component.component_id.input_digest => true,
                Some(_) => false,
            })
        {
            let res_component = grpc_gen::Component {
                component_id: Some(component.component_id.into()),
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
        let component_digest = InputContentDigest::try_from(
            request
                .component_digest
                .argument_must_exist("component_digest")?,
        )?;
        let wit = self
            .component_registry_ro
            .get_wit(&component_digest)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "WIT not found for component digest '{component_digest}'"
                ))
            })?;
        Ok(tonic::Response::new(grpc_gen::GetWitResponse {
            content: wit.map(ToString::to_string),
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
            return_type: Some(grpc_gen::WitType {
                wit_type: return_type.wit_type().to_string(),
                type_wrapper: serde_json::to_string(&return_type.type_wrapper())
                    .expect("`TypeWrapper` must be serializable"),
            }),
            function_name: Some(ffqn.into()),
            extension: extension.map(|it| {
                match it {
                    FunctionExtension::Submit => grpc_gen::FunctionExtension::Submit,
                    FunctionExtension::AwaitNext => grpc_gen::FunctionExtension::AwaitNext,
                    FunctionExtension::Schedule => grpc_gen::FunctionExtension::Schedule,
                    FunctionExtension::Stub => grpc_gen::FunctionExtension::Stub,
                    FunctionExtension::Get => grpc_gen::FunctionExtension::Get,
                }
                .into()
            }),
            submittable,
        };
        vec.push(fun);
    }
    vec
}

impl From<SubmitError> for tonic::Status {
    fn from(value: SubmitError) -> Self {
        match value {
            SubmitError::ExecutionIdMustBeTopLevel => tonic::Status::invalid_argument(
                "argument `execution_id` must be a top-level execution id",
            ),
            SubmitError::FunctionNotFound => tonic::Status::not_found("function not found"),
            SubmitError::ParamsInvalid(reason) => tonic::Status::invalid_argument(reason),
            err @ SubmitError::Conflict => tonic::Status::already_exists(err.to_string()),
            SubmitError::DbErrorWrite(db_err) => db_error_write_to_status(&db_err),
        }
    }
}

#[tonic::async_trait]
impl grpc_gen::deployment_repository_server::DeploymentRepository for GrpcServer {
    #[instrument(skip_all, fields(execution_id, ffqn, params, component_id))]
    async fn list_deployment_states(
        &self,
        request: tonic::Request<grpc_gen::ListDeploymentStatesRequest>,
    ) -> TonicRespResult<grpc_gen::ListDeploymentStatesResponse> {
        let request = request.into_inner();
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;

        let pagination = convert_deployment_pagination(&request)?;

        let mut states = conn
            .list_deployment_states(Utc::now(), pagination)
            .await
            .to_status()?;

        if crate::server::should_add_current_deployment(&pagination, self.deployment_id, &states) {
            states.insert(0, DeploymentState::new(self.deployment_id));
        }

        let deployments: Vec<_> = states
            .into_iter()
            .map(|dep| grpc_gen::DeploymentState {
                deployment_id: Some(dep.deployment_id.into()),
                current: dep.deployment_id == self.deployment_id,
                locked: dep.locked,
                pending: dep.pending,
                scheduled: dep.scheduled,
                blocked: dep.blocked,
                finished: dep.finished,
            })
            .collect();
        Ok(tonic::Response::new(
            grpc_gen::ListDeploymentStatesResponse { deployments },
        ))
    }

    #[instrument(skip_all, fields(execution_id, ffqn, params, component_id))]
    async fn get_current_deployment_id(
        &self,
        _request: tonic::Request<grpc_gen::GetCurrentDeploymentIdRequest>,
    ) -> TonicRespResult<grpc_gen::GetCurrentDeploymentIdResponse> {
        Ok(tonic::Response::new(
            grpc_gen::GetCurrentDeploymentIdResponse {
                deployment_id: Some(self.deployment_id.into()),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc_gen::list_deployment_states_request::{NewerThan, OlderThan};

    #[test]
    fn test_convert_deployment_pagination_newer_than() {
        // Regression test: NewerThan gRPC request must map to Pagination::NewerThan
        // Previously there was a copy-paste bug that mapped NewerThan to OlderThan
        let deployment_id = DeploymentId::generate();
        let request = grpc_gen::ListDeploymentStatesRequest {
            pagination: Some(
                grpc_gen::list_deployment_states_request::Pagination::NewerThan(NewerThan {
                    length: 20,
                    cursor: Some(deployment_id.into()),
                    including_cursor: true,
                }),
            ),
        };

        let pagination = convert_deployment_pagination(&request).unwrap();

        match pagination {
            Pagination::NewerThan {
                length,
                cursor,
                including_cursor,
            } => {
                assert_eq!(length, 20);
                assert_eq!(cursor, Some(deployment_id));
                assert!(including_cursor);
            }
            Pagination::OlderThan { .. } => {
                panic!("NewerThan request was incorrectly converted to OlderThan pagination")
            }
        }
    }

    #[test]
    fn test_convert_deployment_pagination_older_than() {
        let deployment_id = DeploymentId::generate();
        let request = grpc_gen::ListDeploymentStatesRequest {
            pagination: Some(
                grpc_gen::list_deployment_states_request::Pagination::OlderThan(OlderThan {
                    length: 15,
                    cursor: Some(deployment_id.into()),
                    including_cursor: false,
                }),
            ),
        };

        let pagination = convert_deployment_pagination(&request).unwrap();

        match pagination {
            Pagination::OlderThan {
                length,
                cursor,
                including_cursor,
            } => {
                assert_eq!(length, 15);
                assert_eq!(cursor, Some(deployment_id));
                assert!(!including_cursor);
            }
            Pagination::NewerThan { .. } => {
                panic!("OlderThan request was incorrectly converted to NewerThan pagination")
            }
        }
    }

    #[test]
    fn test_convert_deployment_pagination_none_defaults_to_older_than() {
        let request = grpc_gen::ListDeploymentStatesRequest { pagination: None };

        let pagination = convert_deployment_pagination(&request).unwrap();

        assert_eq!(pagination, LIST_DEPLOYMENT_STATES_DEFAULT_PAGINATION);
        match pagination {
            Pagination::OlderThan {
                cursor,
                including_cursor,
                ..
            } => {
                assert_eq!(cursor, None);
                assert!(!including_cursor);
            }
            Pagination::NewerThan { .. } => panic!("Default pagination should be OlderThan"),
        }
    }

    #[test]
    fn test_convert_deployment_pagination_newer_than_no_cursor() {
        let request = grpc_gen::ListDeploymentStatesRequest {
            pagination: Some(
                grpc_gen::list_deployment_states_request::Pagination::NewerThan(NewerThan {
                    length: 10,
                    cursor: None,
                    including_cursor: false,
                }),
            ),
        };

        let pagination = convert_deployment_pagination(&request).unwrap();

        match pagination {
            Pagination::NewerThan {
                length,
                cursor,
                including_cursor,
            } => {
                assert_eq!(length, 10);
                assert_eq!(cursor, None);
                assert!(!including_cursor);
            }
            Pagination::OlderThan { .. } => {
                panic!("NewerThan request was incorrectly converted to OlderThan pagination")
            }
        }
    }
}
