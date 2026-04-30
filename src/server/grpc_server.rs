use crate::command::server;
use crate::command::server::DeploymentContextHandle;
use crate::command::server::PreparedDirs;
use crate::command::server::ServerVerified;
use crate::command::server::SubmitError;
use crate::command::server::SwitchDeploymentAction;
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
use concepts::component_id::ComponentDigest;
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage;
use concepts::storage::BacktraceFilter;
use concepts::storage::CreateRequest;
use concepts::storage::DbConnection;
use concepts::storage::DbErrorGeneric;
use concepts::storage::DbPool;
use concepts::storage::DeploymentState;
use concepts::storage::DeploymentStatus;
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
use wasm_workers::webhook::webhook_registry::WebhookRegistry;
use wasm_workers::workflow::workflow_js_worker::WorkflowJsWorker;
use wasm_workers::workflow::workflow_worker::WorkflowWorker;

pub(crate) struct GrpcServer {
    server_verified: ServerVerified,
    db_pool: Arc<dyn DbPool>,
    termination_watcher: watch::Receiver<()>,
    cancel_registry: CancelRegistry,
    engines: Engines,
    prepared_dirs: PreparedDirs,
    log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
    // config: ServerConfigToml,
    // path_prefixes: Arc<crate::config::config_holder::PathPrefixes>,
    deployment_ctx: DeploymentContextHandle,
    webhook_registry: Arc<WebhookRegistry>,
}

impl GrpcServer {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        server_verified: ServerVerified,
        db_pool: Arc<dyn DbPool>,
        termination_watcher: watch::Receiver<()>,
        cancel_registry: CancelRegistry,
        engines: Engines,
        prepared_dirs: PreparedDirs,
        log_forwarder_sender: mpsc::Sender<LogInfoAppendRow>,
        deployment_ctx: DeploymentContextHandle,
        webhook_registry: Arc<WebhookRegistry>,
    ) -> Self {
        Self {
            server_verified,
            db_pool,
            termination_watcher,
            cancel_registry,
            engines,
            prepared_dirs,
            log_forwarder_sender,
            deployment_ctx,
            webhook_registry,
        }
    }
}

/// Convert gRPC `ListDeploymentsRequest` pagination to internal Pagination type.
fn convert_deployment_pagination(
    request: &grpc_gen::ListDeploymentsRequest,
) -> Result<Pagination<Option<DeploymentId>>, tonic::Status> {
    use grpc_gen::list_deployments_request;

    match request.pagination.as_ref() {
        Some(list_deployments_request::Pagination::NewerThan(p)) => Ok(Pagination::NewerThan {
            length: u16::try_from(p.length)
                .ok()
                .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
            cursor: p
                .cursor
                .as_ref()
                .map(|c| DeploymentId::try_from(c.clone()))
                .transpose()?,
            including_cursor: p.including_cursor,
        }),
        Some(list_deployments_request::Pagination::OlderThan(p)) => Ok(Pagination::OlderThan {
            length: u16::try_from(p.length)
                .ok()
                .unwrap_or(LIST_DEPLOYMENT_STATES_DEFAULT_LENGTH),
            cursor: p
                .cursor
                .as_ref()
                .map(|c| DeploymentId::try_from(c.clone()))
                .transpose()?,
            including_cursor: p.including_cursor,
        }),
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

        let (deployment_id, component_registry_ro) = {
            let ctx = self.deployment_ctx.read().await;
            (ctx.deployment_id, ctx.component_registry_ro.clone())
        };
        let outcome = server::submit(
            deployment_id,
            self.db_pool
                .external_api_conn()
                .await
                .map_err(map_to_status)?
                .as_ref(),
            execution_id,
            ffqn,
            params,
            request.paused,
            &component_registry_ro,
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
        let component_registry_ro = {
            let ctx = self.deployment_ctx.read().await;
            ctx.component_registry_ro.clone()
        };
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
            component_registry_ro.find_by_exported_ffqn_stub(&ffqn)
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
                    retval: finished_result,
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
                .map(ComponentDigest::try_from)
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
                Pagination::NewerThan {
                    length: u16::try_from(request.length).map_err(|_| {
                        tonic::Status::invalid_argument("`length` must be u16".to_string())
                    })?,
                    cursor: request.version_from,
                    including_cursor: true,
                },
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
        let request = request.into_inner();
        let component_id =
            ComponentId::try_from(request.component_id.argument_must_exist("component_id")?)?;
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(|err| tonic::Status::internal(err.to_string()))?;
        match conn
            .get_source_file(&component_id.component_digest, &request.file)
            .await
        {
            Ok(Some(content)) => Ok(tonic::Response::new(grpc_gen::GetBacktraceSourceResponse {
                content,
            })),
            Ok(None) => {
                debug!(
                    "Backtrace source not found for {component_id}, file {}",
                    request.file
                );
                Err(tonic::Status::not_found("backtrace source not found"))
            }
            Err(err) => {
                error!(%component_id, "Cannot fetch backtrace source from DB: {err:?}");
                Err(tonic::Status::internal("cannot fetch source file"))
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
                    .cancel_activity(conn.as_ref(), &execution_id, executed_at)
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
        let (deployment_id, component_registry_ro) = {
            let ctx = self.deployment_ctx.read().await;
            (ctx.deployment_id, ctx.component_registry_ro.clone())
        };
        let conn = self.db_pool.connection().await.map_err(map_to_status)?;
        // Find the execution's ffqn.
        let create_req = conn.get_create_request(&execution_id).await.to_status()?;

        // Check that ffqn exists
        let (component_id, _fn_metadata) = component_registry_ro
            .find_by_exported_ffqn_submittable(&create_req.ffqn)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "component for function '{}' not found",
                    create_req.ffqn
                ))
            })?;

        Span::current().record("component_id", tracing::field::display(component_id));

        let (component_id, replay_info) = component_registry_ro
            .get_workflow_replay_info(&component_id.component_digest)
            .expect("digest taken from found component id");

        let logs_storage_config =
            replay_info
                .logs_store_min_level
                .map(|min_level| LogStrageConfig {
                    min_level,
                    log_sender: self.log_forwarder_sender.clone(),
                });

        let replay_res = if let Some(js_info) = &replay_info.js_workflow_info {
            WorkflowJsWorker::replay(
                deployment_id,
                component_id.clone(),
                replay_info.runnable_component.wasmtime_component.clone(),
                &replay_info.runnable_component.wasm_component.exim,
                self.engines.workflow_engine.clone(),
                Arc::new(component_registry_ro.clone()),
                conn.as_ref(),
                execution_id.clone(),
                logs_storage_config,
                js_info.js_source.clone(),
            )
            .await
        } else {
            WorkflowWorker::replay(
                deployment_id,
                component_id.clone(),
                replay_info.runnable_component.wasmtime_component.clone(),
                &replay_info.runnable_component.wasm_component.exim,
                self.engines.workflow_engine.clone(),
                Arc::new(component_registry_ro.clone()),
                conn.as_ref(),
                execution_id.clone(),
                logs_storage_config,
            )
            .await
        };
        if let Err(err) = replay_res {
            info!("Replay failed: {err:?}");
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
            let (deployment_id, component_registry_ro) = {
                let ctx = self.deployment_ctx.read().await;
                (ctx.deployment_id, ctx.component_registry_ro.clone())
            };
            let (component_id, replay_info) = component_registry_ro
                .get_workflow_replay_info(&new)
                .ok_or_else(|| {
                    tonic::Status::not_found(format!("new component '{new}' not found in registry"))
                })?;

            let logs_storage_config =
                replay_info
                    .logs_store_min_level
                    .map(|min_level| LogStrageConfig {
                        min_level,
                        log_sender: self.log_forwarder_sender.clone(),
                    });
            let conn = self.db_pool.connection().await.map_err(map_to_status)?;

            let replay_res = if let Some(js_info) = &replay_info.js_workflow_info {
                WorkflowJsWorker::replay(
                    deployment_id,
                    component_id.clone(),
                    replay_info.runnable_component.wasmtime_component.clone(),
                    &replay_info.runnable_component.wasm_component.exim,
                    self.engines.workflow_engine.clone(),
                    Arc::new(component_registry_ro.clone()),
                    conn.as_ref(),
                    execution_id.clone(),
                    logs_storage_config,
                    js_info.js_source.clone(),
                )
                .await
            } else {
                WorkflowWorker::replay(
                    deployment_id,
                    component_id.clone(),
                    replay_info.runnable_component.wasmtime_component.clone(),
                    &replay_info.runnable_component.wasm_component.exim,
                    self.engines.workflow_engine.clone(),
                    Arc::new(component_registry_ro.clone()),
                    conn.as_ref(),
                    execution_id.clone(),
                    logs_storage_config,
                )
                .await
            };
            if let Err(err) = replay_res {
                info!("Replay failed: {err:?}");
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
        let show_derived = request.show_derived;
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
                cursor: DateTime::<Utc>::UNIX_EPOCH,
                including_cursor: false,
            }
        };

        let resp = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?
            .list_logs(&execution_id, show_derived, filter, pagination)
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
    NewerThan { cursor: DateTime<Utc> },
    OlderThan { cursor: DateTime<Utc> },
}
impl From<Pagination<DateTime<Utc>>> for ListLogsPagination {
    fn from(value: Pagination<DateTime<Utc>>) -> Self {
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
                                ExecutionRequest::Finished { retval: result, .. } => Ok(result),
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
        let component_registry_ro = self
            .deployment_ctx
            .read()
            .await
            .component_registry_ro
            .clone();
        let mut components = component_registry_ro.list(request.extensions);

        if let Some(digest) = request
            .component_digest
            .map(ComponentDigest::try_from)
            .transpose()?
        {
            components.retain(|c| c.component_id.component_digest == digest);
        }
        if let Some(ffqn) = request
            .function_name
            .map(FunctionFqn::try_from)
            .transpose()?
        {
            components.retain(|c| {
                c.workflow_or_activity_config.as_ref().is_some_and(|exp| {
                    exp.exports_ext
                        .iter()
                        .find(|fn_meta| fn_meta.ffqn == ffqn)
                        .is_some()
                })
            });
        }

        let mut grpc_components = Vec::with_capacity(components.len());
        for component in components {
            // Transform to gRPC Component.
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
            grpc_components.push(res_component);
        }
        Ok(tonic::Response::new(grpc_gen::ListComponentsResponse {
            components: grpc_components,
        }))
    }

    async fn get_wit(
        &self,
        request: tonic::Request<grpc_gen::GetWitRequest>,
    ) -> TonicRespResult<grpc_gen::GetWitResponse> {
        let request = request.into_inner();
        let component_digest = ComponentDigest::try_from(
            request
                .component_digest
                .argument_must_exist("component_digest")?,
        )?;
        let component_registry_ro = self
            .deployment_ctx
            .read()
            .await
            .component_registry_ro
            .clone();
        let wit = component_registry_ro
            .get_wit(&component_digest)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "WIT not found for component digest '{component_digest}'"
                ))
            })?;
        Ok(tonic::Response::new(grpc_gen::GetWitResponse {
            content: Some(wit.to_string()),
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
                        wit_type_inline: param.type_wrapper.to_string(),
                    }),
                })
                .collect(),
            return_type: Some(grpc_gen::WitType {
                wit_type: return_type.wit_type().to_string(),
                type_wrapper: serde_json::to_string(&return_type.type_wrapper())
                    .expect("`TypeWrapper` must be serializable"),
                wit_type_inline: return_type.type_wrapper().to_string(),
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
    async fn list_deployments(
        &self,
        request: tonic::Request<grpc_gen::ListDeploymentsRequest>,
    ) -> TonicRespResult<grpc_gen::ListDeploymentsResponse> {
        let request = request.into_inner();
        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;

        let include_config_json = request.include_config_json;
        let pagination = convert_deployment_pagination(&request)?;

        let summaries = conn
            .list_deployment_states(Utc::now(), pagination, include_config_json)
            .await
            .to_status()?;

        let deployments: Vec<_> = summaries
            .into_iter()
            .map(deployment_summary_to_grpc)
            .collect();
        Ok(tonic::Response::new(grpc_gen::ListDeploymentsResponse {
            deployments,
        }))
    }

    #[instrument(skip_all, fields(execution_id, ffqn, params, component_id))]
    async fn get_current_deployment_id(
        &self,
        _request: tonic::Request<grpc_gen::GetCurrentDeploymentIdRequest>,
    ) -> TonicRespResult<grpc_gen::GetCurrentDeploymentIdResponse> {
        let deployment_id = self.deployment_ctx.read().await.deployment_id;
        Ok(tonic::Response::new(
            grpc_gen::GetCurrentDeploymentIdResponse {
                deployment_id: Some(deployment_id.into()),
            },
        ))
    }

    #[instrument(skip_all, fields(deployment_id))]
    async fn switch_deployment(
        &self,
        request: tonic::Request<grpc_gen::SwitchDeploymentRequest>,
    ) -> TonicRespResult<grpc_gen::SwitchDeploymentResponse> {
        use grpc_gen::switch_deployment_response::Outcome;
        let request = request.into_inner();
        let deployment_id: DeploymentId = request
            .deployment_id
            .argument_must_exist("deployment_id")?
            .try_into()?;
        tracing::Span::current().record("deployment_id", tracing::field::display(&deployment_id));
        let mut termination_watcher = self.termination_watcher.clone();
        let outcome = Box::pin(server::switch_deployment(
            self.server_verified.clone(),
            deployment_id,
            SwitchDeploymentAction::new(request.hot_redeploy, request.verify),
            &self.prepared_dirs,
            self.db_pool.clone(),
            &mut termination_watcher,
            &self.deployment_ctx,
            &self.webhook_registry,
            self.cancel_registry.clone(),
            self.log_forwarder_sender.clone(),
        ))
        .await
        .map_err(|err| match err {
            server::SwitchError::NotFound => tonic::Status::not_found("deployment not found"),
            server::SwitchError::Other(e) => tonic::Status::failed_precondition(format!("{e:#}")),
        })?;
        info!(%deployment_id, "Deployment switch outcome: {outcome}");

        let grpc_outcome = match outcome {
            server::SwitchOutcome::Switched => Outcome::SwitchOutcomeSwitched,
            server::SwitchOutcome::RestartRequired => Outcome::SwitchOutcomeRestartRequired,
        };
        Ok(tonic::Response::new(grpc_gen::SwitchDeploymentResponse {
            outcome: grpc_outcome.into(),
        }))
    }

    #[instrument(skip_all, fields(deployment_id))]
    async fn submit_deployment(
        &self,
        request: tonic::Request<grpc_gen::SubmitDeploymentRequest>,
    ) -> TonicRespResult<grpc_gen::SubmitDeploymentResponse> {
        let request = request.into_inner();
        let mut termination_watcher = self.termination_watcher.clone();
        let result = Box::pin(server::submit_deployment(
            self.server_verified.clone(),
            &request.config_json,
            request.verify,
            request.created_by.clone(),
            &self.prepared_dirs,
            self.db_pool.clone(),
            &mut termination_watcher,
        ))
        .await
        .map_err(|err| tonic::Status::failed_precondition(format!("{err:#}")))?;
        tracing::Span::current().record("deployment_id", tracing::field::display(&result));
        Ok(tonic::Response::new(grpc_gen::SubmitDeploymentResponse {
            deployment_id: Some(result.into()),
        }))
    }

    #[instrument(skip_all, fields(deployment_id))]
    async fn get_deployment(
        &self,
        request: tonic::Request<grpc_gen::GetDeploymentRequest>,
    ) -> TonicRespResult<grpc_gen::GetDeploymentResponse> {
        let request = request.into_inner();
        let deployment_id: DeploymentId = request
            .deployment_id
            .argument_must_exist("deployment_id")?
            .try_into()?;
        tracing::Span::current().record("deployment_id", tracing::field::display(&deployment_id));

        let conn = self
            .db_pool
            .external_api_conn()
            .await
            .map_err(map_to_status)?;
        let record = conn
            .get_deployment(deployment_id)
            .await
            .to_status()?
            .must_exist("deployment")?;

        Ok(tonic::Response::new(grpc_gen::GetDeploymentResponse {
            deployment: Some(deployment_record_to_grpc(record)),
        }))
    }
}

fn status_to_grpc(status: DeploymentStatus) -> grpc_gen::DeploymentStatus {
    match status {
        DeploymentStatus::Inactive => grpc_gen::DeploymentStatus::Inactive,
        DeploymentStatus::Enqueued => grpc_gen::DeploymentStatus::Enqueued,
        DeploymentStatus::Active => grpc_gen::DeploymentStatus::Active,
    }
}

fn deployment_record_to_grpc(record: concepts::storage::DeploymentRecord) -> grpc_gen::Deployment {
    grpc_gen::Deployment {
        deployment_id: Some(record.deployment_id.into()),
        status: status_to_grpc(record.status).into(),
        created_at: Some(prost_wkt_types::Timestamp::from(record.created_at)),
        last_active_at: record.last_active_at.map(prost_wkt_types::Timestamp::from),
        config_json: Some(record.config_json),
    }
}

fn deployment_summary_to_grpc(dep: DeploymentState) -> grpc_gen::DeploymentSummary {
    let deployment = grpc_gen::Deployment {
        deployment_id: Some(dep.deployment_id.into()),
        status: status_to_grpc(dep.status).into(),
        created_at: Some(prost_wkt_types::Timestamp::from(dep.created_at)),
        last_active_at: dep.last_active_at.map(prost_wkt_types::Timestamp::from),
        config_json: dep.config_json,
    };
    grpc_gen::DeploymentSummary {
        deployment: Some(deployment),
        locked: dep.locked,
        pending: dep.pending,
        scheduled: dep.scheduled,
        blocked: dep.blocked,
        finished: dep.finished,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc_gen::list_deployments_request::{NewerThan, OlderThan};

    #[test]
    fn test_convert_deployment_pagination_newer_than() {
        // Regression test: NewerThan gRPC request must map to Pagination::NewerThan
        // Previously there was a copy-paste bug that mapped NewerThan to OlderThan
        let deployment_id = DeploymentId::generate();
        let request = grpc_gen::ListDeploymentsRequest {
            pagination: Some(grpc_gen::list_deployments_request::Pagination::NewerThan(
                NewerThan {
                    length: 20,
                    cursor: Some(deployment_id.into()),
                    including_cursor: true,
                },
            )),
            include_config_json: true,
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
        let request = grpc_gen::ListDeploymentsRequest {
            pagination: Some(grpc_gen::list_deployments_request::Pagination::OlderThan(
                OlderThan {
                    length: 15,
                    cursor: Some(deployment_id.into()),
                    including_cursor: false,
                },
            )),
            include_config_json: true, // TODO test
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
        let request = grpc_gen::ListDeploymentsRequest {
            pagination: None,
            include_config_json: true, // TODO test
        };

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
        let request = grpc_gen::ListDeploymentsRequest {
            pagination: Some(grpc_gen::list_deployments_request::Pagination::NewerThan(
                NewerThan {
                    length: 10,
                    cursor: None,
                    including_cursor: false,
                },
            )),
            include_config_json: true, // TODO test
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
