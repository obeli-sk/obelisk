use super::event_history::{ApplyError, ChildReturnValue, EventCall, EventHistory, HostResource};
use super::host_exports::v2_0_0::obelisk::types::execution as types_execution;
use super::host_exports::v2_0_0::{ClosingStrategy_2_0_0, ScheduleAt_2_0_0};
use super::host_exports::{
    SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_SCHEDULE, SUFFIX_FN_SUBMIT,
    history_event_schedule_at_from_wast_val,
};
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::WasmFileError;
use crate::component_logger::{ComponentLogger, log_activities};
use crate::workflow::host_exports::{SUFFIX_FN_GET, SUFFIX_FN_STUB};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::storage::{self, DbError, DbPool, HistoryEventScheduleAt, Version, WasmBacktrace};
use concepts::storage::{HistoryEvent, JoinSetResponseEvent};
use concepts::time::ClockFn;
use concepts::{
    ClosingStrategy, ComponentId, ExecutionId, FinishedExecutionError, FunctionRegistry,
    IfcFqnName, InvalidNameError, SUFFIX_PKG_EXT, SUFFIX_PKG_SCHEDULE, StrVariant,
    SupportedFunctionReturnValue,
};
use concepts::{FunctionFqn, Params};
use concepts::{JoinSetId, JoinSetKind};
use executor::worker::FatalError;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{Span, error, instrument, trace};
use val_json::wast_val::{WastVal, WastValWithType};
use wasmtime::component::{Linker, Resource, Val};
use wasmtime_wasi::{
    ResourceTable, ResourceTableError, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView,
};
use wasmtime_wasi_io::IoView;

/// Result that is passed from guest to host as an error, must be downcast from anyhow.
#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum WorkflowFunctionError {
    // fatal errors:
    #[error("non deterministic execution: {0}")]
    NondeterminismDetected(String),
    #[error("child execution finished with an execution error: {child_execution_id}")]
    UnhandledChildExecutionError {
        child_execution_id: ExecutionIdDerived,
        root_cause_id: ExecutionIdDerived,
    },
    #[error("error calling imported function {ffqn} - {reason}")]
    ImportedFunctionCallError {
        ffqn: FunctionFqn,
        reason: StrVariant,
        detail: Option<String>,
    },
    #[error("{reason}")]
    JoinSetNameError { reason: String },
    // retriable errors:
    #[error("interrupt requested")]
    InterruptRequested,
    #[error(transparent)]
    DbError(DbError),
}

#[derive(Debug)]
pub(crate) enum WorkerPartialResult {
    FatalError(FatalError, Version),
    // retriable:
    InterruptRequested,
    DbError(DbError),
}

impl WorkflowFunctionError {
    pub(crate) fn into_worker_partial_result(self, version: Version) -> WorkerPartialResult {
        match self {
            WorkflowFunctionError::InterruptRequested => WorkerPartialResult::InterruptRequested,
            WorkflowFunctionError::DbError(db_error) => WorkerPartialResult::DbError(db_error),
            // fatal errors:
            WorkflowFunctionError::NondeterminismDetected(detail) => {
                WorkerPartialResult::FatalError(
                    FatalError::NondeterminismDetected { detail },
                    version,
                )
            }
            WorkflowFunctionError::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            } => WorkerPartialResult::FatalError(
                FatalError::UnhandledChildExecutionError {
                    child_execution_id,
                    root_cause_id,
                },
                version,
            ),
            WorkflowFunctionError::ImportedFunctionCallError {
                ffqn,
                reason,
                detail,
            } => WorkerPartialResult::FatalError(
                FatalError::ImportedFunctionCallError {
                    ffqn,
                    reason,
                    detail,
                },
                version,
            ),
            WorkflowFunctionError::JoinSetNameError { reason } => {
                WorkerPartialResult::FatalError(FatalError::JoinSetNameError { reason }, version)
            }
        }
    }
}

impl From<ApplyError> for WorkflowFunctionError {
    fn from(value: ApplyError) -> Self {
        match value {
            ApplyError::NondeterminismDetected(reason) => Self::NondeterminismDetected(reason),
            ApplyError::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            } => Self::UnhandledChildExecutionError {
                child_execution_id,
                root_cause_id,
            },
            ApplyError::InterruptRequested => Self::InterruptRequested,
            ApplyError::DbError(db_error) => Self::DbError(db_error),
        }
    }
}

pub(crate) struct WorkflowCtx<C: ClockFn> {
    pub(crate) execution_id: ExecutionId,
    event_history: EventHistory,
    rng: StdRng,
    pub(crate) clock_fn: C,
    pub(crate) db_pool: Arc<dyn DbPool>,
    pub(crate) version: Version,
    component_logger: ComponentLogger,
    pub(crate) resource_table: wasmtime::component::ResourceTable,
    backtrace: Option<WasmBacktrace>, // Temporary storage of current backtrace
    backtrace_persist: bool,
    wasi_ctx: WasiCtx,
}

#[derive(derive_more::Debug)]
pub(crate) enum ImportedFnCall<'a> {
    Direct {
        ffqn: FunctionFqn,
        params: &'a [Val],
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Schedule {
        target_ffqn: FunctionFqn,
        schedule_at: HistoryEventScheduleAt,
        #[debug(skip)]
        target_params: &'a [Val],
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    SubmitExecution {
        target_ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        #[debug(skip)]
        target_params: &'a [Val],
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    AwaitNext {
        target_ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Stub {
        target_ffqn: FunctionFqn,
        target_execution_id: ExecutionIdDerived,
        parent_id: ExecutionId,
        join_set_id: JoinSetId,
        #[debug(skip)]
        return_value: wasmtime::component::Val,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Get {
        target_ffqn: FunctionFqn,
        child_execution_id: ExecutionIdDerived,
    },
}

impl<'a> ImportedFnCall<'a> {
    fn extract_join_set_id<'ctx, C: ClockFn>(
        called_ffqn: &FunctionFqn,
        store_ctx: &'ctx mut wasmtime::StoreContextMut<'a, WorkflowCtx<C>>,
        params: &'a [Val],
    ) -> Result<(JoinSetId, &'a [Val]), String> {
        let Some((join_set_id, params)) = params.split_first() else {
            error!("Got empty params, expected JoinSetId");
            return Err(format!(
                "error running {called_ffqn} extension function: exepcted at least one parameter with JoinSetId, got empty parameter list"
            ));
        };
        if let Val::Resource(resource) = join_set_id {
            let resource: Resource<JoinSetId> = resource
                .try_into_resource(&mut *store_ctx)
                .inspect_err(|err| error!("Cannot turn `ResourceAny` into a `Resource` - {err:?}"))
                .map_err(|err| format!("cannot turn `ResourceAny` into a `Resource` - {err:?}"))?;
            let join_set_id = store_ctx
                .data()
                .resource_to_join_set_id(&resource)
                .map_err(|resource_err| {
                    format!("error running {called_ffqn} extension function: {resource_err:?}")
                })?;
            Ok((join_set_id.clone(), params))
        } else {
            error!("Wrong type for JoinSetId, expected join-set-id, got `{join_set_id:?}`");
            Err(format!(
                "wrong type for JoinSetId, expected join-set-id, got `{join_set_id:?}`"
            ))
        }
    }

    #[instrument(skip_all, fields(ffqn = %called_ffqn, otel.name = format!("imported_fn_call {called_ffqn}")), name = "imported_fn_call")]
    pub(crate) fn new<'ctx, C: ClockFn>(
        called_ffqn: FunctionFqn,
        store_ctx: &'ctx mut wasmtime::StoreContextMut<'a, WorkflowCtx<C>>,
        params: &'a [Val],
        backtrace_persist: bool,
    ) -> Result<ImportedFnCall<'a>, WorkflowFunctionError> {
        let wasm_backtrace = if backtrace_persist {
            let wasm_backtrace = wasmtime::WasmBacktrace::capture(&store_ctx);
            concepts::storage::WasmBacktrace::maybe_from(&wasm_backtrace)
        } else {
            None
        };

        if let Some(target_package_name) = called_ffqn.ifc_fqn.package_strip_obelisk_ext_suffix() {
            let target_ifc_fqn = IfcFqnName::from_parts(
                called_ffqn.ifc_fqn.namespace(),
                target_package_name,
                called_ffqn.ifc_fqn.ifc_name(),
                called_ffqn.ifc_fqn.version(),
            );

            if let Some(function_name) = called_ffqn.function_name.strip_suffix(SUFFIX_FN_SUBMIT) {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let (join_set_id, params) =
                    Self::extract_join_set_id(&called_ffqn, store_ctx, params).map_err(
                        |detail| WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: StrVariant::Static("cannot extract join set id"),
                            detail: Some(detail),
                        },
                    )?;
                Ok(ImportedFnCall::SubmitExecution {
                    target_ffqn,
                    join_set_id,
                    target_params: params,
                    wasm_backtrace,
                })
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let (join_set_id, params) =
                    match Self::extract_join_set_id(&called_ffqn, store_ctx, params) {
                        Ok(ok) => ok,
                        Err(detail) => {
                            return Err(WorkflowFunctionError::ImportedFunctionCallError {
                                ffqn: called_ffqn,
                                reason: "cannot extract join set id".into(),
                                detail: Some(detail),
                            });
                        }
                    };
                if !params.is_empty() {
                    return Err(WorkflowFunctionError::ImportedFunctionCallError {
                        reason: StrVariant::Static("wrong parameter length"),
                        detail: Some(format!(
                            "wrong parameter length, expected single string parameter containing join-set-id, got {} other parameters",
                            params.len()
                        )),
                        ffqn: called_ffqn,
                    });
                }
                Ok(ImportedFnCall::AwaitNext {
                    target_ffqn,
                    join_set_id,
                    wasm_backtrace,
                })
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_GET)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );

                let Some((child_execution_id, [])) = params.split_first() else {
                    return Err(WorkflowFunctionError::ImportedFunctionCallError {
                        ffqn: called_ffqn,
                        reason: StrVariant::Static("-get function exepcts one parameter"),
                        detail: Some(format!(
                            "wrong parameter length, expected single parameter of type `execution-id`, got {} parameters",
                            params.len()
                        )),
                    });
                };

                let child_execution_id = match ExecutionId::try_from(child_execution_id) {
                    Ok(ExecutionId::Derived(derived)) => derived,
                    Ok(ExecutionId::TopLevel(_)) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: "first parameter must not be a top-level `execution-id`".into(),
                            detail: None,
                        });
                    }
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: format!(
                                "cannot parse first parameter as a derived `execution-id`: {err}"
                            )
                            .into(),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };

                Ok(ImportedFnCall::Get {
                    target_ffqn,
                    child_execution_id,
                })
            } else {
                error!("Unrecognized `{SUFFIX_PKG_EXT}` interface function {called_ffqn}");
                Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static("unrecognized `-obelisk-ext` interface function"),
                    detail: None,
                })
            }
        } else if let Some(target_package_name) =
            called_ffqn.ifc_fqn.package_strip_obelisk_schedule_suffix()
        {
            let target_ifc_fqn = IfcFqnName::from_parts(
                called_ffqn.ifc_fqn.namespace(),
                target_package_name,
                called_ffqn.ifc_fqn.ifc_name(),
                called_ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = called_ffqn.function_name.strip_suffix(SUFFIX_FN_SCHEDULE)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let Some((schedule_at, params)) = params.split_first() else {
                    return Err(WorkflowFunctionError::ImportedFunctionCallError {
                        ffqn: called_ffqn,
                        reason: StrVariant::Static(
                            "exepcted at least one parameter of type `schedule-at`",
                        ),
                        detail: None,
                    });
                };
                let schedule_at = match WastVal::try_from(schedule_at.clone()) {
                    Ok(ok) => ok,
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: format!(
                                "cannot convert `schedule-at` to internal representation - {err}"
                            )
                            .into(),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };
                let schedule_at = match history_event_schedule_at_from_wast_val(&schedule_at) {
                    Ok(ok) => ok,
                    Err(detail) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: "first parameter type must be `schedule-at`".into(),
                            detail: Some(detail.to_string()),
                        });
                    }
                };

                Ok(ImportedFnCall::Schedule {
                    target_ffqn,
                    schedule_at,
                    target_params: params,
                    wasm_backtrace,
                })
            } else {
                error!("Unrecognized `{SUFFIX_PKG_SCHEDULE}` interface function {called_ffqn}");
                Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static(
                        "unrecognized `-obelisk-schedule` interface function",
                    ),
                    detail: None,
                })
            }
        } else if let Some(target_package_name) =
            called_ffqn.ifc_fqn.package_strip_obelisk_stub_suffix()
        {
            let target_ifc_fqn = IfcFqnName::from_parts(
                called_ffqn.ifc_fqn.namespace(),
                target_package_name,
                called_ffqn.ifc_fqn.ifc_name(),
                called_ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = called_ffqn.function_name.strip_suffix(SUFFIX_FN_STUB) {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );

                let (target_execution_id, return_value) = match params.split_first() {
                    Some((target_execution_id, [return_value])) => {
                        (target_execution_id, return_value.clone())
                    }
                    _ => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: StrVariant::Static("-stub function exepcts two parameters"),
                            detail: None,
                        });
                    }
                };

                let target_execution_id = match ExecutionId::try_from(target_execution_id) {
                    Ok(ExecutionId::Derived(derived)) => derived,
                    Ok(ExecutionId::TopLevel(_)) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: "first parameter must not be a top-level `execution-id`".into(),
                            detail: None,
                        });
                    }
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: format!(
                                "cannot parse first parameter as `execution-id`: {err}"
                            )
                            .into(),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };

                let (parent_id, join_set_id) = match target_execution_id.split_to_parts() {
                    Ok(ok) => ok,
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: format!("cannot split target execution id to parts in `-stub` function - {err}")
                                .into(),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };
                Ok(ImportedFnCall::Stub {
                    target_ffqn,
                    target_execution_id,
                    parent_id,
                    join_set_id,
                    return_value,
                    wasm_backtrace,
                })
            } else {
                error!("Unrecognized `-obelisk-stub` interface function {called_ffqn}");
                Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static("unrecognized `-obelisk-stub` interface function"),
                    detail: None,
                })
            }
        } else {
            Ok(ImportedFnCall::Direct {
                ffqn: called_ffqn,
                params,
                wasm_backtrace,
            })
        }
    }

    fn ffqn(&self) -> &FunctionFqn {
        match self {
            Self::Direct { ffqn, .. }
            | Self::Schedule {
                target_ffqn: ffqn, ..
            }
            | Self::SubmitExecution {
                target_ffqn: ffqn, ..
            }
            | Self::AwaitNext {
                target_ffqn: ffqn, ..
            }
            | Self::Stub {
                target_ffqn: ffqn, ..
            }
            | Self::Get {
                target_ffqn: ffqn, ..
            } => ffqn,
        }
    }
}

impl<C: ClockFn> wasmtime::component::HasData for WorkflowCtx<C> {
    type Data<'a> = &'a mut WorkflowCtx<C>;
}
impl<C: ClockFn> IoView for WorkflowCtx<C> {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }
}
impl<C: ClockFn> WasiView for WorkflowCtx<C> {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

#[derive(Copy, Clone)]
enum WorkflowSupportVersion {
    Version2,
}
impl WorkflowSupportVersion {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Version2 => IFC_FQN_WORKFLOW_SUPPORT_2,
        }
    }
}

const IFC_FQN_WORKFLOW_SUPPORT_2: &str = "obelisk:workflow/workflow-support@2.0.0";

impl<C: ClockFn> WorkflowCtx<C> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        component_id: ComponentId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        seed: u64,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        db_pool: Arc<dyn DbPool>,
        version: Version,
        execution_deadline: DateTime<Utc>,
        worker_span: Span,
        forward_unhandled_child_errors_in_join_set_close: bool,
        backtrace_persist: bool,
    ) -> Self {
        let mut wasi_ctx_builder = WasiCtxBuilder::new();
        wasi_ctx_builder.allow_tcp(false);
        wasi_ctx_builder.allow_udp(false);
        wasi_ctx_builder.insecure_random_seed(0);

        Self {
            execution_id: execution_id.clone(),
            event_history: EventHistory::new(
                execution_id,
                component_id,
                event_history,
                responses,
                join_next_blocking_strategy,
                execution_deadline,
                worker_span.clone(),
                forward_unhandled_child_errors_in_join_set_close,
            ),
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            db_pool,
            version,
            component_logger: ComponentLogger { span: worker_span },
            resource_table: wasmtime::component::ResourceTable::default(),
            backtrace: None,
            backtrace_persist,
            wasi_ctx: wasi_ctx_builder.build(),
        }
    }

    pub(crate) async fn flush(&mut self) -> Result<(), DbError> {
        self.event_history
            .flush(self.db_pool.connection().as_ref(), self.clock_fn.now())
            .await
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(ffqn = %imported_fn_call.ffqn()))]
    pub(crate) async fn call_imported_fn(
        &mut self,
        imported_fn_call: ImportedFnCall<'_>,
        results: &mut [Val],
        called_ffqn: FunctionFqn,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<(), WorkflowFunctionError> {
        let called_at = self.clock_fn.now();
        trace!(?imported_fn_call, %called_at, "call_imported_fn start");
        let val = self
            .imported_fn_to_val(imported_fn_call, fn_registry, called_at, &called_ffqn)
            .await?;

        match (results.len(), val) {
            (0, None) => {}
            (1, Some(val)) => {
                results[0] = val;
            }
            (expected, got) => {
                error!(
                    "Unexpected result length or type, runtime expects {expected}, got: {got:?}",
                );
                return Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static("unexpected result length"),
                    detail: Some(format!("expected {expected}, got: {got:?}")),
                });
            }
        }
        trace!(?results, "call_imported_fn finish");
        Ok(())
    }

    async fn persist_sleep(
        &mut self,
        schedule_at: HistoryEventScheduleAt,
    ) -> Result<(), WorkflowFunctionError> {
        let expires_at_if_new = schedule_at
            .as_date_time(self.clock_fn.now())
            .map_err(|err| {
                const FFQN: FunctionFqn =
                    FunctionFqn::new_static(IFC_FQN_WORKFLOW_SUPPORT_2, "sleep");
                WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: FFQN,
                    reason: "schedule-at conversion error".into(),
                    detail: Some(format!("{err:?}")),
                }
            })?;
        self.event_history
            .apply(
                EventCall::BlockingDelayRequest(self.event_history.next_blocking_delay_request(
                    schedule_at,
                    expires_at_if_new,
                    self.backtrace.take(),
                )),
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await?;
        Ok(())
    }

    // Must be persisted by the caller.
    fn next_u128(&mut self) -> u128 {
        rand::Rng::r#gen(&mut self.rng)
    }

    pub(crate) fn resource_to_join_set_id(
        &self,
        resource: &Resource<JoinSetId>,
    ) -> Result<&JoinSetId, ResourceTableError> {
        self.resource_table
            .get(resource)
            .inspect_err(|err| error!("Cannot get resource - {err:?}"))
    }

    async fn persist_join_set_with_kind(
        &mut self,
        name: String,
        kind: JoinSetKind,
        closing_strategy: ClosingStrategy,
    ) -> Result<Resource<JoinSetId>, JoinSetCreateError> {
        if !self.event_history.join_set_name_exists(&name, kind) {
            let join_set_id = JoinSetId::new(kind, StrVariant::from(name))
                .map_err(JoinSetCreateError::InvalidNameError)?;
            let res = self
                .event_history
                .apply(
                    EventCall::CreateJoinSet {
                        join_set_id,
                        closing_strategy,
                        wasm_backtrace: self.backtrace.take(),
                    },
                    self.db_pool.connection().as_ref(),
                    &mut self.version,
                    self.clock_fn.now(),
                )
                .await
                .map_err(JoinSetCreateError::ApplyError)?;
            let join_set_id = assert_matches!(res,
                ChildReturnValue::HostResource(HostResource::CreateJoinSetResp(join_set_id)) => join_set_id);
            let join_set_id = self
                .resource_table
                .push(join_set_id)
                .map_err(JoinSetCreateError::ResourceTableError)?;
            Ok(join_set_id)
        } else {
            Err(JoinSetCreateError::NameConflict)
        }
    }

    // NB: Caller must not forget to clear `host.backtrace` afterwards.
    // TODO: Either add RAII-style guard or return a tuple.
    fn get_host_maybe_capture_backtrace<'a>(
        caller: &'a mut wasmtime::StoreContextMut<'_, Self>,
    ) -> &'a mut WorkflowCtx<C> {
        let backtrace = if caller.data().backtrace_persist {
            let backtrace = wasmtime::WasmBacktrace::capture(&caller);
            WasmBacktrace::maybe_from(&backtrace)
        } else {
            None
        };
        let host = caller.data_mut();
        host.backtrace = backtrace;
        host
    }

    // Adding host functions using `func_wrap_async` so that we can capture the backtrace.
    pub(crate) fn add_to_linker(
        linker: &mut Linker<Self>,
        stub_wasi: bool,
    ) -> Result<(), WasmFileError> {
        let linking_err = |err: wasmtime::Error| WasmFileError::LinkingError {
            context: StrVariant::Static("linking error"),
            err: err.into(),
        };

        log_activities::obelisk::log::log::add_to_linker::<_, WorkflowCtx<C>>(
            linker,
            |state: &mut Self| state,
        )
        .map_err(linking_err)?;
        // link obelisk:types@2.0.0
        types_execution::add_to_linker::<_, WorkflowCtx<C>>(linker, |state: &mut Self| state)
            .map_err(linking_err)?;

        // link workflow-support 2.0.0
        Self::add_to_linker_workflow_support(linker, WorkflowSupportVersion::Version2)?;
        if stub_wasi {
            super::wasi::add_to_linker_async(linker)?;
        }
        Ok(())
    }

    fn add_to_linker_workflow_support(
        linker: &mut Linker<Self>,
        workflow_support_version: WorkflowSupportVersion,
    ) -> Result<(), WasmFileError> {
        let mut inst_workflow_support = linker
            .instance(workflow_support_version.as_str())
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static(workflow_support_version.as_str()),
                err: err.into(),
            })?;
        inst_workflow_support
            .func_wrap_async(
                "random-u64",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (min, max_exclusive): (u64, u64)| {
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.random_u64(min, max_exclusive).await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function random-u64"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "random-u64-inclusive",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (min, max_inclusive): (u64, u64)| {
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.random_u64_inclusive(min, max_inclusive).await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function random-u64-inclusive"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "random-string",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (min_length, max_length_exclusive): (u16, u16)| {
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.random_string(min_length, max_length_exclusive).await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function random-string"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "submit-delay",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (join_set_id, schedule_at): (Resource<JoinSetId>, ScheduleAt_2_0_0)| {
                    let schedule_at = HistoryEventScheduleAt::from(schedule_at);
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let join_set_id = host.resource_to_join_set_id(&join_set_id)?.clone();
                        let result = host.submit_delay(join_set_id, schedule_at).await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function submit-delay"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "sleep",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (schedule_at,): (ScheduleAt_2_0_0,)| {
                    let schedule_at = HistoryEventScheduleAt::from(schedule_at);
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.sleep(schedule_at).await;
                        host.backtrace = None;
                        result
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function sleep"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "new-join-set-named",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (name, _closing_strategy): (String, ClosingStrategy_2_0_0)| {
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.new_join_set_named(name).await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function new-join-set-named"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "new-join-set-generated",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (_closing_strategy,): (ClosingStrategy_2_0_0,)| {
                    wasmtime::component::__internal::Box::new(async move {
                        let host = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let result = host.new_join_set_generated().await;
                        host.backtrace = None;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function new-join-set-generated"),
                err: err.into(),
            })?;

        Ok(())
    }

    pub(crate) async fn close_opened_join_sets(&mut self) -> Result<(), ApplyError> {
        self.event_history
            .close_opened_join_sets(
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await
    }

    fn next_child_id(&mut self, join_set_id: &JoinSetId) -> ExecutionIdDerived {
        let count = self.event_history.execution_count(join_set_id);
        self.execution_id
            .next_level(join_set_id)
            .get_incremented_by(u64::try_from(count).unwrap())
    }

    async fn apply_event(
        &mut self,
        event: EventCall,
        called_at: DateTime<Utc>,
    ) -> Result<Option<wasmtime::component::Val>, WorkflowFunctionError> {
        Ok(self
            .event_history
            .apply(
                event,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                called_at,
            )
            .await?
            .into_wast_val()
            .map(|wast_val| wast_val.as_val()))
    }

    async fn imported_fn_to_val(
        &mut self,
        imported_fn_call: ImportedFnCall<'_>,
        fn_registry: &dyn FunctionRegistry,
        called_at: DateTime<Utc>,
        called_ffqn: &FunctionFqn,
    ) -> Result<Option<wasmtime::component::Val>, WorkflowFunctionError> {
        match imported_fn_call {
            ImportedFnCall::Direct {
                ffqn,
                params,
                wasm_backtrace,
            } => {
                let (_fn_meta, fn_component_id, fn_retry_config) = fn_registry
                    .get_by_exported_function(&ffqn)
                    .expect("function obtained from fn_registry exports must be found");

                self.apply_event(
                    EventCall::BlockingChildDirectCall(
                        self.event_history.next_blocking_child_direct_call(
                            ffqn,
                            fn_component_id,
                            fn_retry_config,
                            Params::from_wasmtime(Arc::from(params)),
                            wasm_backtrace,
                        ),
                    ),
                    called_at,
                )
                .await
            }
            ImportedFnCall::Schedule {
                target_ffqn,
                schedule_at,
                target_params,
                wasm_backtrace,
            } => {
                // TODO(edge case): handle ExecutionId conflict: This does not have to be deterministicly generated.
                // Remove execution_id from EventCall::ScheduleRequest and add retries.
                // Or use ExecutionId::generate(), but ignore the id when checking determinism.
                let execution_id = ExecutionId::from_parts(
                    self.execution_id.get_top_level().timestamp_part(),
                    self.next_u128(),
                );
                let (_fn_meta, fn_component_id, fn_retry_config) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from fn_registry exports must be found");
                let scheduled_at_if_new = schedule_at.as_date_time(called_at).map_err(|err| {
                    WorkflowFunctionError::ImportedFunctionCallError {
                        ffqn: called_ffqn.clone(),
                        reason: "schedule-at conversion error".into(),
                        detail: Some(format!("{err:?}")),
                    }
                })?;
                self.apply_event(
                    EventCall::ScheduleRequest {
                        schedule_at,
                        scheduled_at_if_new,
                        execution_id,
                        ffqn: target_ffqn,
                        fn_component_id,
                        fn_retry_config,
                        params: Params::from_wasmtime(Arc::from(target_params)),
                        wasm_backtrace,
                    },
                    called_at,
                )
                .await
            }
            ImportedFnCall::SubmitExecution {
                target_ffqn,
                join_set_id,
                target_params,
                wasm_backtrace,
            } => {
                let child_execution_id = self.next_child_id(&join_set_id);
                let (_fn_meta, fn_component_id, fn_retry_config) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from fn_registry exports must be found");
                self.apply_event(
                    EventCall::StartAsync {
                        target_ffqn,
                        fn_component_id,
                        fn_retry_config,
                        join_set_id,
                        params: Params::from_wasmtime(Arc::from(target_params)),
                        child_execution_id,
                        wasm_backtrace,
                    },
                    called_at,
                )
                .await
            }
            ImportedFnCall::AwaitNext {
                target_ffqn,
                join_set_id,
                wasm_backtrace,
            } => {
                self.apply_event(
                    EventCall::BlockingChildAwaitNext {
                        join_set_id,
                        wasm_backtrace,
                        requested_ffqn: target_ffqn,
                    },
                    called_at,
                )
                .await
            }
            ImportedFnCall::Stub {
                target_ffqn,
                target_execution_id,
                parent_id,
                join_set_id,
                return_value,
                wasm_backtrace,
            } => {
                let (fn_meta, _, _) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from fn_registry exports must be found");

                let return_value_or_err = WastVal::try_from(return_value).map_err(|err| {
                    WorkflowFunctionError::ImportedFunctionCallError {
                        ffqn: called_ffqn.clone(),
                        reason: "cannot convert stub return value".into(),
                        detail: Some(format!("{err:?}")),
                    }
                })?;

                let result = match (return_value_or_err, fn_meta.return_type) {
                    (WastVal::Result(Err(None)), _) => {
                        Err(FinishedExecutionError::new_stubbed_error())
                    }
                    (WastVal::Result(Ok(Some(return_value))), Some(return_type)) => {
                        // TODO: type check
                        Ok(SupportedFunctionReturnValue::from(WastValWithType {
                            r#type: return_type.type_wrapper,
                            value: *return_value,
                        }))
                    }
                    (WastVal::Result(Ok(None)), None) => Ok(SupportedFunctionReturnValue::None),
                    other => {
                        error!("Unexpected stub return value {other:?}");
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn.clone(),
                            reason: "unexpected stub return value".into(),
                            detail: None,
                        });
                    }
                };

                self.apply_event(
                    EventCall::Stub {
                        target_ffqn: target_ffqn.clone(),
                        target_execution_id: target_execution_id.clone(),
                        parent_id,
                        join_set_id,
                        result,
                        wasm_backtrace,
                    },
                    called_at,
                )
                .await
            }
            ImportedFnCall::Get {
                target_ffqn,
                child_execution_id,
            } => {
                let val = match self
                    .event_history
                    .get_processed_response(&child_execution_id, &target_ffqn)
                {
                    Ok(option) => wasmtime::component::Val::Result(Ok(
                        option.map(|wast_val| Box::new(wast_val.as_val()))
                    )),
                    Err(get_extension_err) => wasmtime::component::Val::Result(Err(Some(
                        Box::new(wasmtime::component::Val::from(get_extension_err)),
                    ))),
                };

                Ok(Some(val))
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum JoinSetCreateError {
    #[error(transparent)]
    InvalidNameError(InvalidNameError<JoinSetId>),
    #[error(transparent)]
    ApplyError(ApplyError),
    #[error(transparent)]
    ResourceTableError(ResourceTableError),
    #[error("join set name conflict")]
    NameConflict,
}

mod workflow_support {
    use super::types_execution;
    use super::{ClockFn, EventCall, WorkflowCtx, WorkflowFunctionError, assert_matches};
    use crate::workflow::event_history::ChildReturnValue;
    use crate::workflow::host_exports::v2_0_0::obelisk::types::execution::Host as ExecutionIfcHost;
    use crate::workflow::host_exports::v2_0_0::obelisk::types::execution::HostJoinSetId;
    use crate::workflow::workflow_ctx::{IFC_FQN_WORKFLOW_SUPPORT_2, JoinSetCreateError};
    use concepts::FunctionFqn;
    use concepts::storage::HistoryEventScheduleAt;
    use concepts::{
        CHARSET_ALPHANUMERIC, JoinSetId, JoinSetKind,
        storage::{self, PersistKind},
    };
    use tracing::trace;
    use val_json::wast_val::WastVal;
    use wasmtime::component::Resource;

    impl<C: ClockFn> HostJoinSetId for WorkflowCtx<C> {
        fn id(
            &mut self,
            resource: wasmtime::component::Resource<JoinSetId>,
        ) -> wasmtime::Result<String> {
            Ok(self.resource_to_join_set_id(&resource)?.to_string())
        }

        fn drop(&mut self, resource: Resource<JoinSetId>) -> wasmtime::Result<()> {
            self.resource_table.delete(resource)?;
            Ok(())
        }
    }

    impl<C: ClockFn> ExecutionIfcHost for WorkflowCtx<C> {}

    // Functions dynamically linked for Workflow Support 1.0.0 and 1.1.0
    impl<C: ClockFn> WorkflowCtx<C> {
        pub(crate) async fn random_u64(
            &mut self,
            min: u64,
            max_exclusive: u64,
        ) -> wasmtime::Result<u64> {
            let Some(max_inclusive) = max_exclusive.checked_add(1) else {
                // Panic in host function would kill the worker task.
                return Err(wasmtime::Error::msg("integer overflow"));
            };
            self.random_u64_inclusive(min, max_inclusive).await
        }

        pub(crate) async fn random_u64_inclusive(
            &mut self,
            min: u64,
            max_inclusive: u64,
        ) -> wasmtime::Result<u64> {
            let range = min..=max_inclusive;
            if range.is_empty() {
                // Panic in host function would kill the worker task.
                return Err(wasmtime::Error::msg("range must not be empty"));
            }
            let value = rand::Rng::gen_range(&mut self.rng, range);
            let value = Vec::from(storage::from_u64_to_bytes(value));
            let value = self
                .event_history
                .apply(
                    EventCall::Persist {
                        value,
                        kind: PersistKind::RandomU64 { min, max_inclusive },
                        wasm_backtrace: self.backtrace.take(),
                    },
                    self.db_pool.connection().as_ref(),
                    &mut self.version,
                    self.clock_fn.now(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let value =
                assert_matches!(value, ChildReturnValue::WastVal(WastVal::U64(value)) => value);
            Ok(value)
        }

        pub(crate) async fn random_string(
            &mut self,
            min_length: u16,
            max_length_exclusive: u16,
        ) -> wasmtime::Result<String> {
            let value: String = {
                let range = min_length..max_length_exclusive;
                if range.is_empty() {
                    // Panic in host function would kill the worker task.
                    return Err(wasmtime::Error::msg("range must not be empty"));
                }
                let length_inclusive = rand::Rng::gen_range(&mut self.rng, range);
                (0..=length_inclusive)
                    .map(|_| {
                        let idx =
                            rand::Rng::gen_range(&mut self.rng, 0..CHARSET_ALPHANUMERIC.len());
                        CHARSET_ALPHANUMERIC
                            .chars()
                            .nth(idx)
                            .expect("idx is < charset.len()")
                    })
                    .collect()
            };
            // Persist
            let value = Vec::from_iter(value.bytes());
            let value = self
                .event_history
                .apply(
                    EventCall::Persist {
                        value,
                        kind: PersistKind::RandomString {
                            min_length: u64::from(min_length),
                            max_length_exclusive: u64::from(max_length_exclusive),
                        },
                        wasm_backtrace: self.backtrace.take(),
                    },
                    self.db_pool.connection().as_ref(),
                    &mut self.version,
                    self.clock_fn.now(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let value =
                assert_matches!(value, ChildReturnValue::WastVal(WastVal::String(value)) => value);
            Ok(value)
        }

        pub(crate) async fn submit_delay(
            &mut self,
            join_set_id: JoinSetId,
            schedule_at: HistoryEventScheduleAt,
        ) -> Result<types_execution::DelayId, WorkflowFunctionError> {
            let delay_id = self.event_history.next_delay_id(&join_set_id);
            let expires_at_if_new =
                schedule_at
                    .as_date_time(self.clock_fn.now())
                    .map_err(|err| {
                        const FFQN: FunctionFqn =
                            FunctionFqn::new_static(IFC_FQN_WORKFLOW_SUPPORT_2, "submit-delay");

                        WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: FFQN,
                            reason: "schedule-at conversion error".into(),
                            detail: Some(format!("{err:?}")),
                        }
                    })?;
            let value = self
                .event_history
                .apply(
                    EventCall::SubmitDelay {
                        delay_id,
                        join_set_id,
                        schedule_at,
                        expires_at_if_new,
                        wasm_backtrace: self.backtrace.take(),
                    },
                    self.db_pool.connection().as_ref(),
                    &mut self.version,
                    self.clock_fn.now(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let id = assert_matches!(value, ChildReturnValue::WastVal(WastVal::Record(mut map)) => map.shift_remove("id").expect(
                "DelayId must be serialized as WastVal::Record"));
            let id = assert_matches!(id, WastVal::String(id) => id, "DelayId record must have `id` serialized as WastVal::String");
            Ok(types_execution::DelayId { id })
        }

        pub(crate) async fn sleep(
            &mut self,
            schedule_at: HistoryEventScheduleAt,
        ) -> wasmtime::Result<()> {
            Ok(self.persist_sleep(schedule_at).await?)
        }

        pub(crate) async fn new_join_set_named(
            &mut self,
            name: String,
        ) -> wasmtime::Result<Resource<JoinSetId>> {
            self.persist_join_set_with_kind(
                name,
                JoinSetKind::Named,
                concepts::ClosingStrategy::Complete,
            )
            .await
            .map_err(|err| match err {
                JoinSetCreateError::InvalidNameError(_) | JoinSetCreateError::NameConflict => {
                    wasmtime::Error::new(WorkflowFunctionError::JoinSetNameError {
                        reason: err.to_string(),
                    })
                }
                JoinSetCreateError::ApplyError(apply_err) => {
                    wasmtime::Error::new(WorkflowFunctionError::from(apply_err))
                }
                JoinSetCreateError::ResourceTableError(resource_table_error) => {
                    wasmtime::Error::new(resource_table_error)
                }
            })
        }

        pub(crate) async fn new_join_set_generated(
            &mut self,
        ) -> wasmtime::Result<Resource<JoinSetId>> {
            let name = self.event_history.next_join_set_name_generated();
            trace!("new_join_set_generated: {name}");
            self.persist_join_set_with_kind(
                name,
                JoinSetKind::Generated,
                concepts::ClosingStrategy::Complete,
            )
            .await
            .map_err(|err| match err {
                JoinSetCreateError::InvalidNameError(_) | JoinSetCreateError::NameConflict => {
                    unreachable!("generated index has been incremented")
                }
                JoinSetCreateError::ApplyError(apply_err) => {
                    wasmtime::Error::new(WorkflowFunctionError::from(apply_err))
                }
                JoinSetCreateError::ResourceTableError(resource_table_error) => {
                    wasmtime::Error::new(resource_table_error)
                }
            })
        }
    }
}

impl<C: ClockFn> log_activities::obelisk::log::log::Host for WorkflowCtx<C> {
    fn trace(&mut self, message: String) {
        self.component_logger.trace(&message);
    }

    fn debug(&mut self, message: String) {
        self.component_logger.debug(&message);
    }

    fn info(&mut self, message: String) {
        self.component_logger.info(&message);
    }

    fn warn(&mut self, message: String) {
        self.component_logger.warn(&message);
    }

    fn error(&mut self, message: String) {
        self.component_logger.error(&message);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::workflow::host_exports::SUFFIX_FN_SUBMIT;
    use crate::workflow::workflow_ctx::ApplyError;
    use crate::workflow::workflow_ctx::{ImportedFnCall, WorkerPartialResult};
    use crate::{
        workflow::workflow_ctx::WorkflowCtx, workflow::workflow_worker::JoinNextBlockingStrategy,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use chrono::DateTime;
    use concepts::prefixed_ulid::{ExecutionIdDerived, ExecutorId, RunId};
    use concepts::storage::{
        AppendRequest, CreateRequest, DbPool, ExecutionEvent, ExecutionEventInner, HistoryEvent,
        HistoryEventScheduleAt, JoinSetRequest, JoinSetResponse, PendingState,
        PendingStateFinished, PendingStateFinishedResultKind,
    };
    use concepts::storage::{ExecutionLog, JoinSetResponseEvent, JoinSetResponseEventOuter};
    use concepts::time::{ClockFn, Now};
    use concepts::{ComponentId, ExecutionMetadata, FunctionRegistry, IfcFqnName, SUFFIX_PKG_EXT};
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue};
    use concepts::{FunctionMetadata, ParameterTypes};
    use db_tests::Database;
    use executor::expired_timers_watcher::TickProgress;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerContext, WorkerResult},
    };
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;
    use std::{fmt::Debug, sync::Arc, time::Duration};
    use test_utils::get_seed;
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::{debug, info, info_span};
    use utils::testing_fn_registry::fn_registry_dummy;
    use wasmtime::component::Val;

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    pub const FFQN_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    impl From<WorkerPartialResult> for WorkerResult {
        fn from(worker_partial_result: WorkerPartialResult) -> Self {
            match worker_partial_result {
                WorkerPartialResult::FatalError(err, version) => {
                    WorkerResult::Err(executor::worker::WorkerError::FatalError(err, version))
                }
                WorkerPartialResult::InterruptRequested => WorkerResult::DbUpdatedByWorkerOrWatcher,
                WorkerPartialResult::DbError(db_err) => {
                    WorkerResult::Err(executor::worker::WorkerError::DbError(db_err))
                }
            }
        }
    }

    #[derive(Debug, Clone, arbitrary::Arbitrary)]
    enum WorkflowStep {
        Sleep {
            millis: u32, // Avoid ScheduleAtConversionError::OutOfRangeError
        },
        Call {
            ffqn: FunctionFqn,
        },
        SubmitExecution {
            target_ffqn: FunctionFqn,
        },
        SubmitDelay {
            millis: u32, // Avoid ScheduleAtConversionError::OutOfRangeError
        },
        RandomU64 {
            min: u64,
            max_inclusive: u64,
        },
        RandomString {
            min_length: u16,
            max_length_exclusive: u16,
        },
    }

    impl WorkflowStep {
        fn is_valid(&self) -> bool {
            match self {
                Self::RandomU64 { min, max_inclusive } => min <= max_inclusive,
                Self::RandomString {
                    min_length,
                    max_length_exclusive,
                } => min_length < max_length_exclusive,
                _ => true,
            }
        }
    }

    #[derive(Clone, derive_more::Debug)]
    struct WorkflowWorkerMock<C: ClockFn> {
        #[expect(dead_code)]
        ffqn: FunctionFqn, // For debugging
        steps: Vec<WorkflowStep>,
        #[debug(skip)]
        clock_fn: C,
        #[debug(skip)]
        db_pool: Arc<dyn DbPool>,
        #[debug(skip)]
        fn_registry: Arc<dyn FunctionRegistry>,
        #[debug(skip)]
        exports: [FunctionMetadata; 1],
    }

    impl<C: ClockFn> WorkflowWorkerMock<C> {
        fn new(
            ffqn: FunctionFqn,
            fn_registry: Arc<dyn FunctionRegistry>,
            steps: Vec<WorkflowStep>,
            clock_fn: C,
            db_pool: Arc<dyn DbPool>,
        ) -> Self {
            Self {
                exports: [FunctionMetadata {
                    ffqn: ffqn.clone(),
                    parameter_types: ParameterTypes::default(),
                    return_type: None,
                    extension: None,
                    submittable: true,
                }],
                ffqn,
                steps,
                clock_fn,
                db_pool,
                fn_registry,
            }
        }
    }

    #[async_trait]
    impl<C: ClockFn + 'static> Worker for WorkflowWorkerMock<C> {
        async fn run(&self, ctx: WorkerContext) -> WorkerResult {
            info!("Starting");
            let seed = ctx.execution_id.random_seed();
            let mut workflow_ctx = WorkflowCtx::new(
                ctx.execution_id.clone(),
                ComponentId::dummy_activity(),
                ctx.event_history,
                ctx.responses,
                seed,
                self.clock_fn.clone(),
                JoinNextBlockingStrategy::Interrupt, // Cannot Await: when moving time forward both worker and timers watcher would race.
                self.db_pool.clone(),
                ctx.version,
                ctx.execution_deadline,
                tracing::info_span!("workflow-test"),
                false,
                false,
            );
            for step in &self.steps {
                info!("Processing step {step:?}");
                let res = match step {
                    WorkflowStep::Sleep { millis } => {
                        workflow_ctx
                            .persist_sleep(HistoryEventScheduleAt::In(Duration::from_millis(
                                u64::from(*millis),
                            )))
                            .await
                    }
                    WorkflowStep::Call { ffqn } => {
                        workflow_ctx
                            .call_imported_fn(
                                ImportedFnCall::Direct {
                                    ffqn: ffqn.clone(),
                                    params: &[],
                                    wasm_backtrace: None,
                                },
                                &mut [],
                                ffqn.clone(),
                                self.fn_registry.as_ref(),
                            )
                            .await
                    }
                    WorkflowStep::SubmitExecution { target_ffqn } => {
                        // Create new join set
                        let join_set_resource =
                            workflow_ctx.new_join_set_generated().await.unwrap();
                        let join_set_id = workflow_ctx
                            .resource_table
                            .get(&join_set_resource)
                            .unwrap()
                            .clone();
                        let mut ret_val = vec![Val::Bool(false)];
                        let target_ifc = target_ffqn.ifc_fqn.clone();
                        let submit_ffqn = FunctionFqn {
                            ifc_fqn: IfcFqnName::from_parts(
                                target_ifc.namespace(),
                                &format!("{}{SUFFIX_PKG_EXT}", target_ifc.package_name()),
                                target_ifc.ifc_name(),
                                target_ifc.version(),
                            ),
                            function_name: concepts::FnName::from(format!(
                                "{}{}",
                                target_ffqn.function_name, SUFFIX_FN_SUBMIT
                            )),
                        };
                        workflow_ctx
                            .call_imported_fn(
                                ImportedFnCall::SubmitExecution {
                                    target_ffqn: target_ffqn.clone(),
                                    join_set_id,
                                    target_params: &[],
                                    wasm_backtrace: None,
                                },
                                &mut ret_val,
                                submit_ffqn,
                                self.fn_registry.as_ref(),
                            )
                            .await
                    }
                    WorkflowStep::SubmitDelay { millis } => {
                        // Create new join set
                        let join_set_resource =
                            workflow_ctx.new_join_set_generated().await.unwrap();
                        let join_set_id = workflow_ctx
                            .resource_table
                            .get(&join_set_resource)
                            .unwrap()
                            .clone();
                        workflow_ctx
                            .submit_delay(
                                join_set_id,
                                HistoryEventScheduleAt::In(Duration::from_millis(u64::from(
                                    *millis,
                                ))),
                            )
                            .await
                            .map(|_| ())
                    }
                    WorkflowStep::RandomU64 { min, max_inclusive } => {
                        let value = workflow_ctx
                            .random_u64_inclusive(*min, *max_inclusive)
                            .await
                            .unwrap();
                        assert!(value > *min);
                        assert!(value <= *max_inclusive);
                        Ok(())
                    }
                    WorkflowStep::RandomString {
                        min_length,
                        max_length_exclusive,
                    } => {
                        let value = workflow_ctx
                            .random_string(*min_length, *max_length_exclusive)
                            .await
                            .unwrap();
                        assert!(value.len() > *min_length as usize);
                        assert!(value.len() < usize::from(*max_length_exclusive));
                        Ok(())
                    }
                };
                if let Err(err) = res {
                    info!("Sending {err:?}");
                    return err.into_worker_partial_result(workflow_ctx.version).into();
                }
            }
            info!("Closing opened join sets");
            let res = match workflow_ctx.close_opened_join_sets().await {
                Ok(()) => {
                    info!("Finishing");
                    WorkerResult::Ok(
                        SupportedFunctionReturnValue::None,
                        workflow_ctx.version,
                        None,
                    )
                }
                Err(ApplyError::InterruptRequested) => {
                    info!("Interrupting");
                    return WorkerResult::DbUpdatedByWorkerOrWatcher;
                }
                other => panic!("Unexpected error: {other:?}"),
            };
            info!("Done");
            res
        }

        fn exported_functions(&self) -> &[FunctionMetadata] {
            &self.exports
        }
    }

    // TODO: verify nondeterminism detection:
    // Start WorkflowWorkerMock, wait until it completes.
    // Copy its execution history to a new database
    // A. Swap two event history items
    // B. Swap two steps in WorkflowWorkerMock
    // C. Add new event history item
    // D. Add new step - needs whole execution history, must be done on another layer
    // E. Remove a step
    // F. Change the final result

    #[tokio::test]
    async fn generate_steps_execute_twice_check_determinism() {
        test_utils::set_up();
        for seed in get_seed() {
            let steps = generate_steps(seed);
            let closure = |steps, mut sim_clock, seed| async move {
                let (_guard, db_pool) = Database::Memory.set_up().await;
                let mut seedable_rng = StdRng::seed_from_u64(seed);
                let next_u128 = || rand::Rng::r#gen(&mut seedable_rng);
                let res = execute_steps(steps, db_pool.clone(), &mut sim_clock, next_u128).await;
                db_pool.close().await.unwrap();
                res
            };
            println!("Run 1");
            let res1 = closure(steps.clone(), SimClock::epoch(), seed).await;
            println!("Run 2");
            let res2 = closure(steps, SimClock::epoch(), seed).await;
            assert_eq!(res1, res2);
        }
    }

    #[tokio::test]
    async fn generate_steps_execute_keep_responses_reexecute_should_generate_same_exec_log() {
        test_utils::set_up();
        for seed in get_seed() {
            let steps = generate_steps(seed);

            println!("Run 1");
            let (execution_id, execution_log) = {
                let (_guard, db_pool) = Database::Memory.set_up().await;
                let mut seedable_rng = StdRng::seed_from_u64(seed);
                let next_u128 = || rand::Rng::r#gen(&mut seedable_rng);
                let (execution_id, execution_log) = execute_steps(
                    steps.clone(),
                    db_pool.clone(),
                    &mut SimClock::epoch(),
                    next_u128,
                )
                .await;
                println!("{execution_log:?}");
                (execution_id, execution_log)
            };
            println!("Run 2");
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let sim_clock = SimClock::epoch();
            let fn_registry = steps_to_registry(&steps);
            let workflow_exec = {
                let worker = Arc::new(WorkflowWorkerMock::new(
                    FFQN_MOCK,
                    fn_registry.clone(),
                    steps,
                    sim_clock.clone(),
                    db_pool.clone(),
                ));
                let exec_config = ExecConfig {
                    batch_size: 1,
                    lock_expiry: Duration::from_secs(1),
                    tick_sleep: TICK_SLEEP,
                    component_id: ComponentId::dummy_activity(),
                    task_limiter: None,
                    executor_id: ExecutorId::from_parts(0, 0), // only appears in Locked events
                };
                ExecTask::new(
                    worker,
                    exec_config,
                    sim_clock.clone(),
                    db_pool.clone(),
                    Arc::new([FFQN_MOCK]),
                )
            };
            // Create an execution.
            let db_connection = db_pool.connection();
            db_connection
                .create(CreateRequest {
                    created_at: sim_clock.now(),
                    execution_id: execution_id.clone(),
                    ffqn: FFQN_MOCK,
                    params: Params::default(),
                    parent: None,
                    metadata: concepts::ExecutionMetadata::empty(),
                    scheduled_at: sim_clock.now(),
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                    component_id: ComponentId::dummy_activity(),
                    scheduled_by: None,
                })
                .await
                .unwrap();
            // Append responses
            for response_event in execution_log.responses {
                db_connection
                    .append_response(
                        response_event.created_at,
                        execution_id.clone(),
                        response_event.event,
                    )
                    .await
                    .unwrap();
            }
            // Expect that the same steps + same responses produce same exec log (lock events and timestamps may differ)
            loop {
                workflow_exec
                    .tick_test(
                        sim_clock.now(),
                        RunId::from_parts(
                            u64::try_from(sim_clock.now().timestamp_millis()).unwrap(),
                            0,
                        ),
                    )
                    .await
                    .unwrap()
                    .wait_for_tasks()
                    .await
                    .unwrap();
                let pending_state = db_connection
                    .get_pending_state(&execution_id)
                    .await
                    .unwrap();
                if pending_state.is_finished() {
                    break;
                }
            }
            let execution_log2 = db_connection.get(&execution_id).await.unwrap();
            println!("{execution_log2:?}");
            assert_eq!(
                skip_locked(execution_log.events.iter()).count(),
                skip_locked(execution_log2.events.iter()).count()
            );
            let sanitize = |mut event| {
                match &mut event {
                    ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinNext { run_expires_at, .. },
                        ..
                    } => {
                        *run_expires_at = DateTime::UNIX_EPOCH;
                    }
                    ExecutionEventInner::HistoryEvent {
                        event:
                            HistoryEvent::JoinSetRequest {
                                request: JoinSetRequest::DelayRequest { expires_at, .. },
                                ..
                            },
                    } => {
                        *expires_at = DateTime::UNIX_EPOCH;
                    }
                    _ => {}
                }
                event
            };

            for (idx, (event, event2)) in skip_locked(execution_log.events.iter())
                .cloned()
                .map(sanitize)
                .zip(
                    skip_locked(execution_log2.events.iter())
                        .cloned()
                        .map(sanitize),
                )
                .enumerate()
            {
                println!("Comparing {idx}");
                println!("{event:?}");
                println!("{event2:?}");

                assert_eq!(event, event2, "mismatch at {idx}");
            }
        }
    }

    #[tokio::test]
    async fn creating_oneoff_and_generated_join_sets_with_same_name_should_work() {
        test_utils::set_up();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let steps = vec![
            WorkflowStep::Call {
                ffqn: FFQN_CHILD_MOCK,
            },
            WorkflowStep::SubmitExecution {
                target_ffqn: FFQN_CHILD_MOCK,
            },
        ];
        execute_steps(steps, db_pool.clone(), &mut SimClock::epoch(), || 0).await;
        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn submit_delay_should_work() {
        test_utils::set_up();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let steps = vec![WorkflowStep::SubmitDelay { millis: 1 }];
        execute_steps(steps, db_pool.clone(), &mut SimClock::epoch(), || 0).await;
        db_pool.close().await.unwrap();
    }

    const FFQN_CHILD_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn-child");

    #[tokio::test]
    #[expect(clippy::search_is_some)]
    async fn check_determinism_closing_multiple_join_sets() {
        const SUBMITS: usize = 10;
        test_utils::set_up();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let sim_clock = SimClock::new(Now.now());
        let db_connection = db_pool.connection();

        // Create an execution.
        let execution_id = ExecutionId::generate();
        let version = db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: FFQN_MOCK,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let steps: Vec<_> = std::iter::repeat_n(
            WorkflowStep::SubmitExecution {
                target_ffqn: FFQN_CHILD_MOCK,
            },
            SUBMITS,
        )
        .collect();
        let worker = Arc::new(WorkflowWorkerMock::new(
            FFQN_MOCK,
            steps_to_registry(&steps),
            steps,
            sim_clock.clone(),
            db_pool.clone(),
        ));
        {
            let mut worker_result = worker
                .run(WorkerContext {
                    execution_id: execution_id.clone(),
                    metadata: ExecutionMetadata::empty(),
                    ffqn: FFQN_MOCK,
                    params: Params::empty(),
                    event_history: vec![],
                    responses: vec![],
                    version,
                    execution_deadline: sim_clock.now() + Duration::from_secs(1),
                    can_be_retried: false,
                    run_id: RunId::generate(),
                    worker_span: info_span!("check_determinism"),
                })
                .await;
            assert_matches!(
                worker_result,
                WorkerResult::DbUpdatedByWorkerOrWatcher,
                "At least one -submit is expected"
            );
            // Run it SUBMITS times to close all join sets.
            for run in 0..SUBMITS {
                assert_matches!(worker_result, WorkerResult::DbUpdatedByWorkerOrWatcher);
                let execution_log = db_connection.get(&execution_id).await.unwrap();
                let closing_join_nexts: hashbrown::HashSet<_> = execution_log
                    .event_history()
                    .filter_map(|event| match event {
                        HistoryEvent::JoinNext {
                            closing: true,
                            join_set_id,
                            ..
                        } => Some(join_set_id),
                        _ => None,
                    })
                    .collect();
                assert_eq!(run + 1, closing_join_nexts.len());
                // The last execution log entry must be join next.
                let last_join_set = assert_matches!(&execution_log.pending_state, PendingState::BlockedByJoinSet {join_set_id, ..} => join_set_id.clone());
                // Find a child execution id that belongs to this join set and has not been executed.
                let child_execution_id = {
                    let mut childs: Vec<_> = execution_log
                        .event_history()
                        .filter_map(|hevent| match hevent {
                            HistoryEvent::JoinSetRequest {
                                join_set_id: found,
                                request:
                                    JoinSetRequest::ChildExecutionRequest { child_execution_id },
                            } if last_join_set == found => Some(child_execution_id),
                            _ => None,
                        })
                        .collect();
                    childs.retain(|child| {
                        execution_log
                            .responses
                            .iter()
                            .find(|r| {
                                matches!(
                                    r,
                                    JoinSetResponseEventOuter {
                                        event: JoinSetResponseEvent {
                                            event: JoinSetResponse::ChildExecutionFinished {
                                                child_execution_id: found,
                                                ..
                                            },
                                            ..
                                        },
                                        ..
                                    }
                                    if child == found
                                )
                            })
                            .is_none() // retain only if response is not found.
                    });
                    assert_eq!(1, childs.len());
                    childs.pop().unwrap()
                };
                info!("Found child to be executed: {child_execution_id}");
                exec_child(child_execution_id, db_pool.clone(), sim_clock.clone()).await;
                info!("Advancing the worker");
                let execution_log = db_connection.get(&execution_id).await.unwrap();
                assert_eq!(run + 1, execution_log.responses.len());
                worker_result = worker
                    .run(WorkerContext {
                        execution_id: execution_id.clone(),
                        metadata: ExecutionMetadata::empty(),
                        ffqn: FFQN_MOCK,
                        params: Params::empty(),
                        event_history: execution_log.event_history().collect(),
                        responses: execution_log
                            .responses
                            .into_iter()
                            .map(|outer| outer.event)
                            .collect(),
                        version: execution_log.next_version,
                        execution_deadline: sim_clock.now() + Duration::from_secs(1),
                        can_be_retried: false,
                        run_id: RunId::generate(),
                        worker_span: info_span!("check_determinism"),
                    })
                    .await;
                if run + 1 < SUBMITS {
                    assert_matches!(
                        worker_result,
                        WorkerResult::DbUpdatedByWorkerOrWatcher,
                        "At another -submit is expected"
                    );
                }
            }
            let (finished_value, version) = assert_matches!(
                worker_result,
                WorkerResult::Ok(finished_value, version, _http_client_traces) => (finished_value, version),
                "should be finished"
            );
            info!("Appending finished result");
            db_connection
                .append(
                    execution_id.clone(),
                    version,
                    AppendRequest {
                        created_at: sim_clock.now(),
                        event: concepts::storage::ExecutionEventInner::Finished {
                            result: Ok(finished_value),
                            http_client_traces: None,
                        },
                    },
                )
                .await
                .unwrap();
        }

        let execution_log = db_connection.get(&execution_id).await.unwrap();
        assert_matches!(
            execution_log.pending_state,
            PendingState::Finished {
                finished: PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind(Ok(())),
                    ..
                }
            }
        );

        // Currently only finished executions can be checked. Otherwise a noop
        // response in `find_matching_atomic` would need to be implemented.
        info!("Run again to test determinism");
        let worker_result = worker
            .run(WorkerContext {
                execution_id: execution_id.clone(),
                metadata: ExecutionMetadata::empty(),
                ffqn: FFQN_MOCK,
                params: Params::empty(),
                event_history: execution_log.event_history().collect(),
                responses: execution_log
                    .responses
                    .clone()
                    .into_iter()
                    .map(|outer| outer.event)
                    .collect(),
                version: execution_log.next_version.clone(),
                execution_deadline: sim_clock.now() + Duration::from_secs(1),
                can_be_retried: false,
                run_id: RunId::generate(),
                worker_span: info_span!("check_determinism"),
            })
            .await;
        assert_matches!(worker_result, WorkerResult::Ok(..), "should be finished");
        assert_eq!(
            execution_log,
            db_connection.get(&execution_id).await.unwrap(),
            "nothing should be written when verifying determinism"
        );
        db_pool.close().await.unwrap();
    }

    fn generate_steps(seed: u64) -> Vec<WorkflowStep> {
        let unstructured_holder = UnstructuredHolder::new(seed);
        let mut unstructured = unstructured_holder.unstructured();
        let mut steps = unstructured
            .arbitrary_iter()
            .unwrap()
            .map(std::result::Result::unwrap)
            .filter(|step: &WorkflowStep| step.is_valid())
            .collect::<Vec<_>>();
        // FIXME: the test harness supports a single child/delay request per join set
        let mut join_sets = hashbrown::HashSet::new();
        steps.retain(|step| match step {
            WorkflowStep::Call { ffqn } | WorkflowStep::SubmitExecution { target_ffqn: ffqn } => {
                join_sets.insert(ffqn.clone()) // Retain only the first step for a given ffqn
            }
            _ => true,
        });
        println!("Generated steps: {steps:?}");
        steps
    }

    fn steps_to_registry(steps: &[WorkflowStep]) -> Arc<dyn FunctionRegistry> {
        let ffqns = steps
            .iter()
            .filter_map(|step| match step {
                WorkflowStep::Call { ffqn }
                | WorkflowStep::SubmitExecution { target_ffqn: ffqn } => Some(ffqn.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        fn_registry_dummy(ffqns.as_slice())
    }

    async fn execute_steps(
        steps: Vec<WorkflowStep>,
        db_pool: Arc<dyn DbPool>,
        sim_clock: &mut SimClock,
        mut next_u128: impl FnMut() -> u128,
    ) -> (ExecutionId, ExecutionLog) {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Steps: {steps:?}");
        let execution_id = ExecutionId::from_parts(
            u64::try_from(sim_clock.now().timestamp_millis()).unwrap(),
            next_u128(),
        );

        let mut child_execution_count = steps
            .iter()
            .filter(|step| {
                matches!(
                    step,
                    WorkflowStep::Call { .. } | WorkflowStep::SubmitExecution { .. }
                )
            })
            .count();
        let mut delay_request_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Sleep { .. }))
            .count();
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = steps_to_registry(&steps);

        let workflow_exec = {
            let worker = Arc::new(WorkflowWorkerMock::new(
                FFQN_MOCK,
                fn_registry.clone(),
                steps,
                sim_clock.clone(),
                db_pool.clone(),
            ));
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                component_id: ComponentId::dummy_activity(),
                task_limiter: None,
                executor_id: ExecutorId::from_parts(
                    u64::try_from(sim_clock.now().timestamp_millis()).unwrap(),
                    next_u128(),
                ),
            };
            ExecTask::new(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                Arc::new([FFQN_MOCK]),
            )
        };
        // Create an execution.
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FFQN_MOCK,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let mut processed = Vec::new();
        loop {
            workflow_exec
                .tick_test(
                    sim_clock.now(),
                    RunId::from_parts(
                        u64::try_from(sim_clock.now().timestamp_millis()).unwrap(),
                        next_u128(),
                    ),
                )
                .await
                .unwrap()
                .wait_for_tasks()
                .await
                .unwrap();
            let pending_state = db_connection
                .get_pending_state(&execution_id)
                .await
                .unwrap();
            if pending_state.is_finished() {
                break;
            }

            let join_set_id = assert_matches!(pending_state, PendingState::BlockedByJoinSet { join_set_id, .. } => join_set_id);
            let execution_log = db_connection.get(&execution_id).await.unwrap();
            let join_set_req = execution_log
                .find_join_set_request(&join_set_id)
                .cloned()
                .expect("must be found");
            if !processed.contains(&join_set_id) {
                match join_set_req {
                    JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at,
                        ..
                    } => {
                        info!("Moving time to {expires_at} - {delay_id}");
                        assert!(delay_request_count > 0);
                        sim_clock.move_time_to(expires_at);
                        delay_request_count -= 1;
                        let actual_progress =
                            expired_timers_watcher::tick_test(db_connection.as_ref(), expires_at)
                                .await
                                .unwrap();
                        assert_eq!(
                            TickProgress {
                                expired_locks: 0,
                                expired_async_timers: 1
                            },
                            actual_progress
                        );
                    }
                    JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                        assert!(child_execution_count > 0);
                        exec_child(child_execution_id, db_pool.clone(), sim_clock.clone()).await;
                        child_execution_count -= 1;
                    }
                }
                processed.push(join_set_id);
            }
        }
        // must be finished at this point
        assert_eq!(0, child_execution_count);
        assert_eq!(0, delay_request_count);
        let execution_log = db_connection.get(&execution_id).await.unwrap();
        assert!(execution_log.pending_state.is_finished());
        drop(db_connection);
        (execution_id, execution_log)
    }

    async fn exec_child(
        child_execution_id: ExecutionIdDerived,
        db_pool: Arc<dyn DbPool>,
        sim_clock: SimClock,
    ) {
        info!("Executing child {child_execution_id}");
        let db_connection = db_pool.connection();
        let child_log = db_connection
            .get(&ExecutionId::Derived(child_execution_id.clone()))
            .await
            .unwrap();
        let (parent_execution_id, join_set_id) = assert_matches!(
            child_log.parent(),
            Some(v) => v
        );
        db_connection
            .append_batch_respond_to_parent(
                child_execution_id.clone(),
                sim_clock.now(),
                vec![AppendRequest {
                    created_at: sim_clock.now(),
                    event: concepts::storage::ExecutionEventInner::Finished {
                        result: Ok(SupportedFunctionReturnValue::None),
                        http_client_traces: None,
                    },
                }],
                child_log.next_version.clone(),
                parent_execution_id,
                JoinSetResponseEventOuter {
                    created_at: sim_clock.now(),
                    event: JoinSetResponseEvent {
                        join_set_id,
                        event: JoinSetResponse::ChildExecutionFinished {
                            child_execution_id: child_execution_id.clone(),
                            finished_version: child_log.next_version.clone(),
                            result: Ok(SupportedFunctionReturnValue::None),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let child_log = db_connection
            .get(&ExecutionId::Derived(child_execution_id.clone()))
            .await
            .unwrap();
        debug!(
            "Child execution {child_execution_id} should be finished: {:?}",
            &child_log.events
        );
        let child_res = child_log.into_finished_result().unwrap();
        assert_matches!(child_res, Ok(SupportedFunctionReturnValue::None));
    }

    fn skip_locked<'a>(
        events_iter: impl Iterator<Item = &'a ExecutionEvent>,
    ) -> impl Iterator<Item = &'a ExecutionEventInner> {
        events_iter.filter_map(|event| {
            if matches!(event.event, ExecutionEventInner::Locked { .. }) {
                None
            } else {
                Some(&event.event)
            }
        })
    }
}
