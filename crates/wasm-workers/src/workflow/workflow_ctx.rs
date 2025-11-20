use super::deadline_tracker::DeadlineTracker;
use super::event_history::{
    ApplyError, EventHistory, JoinNextRequestingFfqn, OneOffChildExecutionRequest,
    OneOffDelayRequest, Schedule, Stub, SubmitChildExecution,
};
use super::host_exports::v3_0_0::obelisk::types::execution as types_execution;
use super::host_exports::v3_0_0::{ClosingStrategy_3_0_0, ScheduleAt_3_0_0};
use super::host_exports::{
    SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_SCHEDULE, SUFFIX_FN_SUBMIT,
    history_event_schedule_at_from_wast_val,
};
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::WasmFileError;
use crate::component_logger::{ComponentLogger, log_activities};
use crate::workflow::event_history::JoinSetCreate;
use crate::workflow::host_exports::{SUFFIX_FN_GET, SUFFIX_FN_INVOKE, SUFFIX_FN_STUB};
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::storage::{
    self, DbErrorWrite, DbPool, HistoryEventScheduleAt, Version, WasmBacktrace,
};
use concepts::storage::{HistoryEvent, JoinSetResponseEvent};
use concepts::time::ClockFn;
use concepts::{
    ClosingStrategy, ComponentId, ExecutionId, FunctionMetadata, FunctionRegistry, IfcFqnName,
    InvalidNameError, ReturnType, ReturnTypeExtendable, StrVariant, SupportedFunctionReturnValue,
};
use concepts::{FunctionFqn, Params};
use concepts::{JoinSetId, JoinSetKind};
use executor::worker::FatalError;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{Span, error, instrument};
use val_json::wast_val::WastVal;
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
    #[error("error calling imported function {ffqn} - {reason}")]
    ImportedFunctionCallError {
        ffqn: FunctionFqn,
        reason: StrVariant,
        detail: Option<String>,
    },
    // retriable errors:
    #[error("interrupt, db updated")]
    InterruptDbUpdated,
    #[error(transparent)]
    DbError(DbErrorWrite),
}

#[derive(Debug)]
pub(crate) enum WorkerPartialResult {
    FatalError(FatalError, Version),
    // retriable:
    InterruptDbUpdated,
    DbError(DbErrorWrite),
}

impl WorkflowFunctionError {
    pub(crate) fn into_worker_partial_result(self, version: Version) -> WorkerPartialResult {
        match self {
            WorkflowFunctionError::InterruptDbUpdated => WorkerPartialResult::InterruptDbUpdated,
            WorkflowFunctionError::DbError(db_error) => WorkerPartialResult::DbError(db_error),
            // fatal errors:
            WorkflowFunctionError::NondeterminismDetected(detail) => {
                WorkerPartialResult::FatalError(
                    FatalError::NondeterminismDetected { detail },
                    version,
                )
            }
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
        }
    }
}

impl From<ApplyError> for WorkflowFunctionError {
    fn from(value: ApplyError) -> WorkflowFunctionError {
        match value {
            ApplyError::NondeterminismDetected(reason) => {
                WorkflowFunctionError::NondeterminismDetected(reason)
            }
            ApplyError::InterruptDbUpdated => WorkflowFunctionError::InterruptDbUpdated,
            ApplyError::DbError(db_error) => WorkflowFunctionError::DbError(db_error),
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
    backtrace_persist: bool,
    wasi_ctx: WasiCtx,
}

#[derive(derive_more::Debug)]
pub(crate) enum ImportedFnCall<'a> {
    Direct(DirectFnCall<'a>),
    Schedule(ScheduleFnCall<'a>),
    SubmitExecution(SubmitExecutionFnCall<'a>),
    AwaitNext(AwaitNextFnCall),
    Stub(StubFnCall<'a>),
    Get(GetFnCall),
    Invoke(InvokeFnCall<'a>),
}

#[derive(derive_more::Debug)]
pub(crate) struct DirectFnCall<'a> {
    ffqn: FunctionFqn,
    params: &'a [Val],
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl DirectFnCall<'_> {
    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let DirectFnCall {
            ffqn,
            params,
            wasm_backtrace,
        } = self;
        OneOffChildExecutionRequest::apply(
            ffqn,
            Params::from_wasmtime(Arc::from(params)),
            wasm_backtrace,
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct ScheduleFnCall<'a> {
    target_ffqn: FunctionFqn,
    schedule_at: HistoryEventScheduleAt,
    #[debug(skip)]
    target_params: &'a [Val],
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl ScheduleFnCall<'_> {
    fn new(
        target_ffqn: FunctionFqn,
        called_ffqn: FunctionFqn,
        params: &[Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<ScheduleFnCall<'_>, WorkflowFunctionError> {
        let Some((schedule_at, params)) = params.split_first() else {
            return Err(WorkflowFunctionError::ImportedFunctionCallError {
                ffqn: called_ffqn,
                reason: StrVariant::Static("exepcted at least one parameter of type `schedule-at`"),
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
        Ok(ScheduleFnCall {
            target_ffqn,
            schedule_at,
            target_params: params,
            wasm_backtrace,
        })
    }

    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
        called_ffqn: &FunctionFqn,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let ScheduleFnCall {
            target_ffqn,
            schedule_at,
            target_params,
            wasm_backtrace,
        } = self;
        // Generate new ULID using this execution, changing the randomness part using this execution's seed.
        // We are ignoring the possibility of ExecutionId conflict:
        // According to birthday paradox, with 80 bits of randomness, a 50% chance
        // of a conflict will occur only after we generate 10^12 ULIDs.
        let execution_id = ExecutionId::from_parts(
            ctx.execution_id.get_top_level().timestamp_part(),
            ctx.next_u128(),
        );
        let scheduled_at_if_new = schedule_at.as_date_time(called_at).map_err(|err| {
            WorkflowFunctionError::ImportedFunctionCallError {
                ffqn: called_ffqn.clone(),
                reason: "schedule-at conversion error".into(),
                detail: Some(format!("{err:?}")),
            }
        })?;
        Schedule {
            schedule_at,
            scheduled_at_if_new,
            execution_id,
            ffqn: target_ffqn,
            params: Params::from_wasmtime(Arc::from(target_params)),
            wasm_backtrace,
        }
        .apply(
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct SubmitExecutionFnCall<'a> {
    target_ffqn: FunctionFqn,
    join_set_id: JoinSetId,
    #[debug(skip)]
    target_params: &'a [Val],
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl SubmitExecutionFnCall<'_> {
    fn new<'a, C: ClockFn>(
        target_ffqn: FunctionFqn,
        called_ffqn: FunctionFqn,
        store_ctx: &mut wasmtime::StoreContextMut<'a, WorkflowCtx<C>>,
        params: &'a [Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<SubmitExecutionFnCall<'a>, WorkflowFunctionError> {
        let (join_set_id, params) =
            ImportedFnCall::extract_join_set_id(&called_ffqn, store_ctx, params).map_err(
                |detail| WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static("cannot extract join set id"),
                    detail: Some(detail),
                },
            )?;
        Ok(SubmitExecutionFnCall {
            target_ffqn,
            join_set_id,
            target_params: params,
            wasm_backtrace,
        })
    }

    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let SubmitExecutionFnCall {
            target_ffqn,
            join_set_id,
            target_params,
            wasm_backtrace,
        } = self;
        let child_execution_id = ctx.next_child_id(&join_set_id);
        SubmitChildExecution {
            target_ffqn,
            join_set_id,
            params: Params::from_wasmtime(Arc::from(target_params)),
            child_execution_id,
            wasm_backtrace,
        }
        .apply(
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct AwaitNextFnCall {
    target_ffqn: FunctionFqn,
    join_set_id: JoinSetId,
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl AwaitNextFnCall {
    fn new<'a, C: ClockFn>(
        target_ffqn: FunctionFqn,
        called_ffqn: FunctionFqn,
        store_ctx: &mut wasmtime::StoreContextMut<'a, WorkflowCtx<C>>,
        params: &'a [Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<AwaitNextFnCall, WorkflowFunctionError> {
        let (join_set_id, params) =
            match ImportedFnCall::extract_join_set_id(&called_ffqn, store_ctx, params) {
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
        Ok(AwaitNextFnCall {
            target_ffqn,
            join_set_id,
            wasm_backtrace,
        })
    }

    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let AwaitNextFnCall {
            target_ffqn,
            join_set_id,
            wasm_backtrace,
        } = self;
        JoinNextRequestingFfqn {
            join_set_id,
            wasm_backtrace,
            requested_ffqn: target_ffqn,
        }
        .apply(
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct StubFnCall<'a> {
    target_ffqn: FunctionFqn,
    target_execution_id: ExecutionIdDerived,
    target_return_type: ReturnTypeExtendable,
    parent_id: ExecutionId,
    join_set_id: JoinSetId,
    #[debug(skip)]
    return_value: &'a wasmtime::component::Val,
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl StubFnCall<'_> {
    fn new(
        target_ffqn: FunctionFqn,
        called_ffqn: FunctionFqn,
        params: &[Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
        target_fn_metadata: FunctionMetadata,
    ) -> Result<StubFnCall<'_>, WorkflowFunctionError> {
        let Some((target_execution_id, [return_value])) = params.split_first() else {
            return Err(WorkflowFunctionError::ImportedFunctionCallError {
                ffqn: called_ffqn,
                reason: StrVariant::Static("-stub function exepcts two parameters"),
                detail: None,
            });
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
                    reason: format!("cannot parse first parameter as `execution-id`: {err}").into(),
                    detail: Some(format!("{err:?}")),
                });
            }
        };

        let (parent_id, join_set_id) = match target_execution_id.split_to_parts() {
            Ok(ok) => ok,
            Err(err) => {
                return Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: format!(
                        "cannot split target execution id to parts in `-stub` function - {err}"
                    )
                    .into(),
                    detail: Some(format!("{err:?}")),
                });
            }
        };
        let ReturnType::Extendable(target_return_type) = target_fn_metadata.return_type else {
            unreachable!("only functions with compatible return types are exported and extended")
        };

        Ok(StubFnCall {
            target_ffqn,
            target_execution_id,
            target_return_type,
            parent_id,
            join_set_id,
            return_value,
            wasm_backtrace,
        })
    }

    // -stub is called
    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let StubFnCall {
            target_ffqn,
            target_execution_id,
            target_return_type,
            parent_id,
            join_set_id,
            return_value,
            wasm_backtrace,
        } = self;

        let return_value = SupportedFunctionReturnValue::from_val_and_type_wrapper_tl(
            return_value.clone(),
            target_return_type.type_wrapper_tl,
        )
        .expect("only functions with compatible return types are exported and extended");

        Stub {
            target_ffqn: target_ffqn.clone(),
            target_execution_id: target_execution_id.clone(),
            parent_id,
            join_set_id,
            result: return_value,
            wasm_backtrace,
        }
        .apply(
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct GetFnCall {
    target_ffqn: FunctionFqn,
    child_execution_id: ExecutionIdDerived,
}
impl GetFnCall {
    fn new(
        target_ffqn: FunctionFqn,
        called_ffqn: FunctionFqn,
        params: &[Val],
    ) -> Result<GetFnCall, WorkflowFunctionError> {
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
        Ok(GetFnCall {
            target_ffqn,
            child_execution_id,
        })
    }

    // Never interrupts the execution
    fn call_imported_fn<C: ClockFn>(self, ctx: &mut WorkflowCtx<C>) -> wasmtime::component::Val {
        let GetFnCall {
            target_ffqn,
            child_execution_id,
        } = self;

        match ctx
            .event_history
            .get_processed_response(&child_execution_id, &target_ffqn)
        {
            // Return Ok(None) or Ok(retval)
            Ok(wast_val) => wasmtime::component::Val::Result(Ok(Some(Box::new(wast_val.as_val())))),
            Err(get_extension_err) => wasmtime::component::Val::Result(Err(Some(Box::new(
                wasmtime::component::Val::from(get_extension_err),
            )))),
        }
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct InvokeFnCall<'a> {
    target_ffqn: FunctionFqn,
    label: &'a str,
    params: &'a [Val],
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl InvokeFnCall<'_> {
    fn new(
        called_ffqn: FunctionFqn,
        target_ffqn: FunctionFqn,
        params: &[Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<InvokeFnCall<'_>, WorkflowFunctionError> {
        let Some((Val::String(label), params)) = params.split_first() else {
            return Err(WorkflowFunctionError::ImportedFunctionCallError {
                ffqn: called_ffqn,
                reason: StrVariant::Static("exepcted first parameter `name` of type `string`"),
                detail: None,
            });
        };
        Ok(InvokeFnCall {
            target_ffqn,
            label,
            params,
            wasm_backtrace,
        })
    }

    async fn call_imported_fn<C: ClockFn>(
        self,
        ctx: &mut WorkflowCtx<C>,
        called_at: DateTime<Utc>,
    ) -> Result<Val, WorkflowFunctionError> {
        let InvokeFnCall {
            target_ffqn,
            label,
            params,
            wasm_backtrace,
        } = self;
        OneOffChildExecutionRequest::apply_invoke(
            target_ffqn,
            label,
            Params::from_wasmtime(Arc::from(params)),
            wasm_backtrace,
            &mut ctx.event_history,
            ctx.db_pool.connection().as_ref(),
            &mut ctx.version,
            called_at,
        )
        .await
    }
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

    #[instrument(skip_all, fields(ffqn = %called_ffqn, otel.name = format!("ImportedFnCall::new {called_ffqn}")), name = "ImportedFnCall::new")]
    pub(crate) fn new<'ctx, C: ClockFn>(
        called_ffqn: FunctionFqn,
        store_ctx: &'ctx mut wasmtime::StoreContextMut<'a, WorkflowCtx<C>>,
        params: &'a [Val],
        backtrace_persist: bool,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<ImportedFnCall<'a>, WorkflowFunctionError> {
        let wasm_backtrace = if backtrace_persist {
            let wasm_backtrace = wasmtime::WasmBacktrace::capture(&store_ctx);
            concepts::storage::WasmBacktrace::maybe_from(&wasm_backtrace)
        } else {
            None
        };
        // TODO: Instead of stripping the suffix here use registry and switch based on extension.
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
                let (target_fn_metadata, _target_component_id) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from `fn_registry.all_exports()` must be found");
                assert_eq!(None, target_fn_metadata.extension);
                let submit = SubmitExecutionFnCall::new(
                    target_ffqn,
                    called_ffqn,
                    store_ctx,
                    params,
                    wasm_backtrace,
                )?;
                Ok(ImportedFnCall::SubmitExecution(submit))
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let await_next = AwaitNextFnCall::new(
                    target_ffqn,
                    called_ffqn,
                    store_ctx,
                    params,
                    wasm_backtrace,
                )?;

                Ok(ImportedFnCall::AwaitNext(await_next))
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_GET)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let get = GetFnCall::new(target_ffqn, called_ffqn, params)?;
                Ok(ImportedFnCall::Get(get))
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_INVOKE)
            {
                let target_ffqn = FunctionFqn::new_arc(
                    Arc::from(target_ifc_fqn.to_string()),
                    Arc::from(function_name),
                );
                let (target_fn_metadata, _fn_component_id) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from `fn_registry.all_exports()` must be found");
                assert_eq!(None, target_fn_metadata.extension);
                let invoke = InvokeFnCall::new(called_ffqn, target_ffqn, params, wasm_backtrace)?;
                Ok(ImportedFnCall::Invoke(invoke))
            } else {
                unreachable!(
                    "all extensions were covered, no way fn_registry contains {called_ffqn}"
                );
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
                let (target_fn_metadata, _target_component_id) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from `fn_registry.all_exports()` must be found");
                assert_eq!(None, target_fn_metadata.extension);
                let schedule =
                    ScheduleFnCall::new(target_ffqn, called_ffqn, params, wasm_backtrace)?;
                Ok(ImportedFnCall::Schedule(schedule))
            } else {
                unreachable!(
                    "all extensions were covered, no way fn_registry contains {called_ffqn}"
                );
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
                let (target_fn_metadata, _target_component_id) = fn_registry
                    .get_by_exported_function(&target_ffqn)
                    .expect("function obtained from `fn_registry.all_exports()` must be found");
                assert_eq!(None, target_fn_metadata.extension);
                let stub = StubFnCall::new(
                    target_ffqn,
                    called_ffqn,
                    params,
                    wasm_backtrace,
                    target_fn_metadata,
                )?;
                Ok(ImportedFnCall::Stub(stub))
            } else {
                unreachable!(
                    "all extensions were covered, no way fn_registry contains {called_ffqn}"
                );
            }
        } else {
            let (fn_metadata, _fn_component_id) = fn_registry
                .get_by_exported_function(&called_ffqn)
                .expect("function obtained from `fn_registry.all_exports()` must be found");
            assert_eq!(None, fn_metadata.extension);
            Ok(ImportedFnCall::Direct(DirectFnCall {
                ffqn: called_ffqn,
                params,
                wasm_backtrace,
            }))
        }
    }

    fn ffqn(&self) -> &FunctionFqn {
        match self {
            ImportedFnCall::Direct(DirectFnCall { ffqn, .. })
            | ImportedFnCall::Schedule(ScheduleFnCall {
                target_ffqn: ffqn, ..
            })
            | ImportedFnCall::SubmitExecution(SubmitExecutionFnCall {
                target_ffqn: ffqn, ..
            })
            | ImportedFnCall::AwaitNext(AwaitNextFnCall {
                target_ffqn: ffqn, ..
            })
            | ImportedFnCall::Stub(StubFnCall {
                target_ffqn: ffqn, ..
            })
            | ImportedFnCall::Get(GetFnCall {
                target_ffqn: ffqn, ..
            })
            | ImportedFnCall::Invoke(InvokeFnCall {
                target_ffqn: ffqn, ..
            }) => ffqn,
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

const IFC_FQN_WORKFLOW_SUPPORT_3: &str = "obelisk:workflow/workflow-support@3.0.0";

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
        deadline: DateTime<Utc>,
        worker_span: Span,
        backtrace_persist: bool,
        deadline_tracker: Arc<dyn DeadlineTracker>,
        fn_registry: Arc<dyn FunctionRegistry>,
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
                deadline,
                worker_span.clone(),
                deadline_tracker,
                fn_registry,
            ),
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            db_pool,
            version,
            component_logger: ComponentLogger { span: worker_span },
            resource_table: wasmtime::component::ResourceTable::default(),
            backtrace_persist,
            wasi_ctx: wasi_ctx_builder.build(),
        }
    }

    pub(crate) async fn flush(&mut self) -> Result<(), DbErrorWrite> {
        self.event_history
            .flush(self.db_pool.connection().as_ref(), self.clock_fn.now())
            .await
    }

    async fn persist_sleep(
        &mut self,
        schedule_at: HistoryEventScheduleAt,
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<DateTime<Utc>, WorkflowFunctionError> {
        let expires_at_if_new = schedule_at
            .as_date_time(self.clock_fn.now())
            .map_err(|err| {
                const FFQN: FunctionFqn =
                    FunctionFqn::new_static(IFC_FQN_WORKFLOW_SUPPORT_3, "sleep");
                WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: FFQN,
                    reason: "schedule-at conversion error".into(),
                    detail: Some(format!("{err:?}")),
                }
            })?;
        OneOffDelayRequest::apply(
            schedule_at,
            expires_at_if_new,
            wasm_backtrace,
            &mut self.event_history,
            self.db_pool.connection().as_ref(),
            &mut self.version,
            self.clock_fn.now(),
        )
        .await
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
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<Resource<JoinSetId>, JoinSetCreateError> {
        if !self.event_history.join_set_name_exists(&name, kind) {
            let join_set_id = JoinSetId::new(kind, StrVariant::from(name))
                .map_err(JoinSetCreateError::InvalidNameError)?;
            let join_set_id = JoinSetCreate {
                join_set_id,
                closing_strategy,
                wasm_backtrace,
            }
            .apply(
                &mut self.event_history,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await
            .map_err(JoinSetCreateError::ApplyError)?;

            let join_set_id = self
                .resource_table
                .push(join_set_id)
                .map_err(JoinSetCreateError::ResourceTableError)?;
            Ok(join_set_id)
        } else {
            Err(JoinSetCreateError::Conflict)
        }
    }

    fn get_host_maybe_capture_backtrace<'a>(
        caller: &'a mut wasmtime::StoreContextMut<'_, Self>,
    ) -> (&'a mut WorkflowCtx<C>, Option<storage::WasmBacktrace>) {
        let backtrace = if caller.data().backtrace_persist {
            let backtrace = wasmtime::WasmBacktrace::capture(&caller);
            WasmBacktrace::maybe_from(&backtrace)
        } else {
            None
        };
        let host = caller.data_mut();
        (host, backtrace)
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
        // link obelisk:types
        types_execution::add_to_linker::<_, WorkflowCtx<C>>(linker, |state: &mut Self| state)
            .map_err(linking_err)?;

        // link workflow-support
        Self::add_to_linker_workflow_support(linker)?;
        if stub_wasi {
            super::wasi::add_to_linker_async(linker)?;
        }
        Ok(())
    }

    fn add_to_linker_workflow_support(linker: &mut Linker<Self>) -> Result<(), WasmFileError> {
        let mut inst_workflow_support =
            linker.instance(IFC_FQN_WORKFLOW_SUPPORT_3).map_err(|err| {
                WasmFileError::LinkingError {
                    context: StrVariant::Static(IFC_FQN_WORKFLOW_SUPPORT_3),
                    err: err.into(),
                }
            })?;
        inst_workflow_support
            .func_wrap_async(
                "random-u64",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (min, max_exclusive): (u64, u64)| {
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let random_u64 =
                            host.random_u64(min, max_exclusive, wasm_backtrace).await?;
                        Ok((random_u64,))
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
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let random_u64 = host
                            .random_u64_inclusive(min, max_inclusive, wasm_backtrace)
                            .await?;
                        Ok((random_u64,))
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
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let random_string = host
                            .random_string(min_length, max_length_exclusive, wasm_backtrace)
                            .await?;
                        Ok((random_string,))
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
                      (join_set_id, schedule_at): (Resource<JoinSetId>, ScheduleAt_3_0_0)| {
                    let schedule_at = HistoryEventScheduleAt::from(schedule_at);
                    Box::new(async move {
                        let (host, wasm_backtrace) = Self::get_host_maybe_capture_backtrace(&mut caller);
                        let join_set_id = host.resource_to_join_set_id(&join_set_id)?.clone();
                        let delay_id = host.submit_delay(join_set_id, schedule_at, wasm_backtrace).await?;
                        Ok((delay_id,))
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
                      (schedule_at,): (ScheduleAt_3_0_0,)| {
                    let schedule_at = HistoryEventScheduleAt::from(schedule_at);
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let expires_at = host.sleep(schedule_at, wasm_backtrace).await?;
                        Ok((expires_at,))
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
                      (name, _closing_strategy): (String, ClosingStrategy_3_0_0)| {
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let resource_js = host.new_join_set_named(name, wasm_backtrace).await?;
                        Ok((resource_js,))
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
                      (_closing_strategy,): (ClosingStrategy_3_0_0,)| {
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let resource_js = host.new_join_set_generated(wasm_backtrace).await?;
                        Ok((resource_js,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function new-join-set-generated"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "join-next",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (join_set_id,): (Resource<JoinSetId>,)| {
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        let join_set_id = host.resource_to_join_set_id(&join_set_id)?.clone();
                        let result = host.join_next(join_set_id, wasm_backtrace).await;
                        Ok((result?,))
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function new-join-set-generated"),
                err: err.into(),
            })?;

        inst_workflow_support
            .func_wrap_async(
                "close",
                move |mut caller: wasmtime::StoreContextMut<'_, WorkflowCtx<C>>,
                      (join_set_resource,): (Resource<JoinSetId>,)| {
                    Box::new(async move {
                        let (host, wasm_backtrace) =
                            Self::get_host_maybe_capture_backtrace(&mut caller);
                        host.join_set_close(join_set_resource, wasm_backtrace)
                            .await?;

                        Ok(())
                    })
                },
            )
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking function new-join-set-generated"),
                err: err.into(),
            })?;

        Ok(())
    }

    pub(crate) async fn close_forgotten_join_sets(&mut self) -> Result<(), ApplyError> {
        self.event_history
            .close_forgotten_join_sets(
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

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%called_ffqn, ffqn = %imported_fn_call.ffqn()))]
    pub(crate) async fn call_imported_fn(
        &mut self,
        imported_fn_call: ImportedFnCall<'_>,
        called_at: DateTime<Utc>,
        called_ffqn: &FunctionFqn,
    ) -> Result<Val, WorkflowFunctionError> {
        match imported_fn_call {
            ImportedFnCall::Direct(direct) => direct.call_imported_fn(self, called_at).await,
            ImportedFnCall::Schedule(schedule) => {
                schedule
                    .call_imported_fn(self, called_at, called_ffqn)
                    .await
            }
            ImportedFnCall::SubmitExecution(submit) => {
                submit.call_imported_fn(self, called_at).await
            }
            ImportedFnCall::AwaitNext(await_next) => {
                await_next.call_imported_fn(self, called_at).await
            }
            ImportedFnCall::Stub(stub) => stub.call_imported_fn(self, called_at).await,
            ImportedFnCall::Get(get) => Ok(get.call_imported_fn(self)),
            ImportedFnCall::Invoke(invoke) => invoke.call_imported_fn(self, called_at).await,
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
    Conflict,
}

mod workflow_support {
    use super::types_execution;
    use super::{ClockFn, WorkflowCtx, WorkflowFunctionError};
    use crate::workflow::event_history::{JoinNext, Persist, SubmitDelay};
    use crate::workflow::host_exports;
    use crate::workflow::host_exports::v3_0_0::obelisk::types::execution::Host as ExecutionIfcHost;
    use crate::workflow::host_exports::v3_0_0::obelisk::types::execution::HostJoinSet;
    use crate::workflow::host_exports::v3_0_0::obelisk::workflow::workflow_support;
    use crate::workflow::workflow_ctx::{IFC_FQN_WORKFLOW_SUPPORT_3, JoinSetCreateError};
    use concepts::storage::HistoryEventScheduleAt;
    use concepts::{CHARSET_ALPHANUMERIC, JoinSetId, JoinSetKind};
    use concepts::{FunctionFqn, storage};
    use tracing::trace;
    use wasmtime::component::Resource;

    impl<C: ClockFn> HostJoinSet for WorkflowCtx<C> {
        async fn id(
            &mut self,
            resource: wasmtime::component::Resource<JoinSetId>,
        ) -> wasmtime::Result<String> {
            Ok(self.resource_to_join_set_id(&resource)?.to_string())
        }

        async fn drop(&mut self, resource: Resource<JoinSetId>) -> wasmtime::Result<()> {
            self.join_set_close(resource, None).await
        }
    }

    impl<C: ClockFn> ExecutionIfcHost for WorkflowCtx<C> {}

    impl<C: ClockFn> WorkflowCtx<C> {
        pub(crate) async fn join_set_close(
            &mut self,
            resource: Resource<JoinSetId>,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<()> {
            let join_set_id = self.resource_table.delete(resource)?;
            self.event_history
                .join_set_close(
                    join_set_id,
                    self.db_pool.connection().as_ref(),
                    &mut self.version,
                    self.clock_fn.now(),
                    wasm_backtrace,
                )
                .await?;
            Ok(())
        }

        pub(crate) async fn random_u64(
            &mut self,
            min: u64,
            max_exclusive: u64,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<u64> {
            let Some(max_inclusive) = max_exclusive.checked_add(1) else {
                // Panic in host function would kill the worker task.
                return Err(wasmtime::Error::msg("integer overflow"));
            };
            self.random_u64_inclusive(min, max_inclusive, wasm_backtrace)
                .await
        }

        pub(crate) async fn random_u64_inclusive(
            &mut self,
            min: u64,
            max_inclusive: u64,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<u64> {
            let range = min..=max_inclusive;
            if range.is_empty() {
                // Panic in host function would kill the worker task.
                return Err(wasmtime::Error::msg("range must not be empty"));
            }
            let value = rand::Rng::gen_range(&mut self.rng, range);

            let value = Persist::apply_u64(
                value,
                min,
                max_inclusive,
                wasm_backtrace,
                &mut self.event_history,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await?;

            Ok(value)
        }

        pub(crate) async fn random_string(
            &mut self,
            min_length: u16,
            max_length_exclusive: u16,
            wasm_backtrace: Option<storage::WasmBacktrace>,
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
            let value = Persist::apply_string(
                value,
                u64::from(min_length),
                u64::from(max_length_exclusive),
                wasm_backtrace,
                &mut self.event_history,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await?;
            Ok(value)
        }

        pub(crate) async fn submit_delay(
            &mut self,
            join_set_id: JoinSetId,
            schedule_at: HistoryEventScheduleAt,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> Result<types_execution::DelayId, WorkflowFunctionError> {
            let delay_id = self.event_history.next_delay_id(&join_set_id);
            let expires_at_if_new =
                schedule_at
                    .as_date_time(self.clock_fn.now())
                    .map_err(|err| {
                        const FFQN: FunctionFqn =
                            FunctionFqn::new_static(IFC_FQN_WORKFLOW_SUPPORT_3, "submit-delay");

                        WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: FFQN,
                            reason: "schedule-at conversion error".into(),
                            detail: Some(format!("{err:?}")),
                        }
                    })?;
            SubmitDelay {
                join_set_id,
                delay_id,
                schedule_at,
                expires_at_if_new,
                wasm_backtrace,
            }
            .apply(
                &mut self.event_history,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await
        }

        pub(crate) async fn sleep(
            &mut self,
            schedule_at: HistoryEventScheduleAt,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<host_exports::v3_0_0::obelisk::types::time::Datetime> {
            let expires_at = self.persist_sleep(schedule_at, wasm_backtrace).await?;
            Ok(host_exports::v3_0_0::obelisk::types::time::Datetime::try_from(expires_at)?)
        }

        pub(crate) async fn new_join_set_named(
            &mut self,
            name: String,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<
            Result<
                Resource<JoinSetId>,
                host_exports::v3_0_0::obelisk::workflow::workflow_support::JoinSetCreateError,
            >,
        > {
            match self
                .persist_join_set_with_kind(
                    name,
                    JoinSetKind::Named,
                    concepts::ClosingStrategy::Complete,
                    wasm_backtrace,
                )
                .await
            {
                Ok(resource) => Ok(Ok(resource)),
                Err(JoinSetCreateError::InvalidNameError(err)) => {
                    Ok(Err(host_exports::v3_0_0::obelisk::workflow::workflow_support::JoinSetCreateError::InvalidName(err.to_string())))
                }
                Err(JoinSetCreateError::Conflict) => {
                    Ok(Err(host_exports::v3_0_0::obelisk::workflow::workflow_support::JoinSetCreateError::Conflict))
                }
                Err(JoinSetCreateError::ApplyError(apply_err)) => {
                    // db errors etc
                    Err(wasmtime::Error::new(WorkflowFunctionError::from(apply_err)))
                }
                Err(JoinSetCreateError::ResourceTableError(resource_table_error)) => {
                    // trap
                    Err(wasmtime::Error::new(resource_table_error))
                }
            }
        }

        pub(crate) async fn new_join_set_generated(
            &mut self,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> wasmtime::Result<Resource<JoinSetId>> {
            let name = self.event_history.next_join_set_name_generated();
            trace!("new_join_set_generated: {name}");
            self.persist_join_set_with_kind(
                name,
                JoinSetKind::Generated,
                concepts::ClosingStrategy::Complete,
                wasm_backtrace,
            )
            .await
            .map_err(|err| match err {
                JoinSetCreateError::InvalidNameError(_) | JoinSetCreateError::Conflict => {
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

        pub(crate) async fn join_next(
            &mut self,
            join_set_id: JoinSetId,
            wasm_backtrace: Option<storage::WasmBacktrace>,
        ) -> Result<
            Result<types_execution::ResponseId, workflow_support::JoinNextError>,
            WorkflowFunctionError,
        > {
            JoinNext {
                join_set_id,
                closing: false,
                wasm_backtrace,
            }
            .apply(
                &mut self.event_history,
                self.db_pool.connection().as_ref(),
                &mut self.version,
                self.clock_fn.now(),
            )
            .await
            .map_err(WorkflowFunctionError::from)
        }
    }
}

fn trace_on_replay<C: ClockFn>(ctx: &WorkflowCtx<C>, message: &str) -> bool {
    if ctx.event_history.has_unprocessed_requests() {
        ctx.component_logger.trace(&format!("(replay) {message}"));
        true
    } else {
        false
    }
}

impl<C: ClockFn> log_activities::obelisk::log::log::Host for WorkflowCtx<C> {
    fn trace(&mut self, message: String) {
        if !trace_on_replay(self, &message) {
            self.component_logger.trace(&message);
        }
    }

    fn debug(&mut self, message: String) {
        if !trace_on_replay(self, &message) {
            self.component_logger.debug(&message);
        }
    }

    fn info(&mut self, message: String) {
        if !trace_on_replay(self, &message) {
            self.component_logger.info(&message);
        }
    }

    fn warn(&mut self, message: String) {
        if !trace_on_replay(self, &message) {
            self.component_logger.warn(&message);
        }
    }

    fn error(&mut self, message: String) {
        if !trace_on_replay(self, &message) {
            self.component_logger.error(&message);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::testing_fn_registry::fn_registry_dummy;
    use crate::workflow::deadline_tracker::DeadlineTrackerTokio;
    use crate::workflow::event_history::ApplyError;
    use crate::workflow::host_exports::SUFFIX_FN_SUBMIT;
    use crate::workflow::workflow_ctx::{
        DirectFnCall, ImportedFnCall, SubmitExecutionFnCall, WorkerPartialResult,
    };
    use crate::{
        workflow::workflow_ctx::WorkflowCtx, workflow::workflow_worker::JoinNextBlockingStrategy,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use chrono::DateTime;
    use concepts::prefixed_ulid::{ExecutionIdDerived, ExecutorId, RunId};
    use concepts::storage::{
        AppendEventsToExecution, AppendRequest, AppendResponseToExecution, CreateRequest,
        DbExecutor, DbPool, ExecutionEvent, ExecutionEventInner, HistoryEvent,
        HistoryEventScheduleAt, JoinSetRequest, JoinSetResponse, PendingState,
        PendingStateFinished, PendingStateFinishedResultKind,
    };
    use concepts::storage::{
        DbPoolCloseable, ExecutionLog, JoinSetResponseEvent, JoinSetResponseEventOuter,
    };
    use concepts::time::{ClockFn, Now};
    use concepts::{
        ComponentId, ComponentRetryConfig, ExecutionMetadata, FunctionRegistry, IfcFqnName,
        JoinSetId, JoinSetKind, RETURN_TYPE_DUMMY, SUFFIX_PKG_EXT, SUPPORTED_RETURN_VALUE_OK_EMPTY,
    };
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue};
    use concepts::{FunctionMetadata, ParameterTypes};
    use db_tests::Database;
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

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    pub const FFQN_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    impl From<WorkerPartialResult> for WorkerResult {
        fn from(worker_partial_result: WorkerPartialResult) -> Self {
            match worker_partial_result {
                WorkerPartialResult::FatalError(err, version) => {
                    WorkerResult::Err(executor::worker::WorkerError::FatalError(err, version))
                }
                WorkerPartialResult::InterruptDbUpdated => WorkerResult::DbUpdatedByWorkerOrWatcher,
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
        JoinSetCreateNamed {
            join_set_id: JoinSetId,
        },
        SubmitExecution {
            target_ffqn: FunctionFqn,
        },
        SubmitDelay {
            millis: u32, // Avoid ScheduleAtConversionError::OutOfRangeError
            join_set_id: Option<JoinSetId>,
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
                    return_type: RETURN_TYPE_DUMMY,
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

            let deadline_duration = (ctx.execution_deadline - self.clock_fn.now())
                .to_std()
                .unwrap();

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
                Arc::new(DeadlineTrackerTokio::new(deadline_duration)),
                self.fn_registry.clone(),
            );
            for step in &self.steps {
                info!("Processing step {step:?}");
                let res = match step {
                    WorkflowStep::Sleep { millis } => workflow_ctx
                        .persist_sleep(
                            HistoryEventScheduleAt::In(Duration::from_millis(u64::from(*millis))),
                            None,
                        )
                        .await
                        .map(|_| ()),
                    WorkflowStep::Call { ffqn } => {
                        let (_fn_metadata, _fn_component_id) =
                            self.fn_registry.get_by_exported_function(ffqn).expect(
                                "function obtained from `fn_registry.all_exports()` must be found",
                            );
                        workflow_ctx
                            .call_imported_fn(
                                ImportedFnCall::Direct(DirectFnCall {
                                    ffqn: ffqn.clone(),
                                    params: &[],
                                    wasm_backtrace: None,
                                }),
                                self.clock_fn.now(),
                                ffqn,
                            )
                            .await
                            .map(|_| ())
                    }

                    WorkflowStep::JoinSetCreateNamed { join_set_id } => {
                        assert_eq!(JoinSetKind::Named, join_set_id.kind);
                        workflow_ctx
                            .new_join_set_named(join_set_id.name.to_string(), None)
                            .await
                            .unwrap()
                            .unwrap();
                        Ok(())
                    }

                    WorkflowStep::SubmitExecution { target_ffqn } => {
                        // Create new join set
                        let join_set_resource =
                            workflow_ctx.new_join_set_generated(None).await.unwrap();
                        let join_set_id = workflow_ctx
                            .resource_table
                            .get(&join_set_resource)
                            .unwrap()
                            .clone();
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
                        let (_fn_metadata, _target_component_id) = self
                            .fn_registry
                            .get_by_exported_function(target_ffqn)
                            .expect(
                                "function obtained from `fn_registry.all_exports()` must be found",
                            );
                        workflow_ctx
                            .call_imported_fn(
                                ImportedFnCall::SubmitExecution(SubmitExecutionFnCall {
                                    target_ffqn: target_ffqn.clone(),
                                    join_set_id,
                                    target_params: &[],
                                    wasm_backtrace: None,
                                }),
                                self.clock_fn.now(),
                                &submit_ffqn,
                            )
                            .await
                            .map(|_| ())
                    }
                    WorkflowStep::SubmitDelay {
                        millis,
                        join_set_id,
                    } => {
                        // Create new join set
                        let join_set_id = if let Some(join_set_id) = join_set_id {
                            join_set_id.clone()
                        } else {
                            let join_set_resource =
                                workflow_ctx.new_join_set_generated(None).await.unwrap();
                            workflow_ctx
                                .resource_table
                                .get(&join_set_resource)
                                .unwrap()
                                .clone()
                        };
                        workflow_ctx
                            .submit_delay(
                                join_set_id,
                                HistoryEventScheduleAt::In(Duration::from_millis(u64::from(
                                    *millis,
                                ))),
                                None,
                            )
                            .await
                            .map(|_| ())
                    }
                    WorkflowStep::RandomU64 { min, max_inclusive } => {
                        let value = workflow_ctx
                            .random_u64_inclusive(*min, *max_inclusive, None)
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
                            .random_string(*min_length, *max_length_exclusive, None)
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
            let res = match workflow_ctx.close_forgotten_join_sets().await {
                Ok(()) => {
                    info!("Finishing");
                    WorkerResult::Ok(SUPPORTED_RETURN_VALUE_OK_EMPTY, workflow_ctx.version, None)
                }
                Err(ApplyError::InterruptDbUpdated) => {
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
                let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
                let mut seedable_rng = StdRng::seed_from_u64(seed);
                let next_u128 = || rand::Rng::r#gen(&mut seedable_rng);
                let res =
                    execute_steps(steps, db_pool.clone(), db_exec, &mut sim_clock, next_u128).await;
                db_close.close().await;
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
                let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
                let mut seedable_rng = StdRng::seed_from_u64(seed);
                let next_u128 = || rand::Rng::r#gen(&mut seedable_rng);
                let (execution_id, execution_log) = execute_steps(
                    steps.clone(),
                    db_pool.clone(),
                    db_exec,
                    &mut SimClock::epoch(),
                    next_u128,
                )
                .await;
                println!("{execution_log:?}");
                db_close.close().await;
                (execution_id, execution_log)
            };
            println!("Run 2");
            let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
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
                    retry_config: ComponentRetryConfig::ZERO,
                };
                ExecTask::new_test(
                    worker,
                    exec_config,
                    sim_clock.clone(),
                    db_exec,
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
                debug!("Ticking");
                let executed = workflow_exec
                    .tick_test(
                        sim_clock.now(),
                        RunId::from_parts(
                            u64::try_from(sim_clock.now().timestamp_millis()).unwrap(),
                            0,
                        ),
                    )
                    .await
                    .wait_for_tasks()
                    .await
                    .len();
                assert!(executed > 0);
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
            db_close.close().await;
        }
    }

    #[tokio::test]
    async fn creating_oneoff_and_generated_join_sets_with_same_name_should_work() {
        test_utils::set_up();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let steps = vec![
            WorkflowStep::Call {
                ffqn: FFQN_CHILD_MOCK,
            },
            WorkflowStep::SubmitExecution {
                target_ffqn: FFQN_CHILD_MOCK,
            },
        ];
        execute_steps(
            steps,
            db_pool.clone(),
            db_exec,
            &mut SimClock::epoch(),
            || 0,
        )
        .await;
        db_close.close().await;
    }

    #[tokio::test]
    async fn submitting_two_delays_should_work() {
        test_utils::set_up();
        let (_guard, db_pool, db_exec, db_close) = Database::Memory.set_up().await;
        let join_set_id = JoinSetId::new(concepts::JoinSetKind::Named, "".into()).unwrap();

        let steps = vec![
            WorkflowStep::JoinSetCreateNamed {
                join_set_id: join_set_id.clone(),
            },
            WorkflowStep::SubmitDelay {
                millis: 1,
                join_set_id: Some(join_set_id.clone()),
            },
            WorkflowStep::SubmitDelay {
                millis: 10,
                join_set_id: Some(join_set_id),
            },
        ];
        execute_steps(
            steps,
            db_pool.clone(),
            db_exec,
            &mut SimClock::epoch(),
            || 0,
        )
        .await;
        db_close.close().await;
    }

    const FFQN_CHILD_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn-child");

    #[tokio::test]
    #[expect(clippy::search_is_some)]
    async fn check_determinism_closing_multiple_join_sets() {
        const SUBMITS: usize = 10;
        test_utils::set_up();
        let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
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
                            result: finished_value,
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
        db_close.close().await;
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
        // TODO: the test harness supports a single child/delay request per join set
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
        db_exec: Arc<dyn DbExecutor>,
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
                retry_config: ComponentRetryConfig::ZERO,
            };
            ExecTask::new_test(
                worker,
                exec_config,
                sim_clock.clone(),
                db_exec,
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
                .wait_for_tasks()
                .await;
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
                        info!("DelayRequest {delay_id} moving time to {expires_at}");
                        assert!(delay_request_count > 0);
                        sim_clock.move_time_to(expires_at);
                        delay_request_count -= 1;
                        let actual_progress =
                            expired_timers_watcher::tick_test(db_connection.as_ref(), expires_at)
                                .await
                                .unwrap();
                        assert!(actual_progress.expired_async_timers > 0); // Ignore SubmitDelay-s that were fullfiled by the watcher.
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
                AppendEventsToExecution {
                    execution_id: ExecutionId::Derived(child_execution_id.clone()),
                    version: child_log.next_version.clone(),
                    batch: vec![AppendRequest {
                        created_at: sim_clock.now(),
                        event: concepts::storage::ExecutionEventInner::Finished {
                            result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                            http_client_traces: None,
                        },
                    }],
                },
                AppendResponseToExecution {
                    parent_execution_id,
                    parent_response_event: JoinSetResponseEventOuter {
                        created_at: sim_clock.now(),
                        event: JoinSetResponseEvent {
                            join_set_id,
                            event: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: child_execution_id.clone(),
                                finished_version: child_log.next_version.clone(),
                                result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                            },
                        },
                    },
                },
                sim_clock.now(),
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
        assert_matches!(child_res, SupportedFunctionReturnValue::Ok { ok: None });
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
