use super::event_history::{ApplyError, ChildReturnValue, EventCall, EventHistory, HostResource};
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::component_logger::{log_activities, ComponentLogger};
use crate::host_exports::{
    val_to_join_set_id, SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_SCHEDULE, SUFFIX_FN_SUBMIT,
};
use crate::{host_exports, WasmFileError};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, ExecutionIdDerived};
use concepts::storage::{self, DbConnection, DbError, DbPool, HistoryEventScheduledAt, Version};
use concepts::storage::{HistoryEvent, JoinSetResponseEvent};
use concepts::time::ClockFn;
use concepts::{
    ClosingStrategy, ComponentId, ExecutionId, FunctionRegistry, IfcFqnName, StrVariant,
};
use concepts::{FunctionFqn, Params};
use concepts::{JoinSetId, JoinSetKind};
use executor::worker::FatalError;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, instrument, trace, Span};
use val_json::wast_val::WastVal;
use wasmtime::component::{Linker, Resource, Val};

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
    #[error("join set already exists with name `{0}`")]
    JoinSetNameConflict(String),
    // retriable errors:
    #[error("interrupt requested")]
    InterruptRequested,
    #[error(transparent)]
    DbError(DbError),
}

pub(crate) struct InterruptRequested;

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
            WorkflowFunctionError::JoinSetNameConflict(name) => {
                WorkerPartialResult::FatalError(FatalError::JoinSetNameConflict { name }, version)
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

pub(crate) struct WorkflowCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    pub(crate) execution_id: ExecutionId,
    event_history: EventHistory<C>,
    rng: StdRng,
    pub(crate) clock_fn: C,
    pub(crate) db_pool: P,
    pub(crate) version: Version,
    fn_registry: Arc<dyn FunctionRegistry>,
    component_logger: ComponentLogger,
    pub(crate) resource_table: wasmtime::component::ResourceTable,
    phantom_data: PhantomData<DB>,
}

#[derive(Debug)]
pub(crate) enum ImportedFnCall<'a> {
    Direct {
        ffqn: FunctionFqn,
        params: &'a [Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Schedule {
        target_ffqn: FunctionFqn,
        scheduled_at: HistoryEventScheduledAt,
        target_params: &'a [Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Submit {
        target_ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        target_params: &'a [Val],
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    AwaitNext {
        target_ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
}

impl<'a> ImportedFnCall<'a> {
    fn extract_join_set_id<'ctx, C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
        called_ffqn: &FunctionFqn,
        store_ctx: &'ctx mut wasmtime::StoreContextMut<'a, WorkflowCtx<C, DB, P>>,
        params: &'a [Val],
    ) -> Result<(JoinSetId, &'a [Val]), String> {
        let Some((join_set_id, params)) = params.split_first() else {
            error!("Got empty params, expected JoinSetId");
            return Err(format!(
                "error running {called_ffqn} extension function: exepcted at least one parameter with JoinSetId, got empty parameter list"
            ));
        };
        let join_set_id = val_to_join_set_id(join_set_id, store_ctx)
            .map_err(|err| format!("error running {called_ffqn} extension function: {err:?}"))?;
        Ok((join_set_id, params))
    }

    #[instrument(skip_all, fields(ffqn = %called_ffqn, otel.name = format!("imported_fn_call {called_ffqn}")), name = "imported_fn_call")]
    pub(crate) fn new<'ctx, C: ClockFn, DB: DbConnection, P: DbPool<DB>>(
        called_ffqn: FunctionFqn,
        store_ctx: &'ctx mut wasmtime::StoreContextMut<'a, WorkflowCtx<C, DB, P>>,
        params: &'a [Val],
    ) -> Result<ImportedFnCall<'a>, WorkflowFunctionError> {
        let wasm_backtrace = wasmtime::WasmBacktrace::capture(&store_ctx);
        let wasm_backtrace = if wasm_backtrace.frames().is_empty() {
            None
        } else {
            Some(concepts::storage::WasmBacktrace::from(wasm_backtrace))
        };

        if let Some(package_name) = called_ffqn.ifc_fqn.package_strip_extension_suffix() {
            let ifc_fqn = IfcFqnName::from_parts(
                called_ffqn.ifc_fqn.namespace(),
                package_name,
                called_ffqn.ifc_fqn.ifc_name(),
                called_ffqn.ifc_fqn.version(),
            );

            if let Some(function_name) = called_ffqn.function_name.strip_suffix(SUFFIX_FN_SUBMIT) {
                let target_ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                let (join_set_id, params) =
                    Self::extract_join_set_id(&called_ffqn, store_ctx, params).map_err(
                        |detail| WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: StrVariant::Static("cannot extract join set id"),
                            detail: Some(detail),
                        },
                    )?;
                Ok(ImportedFnCall::Submit {
                    target_ffqn,
                    join_set_id,
                    target_params: params,
                    wasm_backtrace,
                })
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                let target_ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                let (join_set_id, params) =
                    match Self::extract_join_set_id(&called_ffqn, store_ctx, params) {
                        Ok(ok) => ok,
                        Err(err) => {
                            return Err(WorkflowFunctionError::ImportedFunctionCallError {
                                ffqn: called_ffqn,
                                reason: StrVariant::Static("cannot extract join set id"),
                                detail: Some(err),
                            });
                        }
                    };
                if !params.is_empty() {
                    return Err(WorkflowFunctionError::ImportedFunctionCallError {
                        reason: StrVariant::Static("wrong parameter length"),
                        detail: Some(format!(
                            "error running {called_ffqn}: wrong parameter length, expected single string parameter containing join-set-id, got {} other parameters",
                            params.len()
                        )),
                        ffqn: called_ffqn,
                    });
                };
                Ok(ImportedFnCall::AwaitNext {
                    target_ffqn,
                    join_set_id,
                    wasm_backtrace,
                })
            } else if let Some(function_name) =
                called_ffqn.function_name.strip_suffix(SUFFIX_FN_SCHEDULE)
            {
                let target_ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                let Some((scheduled_at, params)) = params.split_first() else {
                    return Err(WorkflowFunctionError::ImportedFunctionCallError {
                        ffqn: called_ffqn,
                        reason: StrVariant::Static(
                            "exepcted at least one parameter of type `scheduled-at`",
                        ),
                        detail: None,
                    });
                };
                let scheduled_at = match WastVal::try_from(scheduled_at.clone()) {
                    Ok(ok) => ok,
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: StrVariant::Static(
                                "cannot convert `scheduled-at` to internal representation",
                            ),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };
                let scheduled_at = match HistoryEventScheduledAt::try_from(&scheduled_at) {
                    Ok(scheduled_at) => scheduled_at,
                    Err(err) => {
                        return Err(WorkflowFunctionError::ImportedFunctionCallError {
                            ffqn: called_ffqn,
                            reason: StrVariant::Static(
                                "first parameter type must be `scheduled-at`",
                            ),
                            detail: Some(format!("{err:?}")),
                        });
                    }
                };

                Ok(ImportedFnCall::Schedule {
                    target_ffqn,
                    scheduled_at,
                    target_params: params,
                    wasm_backtrace,
                })
            } else {
                error!("Unrecognized extension function {called_ffqn}");
                return Err(WorkflowFunctionError::ImportedFunctionCallError {
                    ffqn: called_ffqn,
                    reason: StrVariant::Static("unrecognized extension function"),
                    detail: None,
                });
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
            | Self::Submit {
                target_ffqn: ffqn, ..
            }
            | Self::AwaitNext {
                target_ffqn: ffqn, ..
            } => ffqn,
        }
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowCtx<C, DB, P> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        component_id: ComponentId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        seed: u64,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        db_pool: P,
        version: Version,
        execution_deadline: DateTime<Utc>,
        non_blocking_event_batching: u32,
        interrupt_on_timeout_container: Arc<std::sync::Mutex<Option<InterruptRequested>>>,
        fn_registry: Arc<dyn FunctionRegistry>,
        worker_span: Span,
        forward_unhandled_child_errors_in_join_set_close: bool,
    ) -> Self {
        Self {
            execution_id: execution_id.clone(),
            event_history: EventHistory::new(
                execution_id,
                component_id,
                event_history,
                responses,
                join_next_blocking_strategy,
                execution_deadline,
                non_blocking_event_batching,
                clock_fn.clone(),
                interrupt_on_timeout_container,
                worker_span.clone(),
                forward_unhandled_child_errors_in_join_set_close,
            ),
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            db_pool,
            version,
            fn_registry,
            component_logger: ComponentLogger { span: worker_span },
            resource_table: wasmtime::component::ResourceTable::default(),
            phantom_data: PhantomData,
        }
    }

    pub(crate) async fn flush(&mut self) -> Result<(), DbError> {
        self.event_history.flush(&self.db_pool.connection()).await
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(ffqn = %imported_fn_call.ffqn()))]
    pub(crate) async fn call_imported_fn(
        &mut self,
        imported_fn_call: ImportedFnCall<'_>,
        results: &mut [Val],
        called_ffqn: FunctionFqn,
    ) -> Result<(), WorkflowFunctionError> {
        trace!(?imported_fn_call, "call_imported_fn start");
        let event_call = self.imported_fn_to_event_call(imported_fn_call);
        let res = self
            .event_history
            .apply(
                event_call,
                &self.db_pool.connection(),
                &mut self.version,
                self.fn_registry.as_ref(),
            )
            .await?;
        let res = res.into_wast_val();
        match (results.len(), res) {
            (0, None) => {}
            (1, Some(res)) => {
                results[0] = res.as_val();
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

    async fn persist_sleep(&mut self, duration: Duration) -> Result<(), WorkflowFunctionError> {
        let join_set_id = self.next_join_set_one_off();
        let delay_id = DelayId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        self.event_history
            .apply(
                EventCall::BlockingDelayRequest {
                    join_set_id,
                    delay_id,
                    expires_at_if_new: self.clock_fn.now() + duration, // FIXME: this can overflow when Duration is converted into TimeDelta
                },
                &self.db_pool.connection(),
                &mut self.version,
                self.fn_registry.as_ref(),
            )
            .await?;
        Ok(())
    }

    // Must be persisted by the caller.
    fn next_u128(&mut self) -> u128 {
        rand::Rng::r#gen(&mut self.rng)
    }

    fn next_join_set_name_index(&mut self, kind: JoinSetKind) -> String {
        assert!(kind != JoinSetKind::Named);
        self.event_history.join_set_count(kind).to_string()
    }

    fn next_join_set_one_off(&mut self) -> JoinSetId {
        JoinSetId::new(
            JoinSetKind::OneOff,
            StrVariant::from(self.next_join_set_name_index(JoinSetKind::OneOff)),
        )
        .expect("next_string_random returns valid join set name")
    }

    async fn persist_join_set_with_kind(
        &mut self,
        name: String,
        kind: JoinSetKind,
        closing_strategy: ClosingStrategy,
    ) -> wasmtime::Result<Resource<JoinSetId>> {
        if !self.event_history.join_set_name_exists(&name, kind) {
            let join_set_id = JoinSetId::new(kind, StrVariant::from(name))?;
            let res = self
                .event_history
                .apply(
                    EventCall::CreateJoinSet {
                        join_set_id,
                        closing_strategy,
                    },
                    &self.db_pool.connection(),
                    &mut self.version,
                    self.fn_registry.as_ref(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let join_set_id = assert_matches!(res,
                ChildReturnValue::HostResource(HostResource::CreateJoinSetResp(join_set_id)) => join_set_id);
            let join_set_id = self.resource_table.push(join_set_id)?;
            Ok(join_set_id)
        } else {
            Err(wasmtime::Error::new(
                WorkflowFunctionError::JoinSetNameConflict(name),
            ))
        }
    }

    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), WasmFileError> {
        host_exports::obelisk::workflow::workflow_support::add_to_linker(
            linker,
            |state: &mut Self| state,
        )
        .map_err(|err| WasmFileError::LinkingError {
            context: StrVariant::Static("linking host activities"),
            err: err.into(),
        })?;
        log_activities::obelisk::log::log::add_to_linker(linker, |state: &mut Self| state)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking log activities"),
                err: err.into(),
            })?;
        host_exports::obelisk::types::execution::add_to_linker(linker, |state: &mut Self| state)
            .map_err(|err| WasmFileError::LinkingError {
                context: StrVariant::Static("linking join set resource"),
                err: err.into(),
            })?;
        Ok(())
    }

    pub(crate) async fn close_opened_join_sets(&mut self) -> Result<(), ApplyError> {
        self.event_history
            .close_opened_join_sets(
                &self.db_pool.connection(),
                &mut self.version,
                self.fn_registry.as_ref(),
            )
            .await
    }

    fn next_child_id(&mut self, join_set_id: &JoinSetId) -> ExecutionIdDerived {
        let count = self.event_history.execution_count(join_set_id);
        self.execution_id
            .next_level(join_set_id)
            .get_incremented_by(u64::try_from(count).unwrap())
    }

    fn imported_fn_to_event_call(&mut self, imported_fn_call: ImportedFnCall) -> EventCall {
        match imported_fn_call {
            ImportedFnCall::Direct {
                ffqn,
                params,
                wasm_backtrace,
            } => {
                let join_set_id = self.next_join_set_one_off();
                let child_execution_id = self.execution_id.next_level(&join_set_id);
                EventCall::BlockingChildDirectCall {
                    ffqn,
                    join_set_id,
                    params: Params::from_wasmtime(Arc::from(params)),
                    child_execution_id,
                    wasm_backtrace,
                }
            }
            ImportedFnCall::Schedule {
                target_ffqn,
                scheduled_at,
                target_params,
                wasm_backtrace,
            } => {
                // TODO(edge case): handle ExecutionId conflict: This does not have to be deterministicly generated.
                // Remove execution_id from EventCall::ScheduleRequest and add retries.
                // Or use ExecutionId::generate(), but ignore the id when checking determinism.
                let execution_id =
                    ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
                EventCall::ScheduleRequest {
                    scheduled_at,
                    execution_id,
                    ffqn: target_ffqn,
                    params: Params::from_wasmtime(Arc::from(target_params)),
                    wasm_backtrace,
                }
            }
            ImportedFnCall::Submit {
                target_ffqn,
                join_set_id,
                target_params,
                wasm_backtrace,
            } => {
                let child_execution_id = self.next_child_id(&join_set_id);
                EventCall::StartAsync {
                    ffqn: target_ffqn,
                    join_set_id,
                    params: Params::from_wasmtime(Arc::from(target_params)),
                    child_execution_id,
                    wasm_backtrace,
                }
            }
            ImportedFnCall::AwaitNext {
                target_ffqn: _, // Currently multiple functions are not supported in one join set.
                join_set_id,
                wasm_backtrace,
            } => EventCall::BlockingChildAwaitNext {
                join_set_id,
                closing: false,
                wasm_backtrace,
            },
        }
    }
}

mod workflow_support {
    use super::{
        assert_matches, ClockFn, DbConnection, DbPool, Duration, EventCall, WorkflowCtx,
        WorkflowFunctionError,
    };
    use crate::{
        host_exports::{
            self, obelisk::workflow::workflow_support::ClosingStrategy as WitClosingStrategy,
            DurationEnum,
        },
        workflow::event_history::ChildReturnValue,
    };
    use concepts::{
        random_string,
        storage::{self, PersistKind},
        JoinSetId, JoinSetKind, CHARSET_ALPHANUMERIC,
    };
    use tracing::{trace, warn};
    use val_json::wast_val::WastVal;
    use wasmtime::component::Resource;

    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>>
        host_exports::obelisk::types::execution::HostJoinSetId for WorkflowCtx<C, DB, P>
    {
        async fn drop(&mut self, resource: Resource<JoinSetId>) -> wasmtime::Result<()> {
            self.resource_table.delete(resource)?;
            Ok(())
        }
    }

    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> host_exports::obelisk::types::execution::Host
        for WorkflowCtx<C, DB, P>
    {
    }

    // TODO: Capture and persist backtrace for host functions.
    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>>
        host_exports::obelisk::workflow::workflow_support::Host for WorkflowCtx<C, DB, P>
    {
        async fn random_u64(&mut self, min: u64, max_exclusive: u64) -> wasmtime::Result<u64> {
            self.random_u64_inclusive(min, max_exclusive + 1).await
        }

        async fn random_u64_inclusive(
            &mut self,
            min: u64,
            max_inclusive: u64,
        ) -> wasmtime::Result<u64> {
            let value = rand::Rng::gen_range(&mut self.rng, min..=max_inclusive);
            let value = Vec::from(storage::from_u64_to_bytes(value));
            let value = self
                .event_history
                .apply(
                    EventCall::Persist {
                        value,
                        kind: PersistKind::RandomU64 { min, max_inclusive },
                    },
                    &self.db_pool.connection(),
                    &mut self.version,
                    self.fn_registry.as_ref(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let value =
                assert_matches!(value, ChildReturnValue::WastVal(WastVal::U64(value)) => value);
            Ok(value)
        }

        async fn random_string(
            &mut self,
            min_length: u16,
            max_length_exclusive: u16,
        ) -> wasmtime::Result<String> {
            let value = random_string(
                &mut self.rng,
                min_length,
                max_length_exclusive,
                CHARSET_ALPHANUMERIC,
            );
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
                    },
                    &self.db_pool.connection(),
                    &mut self.version,
                    self.fn_registry.as_ref(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let value =
                assert_matches!(value, ChildReturnValue::WastVal(WastVal::String(value)) => value);
            Ok(value)
        }

        // TODO: Apply jitter, should be configured on the component level
        async fn sleep(&mut self, duration: DurationEnum) -> wasmtime::Result<()> {
            Ok(self.persist_sleep(Duration::from(duration)).await?)
        }

        async fn new_join_set_named(
            &mut self,
            name: String,
            closing_strategy: WitClosingStrategy,
        ) -> wasmtime::Result<Resource<JoinSetId>> {
            match closing_strategy {
                WitClosingStrategy::Complete => {}
                WitClosingStrategy::Cancel => {
                    warn!("ClosingStrategy::Cancel is not supported yet, using ClosingStrategy::Complete");
                }
            }
            self.persist_join_set_with_kind(
                name,
                JoinSetKind::Named,
                concepts::ClosingStrategy::Complete,
            )
            .await
        }

        async fn new_join_set_generated(
            &mut self,
            closing_strategy: WitClosingStrategy,
        ) -> wasmtime::Result<Resource<JoinSetId>> {
            match closing_strategy {
                WitClosingStrategy::Complete => {}
                WitClosingStrategy::Cancel => {
                    warn!("ClosingStrategy::Cancel is not supported yet, using ClosingStrategy::Complete");
                }
            }
            let name = self.next_join_set_name_index(JoinSetKind::Generated);
            trace!("new_join_set_generated: {name}");
            self.persist_join_set_with_kind(
                name,
                JoinSetKind::Generated,
                concepts::ClosingStrategy::Complete,
            )
            .await
        }
    }
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> log_activities::obelisk::log::log::Host
    for WorkflowCtx<C, DB, P>
{
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

#[cfg(madsim)]
#[cfg(test)]
pub(crate) mod tests {
    use crate::host_exports::obelisk::workflow::workflow_support::Host as _;
    use crate::host_exports::SUFFIX_FN_SUBMIT;
    use crate::workflow::workflow_ctx::ApplyError;
    use crate::workflow::workflow_ctx::{ImportedFnCall, WorkerPartialResult};
    use crate::{
        tests::fn_registry_dummy, workflow::workflow_ctx::WorkflowCtx,
        workflow::workflow_worker::JoinNextBlockingStrategy,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::prefixed_ulid::{ExecutionIdDerived, RunId};
    use concepts::storage::{
        wait_for_pending_state_fn, AppendRequest, CreateRequest, DbConnection, DbPool,
        HistoryEvent, JoinSetRequest, JoinSetResponse, PendingState, PendingStateFinished,
        PendingStateFinishedResultKind,
    };
    use concepts::storage::{ExecutionLog, JoinSetResponseEvent, JoinSetResponseEventOuter};
    use concepts::time::{ClockFn, Now};
    use concepts::{ComponentId, ExecutionMetadata, FunctionRegistry, IfcFqnName, SUFFIX_PKG_EXT};
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionReturnValue};
    use concepts::{FunctionMetadata, ParameterTypes};
    use db_tests::Database;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerContext, WorkerResult},
    };
    use std::{fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::{debug, info, info_span};
    use wasmtime::component::Val;

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    pub const FFQN_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    impl From<WorkerPartialResult> for WorkerResult {
        fn from(worker_partial_result: WorkerPartialResult) -> Self {
            match worker_partial_result {
                WorkerPartialResult::FatalError(err, version) => {
                    WorkerResult::Err(executor::worker::WorkerError::FatalError(err, version))
                }
                WorkerPartialResult::InterruptRequested => WorkerResult::DbUpdatedByWorker,
                WorkerPartialResult::DbError(db_err) => {
                    WorkerResult::Err(executor::worker::WorkerError::DbError(db_err))
                }
            }
        }
    }

    #[derive(Debug, Clone, arbitrary::Arbitrary)]
    enum WorkflowStep {
        Sleep {
            millis: u32,
        },
        Call {
            ffqn: FunctionFqn,
        },
        SubmitWithoutAwait {
            target_ffqn: FunctionFqn,
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
    struct WorkflowWorkerMock<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
        #[expect(dead_code)]
        ffqn: FunctionFqn, // For debugging
        steps: Vec<WorkflowStep>,
        #[debug(skip)]
        clock_fn: C,
        #[debug(skip)]
        db_pool: P,
        #[debug(skip)]
        fn_registry: Arc<dyn FunctionRegistry>,
        #[debug(skip)]
        phantom_data: PhantomData<DB>,
        #[debug(skip)]
        exports: [FunctionMetadata; 1],
    }

    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowWorkerMock<C, DB, P> {
        fn new(
            ffqn: FunctionFqn,
            fn_registry: Arc<dyn FunctionRegistry>,
            steps: Vec<WorkflowStep>,
            clock_fn: C,
            db_pool: P,
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
                phantom_data: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
        for WorkflowWorkerMock<C, DB, P>
    {
        #[expect(clippy::too_many_lines)]
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
                0, // TODO: parametrize batch size
                Arc::new(std::sync::Mutex::new(None)),
                self.fn_registry.clone(),
                tracing::info_span!("workflow-test"),
                false,
            );
            for step in &self.steps {
                info!("Processing step {step:?}");
                let res = match step {
                    WorkflowStep::Sleep { millis } => {
                        workflow_ctx
                            .persist_sleep(Duration::from_millis(u64::from(*millis)))
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
                            )
                            .await
                    }
                    WorkflowStep::SubmitWithoutAwait { target_ffqn } => {
                        // Create new join set
                        let join_set_resource = workflow_ctx
                            .new_join_set_generated(crate::host_exports::obelisk::workflow::workflow_support::ClosingStrategy::Complete)
                            .await
                            .unwrap();
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
                                ImportedFnCall::Submit {
                                    target_ffqn: target_ffqn.clone(),
                                    join_set_id,
                                    target_params: &[],
                                    wasm_backtrace: None,
                                },
                                &mut ret_val,
                                submit_ffqn,
                            )
                            .await
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
                    return WorkerResult::DbUpdatedByWorker;
                }
                other => panic!("Unexpected error: {other:?}"),
            };
            info!("Done");
            res
        }

        fn exported_functions(&self) -> &[FunctionMetadata] {
            &self.exports
        }

        fn imported_functions(&self) -> &[FunctionMetadata] {
            &[]
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

    #[test]
    fn check_determinism() {
        test_utils::set_up();
        let mut builder_a = madsim::runtime::Builder::from_env();
        builder_a.check = false;
        info!("MADSIM_TEST_SEED={}", builder_a.seed);
        let mut builder_b = madsim::runtime::Builder::from_env(); // Builder: Clone would be useful
        builder_b.check = false;
        builder_b.seed = builder_a.seed;

        let closure = || async move {
            let (_guard, db_pool) = Database::Memory.set_up().await;
            let res = execute_steps(generate_steps(), &db_pool).await;
            db_pool.close().await.unwrap();
            res
        };
        assert_eq!(builder_a.run(closure), builder_b.run(closure));
    }

    #[tokio::test]
    async fn creating_oneoff_and_generated_join_sets_with_same_name_should_work() {
        test_utils::set_up();
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let steps = vec![
            WorkflowStep::Call {
                ffqn: FFQN_CHILD_MOCK,
            },
            WorkflowStep::SubmitWithoutAwait {
                target_ffqn: FFQN_CHILD_MOCK,
            },
        ];
        execute_steps(steps, &db_pool).await;
        db_pool.close().await.unwrap();
    }

    const FFQN_CHILD_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn-child");

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
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
            WorkflowStep::SubmitWithoutAwait {
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
                WorkerResult::DbUpdatedByWorker,
                "At least one -submit is expected"
            );
            // Run it SUBMITS times to close all join sets.
            for run in 0..SUBMITS {
                assert_matches!(worker_result, WorkerResult::DbUpdatedByWorker);
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
                        WorkerResult::DbUpdatedByWorker,
                        "At another -submit is expected"
                    );
                }
            }
            let (finished_value, version) = assert_matches!(
                worker_result,
                WorkerResult::Ok(finished_value, version, _http_client_trace) => (finished_value, version),
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
                            http_client_trace: None,
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

    fn generate_steps() -> Vec<WorkflowStep> {
        let unstructured_holder = UnstructuredHolder::new();
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
            WorkflowStep::Call { ffqn }
            | WorkflowStep::SubmitWithoutAwait { target_ffqn: ffqn } => {
                join_sets.insert(ffqn.clone()) // Retain only the first step for a given ffqn
            }
            _ => true,
        });
        steps
    }

    fn steps_to_registry(steps: &[WorkflowStep]) -> Arc<dyn FunctionRegistry> {
        let ffqns = steps
            .iter()
            .filter_map(|step| match step {
                WorkflowStep::Call { ffqn }
                | WorkflowStep::SubmitWithoutAwait { target_ffqn: ffqn } => Some(ffqn.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        fn_registry_dummy(ffqns.as_slice())
    }

    #[expect(clippy::too_many_lines)]
    async fn execute_steps<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
        steps: Vec<WorkflowStep>,
        db_pool: &P,
    ) -> (ExecutionId, ExecutionLog) {
        let created_at = Now.now();
        info!(now = %created_at, "Steps: {steps:?}");
        let execution_id = ExecutionId::generate();
        let sim_clock = SimClock::new(created_at);

        let mut child_execution_count = steps
            .iter()
            .filter(|step| {
                matches!(
                    step,
                    WorkflowStep::Call { .. } | WorkflowStep::SubmitWithoutAwait { .. }
                )
            })
            .count();
        let mut delay_request_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Sleep { .. }))
            .count();
        let timers_watcher_task = expired_timers_watcher::spawn_new(
            db_pool.clone(),
            expired_timers_watcher::TimersWatcherConfig {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clone(),
                leeway: Duration::ZERO,
            },
        );
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = steps_to_registry(&steps);

        let workflow_exec_task = {
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
            };
            ExecTask::spawn_new(
                worker,
                exec_config,
                sim_clock.clone(),
                db_pool.clone(),
                concepts::prefixed_ulid::ExecutorId::generate(),
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
        while let Some((join_set_id, join_set_req)) = wait_for_pending_state_fn(
            &db_connection,
            &execution_id,
            |execution_log| match &execution_log.pending_state {
                PendingState::BlockedByJoinSet { join_set_id, .. } => Some(Some((
                    join_set_id.clone(),
                    execution_log
                        .find_join_set_request(join_set_id)
                        .cloned()
                        .expect("must be found"),
                ))), // Execution is currently blocked, unblock it in the loop body.
                PendingState::Finished { .. } => Some(None), // Exit the while loop.
                _ => None,                                   // Ignore other states.
            },
            None,
        )
        .await
        .unwrap()
        {
            if processed.contains(&join_set_id) {
                continue;
            }

            match join_set_req {
                JoinSetRequest::DelayRequest {
                    delay_id,
                    expires_at,
                } => {
                    info!("Moving time to {expires_at} - {delay_id}");
                    assert!(delay_request_count > 0);
                    sim_clock.move_time_to(expires_at).await;
                    delay_request_count -= 1;
                }
                JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                    assert!(child_execution_count > 0);
                    exec_child(child_execution_id, db_pool.clone(), sim_clock.clone()).await;
                    child_execution_count -= 1;
                }
            }
            processed.push(join_set_id);
        }
        // must be finished at this point
        assert_eq!(0, child_execution_count);
        assert_eq!(0, delay_request_count);
        let execution_log = db_connection.get(&execution_id).await.unwrap();
        assert!(execution_log.pending_state.is_finished());
        drop(db_connection);
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        (execution_id, execution_log)
    }

    async fn exec_child<DB: DbConnection + 'static>(
        child_execution_id: ExecutionIdDerived,
        db_pool: impl DbPool<DB> + 'static,
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
                        http_client_trace: None,
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
}
