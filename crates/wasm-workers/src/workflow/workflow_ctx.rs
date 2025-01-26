use super::event_history::{ApplyError, EventCall, EventHistory};
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::component_logger::{log_activities, ComponentLogger};
use crate::host_exports::{SUFFIX_FN_AWAIT_NEXT, SUFFIX_FN_SCHEDULE, SUFFIX_FN_SUBMIT};
use crate::{host_exports, WasmFileError};
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{DbConnection, DbError, DbPool, HistoryEventScheduledAt, Version};
use concepts::storage::{HistoryEvent, JoinSetResponseEvent};
use concepts::{ExecutionId, FinishedExecutionError, FunctionRegistry, IfcFqnName, StrVariant};
use concepts::{FunctionFqn, Params};
use executor::worker::FatalError;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, instrument, trace, Span};
use utils::time::ClockFn;
use val_json::wast_val::WastVal;
use wasmtime::component::{Linker, Val};

/// Result that is passed from guest to host as an error, must be downcast from anyhow.
#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum WorkflowFunctionError {
    // fatal errors:
    #[error("non deterministic execution: {0}")]
    NondeterminismDetected(StrVariant),
    #[error("child finished with an execution error: {0}")]
    ChildExecutionError(FinishedExecutionError), // only on direct call
    #[error("uncategorized error - {0}")]
    UncategorizedError(&'static str), // Mostly used when an extension function cannot be called. Traps are handled in `RunError::Trap`.
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
            Self::InterruptRequested => WorkerPartialResult::InterruptRequested,
            Self::DbError(db_error) => WorkerPartialResult::DbError(db_error),
            // fatal errors:
            Self::NondeterminismDetected(reason) => {
                WorkerPartialResult::FatalError(FatalError::NondeterminismDetected(reason), version)
            }
            Self::ChildExecutionError(err) => {
                WorkerPartialResult::FatalError(FatalError::ChildExecutionError(err), version)
            }
            Self::UncategorizedError(reason) => WorkerPartialResult::FatalError(
                FatalError::UncategorizedError {
                    reason: reason.to_string(),
                    detail: String::new(),
                },
                version,
            ),
        }
    }
}

impl From<ApplyError> for WorkflowFunctionError {
    fn from(value: ApplyError) -> Self {
        match value {
            ApplyError::NondeterminismDetected(reason) => Self::NondeterminismDetected(reason),
            ApplyError::ChildExecutionError(finished_execution_error) => {
                Self::ChildExecutionError(finished_execution_error)
            }
            ApplyError::InterruptRequested => Self::InterruptRequested,
            ApplyError::DbError(db_error) => Self::DbError(db_error),
        }
    }
}

pub(crate) struct WorkflowCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    execution_id: ExecutionId,
    next_child_execution_id: ExecutionId,
    event_history: EventHistory<C>,
    rng: StdRng,
    pub(crate) clock_fn: C,
    db_pool: P,
    pub(crate) version: Version,
    fn_registry: Arc<dyn FunctionRegistry>,
    component_logger: ComponentLogger,
    pub(crate) resource_table: wasmtime::component::ResourceTable,
    phantom_data: PhantomData<DB>,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowCtx<C, DB, P> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
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
    ) -> Self {
        Self {
            execution_id: execution_id.clone(),
            next_child_execution_id: execution_id.next_level(),
            event_history: EventHistory::new(
                execution_id,
                event_history,
                responses,
                join_next_blocking_strategy,
                execution_deadline,
                non_blocking_event_batching,
                clock_fn.clone(),
                interrupt_on_timeout_container,
                worker_span.clone(),
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

    fn get_and_increment_child_id(&mut self) -> ExecutionId {
        let mut incremented = self.next_child_execution_id.increment();
        std::mem::swap(&mut self.next_child_execution_id, &mut incremented);
        incremented
    }

    pub(crate) async fn flush(&mut self) -> Result<(), DbError> {
        self.event_history.flush(&self.db_pool.connection()).await
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%ffqn))]
    pub(crate) async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        join_set_id: Option<JoinSetId>,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), WorkflowFunctionError> {
        trace!(?params, "call_imported_fn start");
        let event_call = self.imported_fn_to_event_call(ffqn, join_set_id, params)?;
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
                return Err(WorkflowFunctionError::UncategorizedError(
                    "Unexpected result length or type",
                ));
            }
        }
        trace!(?params, ?results, "call_imported_fn finish");
        Ok(())
    }

    async fn call_sleep(&mut self, duration: Duration) -> Result<(), WorkflowFunctionError> {
        let join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
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

    pub(crate) fn next_u128(&mut self) -> u128 {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
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

    fn imported_fn_to_event_call(
        &mut self,
        ffqn: FunctionFqn,
        join_set_id: Option<JoinSetId>,
        params: &[Val],
    ) -> Result<EventCall, WorkflowFunctionError> {
        if let Some(package_name) = ffqn.ifc_fqn.package_strip_extension_suffix() {
            let ifc_fqn = IfcFqnName::from_parts(
                ffqn.ifc_fqn.namespace(),
                package_name,
                ffqn.ifc_fqn.ifc_name(),
                ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_SUBMIT) {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                debug!("Got `-submit` extension for {ffqn}");
                let Some((_join_set_id, params)) = params.split_first() else {
                    error!("Got empty params, expected JoinSetId");
                    return Err(WorkflowFunctionError::UncategorizedError(
                        "error running `-submit` extension function: exepcted at least one parameter with JoinSetId, got empty parameter list",
                    ));
                };
                let join_set_id = join_set_id.ok_or(WorkflowFunctionError::UncategorizedError(
                    "error running `-submit` extension function: cannot get join-set-id",
                ))?;
                let child_execution_id = self.get_and_increment_child_id();
                Ok(EventCall::StartAsync {
                    ffqn,
                    join_set_id,
                    params: Params::from_wasmtime(Arc::from(params)),
                    child_execution_id,
                })
            } else if let Some(function_name) =
                ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                debug!("Got await-next extension for function `{function_name}`");
                if params.len() != 1 {
                    error!("Expected single parameter with join-set-id got {params:?}");
                    return Err(WorkflowFunctionError::UncategorizedError(
                        "error running `-await-next` extension function: wrong parameter length, expected single string parameter containing join-set-id"
                    ));
                };
                let join_set_id = join_set_id.ok_or(WorkflowFunctionError::UncategorizedError(
                    "error running `-await-next` extension function: cannot get join-set-id",
                ))?;
                Ok(EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                })
            } else if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_SCHEDULE)
            {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                debug!("Got `-schedule` extension for {ffqn}");
                let Some((scheduled_at, params)) = params.split_first() else {
                    error!("Error running `-schedule` extension function: exepcted at least one parameter of type `scheduled-at`, got empty parameter list");
                    return Err(WorkflowFunctionError::UncategorizedError(
                        "error running `-schedule` extension function: exepcted at least one parameter of type `scheduled-at`, got empty parameter list",
                    ));
                };
                let scheduled_at =
                    WastVal::try_from(scheduled_at.clone()).map_err(|err| {
                        error!("Error running `-schedule` extension function: cannot convert to internal representation - {err:?}");
                        WorkflowFunctionError::UncategorizedError(
                            "error running `-schedule` extension function: cannot convert to internal representation",
                        )
                    })?;
                let scheduled_at = match HistoryEventScheduledAt::try_from(&scheduled_at) {
                    Ok(scheduled_at) => scheduled_at,
                    Err(err) => {
                        error!("Wrong type for the first `-scheduled-at` parameter, expected `scheduled-at`, got `{scheduled_at:?}` - {err:?}");
                        return Err(WorkflowFunctionError::UncategorizedError(
                                "error running `-schedule` extension function: wrong first parameter type"
                            ));
                    }
                };
                // TODO(edge case): handle ExecutionId conflict: This does not have to be deterministicly generated. Remove execution_id from EventCall::ScheduleRequest
                // and add retries.
                let execution_id =
                    ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
                Ok(EventCall::ScheduleRequest {
                    scheduled_at,
                    execution_id,
                    ffqn,
                    params: Params::from_wasmtime(Arc::from(params)),
                })
            } else {
                error!("unrecognized extension function {ffqn}");
                return Err(WorkflowFunctionError::UncategorizedError(
                    "unrecognized extension function",
                ));
            }
        } else {
            let join_set_id =
                JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
            let child_execution_id = self.get_and_increment_child_id();
            Ok(EventCall::BlockingChildDirectCall {
                ffqn,
                join_set_id,
                params: Params::from_wasmtime(Arc::from(params)),
                child_execution_id,
            })
        }
    }
}

mod workflow_support {
    use wasmtime::component::Resource;

    use super::{
        assert_matches, ClockFn, DbConnection, DbPool, Duration, EventCall, JoinSetId, WorkflowCtx,
        WorkflowFunctionError,
    };
    use crate::{
        host_exports::{self, DurationEnum},
        workflow::event_history::{ChildReturnValue, HostActionResp},
    };

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

    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>>
        host_exports::obelisk::workflow::workflow_support::Host for WorkflowCtx<C, DB, P>
    {
        // TODO: Apply jitter, should be configured on the component level
        async fn sleep(&mut self, duration: DurationEnum) -> wasmtime::Result<()> {
            Ok(self
                .call_sleep(Duration::from(duration))
                .await
                .map_err(WorkflowFunctionError::from)?)
        }

        async fn new_join_set(&mut self) -> wasmtime::Result<Resource<JoinSetId>> {
            // TODO(edge case): handle JoinSetId conflict: This does not have to be deterministicly generated.
            // Figure out caching, add retries.
            // Another strategy to consider is to add hierarchy similar to ExecutionId.
            let join_set_id =
                JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
            let res = self
                .event_history
                .apply(
                    EventCall::CreateJoinSet { join_set_id },
                    &self.db_pool.connection(),
                    &mut self.version,
                    self.fn_registry.as_ref(),
                )
                .await
                .map_err(WorkflowFunctionError::from)?;
            let join_set_id = assert_matches!(res, ChildReturnValue::HostActionResp(HostActionResp::CreateJoinSetResp(join_set_id)) => join_set_id);
            let join_set_id = self.resource_table.push(join_set_id)?;
            Ok(join_set_id)
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
    use crate::workflow::workflow_ctx::WorkerPartialResult;
    use crate::{
        tests::fn_registry_dummy, workflow::workflow_ctx::WorkflowCtx,
        workflow::workflow_worker::JoinNextBlockingStrategy,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{
        storage::{
            wait_for_pending_state_fn, CreateRequest, DbConnection, DbPool, HistoryEvent,
            JoinSetRequest, PendingState,
        },
        FinishedExecutionResult,
    };
    use concepts::{ComponentId, FunctionRegistry};
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
    use tracing::info;
    use utils::time::{ClockFn, Now};

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
        Sleep { millis: u32 },
        Call { ffqn: FunctionFqn },
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
            steps: Vec<WorkflowStep>,
            clock_fn: C,
            db_pool: P,
            fn_registry: Arc<dyn FunctionRegistry>,
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
        async fn run(&self, ctx: WorkerContext) -> WorkerResult {
            info!("Starting");
            let seed = ctx.execution_id.random_part();
            let mut workflow_ctx = WorkflowCtx::new(
                ctx.execution_id.clone(),
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
            );
            for step in &self.steps {
                let res = match step {
                    WorkflowStep::Sleep { millis } => {
                        workflow_ctx
                            .call_sleep(Duration::from_millis(u64::from(*millis)))
                            .await
                    }
                    WorkflowStep::Call { ffqn } => {
                        workflow_ctx
                            .call_imported_fn(ffqn.clone(), None, &[], &mut [])
                            .await
                    }
                };
                if let Err(err) = res {
                    info!("Sending {err:?}");
                    return err.into_worker_partial_result(workflow_ctx.version).into();
                }
            }
            info!("Finishing");
            WorkerResult::Ok(SupportedFunctionReturnValue::None, workflow_ctx.version)
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

        assert_eq!(
            builder_a.run(|| async move { execute_steps().await }),
            builder_b.run(|| async move { execute_steps().await })
        );
    }

    #[expect(clippy::too_many_lines)]
    async fn execute_steps() -> (Vec<HistoryEvent>, FinishedExecutionResult) {
        let unstructured_holder = UnstructuredHolder::new();
        let mut unstructured = unstructured_holder.unstructured();
        let steps = {
            unstructured
                .arbitrary_iter()
                .unwrap()
                .map(std::result::Result::unwrap)
                .collect::<Vec<_>>()
        };
        let created_at = Now.now();
        info!(now = %created_at, "Generated steps: {steps:?}");
        let execution_id = ExecutionId::generate();
        let sim_clock = SimClock::new(created_at);
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let mut child_execution_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Call { .. }))
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
        let ffqns = steps
            .iter()
            .filter_map(|step| match step {
                WorkflowStep::Call { ffqn } => Some(ffqn.clone()),
                WorkflowStep::Sleep { .. } => None,
            })
            .collect::<Vec<_>>();
        let fn_registry = fn_registry_dummy(ffqns.as_slice());

        let workflow_exec_task = {
            let worker = Arc::new(WorkflowWorkerMock::new(
                FFQN_MOCK,
                steps,
                sim_clock.clone(),
                db_pool.clone(),
                fn_registry.clone(),
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
                    *join_set_id,
                    execution_log
                        .find_join_set_request(*join_set_id)
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
                    info!("Executing child {child_execution_id}");
                    assert!(child_execution_count > 0);
                    let child_log = db_connection.get(&child_execution_id).await.unwrap();
                    assert_eq!(
                        Some((execution_id.clone(), join_set_id)),
                        child_log.parent()
                    );
                    // execute
                    let child_exec_tick = {
                        let worker = Arc::new(WorkflowWorkerMock::new(
                            child_log.ffqn().clone(),
                            vec![],
                            sim_clock.clone(),
                            db_pool.clone(),
                            fn_registry.clone(),
                        ));
                        let exec_config = ExecConfig {
                            batch_size: 1,
                            lock_expiry: Duration::from_secs(1),
                            tick_sleep: TICK_SLEEP,
                            component_id: ComponentId::dummy_activity(),
                            task_limiter: None,
                        };
                        let exec_task = ExecTask::new(
                            worker,
                            exec_config,
                            sim_clock.clone(),
                            db_pool.clone(),
                            Arc::new([child_log.ffqn().clone()]),
                        );
                        exec_task.tick2(sim_clock.now()).await.unwrap()
                    };
                    assert_eq!(child_exec_tick.wait_for_tasks().await.unwrap(), 1);
                    child_execution_count -= 1;
                    let child_log = db_connection.get(&child_execution_id).await.unwrap();
                    let child_res = child_log.into_finished_result().unwrap();
                    assert_matches!(child_res, Ok(SupportedFunctionReturnValue::None));
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
        db_pool.close().await.unwrap();
        (
            execution_log.event_history().collect(),
            execution_log.into_finished_result().unwrap(),
        )
    }
}
