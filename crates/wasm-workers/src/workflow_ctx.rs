use crate::event_history::{EventCall, EventHistory};
use crate::workflow_worker::{JoinNextBlockingStrategy, NonBlockingEventBatching};
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{DbConnection, DbError, DbPool, Version};
use concepts::storage::{HistoryEvent, JoinSetResponseEvent};
use concepts::{ExecutionId, FinishedExecutionError, IfcFqnName, StrVariant};
use concepts::{FunctionFqn, Params};
use executor::worker::{FatalError, WorkerError, WorkerResult};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, instrument, trace};
use utils::time::ClockFn;
use wasmtime::component::{Linker, Val};

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum FunctionError {
    #[error("non deterministic execution: {0}")]
    NonDeterminismDetected(StrVariant),
    #[error("child request")]
    ChildExecutionRequest,
    #[error("delay request")]
    DelayRequest,
    #[error(transparent)]
    DbError(#[from] DbError),
    #[error("child finished with an execution error: {0}")]
    ChildExecutionError(FinishedExecutionError), // FIXME Add parameter/result parsing errors
}

impl FunctionError {
    pub(crate) fn into_worker_result(self, version: Version) -> WorkerResult {
        match self {
            Self::NonDeterminismDetected(reason) => WorkerResult::Err(WorkerError::FatalError(
                FatalError::NonDeterminismDetected(reason),
                version,
            )),
            Self::ChildExecutionRequest => WorkerResult::ChildExecutionRequest,
            Self::DelayRequest => WorkerResult::DelayRequest,
            Self::DbError(db_error) => WorkerResult::Err(WorkerError::DbError(db_error)),
            Self::ChildExecutionError(err) => WorkerResult::Err(WorkerError::FatalError(
                FatalError::ChildExecutionError(err),
                version,
            )),
        }
    }
}

// Generate `host_activities::Host` trait
wasmtime::component::bindgen!({
    path: "host-wit/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
    trappable_imports: true,
});

pub(crate) struct WorkflowCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    execution_id: ExecutionId,
    event_history: EventHistory<C>,
    rng: StdRng,
    pub(crate) clock_fn: C,
    db_pool: P,
    pub(crate) version: Version,
    phantom_data: PhantomData<DB>,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowCtx<C, DB, P> {
    #[allow(clippy::too_many_arguments)]
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
        retry_exp_backoff: Duration,
        max_retries: u32,
        non_blocking_event_batching: NonBlockingEventBatching,
        timeout_error: Arc<std::sync::Mutex<WorkerResult>>,
    ) -> Self {
        Self {
            execution_id,
            event_history: EventHistory::new(
                execution_id,
                event_history,
                responses,
                join_next_blocking_strategy,
                execution_deadline,
                retry_exp_backoff,
                max_retries,
                non_blocking_event_batching,
                clock_fn.clone(),
                timeout_error,
            ),
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            db_pool,
            version,
            phantom_data: PhantomData,
        }
    }

    #[instrument(skip_all, fields(%ffqn))]
    pub(crate) async fn call_imported_fn(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), FunctionError> {
        trace!(?params, "call_imported_fn start");
        let event_call = self.imported_fn_to_event_call(ffqn, params).expect("FIXME");
        let res = self
            .event_history
            .replay_or_interrupt(event_call, &self.db_pool.connection(), &mut self.version)
            .await?;
        assert_eq!(results.len(), res.len(), "unexpected results length"); // FIXME: FunctionError
        for (idx, item) in res.value().into_iter().enumerate() {
            results[idx] = item.as_val();
        }
        trace!(?params, ?results, "call_imported_fn finish");
        Ok(())
    }

    async fn call_sleep(&mut self, millis: u32) -> Result<(), FunctionError> {
        let join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        let delay_id = DelayId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        self.event_history
            .replay_or_interrupt(
                EventCall::BlockingDelayRequest {
                    join_set_id,
                    delay_id,
                    expires_at_if_new: (self.clock_fn)() + Duration::from_millis(u64::from(millis)),
                },
                &self.db_pool.connection(),
                &mut self.version,
            )
            .await?;
        Ok(())
    }

    pub(crate) fn next_u128(&mut self) -> u128 {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
    }

    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), wasmtime::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(linker, |state: &mut Self| state)
    }

    fn imported_fn_to_event_call(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
    ) -> Result<EventCall, StrVariant> {
        if let Some(package_name) = ffqn.ifc_fqn.package_name().strip_suffix(SUFFIX_PKG_EXT) {
            let ifc_fqn = IfcFqnName::from_parts(
                ffqn.ifc_fqn.namespace(),
                package_name,
                ffqn.ifc_fqn.ifc_name(),
                ffqn.ifc_fqn.version(),
            );
            if let Some(function_name) = ffqn.function_name.strip_suffix(SUFFIX_FN_START_ASYNC) {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                if params.is_empty() {
                    return Err(StrVariant::Static(
                        "got empty params for {ffqn}, expected JoinSetId",
                    ));
                    // TODO Replace with `split_at_checked` once stable
                }
                let (join_set_id, params) = params.split_at(1);

                let join_set_id = join_set_id.first().expect("split so that the size is 1");
                let Val::String(join_set_id) = join_set_id else {
                    return Err(StrVariant::Arc(Arc::from(format!(
                        "wrong type for JoinSetId for {ffqn}, expected string, got `{join_set_id:?}`"
                    ))));
                };
                let join_set_id = join_set_id.parse()?;
                let execution_id =
                    ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
                Ok(EventCall::StartAsync {
                    ffqn,
                    join_set_id,
                    params: Params::from_wasmtime(Arc::from(params)),
                    child_execution_id: execution_id,
                })
            } else if let Some(function_name) =
                ffqn.function_name.strip_suffix(SUFFIX_FN_AWAIT_NEXT)
            {
                let ffqn =
                    FunctionFqn::new_arc(Arc::from(ifc_fqn.to_string()), Arc::from(function_name));
                if params.len() != 1 {
                    return Err(StrVariant::Static(
                        "expected single parameter with JoinSetId for {ffqn}, got {params:?}",
                    ));
                }
                let join_set_id = params.first().expect("checked that the size is 1");
                let Val::String(join_set_id) = join_set_id else {
                    return Err(StrVariant::Arc(Arc::from(format!(
                        "wrong type for JoinSetId for {ffqn}, expected string, got `{join_set_id:?}`"
                    ))));
                };
                let join_set_id = join_set_id.parse()?;
                Ok(EventCall::BlockingChildJoinNext { join_set_id })
            } else {
                Err(StrVariant::Arc(Arc::from(format!(
                    "unrecognized extension function {ffqn}"
                ))))
            }
        } else {
            let join_set_id =
                JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
            let execution_id =
                ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
            Ok(EventCall::BlockingChildExecutionRequest {
                ffqn,
                join_set_id,
                params: Params::from_wasmtime(Arc::from(params)),
                child_execution_id: execution_id,
            })
        }
    }
}

#[async_trait::async_trait]
impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> my_org::workflow_engine::host_activities::Host
    for WorkflowCtx<C, DB, P>
{
    async fn sleep(&mut self, millis: u32) -> wasmtime::Result<()> {
        Ok(self.call_sleep(millis).await?)
    }

    async fn new_join_set(&mut self) -> wasmtime::Result<String> {
        let join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        self.event_history
            .replay_or_interrupt(
                EventCall::CreateJoinSet { join_set_id },
                &self.db_pool.connection(),
                &mut self.version,
            )
            .await?;
        Ok(join_set_id.to_string())
    }
}

const SUFFIX_PKG_EXT: &str = "-obelisk-ext";
const SUFFIX_FN_START_ASYNC: &str = "-future";
const SUFFIX_FN_AWAIT_NEXT: &str = "-await-next";

#[cfg(madsim)]
#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        workflow_ctx::WorkflowCtx,
        workflow_worker::{JoinNextBlockingStrategy, NonBlockingEventBatching},
    };
    use async_trait::async_trait;
    use concepts::prefixed_ulid::ConfigId;
    use concepts::{
        storage::{
            wait_for_pending_state_fn, CreateRequest, DbConnection, DbPool, HistoryEvent,
            JoinSetRequest, PendingState,
        },
        FinishedExecutionResult,
    };
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
    use db_tests::Database;
    use derivative::Derivative;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerContext, WorkerError, WorkerResult},
    };
    use std::{fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::info;
    use utils::time::{now, ClockFn};
    use val_json::type_wrapper::TypeWrapper;

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    pub const FFQN_MOCK: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    #[derive(Debug, Clone, arbitrary::Arbitrary)]
    #[allow(dead_code)]
    enum WorkflowStep {
        Sleep { millis: u32 },
        Call { ffqn: FunctionFqn },
    }

    #[derive(Clone, Derivative)]
    #[derivative(Debug)]
    struct WorkflowWorkerMock<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
        steps: Vec<WorkflowStep>,
        clock_fn: C,
        #[derivative(Debug = "ignore")]
        db_pool: P,
        phantom_data: PhantomData<DB>,
    }

    #[async_trait]
    impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
        for WorkflowWorkerMock<C, DB, P>
    {
        async fn run(&self, ctx: WorkerContext) -> WorkerResult {
            info!("Starting");
            let seed = ctx.execution_id.random_part();
            let mut workflow_ctx = WorkflowCtx::new(
                ctx.execution_id,
                ctx.event_history,
                ctx.responses,
                seed,
                self.clock_fn.clone(),
                JoinNextBlockingStrategy::default(),
                self.db_pool.clone(),
                ctx.version,
                ctx.execution_deadline,
                Duration::ZERO,
                0,
                NonBlockingEventBatching::default(),
                Arc::new(std::sync::Mutex::new(WorkerResult::Err(
                    WorkerError::IntermittentTimeout,
                ))),
            );
            for step in &self.steps {
                let res = match step {
                    WorkflowStep::Sleep { millis } => workflow_ctx.call_sleep(*millis).await,
                    WorkflowStep::Call { ffqn } => {
                        workflow_ctx
                            .call_imported_fn(ffqn.clone(), &[], &mut [])
                            .await
                    }
                };
                if let Err(err) = res {
                    info!("Sending {err}");
                    return err.into_worker_result(workflow_ctx.version);
                }
            }
            info!("Finishing");
            WorkerResult::Ok(SupportedFunctionResult::None, workflow_ctx.version)
        }

        fn exported_functions(
            &self,
        ) -> impl Iterator<Item = (FunctionFqn, &[TypeWrapper], &Option<TypeWrapper>)> {
            Some((FFQN_MOCK, [].as_ref(), &None)).into_iter()
        }

        fn imported_functions(
            &self,
        ) -> impl Iterator<Item = (FunctionFqn, &[TypeWrapper], &Option<TypeWrapper>)> {
            None.into_iter()
        }
    }

    // TODO: verify non-determinism detection:
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
        let _guard = test_utils::set_up();
        let mut builder_a = madsim::runtime::Builder::from_env();
        builder_a.check = false;

        let mut builder_b = madsim::runtime::Builder::from_env(); // Builder: Clone would be useful
        builder_b.check = false;
        builder_b.seed = builder_a.seed;

        assert_eq!(
            builder_a.run(|| async move { execute_steps().await }),
            builder_b.run(|| async move { execute_steps().await })
        );
    }

    #[allow(clippy::too_many_lines)]
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
        let created_at = now();
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
        let timers_watcher_task = expired_timers_watcher::TimersWatcherTask::spawn_new(
            db_pool.connection(),
            expired_timers_watcher::TimersWatcherConfig {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.get_clock_fn(),
            },
        );
        let workflow_exec_task = {
            let worker = Arc::new(WorkflowWorkerMock {
                steps,
                clock_fn: sim_clock.get_clock_fn(),
                db_pool: db_pool.clone(),
                phantom_data: PhantomData,
            });
            let exec_config = ExecConfig {
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                config_id: ConfigId::generate(),
            };
            ExecTask::spawn_new(
                worker,
                exec_config,
                sim_clock.get_clock_fn(),
                db_pool.clone(),
                None,
            )
        };
        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: FFQN_MOCK,
                params: Params::default(),
                parent: None,
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let mut processed = Vec::new();
        let mut spawned_child_executors = Vec::new();
        while let Some((join_set_id, req)) = wait_for_pending_state_fn(
            &db_connection,
            execution_id,
            |execution_log| match &execution_log.pending_state {
                PendingState::BlockedByJoinSet { join_set_id, .. } => Some(Some((
                    *join_set_id,
                    execution_log
                        .join_set_requests(*join_set_id)
                        .cloned()
                        .collect::<Vec<_>>(),
                ))),
                PendingState::Finished => Some(None),
                _ => None,
            },
            None,
        )
        .await
        .unwrap()
        {
            if processed.contains(&join_set_id) {
                continue;
            }
            assert_eq!(1, req.len());
            match req.first().unwrap() {
                JoinSetRequest::DelayRequest {
                    delay_id,
                    expires_at,
                } => {
                    info!("Moving time to {expires_at} - {delay_id}");
                    assert!(delay_request_count > 0);
                    sim_clock.move_time_to(*expires_at).await;
                    delay_request_count -= 1;
                }
                JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                    info!("Executing child {child_execution_id}");
                    assert!(child_execution_count > 0);
                    let child_request = db_connection.get(*child_execution_id).await.unwrap();
                    assert_eq!(Some((execution_id, join_set_id)), child_request.parent());
                    // execute
                    let child_exec_task = {
                        let worker = Arc::new(WorkflowWorkerMock {
                            steps: vec![],
                            clock_fn: sim_clock.get_clock_fn(),
                            db_pool: db_pool.clone(),
                            phantom_data: PhantomData,
                        });
                        let exec_config = ExecConfig {
                            batch_size: 1,
                            lock_expiry: Duration::from_secs(1),
                            tick_sleep: TICK_SLEEP,
                            config_id: ConfigId::generate(),
                        };
                        ExecTask::spawn_new(
                            worker,
                            exec_config,
                            sim_clock.get_clock_fn(),
                            db_pool.clone(),
                            None,
                        )
                    };
                    tokio::time::sleep(Duration::ZERO).await; // Hack that makes sure the other task has a chance to start
                    spawned_child_executors.push(child_exec_task);
                    child_execution_count -= 1;
                }
            }
            processed.push(join_set_id);
        }
        // must be finished at this point
        let execution_log = db_connection.get(execution_id).await.unwrap();
        assert_eq!(PendingState::Finished, execution_log.pending_state);
        drop(db_connection);
        for child_task in spawned_child_executors {
            child_task.close().await;
        }
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_pool.close().await.unwrap();
        (
            execution_log.event_history().collect(),
            execution_log.finished_result().unwrap().clone(),
        )
    }
}
