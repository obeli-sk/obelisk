use crate::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{
    AppendRequest, DbConnection, DbError, DbPool, ExecutionEventInner, JoinSetResponse, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use executor::worker::{FatalError, WorkerError};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace};
use utils::time::ClockFn;
use wasmtime::component::{Linker, Val};

const DB_LATENCY_MILLIS: u32 = 10; // do not interrupt if requested to sleep for less time.
const DB_POLL_SLEEP: Duration = Duration::from_millis(50);

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum FunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("child request")]
    ChildExecutionRequest,
    #[error("delay request")]
    DelayRequest,
    #[error(transparent)]
    DbError(#[from] DbError),
}

impl FunctionError {
    pub(crate) fn into_worker_error(self, version: Version) -> WorkerError {
        match self {
            Self::NonDeterminismDetected(reason) => {
                WorkerError::FatalError(FatalError::NonDeterminismDetected(reason), version)
            }
            Self::ChildExecutionRequest => WorkerError::ChildExecutionRequest,
            Self::DelayRequest => WorkerError::DelayRequest,
            Self::DbError(db_error) => WorkerError::DbError(db_error),
        }
    }
}

// Generate `host_activities::Host` trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

pub(crate) struct WorkflowCtx<C: ClockFn, DB: DbConnection, P: DbPool<DB>> {
    execution_id: ExecutionId,
    events: Vec<HistoryEvent>,
    events_idx: usize,
    rng: StdRng,
    pub(crate) clock_fn: C,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    db_pool: P,
    pub(crate) version: Version,
    execution_deadline: DateTime<Utc>,
    child_retry_exp_backoff: Duration,
    child_max_retries: u32,
    phantom_data: PhantomData<DB>,
}

impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> WorkflowCtx<C, DB, P> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        events: Vec<HistoryEvent>,
        seed: u64,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        db_pool: P,
        version: Version,
        execution_deadline: DateTime<Utc>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    ) -> Self {
        Self {
            execution_id,
            events,
            events_idx: 0,
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
            join_next_blocking_strategy,
            db_pool,
            version,
            execution_deadline,
            child_retry_exp_backoff: retry_exp_backoff,
            child_max_retries: max_retries,
            phantom_data: PhantomData,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn replay_or_interrupt(
        &mut self,
        ffqn: FunctionFqn,
        params: Params,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());

        let new_child_execution_id =
            ExecutionId::from_parts(self.execution_id.timestamp_part(), self.next_u128());

        trace!(child_execution_id = %new_child_execution_id,
            join_set_id = %new_join_set_id,
            "Querying history for child result, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );
        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { join_set_id } if *join_set_id == new_join_set_id => {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                } if *join_set_id == new_join_set_id
                    && *child_execution_id == new_child_execution_id =>
                {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched ChildExecutionRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at: _,
                } if *join_set_id == new_join_set_id => {
                    trace!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Matched JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetResponse {
                    response:
                        JoinSetResponse::ChildExecutionFinished {
                            child_execution_id,
                            result,
                        },
                    ..
                } if *child_execution_id == new_child_execution_id => {
                    debug!(child_execution_id = %new_child_execution_id,
                        join_set_id = %new_join_set_id, "Found response in history: {found:?}");
                    self.events_idx += 1;
                    // TODO: Map FinishedExecutionError somehow
                    return Ok(result.clone().expect("FIXME"));
                }
                unexpected => {
                    return Err(FunctionError::NonDeterminismDetected(StrVariant::Arc(
                        Arc::from(format!(
                            "sleep: unexpected event {unexpected:?} at index {}",
                            self.events_idx
                        )),
                    )));
                }
            }
        }
        // not found in the history, persisting the request
        let created_at = (self.clock_fn)();
        let interrupt = self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt;

        let join_set = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSet {
                    join_set_id: new_join_set_id,
                },
            },
        };
        let child_exec_req = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id: new_join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: new_child_execution_id,
                    },
                },
            },
        };
        let join_next = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinNext {
                    join_set_id: new_join_set_id,
                    lock_expires_at: if interrupt {
                        created_at
                    } else {
                        self.execution_deadline
                    },
                },
            },
        };
        debug!(child_execution_id = %new_child_execution_id, join_set_id = %new_join_set_id, "Interrupted, scheduling child execution");

        let parent = (
            vec![join_set, child_exec_req, join_next],
            self.execution_id,
            Some(self.version.clone()),
        );
        let child = (
            vec![AppendRequest {
                created_at,
                event: ExecutionEventInner::Created {
                    ffqn,
                    params,
                    parent: Some((self.execution_id, new_join_set_id)),
                    scheduled_at: None,
                    retry_exp_backoff: self.child_retry_exp_backoff,
                    max_retries: self.child_max_retries,
                },
            }],
            new_child_execution_id,
            None,
        );
        let db_connection = self.db_pool.connection().map_err(DbError::Connection)?;
        let versions = db_connection.append_tx(vec![parent, child]).await?;
        assert_eq!(2, versions.len());
        self.version = versions.first().unwrap().clone();

        if interrupt {
            Err(FunctionError::ChildExecutionRequest)
        } else {
            debug!(child_execution_id = %new_child_execution_id, join_set_id = %new_join_set_id,  "Waiting for child execution result");
            loop {
                // fetch
                let eh = db_connection.get(self.execution_id).await?; // TODO: fetch since the current version

                if let Some(res) = eh.event_history().find_map(|e| match e {
                    HistoryEvent::JoinSetResponse {
                        response:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result,
                            },
                        join_set_id,
                    } if child_execution_id == new_child_execution_id
                        && join_set_id == new_join_set_id =>
                    {
                        Some(result.expect("FIXME")) // TODO: Map FinishedExecutionError somehow
                    }
                    _ => None,
                }) {
                    debug!(join_set_id = %new_join_set_id, child_execution_id = %new_child_execution_id, "Got child execution result");
                    return Ok(res);
                }
                tokio::time::sleep(DB_POLL_SLEEP).await;
            }
        }
    }

    #[allow(dead_code)] // False positive
    pub(crate) async fn call_imported_func(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), FunctionError> {
        let res = self
            .replay_or_interrupt(ffqn, Params::Vals(Arc::new(Vec::from(params))))
            .await?;
        assert_eq!(results.len(), res.len(), "unexpected results length");
        for (idx, item) in res.value().into_iter().enumerate() {
            results[idx] = val_json::wast_val::val(item, &results[idx].ty()).unwrap();
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn sleep(&mut self, millis: u32) -> Result<(), FunctionError> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        let new_delay_id =
            DelayId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        trace!(
            "Querying history for {new_delay_id}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );
        let created_at = (self.clock_fn)();
        let new_expires_at = created_at + Duration::from_millis(u64::from(millis));
        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { join_set_id: found } if *found == new_join_set_id => {
                    trace!(join_set_id = %new_join_set_id, delay_id = %new_delay_id, "Matched JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id,
                            expires_at: _do_not_compare, // already computed in the past, will not match
                        },
                } if *join_set_id == new_join_set_id && *delay_id == new_delay_id => {
                    trace!(join_set_id = %new_join_set_id, delay_id = %new_delay_id, "Matched DelayRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at: _,
                } if *join_set_id == new_join_set_id => {
                    trace!(join_set_id = %new_join_set_id, delay_id = %new_delay_id, "Matched JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetResponse {
                    response: JoinSetResponse::DelayFinished { delay_id },
                    ..
                } if new_delay_id == *delay_id => {
                    debug!(join_set_id = %new_join_set_id, delay_id = %new_delay_id,  "Found response in history");
                    self.events_idx += 1;
                    return Ok(());
                }
                unexpected => {
                    return Err(FunctionError::NonDeterminismDetected(StrVariant::Arc(
                        Arc::from(format!(
                            "sleep: unexpected event {unexpected:?} at index {}",
                            self.events_idx
                        )),
                    )));
                }
            }
        }
        // not found in the history, persisting the request
        let join_set = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSet {
                    join_set_id: new_join_set_id,
                },
            },
        };
        let delayed_until = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinSetRequest {
                    join_set_id: new_join_set_id,
                    request: JoinSetRequest::DelayRequest {
                        delay_id: new_delay_id,
                        expires_at: new_expires_at,
                    },
                },
            },
        };

        let interrupt = self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt
            && millis > DB_LATENCY_MILLIS
            || new_expires_at < self.execution_deadline; // do not interrupt on a short delay
        let join_next = AppendRequest {
            created_at,
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::JoinNext {
                    join_set_id: new_join_set_id,
                    lock_expires_at: if interrupt {
                        created_at
                    } else {
                        self.execution_deadline
                    },
                },
            },
        };

        let db_connection = self.db_pool.connection().map_err(DbError::Connection)?;

        self.version = db_connection
            .append_batch(
                vec![join_set, delayed_until, join_next],
                self.execution_id,
                Some(self.version.clone()),
            )
            .await?;

        if interrupt {
            Err(FunctionError::DelayRequest)
        } else {
            let delay = (new_expires_at - (self.clock_fn)())
                .to_std()
                .unwrap_or_default();
            debug!(join_set_id = %new_join_set_id, delay_id = %new_delay_id,  "Waiting for async timer for {delay:?}");
            tokio::time::sleep(delay).await;
            loop {
                // fetch
                let eh = db_connection.get(self.execution_id).await?; // TODO: fetch since the current version
                let expected = HistoryEvent::JoinSetResponse {
                    response: JoinSetResponse::DelayFinished {
                        delay_id: new_delay_id,
                    },
                    join_set_id: new_join_set_id,
                };
                if eh.event_history().any(|e| e == expected) {
                    debug!(join_set_id = %new_join_set_id, delay_id = %new_delay_id,  "Finished waiting for async timer");
                    return Ok(());
                }
                tokio::time::sleep(DB_POLL_SLEEP).await;
            }
        }
    }

    pub(crate) fn next_u128(&mut self) -> u128 {
        let mut bytes = [0; 16];
        self.rng.fill_bytes(&mut bytes);
        u128::from_be_bytes(bytes)
    }

    #[allow(dead_code)] // False positive
    pub(crate) fn add_to_linker(linker: &mut Linker<Self>) -> Result<(), wasmtime::Error> {
        my_org::workflow_engine::host_activities::add_to_linker(linker, |state: &mut Self| state)
    }
}

#[async_trait::async_trait]
impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> my_org::workflow_engine::host_activities::Host
    for WorkflowCtx<C, DB, P>
{
    async fn sleep(&mut self, millis: u32) -> wasmtime::Result<()> {
        Ok(self.sleep(millis).await?)
    }
}

#[cfg(test)]
mod tests {
    use crate::{workflow_ctx::WorkflowCtx, workflow_worker::JoinNextBlockingStrategy};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{
            wait_for_pending_state_fn, CreateRequest, DbConnection, DbPool, HistoryEvent,
            JoinSetRequest, PendingState, Version,
        },
        FinishedExecutionResult,
    };
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
    use db::inmemory_dao::DbTask;
    use derivative::Derivative;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerResult},
    };
    use std::{fmt::Debug, marker::PhantomData, sync::Arc, time::Duration};
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::info;
    use utils::time::{now, ClockFn};

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    const MOCK_FFQN_PTR: &FunctionFqn = &MOCK_FFQN;

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

    impl<C: ClockFn, DB: DbConnection, P: DbPool<DB>> valuable::Valuable
        for WorkflowWorkerMock<C, DB, P>
    {
        fn as_value(&self) -> valuable::Value<'_> {
            "WorkflowWorkerMock".as_value()
        }

        fn visit(&self, _visit: &mut dyn valuable::Visit) {}
    }

    #[async_trait]
    impl<C: ClockFn + 'static, DB: DbConnection + 'static, P: DbPool<DB> + 'static> Worker
        for WorkflowWorkerMock<C, DB, P>
    {
        async fn run(
            &self,
            execution_id: ExecutionId,
            _ffqn: FunctionFqn,
            _params: Params,
            events: Vec<HistoryEvent>,
            version: Version,
            execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            let seed = execution_id.random_part();
            let mut ctx = WorkflowCtx::new(
                execution_id,
                events,
                seed,
                self.clock_fn.clone(),
                JoinNextBlockingStrategy::default(),
                self.db_pool.clone(),
                version,
                execution_deadline,
                Duration::ZERO,
                0,
            );
            for step in &self.steps {
                let res = match step {
                    WorkflowStep::Sleep { millis } => ctx.sleep(*millis).await,
                    WorkflowStep::Call { ffqn } => {
                        ctx.call_imported_func(ffqn.clone(), &[], &mut []).await
                    }
                };
                if let Err(err) = res {
                    return Err(err.into_worker_error(ctx.version));
                }
            }
            Ok((SupportedFunctionResult::None, ctx.version))
        }

        fn supported_functions(&self) -> impl Iterator<Item = &FunctionFqn> {
            Some(MOCK_FFQN_PTR).into_iter()
        }
    }

    // FIXME: verify non-determinism detection:
    // Start WorkflowWorkerMock, wait until it completes.
    // Copy its execution history to a new database
    // A. Swap two event history items
    // B. Swap two steps in WorkflowWorkerMock
    // C. Add new event history item
    // D. Add new step - needs whole execution history, must be done on another layer
    // E. Remove a step
    // F. Change the final result

    #[tokio::test]
    async fn check_determinism() {
        let _guard = test_utils::set_up();
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
        info!("first execution");
        let first = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        info!("second execution");
        let second = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        assert_eq!(first, second);
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_steps(
        execution_id: ExecutionId,
        steps: Vec<WorkflowStep>,
        sim_clock: SimClock,
    ) -> (Vec<HistoryEvent>, FinishedExecutionResult) {
        let mut db_task = DbTask::spawn_new(10);
        let db_pool = db_task.pool().expect("must be open");
        let mut child_execution_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Call { .. }))
            .count();
        let mut delay_request_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Sleep { .. }))
            .count();
        let timers_watcher_task = expired_timers_watcher::TimersWatcherTask::spawn_new(
            &db_pool,
            expired_timers_watcher::TimersWatcherConfig {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        )
        .unwrap();
        let workflow_exec_task = {
            let worker = Arc::new(WorkflowWorkerMock {
                steps,
                clock_fn: sim_clock.clock_fn(),
                db_pool: db_pool.clone(),
                phantom_data: PhantomData,
            });
            let exec_config = ExecConfig {
                ffqns: vec![MOCK_FFQN],
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            };
            ExecTask::spawn_new(worker, exec_config, db_pool.clone(), None)
        };
        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection().unwrap();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::from([]),
                parent: None,
                scheduled_at: None,
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
                    delay_id: _,
                    expires_at,
                } => {
                    assert!(delay_request_count > 0);
                    sim_clock.sleep_until(*expires_at);
                    delay_request_count -= 1;
                }
                JoinSetRequest::ChildExecutionRequest { child_execution_id } => {
                    assert!(child_execution_count > 0);
                    let child_request = db_connection.get(*child_execution_id).await.unwrap();
                    assert_eq!(Some((execution_id, join_set_id)), child_request.parent());
                    // execute
                    let child_exec_task = {
                        let worker = Arc::new(WorkflowWorkerMock {
                            steps: vec![],
                            clock_fn: sim_clock.clock_fn(),
                            db_pool: db_pool.clone(),
                            phantom_data: PhantomData,
                        });
                        let exec_config = ExecConfig {
                            ffqns: vec![child_request.ffqn().clone()],
                            batch_size: 1,
                            lock_expiry: Duration::from_secs(1),
                            tick_sleep: TICK_SLEEP,
                            clock_fn: sim_clock.clock_fn(),
                        };
                        ExecTask::spawn_new(worker, exec_config, db_pool.clone(), None)
                    };
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
        drop(db_pool);
        for child_task in spawned_child_executors {
            child_task.close().await;
        }
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
        (
            execution_log.event_history().collect(),
            execution_log.finished_result().unwrap().clone(),
        )
    }
}
