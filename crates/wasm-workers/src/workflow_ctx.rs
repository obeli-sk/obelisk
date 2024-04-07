use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::JoinSetResponse;
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use executor::worker::{ChildExecutionRequest, FatalError, SleepRequest, WorkerError};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace};
use utils::time::ClockFn;
use wasmtime::component::{Linker, Val};

#[derive(thiserror::Error, Debug, Clone)]
pub(crate) enum FunctionError {
    #[error("non deterministic execution: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("child({})", .0.child_execution_id)]
    ChildExecutionRequest(ChildExecutionRequest),
    #[error("sleep({})", .0.delay_id)]
    SleepRequest(SleepRequest),
}

impl From<FunctionError> for WorkerError {
    fn from(value: FunctionError) -> Self {
        match value {
            FunctionError::NonDeterminismDetected(reason) => {
                WorkerError::FatalError(FatalError::NonDeterminismDetected(reason.clone()))
            }

            FunctionError::ChildExecutionRequest(request) => {
                WorkerError::ChildExecutionRequest(request.clone())
            }
            FunctionError::SleepRequest(request) => WorkerError::SleepRequest(request.clone()),
        }
    }
}

// Generate `host_activities::Host` trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

pub(crate) struct WorkflowCtx<C: ClockFn> {
    execution_id: ExecutionId,
    events: Vec<HistoryEvent>,
    events_idx: usize,
    rng: StdRng,
    pub(crate) clock_fn: C,
}

impl<C: ClockFn> WorkflowCtx<C> {
    pub(crate) fn new(
        execution_id: ExecutionId,
        events: Vec<HistoryEvent>,
        seed: u64,
        clock_fn: C,
    ) -> Self {
        Self {
            execution_id,
            events,
            events_idx: 0,
            rng: StdRng::seed_from_u64(seed),
            clock_fn,
        }
    }

    fn replay_or_interrupt(
        &mut self,
        request: ChildExecutionRequest,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        trace!(
            "Querying history for {request:?}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );
        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { join_set_id }
                    if *join_set_id == request.new_join_set_id =>
                {
                    trace!("Matched JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                } if *join_set_id == request.new_join_set_id
                    && *child_execution_id == request.child_execution_id =>
                {
                    trace!("Matched ChildExecutionRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { join_set_id }
                    if *join_set_id == request.new_join_set_id =>
                {
                    trace!("Matched JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetResponse {
                    response:
                        JoinSetResponse::ChildExecutionFinished {
                            child_execution_id,
                            result,
                        },
                    ..
                } if *child_execution_id == request.child_execution_id => {
                    debug!("Found response in history: {found:?}");
                    self.events_idx += 1;
                    // TODO: Map FinishedExecutionError somehow
                    return Ok(result.clone().unwrap());
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
        Err(FunctionError::ChildExecutionRequest(request))
    }

    #[allow(dead_code)] // False positive
    pub(crate) fn call_imported_func(
        &mut self,
        ffqn: FunctionFqn,
        params: &[Val],
        results: &mut [Val],
    ) -> Result<(), FunctionError> {
        let request = ChildExecutionRequest {
            new_join_set_id: JoinSetId::from_parts(
                self.execution_id.timestamp_part(),
                self.next_u128(),
            ),
            child_execution_id: ExecutionId::from_parts(
                self.execution_id.timestamp_part(),
                self.next_u128(),
            ),
            ffqn,
            params: Params::Vals(Arc::new(Vec::from(params))),
        };
        let res = self.replay_or_interrupt(request)?;
        assert_eq!(results.len(), res.len(), "unexpected results length");
        for (idx, item) in res.value().into_iter().enumerate() {
            results[idx] = val_json::wast_val::val(item, &results[idx].ty()).unwrap();
        }
        Ok(())
    }

    fn sleep(&mut self, millis: u32) -> Result<(), FunctionError> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        let new_delay_id =
            DelayId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        trace!(
            "Querying history for {new_delay_id}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );

        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { join_set_id: found } if *found == new_join_set_id => {
                    trace!("Matched JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id,
                            expires_at: _,
                        },
                } if *join_set_id == new_join_set_id && *delay_id == new_delay_id => {
                    trace!("Matched DelayRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { join_set_id }
                    if *join_set_id == new_join_set_id =>
                {
                    trace!("Matched JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinSetResponse {
                    response: JoinSetResponse::DelayFinished { delay_id },
                    ..
                } if new_delay_id == *delay_id => {
                    debug!(%delay_id, join_set_id = %new_join_set_id, "Found response in history");
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
        let expires_at = (self.clock_fn)() + Duration::from_millis(u64::from(millis));
        Err(FunctionError::SleepRequest(SleepRequest {
            new_join_set_id,
            delay_id: new_delay_id,
            expires_at,
        }))?
        // TODO: optimize for zero millis - write to db and continue
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
impl<C: ClockFn> my_org::workflow_engine::host_activities::Host for WorkflowCtx<C> {
    async fn sleep(&mut self, millis: u32) -> wasmtime::Result<()> {
        Ok(self.sleep(millis)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::workflow_ctx::WorkflowCtx;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use concepts::{
        storage::{journal::PendingState, DbConnection, HistoryEvent, JoinSetRequest, Version},
        FinishedExecutionResult,
    };
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
    use db::inmemory_dao::DbTask;
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerError, WorkerResult},
    };
    use std::{fmt::Debug, sync::Arc, time::Duration};
    use test_utils::{arbitrary::UnstructuredHolder, sim_clock::SimClock};
    use tracing::{debug, info};
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

    #[derive(Clone, Debug)]
    struct WorkflowWorkerMock<C: ClockFn> {
        steps: Vec<WorkflowStep>,
        clock_fn: C,
    }

    impl<C: ClockFn> valuable::Valuable for WorkflowWorkerMock<C> {
        fn as_value(&self) -> valuable::Value<'_> {
            "WorkflowWorkerMock".as_value()
        }

        fn visit(&self, _visit: &mut dyn valuable::Visit) {}
    }

    #[async_trait]
    impl<C: ClockFn + 'static> Worker for WorkflowWorkerMock<C> {
        async fn run(
            &self,
            execution_id: ExecutionId,
            _ffqn: FunctionFqn,
            _params: Params,
            events: Vec<HistoryEvent>,
            version: Version,
            _execution_deadline: DateTime<Utc>,
        ) -> WorkerResult {
            let seed = execution_id.random_part();
            let mut ctx = WorkflowCtx::new(execution_id, events, seed, self.clock_fn.clone());
            for step in &self.steps {
                match step {
                    WorkflowStep::Sleep { millis } => ctx.sleep(*millis),
                    WorkflowStep::Call { ffqn } => {
                        ctx.call_imported_func(ffqn.clone(), &[], &mut [])
                    }
                }
                .map_err(|err| (WorkerError::from(err), version))?;
            }
            Ok((SupportedFunctionResult::None, version))
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
        debug!(now = %created_at, "Generated steps: {steps:?}");
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
        let db_connection = db_task.as_db_connection().expect("must be open");
        let mut child_execution_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Call { .. }))
            .count();
        let mut delay_request_count = steps
            .iter()
            .filter(|step| matches!(step, WorkflowStep::Sleep { .. }))
            .count();
        let timers_watcher_task = expired_timers_watcher::Task::spawn_new(
            db_connection.clone(),
            expired_timers_watcher::Config {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        );
        let workflow_exec_task = {
            let worker = Arc::new(WorkflowWorkerMock {
                steps,
                clock_fn: sim_clock.clock_fn(),
            });
            let exec_config = ExecConfig {
                ffqns: vec![MOCK_FFQN],
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            };
            ExecTask::spawn_new(db_connection.clone(), worker, exec_config, None)
        };
        // Create an execution.
        let created_at = sim_clock.now();
        db_connection
            .create(
                created_at,
                execution_id,
                MOCK_FFQN,
                Params::from([]),
                None,
                None,
                Duration::ZERO,
                0,
            )
            .await
            .unwrap();

        let mut processed = Vec::new();
        let mut spawned_child_executors = Vec::new();
        while let Some((join_set_id, req)) = db_connection
            .wait_for_pending_state_fn(
                execution_id,
                |execution_history| match &execution_history.pending_state {
                    PendingState::BlockedByJoinSet { join_set_id } => Some(Some((
                        *join_set_id,
                        execution_history
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
                    let child_request = loop {
                        if let Ok(res) = db_connection.get(*child_execution_id).await {
                            break res;
                        }
                    };
                    assert_eq!(Some((execution_id, join_set_id)), child_request.parent());
                    // execute
                    let child_exec_task = {
                        let worker = Arc::new(WorkflowWorkerMock {
                            steps: vec![],
                            clock_fn: sim_clock.clock_fn(),
                        });
                        let exec_config = ExecConfig {
                            ffqns: vec![child_request.ffqn().clone()],
                            batch_size: 1,
                            lock_expiry: Duration::from_secs(1),
                            tick_sleep: TICK_SLEEP,
                            clock_fn: sim_clock.clock_fn(),
                        };
                        ExecTask::spawn_new(db_connection.clone(), worker, exec_config, None)
                    };
                    spawned_child_executors.push(child_exec_task);
                    child_execution_count -= 1;
                }
            }
            processed.push(join_set_id);
        }
        // must be finished at this point
        let execution_history = db_connection.get(execution_id).await.unwrap();
        assert_eq!(PendingState::Finished, execution_history.pending_state);
        drop(db_connection);
        for child_task in spawned_child_executors {
            child_task.close().await;
        }
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
        (
            execution_history.event_history().collect(),
            execution_history.finished_result().unwrap().clone(),
        )
    }
}
