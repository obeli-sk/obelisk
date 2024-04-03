use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use db::storage::AsyncResponse;
use db::storage::HistoryEvent;
use executor::worker::{ChildExecutionRequest, FatalError, SleepRequest, WorkerError};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace};
use utils::time::ClockFn;
use wasmtime::component::{Linker, Val};

#[allow(dead_code)] // FIXME: Implement non determinism check
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
                HistoryEvent::JoinSet { .. } => {
                    debug!("Skipping JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::ChildExecutionAsyncRequest { .. } => {
                    debug!("Skipping ChildExecutionAsyncRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { .. } => {
                    debug!("Skipping JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::AsyncResponse {
                    response:
                        AsyncResponse::ChildExecutionAsyncResponse {
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
                _ => {
                    panic!("{found:?}")
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

    fn sleep(&mut self, millis: u64) -> Result<(), FunctionError> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        let delay_id = DelayId::from_parts(self.execution_id.timestamp_part(), self.next_u128());
        trace!(
            "Querying history for {delay_id}, index: {}, history: {:?}",
            self.events_idx,
            self.events
        );

        while let Some(found) = self.events.get(self.events_idx) {
            match found {
                HistoryEvent::JoinSet { .. } => {
                    debug!("Skipping JoinSet");
                    self.events_idx += 1;
                }
                HistoryEvent::DelayedUntilAsyncRequest { .. } => {
                    debug!("Skipping ChildExecutionAsyncRequest");
                    self.events_idx += 1;
                }
                HistoryEvent::JoinNextBlocking { .. } => {
                    debug!("Skipping JoinNextBlocking");
                    self.events_idx += 1;
                }
                HistoryEvent::AsyncResponse {
                    response:
                        AsyncResponse::DelayFinishedAsyncResponse {
                            delay_id: found_delay_id,
                        },
                    ..
                } if delay_id == *found_delay_id => {
                    debug!("Found response in history: {found:?}");
                    self.events_idx += 1;
                    return Ok(());
                }
                _ => {
                    panic!("{found:?}")
                }
            }
        }
        let expires_at = (self.clock_fn)() + Duration::from_millis(millis);
        Err(FunctionError::SleepRequest(SleepRequest {
            new_join_set_id,
            delay_id,
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
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        Ok(self.sleep(millis)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::workflow_ctx::WorkflowCtx;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
    use db::{
        storage::{
            inmemory_dao::DbTask, journal::PendingState, DbConnection, HistoryEvent, Version,
        },
        FinishedExecutionResult,
    };
    use executor::{
        executor::{ExecConfig, ExecTask},
        expired_timers_watcher,
        worker::{Worker, WorkerError, WorkerResult},
    };
    use std::{fmt::Debug, time::Duration};
    use test_utils::sim_clock::SimClock;

    use utils::time::{now, ClockFn};

    const TICK_SLEEP: Duration = Duration::from_millis(1);
    const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");
    const CHILD_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/child-ifc", "fn");

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    enum WorkflowStep {
        // Yield,
        Sleep { millis: u64 },
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
    }

    const SLEEP_MILLIS_IN_STEP: u64 = 10;
    #[tokio::test]
    async fn check_determinism_sleep() {
        let _guard = test_utils::set_up();
        let steps = vec![WorkflowStep::Sleep {
            millis: SLEEP_MILLIS_IN_STEP,
        }];
        let created_at = now();
        let execution_id = ExecutionId::generate();
        let first = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        let second = execute_steps(execution_id, steps.clone(), SimClock::new(created_at)).await;
        assert_eq!(first, second);
    }

    async fn execute_steps(
        execution_id: ExecutionId,
        steps: Vec<WorkflowStep>,
        sim_clock: SimClock,
    ) -> (Vec<HistoryEvent>, FinishedExecutionResult) {
        let mut db_task = DbTask::spawn_new(10);
        let db_connection = db_task.as_db_connection().expect("must be open");
        let total_sleep_count = u64::try_from(
            steps
                .iter()
                .filter_map(|step| {
                    if let WorkflowStep::Sleep { millis } = step {
                        Some(millis)
                    } else {
                        None
                    }
                })
                .count(),
        )
        .unwrap();
        let timers_watcher_task = expired_timers_watcher::Task::spawn_new(
            db_connection.clone(),
            expired_timers_watcher::Config {
                tick_sleep: TICK_SLEEP,
                clock_fn: sim_clock.clock_fn(),
            },
        );
        let workflow_exec_task = {
            let worker = WorkflowWorkerMock {
                steps,
                clock_fn: sim_clock.clock_fn(),
            };
            let exec_config = ExecConfig {
                ffqns: vec![MOCK_FFQN],
                batch_size: 1,
                lock_expiry: Duration::from_secs(1),
                tick_sleep: TICK_SLEEP,
                clock_fn: now,
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

        sim_clock.sleep(Duration::from_millis(
            SLEEP_MILLIS_IN_STEP * total_sleep_count,
        ));
        db_connection
            .wait_for_pending_state(execution_id, PendingState::Finished, None)
            .await
            .unwrap();
        let execution_history = db_connection.get(execution_id).await.unwrap();
        drop(db_connection);
        workflow_exec_task.close().await;
        timers_watcher_task.close().await;
        db_task.close().await;
        (
            execution_history.event_history().collect(),
            execution_history.finished_result().unwrap().clone(),
        )
    }
}
