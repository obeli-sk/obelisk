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
}
