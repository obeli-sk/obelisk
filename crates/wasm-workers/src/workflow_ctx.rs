use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::SupportedFunctionResult;
use concepts::{ExecutionId, StrVariant};
use db::storage::AsyncResponse;
use db::storage::HistoryEvent;
use executor::worker::{ChildExecutionRequest, FatalError, SleepRequest, WorkerError};

use rand::rngs::StdRng;
use rand::RngCore;
use std::fmt::Debug;
use std::time::Duration;
use tracing::{debug, trace};
use wasmtime::component::Linker;

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

pub(crate) struct WorkflowCtx<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> {
    pub(crate) execution_id: ExecutionId,
    pub(crate) events: Vec<HistoryEvent>,
    pub(crate) events_idx: usize,
    pub(crate) rng: StdRng,
    pub(crate) clock_fn: C,
}
impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static> WorkflowCtx<C> {
    #[allow(dead_code)] // False positive
    pub(crate) fn replay_or_interrupt(
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
impl<C: Fn() -> DateTime<Utc> + Send + Sync + Clone + 'static>
    my_org::workflow_engine::host_activities::Host for WorkflowCtx<C>
{
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let new_join_set_id =
            JoinSetId::from_parts(self.execution_id.timestamp(), self.next_u128());
        let delay_id = DelayId::from_parts(self.execution_id.timestamp(), self.next_u128());
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
}
