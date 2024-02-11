use crate::{
    activity::ActivityRequest, database::ActivityQueueSender, error::HostFunctionError,
    workflow::AsyncActivityBehavior, ActivityFailed, ActivityResponse, SupportedFunctionResult,
};
use concepts::{workflow_id::WorkflowId, FunctionFqn};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, trace};
use tracing_unwrap::OptionExt;
use wasmtime::component::Val;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    pub(crate) request: ActivityRequest,
}

impl Event {
    pub fn new_from_wasm_activity(
        workflow_id: WorkflowId,
        fqn: FunctionFqn,
        params: Arc<Vec<Val>>,
    ) -> Self {
        Self {
            request: ActivityRequest {
                workflow_id,
                activity_fqn: fqn,
                params,
            },
        }
    }
}

#[derive(Debug)]
enum CurrentHistoryResult<'a> {
    Empty,
    FoundMatching(&'a Result<SupportedFunctionResult, ActivityFailed>),
    FoundNotMatching(
        (
            &'a FunctionFqn,
            &'a Arc<Vec<Val>>,
            &'a Result<SupportedFunctionResult, ActivityFailed>,
        ),
    ),
}

pub(crate) struct CurrentEventHistory {
    pub(crate) workflow_id: WorkflowId,
    run_id: u64,
    activity_queue_sender: ActivityQueueSender,
    pub(crate) event_history: EventHistory,
    async_activity_behavior: AsyncActivityBehavior,
    replay_idx: usize,
    replay_len: usize,
}

impl CurrentEventHistory {
    pub(crate) fn new(
        workflow_id: WorkflowId,
        run_id: u64,
        event_history: EventHistory,
        activity_queue_writer: ActivityQueueSender,
        async_activity_behavior: AsyncActivityBehavior,
    ) -> Self {
        Self {
            workflow_id,
            run_id,
            activity_queue_sender: activity_queue_writer,
            replay_len: event_history.len(),
            event_history,
            async_activity_behavior,
            replay_idx: 0,
        }
    }

    pub(crate) fn replay_is_drained(&self) -> bool {
        self.replay_idx == self.replay_len
    }

    #[allow(clippy::type_complexity)]
    fn next(
        &mut self,
    ) -> Option<(
        &FunctionFqn,
        &Arc<Vec<Val>>,
        &Result<SupportedFunctionResult, ActivityFailed>,
    )> {
        if self.replay_idx < self.replay_len {
            let (fqn, params, res) = self
                .event_history
                .get(self.replay_idx)
                .expect("must contain value");
            self.replay_idx += 1;
            Some((fqn, params, res))
        } else {
            None
        }
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn replay_enqueue_interrupt(
        &mut self,
        event: Event,
    ) -> Result<SupportedFunctionResult, HostFunctionError> {
        let workflow_id = self.workflow_id.clone();
        let run_id = self.run_id;
        let async_activity_behavior = self.async_activity_behavior;
        let current_history_result = match self.next() {
            None => CurrentHistoryResult::Empty,
            Some((found_fqn, found_params, replay_result))
                if event.request.activity_fqn == *found_fqn
                    && event.request.params == *found_params =>
            {
                CurrentHistoryResult::FoundMatching(replay_result)
            }
            Some(replay) => CurrentHistoryResult::FoundNotMatching(replay),
        };
        trace!(
            "[{workflow_id},{run_id}] replay_handle_interrupt {fqn}, current_history_result: {r:?}",
            fqn = event.request.activity_fqn,
            r = current_history_result
        );

        match (event, current_history_result, async_activity_behavior) {
            // Replay the result if found.
            (event, CurrentHistoryResult::FoundMatching(replay_result), _) => {
                debug!(
                    "[{workflow_id},{run_id}] Replaying {fqn}",
                    fqn = event.request.activity_fqn
                );
                Ok(replay_result.clone()?)
            }
            // activity: Enqueue and wait for response.
            (
                Event { request },
                CurrentHistoryResult::Empty,
                AsyncActivityBehavior::KeepWaiting,
            ) => {
                debug!(
                    "[{workflow_id},{run_id}] Enqueuing {fqn}",
                    fqn = request.activity_fqn
                );
                let res = self
                    .activity_queue_sender
                    .push(request, &mut self.event_history)
                    .await;
                Ok(res?)
            }
            // activity: Interrupt wasm and let the outer runtime handle it.
            (Event { request }, CurrentHistoryResult::Empty, AsyncActivityBehavior::Restart) => {
                debug!(
                    "[{workflow_id},{run_id}] Interrupting {fqn}",
                    fqn = request.activity_fqn
                );
                Err(HostFunctionError::Interrupt { request })
            }
            // Non determinism
            (event, CurrentHistoryResult::FoundNotMatching(expected), _) => {
                Err(HostFunctionError::NonDeterminismDetected(format!(
                    "[{workflow_id},{run_id}] Expected {expected:?}, got {event:?}"
                )))
            }
        }
    }
}

pub(crate) type ActivityRequestId = usize;

#[allow(clippy::module_name_repetitions)]
pub type EventHistoryTriple = (FunctionFqn, Arc<Vec<Val>>, ActivityResponse);

#[derive(Debug, Default)]
pub struct EventHistory {
    next_request: Option<ActivityRequest>,
    vec: Vec<EventHistoryTriple>,
}

impl EventHistory {
    #[allow(clippy::unused_async)]
    pub(crate) async fn persist_activity_request(
        &mut self,
        request: ActivityRequest,
    ) -> ActivityRequestId {
        // Concurrent activities are not implemented.
        assert!(self.next_request.is_none());
        self.next_request = Some(request);
        self.vec.len() + 1
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn persist_activity_response(
        &mut self,
        request_id: ActivityRequestId,
        resp: ActivityResponse,
    ) {
        assert_eq!(self.vec.len() + 1, request_id);
        let request = self.next_request.take().unwrap_or_log();
        self.vec.push((request.activity_fqn, request.params, resp));
    }

    #[must_use]
    pub fn successful_activities(&self) -> usize {
        self.vec.iter().filter(|(_, _, res)| res.is_ok()).count()
    }

    pub(crate) fn len(&self) -> usize {
        self.vec.len()
    }

    pub(crate) fn get(&self, idx: usize) -> Option<&EventHistoryTriple> {
        self.vec.get(idx)
    }
}

impl AsMut<EventHistory> for EventHistory {
    fn as_mut(&mut self) -> &mut EventHistory {
        self
    }
}

impl From<EventHistory> for Vec<EventHistoryTriple> {
    fn from(value: EventHistory) -> Self {
        assert!(value.next_request.is_none());
        value.vec
    }
}

impl From<Vec<EventHistoryTriple>> for EventHistory {
    fn from(vec: Vec<EventHistoryTriple>) -> Self {
        Self {
            vec,
            next_request: None,
        }
    }
}
