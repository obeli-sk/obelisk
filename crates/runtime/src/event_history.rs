use crate::{
    activity::ActivityRequest, database::ActivityQueueSender, error::HostFunctionError,
    host_activity::HostActivitySync, workflow::AsyncActivityBehavior, workflow_id::WorkflowId,
    ActivityFailed, ActivityResponse, FunctionFqn, SupportedFunctionResult,
};
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, trace};
use wasmtime::component::Val;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Event {
    pub(crate) request: ActivityRequest,
    pub(crate) kind: EventKind,
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
            kind: EventKind::ActivityAsync,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum EventKind {
    HostActivitySync(HostActivitySync),
    ActivityAsync,
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

    fn assert_replay_is_drained(&self) {
        assert_eq!(
            self.replay_idx, self.replay_len,
            "replay log has not been drained"
        );
    }

    pub(crate) async fn persist_activity_request(
        &mut self,
        request: ActivityRequest,
    ) -> ActivityRequestId {
        self.assert_replay_is_drained();
        self.event_history.persist_activity_request(request).await
    }

    pub(crate) async fn persist_activity_response(
        &mut self,
        request_id: ActivityRequestId,
        resp: ActivityResponse,
    ) {
        self.assert_replay_is_drained();
        self.event_history
            .persist_activity_response(request_id, resp)
            .await;
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
                .vec
                .get(self.replay_idx)
                .expect("must contain value");
            self.replay_idx += 1;
            Some((fqn, params, res))
        } else {
            None
        }
    }

    pub(crate) async fn replay_handle_interrupt(
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
            // Continue running on HostActivitySync
            (
                Event {
                    request,
                    kind: EventKind::HostActivitySync(host_activity_sync),
                },
                CurrentHistoryResult::Empty,
                _,
            ) => {
                debug!("[{workflow_id},{run_id}] Running {host_activity_sync:?}");
                let id = self.persist_activity_request(request.clone()).await;
                let res = host_activity_sync.handle(request);
                self.persist_activity_response(id, res.clone()).await;
                Ok(res?)
            }
            // Replay if found
            (event, CurrentHistoryResult::FoundMatching(replay_result), _) => {
                debug!(
                    "[{workflow_id},{run_id}] Replaying {fqn}",
                    fqn = event.request.activity_fqn
                );
                Ok(replay_result.clone()?)
            }
            // Async activity: Interrupt
            (
                Event {
                    request,
                    kind: EventKind::ActivityAsync,
                },
                CurrentHistoryResult::Empty,
                AsyncActivityBehavior::Restart,
            ) => {
                debug!(
                    "[{workflow_id},{run_id}] Interrupting {fqn}",
                    fqn = request.activity_fqn
                );
                Err(HostFunctionError::Interrupt { request })
            }

            // Async activity: Add to queue and wait for response.
            (
                Event {
                    request,
                    kind: EventKind::ActivityAsync,
                },
                CurrentHistoryResult::Empty,
                AsyncActivityBehavior::KeepWaiting,
            ) => {
                debug!(
                    "[{workflow_id},{run_id}] Enqueuing {fqn}",
                    fqn = request.activity_fqn
                );
                let id = self.persist_activity_request(request.clone()).await;
                let res = self
                    .activity_queue_sender
                    .push(request)
                    .await
                    .await
                    .expect("sender should not be dropped");
                self.persist_activity_response(id, res.clone()).await;
                Ok(res?)
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

pub type EventHistoryTriple = (FunctionFqn, Arc<Vec<Val>>, ActivityResponse);

#[derive(Debug, Default)]
pub struct EventHistory {
    next_request: Option<ActivityRequest>,
    vec: Vec<EventHistoryTriple>,
}

impl EventHistory {
    pub(crate) async fn persist_activity_request(
        &mut self,
        request: ActivityRequest,
    ) -> ActivityRequestId {
        // Concurrent activities are not implemented.
        assert!(self.next_request.is_none());
        self.next_request = Some(request);
        self.vec.len() + 1
    }

    pub(crate) async fn persist_activity_response(
        &mut self,
        request_id: ActivityRequestId,
        resp: ActivityResponse,
    ) {
        assert_eq!(self.vec.len() + 1, request_id);
        let request = self.next_request.take().unwrap();
        self.vec.push((request.activity_fqn, request.params, resp));
    }

    pub fn successful_activities(&self) -> usize {
        self.vec.iter().filter(|(_, _, res)| res.is_ok()).count()
    }

    pub(crate) fn len(&self) -> usize {
        self.vec.len()
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
