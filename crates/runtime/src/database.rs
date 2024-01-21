use crate::activity::ActivityRequest;
use crate::error::ExecutionError;
use crate::event_history::EventHistory;
use crate::workflow_id::WorkflowId;
use crate::SupportedFunctionResult;
use crate::{ActivityResponse, FunctionFqn};
use async_channel::{bounded, Receiver, Sender};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tracing::info;
use wasmtime::component::Val;

pub(crate) type WorkflowEvent = (
    WorkflowRequest,
    oneshot::Sender<Result<SupportedFunctionResult, ExecutionError>>,
);
pub(crate) type ActivityEvent = (ActivityRequest, oneshot::Sender<ActivityResponse>);

#[derive(Clone, Debug)]
pub struct WorkflowRequest {
    pub workflow_id: WorkflowId,
    pub event_history: Arc<Mutex<EventHistory>>,
    pub workflow_fqn: FunctionFqn,
    pub params: Arc<Vec<Val>>,
}

#[derive(Debug)]
pub struct Database {
    workflow_sender: Sender<WorkflowEvent>,
    workflow_receiver: Receiver<WorkflowEvent>,
    activity_sender: Sender<ActivityEvent>,
    activity_receiver: Receiver<ActivityEvent>,
}

impl Database {
    pub fn new(workflow_cap: usize, activity_cap: usize) -> Self {
        let (workflow_sender, workflow_receiver) = bounded(workflow_cap);
        let (activity_sender, activity_receiver) = bounded(activity_cap);
        Self {
            workflow_sender,
            workflow_receiver,
            activity_sender,
            activity_receiver,
        }
    }

    pub fn workflow_scheduler(&self) -> WorkflowScheduler {
        WorkflowScheduler {
            workflow_sender: self.workflow_sender.clone(),
        }
    }

    pub(crate) fn workflow_event_fetcher(&self) -> WorkflowEventFetcher {
        WorkflowEventFetcher {
            workflow_receiver: self.workflow_receiver.clone(),
        }
    }

    pub(crate) fn activity_event_fetcher(&self) -> ActivityEventFetcher {
        ActivityEventFetcher {
            activity_receiver: self.activity_receiver.clone(),
        }
    }

    pub(crate) fn activity_queue_sender(&self) -> ActivityQueueSender {
        ActivityQueueSender {
            activity_sender: self.activity_sender.clone(),
        }
    }
}

/// Used for storing new workflow execution requests.
// TODO: Back pressure options: Workflow or activity queue too big (total or limited by FQN),
// workflow has no active runtime, etc.
#[derive(Debug, Clone)]
pub struct WorkflowScheduler {
    workflow_sender: Sender<WorkflowEvent>,
}

/// Fetches workflow execution requests in a batch.
#[derive(Debug, Clone)]
pub struct WorkflowEventFetcher {
    workflow_receiver: Receiver<WorkflowEvent>,
}

impl WorkflowEventFetcher {
    /// Fetch one `WorkflowEvent`
    /// Returns None if the channel is closed
    pub async fn fetch_one(&self) -> Option<WorkflowEvent> {
        self.workflow_receiver.recv().await.ok()
    }
}

#[derive(Debug, Clone)]
pub struct ActivityEventFetcher {
    activity_receiver: Receiver<ActivityEvent>,
}
impl ActivityEventFetcher {
    pub async fn fetch_one(&self) -> Option<ActivityEvent> {
        self.activity_receiver.recv().await.ok()
    }
}

impl WorkflowScheduler {
    pub async fn schedule_workflow(
        &self,
        workflow_id: WorkflowId,
        event_history: Arc<Mutex<EventHistory>>,
        workflow_fqn: FunctionFqn,
        params: Arc<Vec<Val>>,
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        info!("[{workflow_id}] Scheduling workflow `{workflow_fqn}`",);
        let request = WorkflowRequest {
            workflow_id,
            event_history,
            workflow_fqn,
            params,
        };
        let (oneshot_sender, oneshot_rec) = oneshot::channel();
        if let Err(err) = self.workflow_sender.send((request, oneshot_sender)).await {
            let request = err.0 .0;
            return Err(ExecutionError::SchedulingError {
                workflow_id: request.workflow_id,
                workflow_fqn: request.workflow_fqn,
                reason: "cannot add to queue".to_string(),
            });
        }
        oneshot_rec.await.unwrap() // TODO

        // TODO: persist final result
    }
}

// TODO: back-pressure
#[derive(Debug, Clone)]
pub(crate) struct ActivityQueueSender {
    pub(crate) activity_sender: Sender<ActivityEvent>,
}

impl ActivityQueueSender {
    pub async fn push(
        &self,
        request: ActivityRequest,
        event_history: &mut EventHistory,
    ) -> ActivityResponse {
        let (resp_sender, resp_receiver) = oneshot::channel();

        let id = event_history
            .persist_activity_request(request.clone())
            .await;

        self.activity_sender
            .send((request, resp_sender))
            .await
            .expect("activity queue receiver must be running");
        let resp = resp_receiver
            .await
            .expect("activity queue receiver must be running");
        event_history
            .persist_activity_response(id, resp.clone())
            .await;
        resp
    }
}
