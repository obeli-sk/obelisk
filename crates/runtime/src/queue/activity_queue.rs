use crate::{
    activity::{Activity, ActivityRequest},
    workflow_id::WorkflowId,
    ActivityResponse, FunctionFqn,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

pub type QueueItem = (ActivityRequest, oneshot::Sender<ActivityResponse>);

pub(crate) struct ActivityQueueReceiver {
    pub(crate) receiver: mpsc::Receiver<QueueItem>,
    pub(crate) functions_to_activities: HashMap<Arc<FunctionFqn<'static>>, Arc<Activity>>,
    pub(crate) workflow_id: WorkflowId,
}

impl ActivityQueueReceiver {
    pub(crate) async fn process(&mut self) {
        while let Some((request, resp_tx)) = self.receiver.recv().await {
            let activity = self
                .functions_to_activities
                .get(&request.fqn)
                .unwrap_or_else(|| {
                    panic!(
                        "[{}] instance must have checked its imports, yet `{}` is not found",
                        self.workflow_id, request.fqn
                    )
                });
            let activity_res = activity.run(&request, &self.workflow_id).await;
            if let Err(err) = &activity_res {
                warn!("[{}] Activity failed: {err:?}", self.workflow_id);
            }
            if let Err(_err) = resp_tx.send(activity_res) {
                warn!(
                    "[{}] Not sending back the activity result",
                    self.workflow_id
                );
            }
        }
        debug!(
            "[{}] ActivityQueueReceiver::process exiting",
            self.workflow_id
        );
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ActivityQueueSender {
    pub(crate) sender: mpsc::Sender<QueueItem>,
}

impl ActivityQueueSender {
    pub async fn push(&self, request: ActivityRequest) -> oneshot::Receiver<ActivityResponse> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        self.sender
            .send((request, resp_sender))
            .await
            .expect("activity queue receiver must be running");
        resp_receiver
    }
}
