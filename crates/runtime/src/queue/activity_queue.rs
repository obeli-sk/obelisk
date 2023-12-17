use crate::{
    activity::{Activities, ActivityRequest},
    // event_history::EventHistory,
    ActivityResponse,
};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

pub type QueueItem = (ActivityRequest, oneshot::Sender<ActivityResponse>);

pub(crate) struct ActivityQueueReceiver {
    pub(crate) receiver: mpsc::Receiver<QueueItem>,
    pub(crate) activities: Arc<Activities>,
    // pub(crate) event_history: EventHistory,
}

impl ActivityQueueReceiver {
    pub(crate) async fn process(&mut self) {
        while let Some((request, resp_tx)) = self.receiver.recv().await {
            // self.event_history.persist_start(&request);
            let activity_res = self.activities.run(&request).await;

            // self.event_history.persist_end(request, activity_res.clone());
            if let Err(_err) = resp_tx.send(activity_res) {
                warn!("Not sending back the activity result");
            }
        }
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
