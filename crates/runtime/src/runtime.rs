use crate::activity::Activities;
use crate::event_history::{EventHistory, SupportedFunctionResult};
use crate::queue::activity_queue::{ActivityQueueReceiver, ActivityQueueSender, QueueItem};
use crate::workflow::{ExecutionError, Workflow, WorkflowConfig};
use crate::workflow_id::WorkflowId;
use crate::FunctionFqn;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::{sync::mpsc, task::AbortHandle};
use tracing::warn;
use wasmtime::component::Val;

pub struct Runtime {
    activities: Arc<Activities>,
    queue_sender: mpsc::Sender<QueueItem>,
    queue_task: AbortHandle,
    workflows: Vec<Arc<Workflow>>,
    functions_to_workflows: HashMap<FunctionFqn<'static>, Arc<Workflow>>,
}

impl Runtime {
    pub fn new(activities: Arc<Activities>) -> Self {
        let (queue_sender, queue_receiver) = mpsc::channel(100); // FIXME
        let mut activity_queue_receiver = ActivityQueueReceiver {
            receiver: queue_receiver,
            activities: activities.clone(),
        };
        let queue_task =
            tokio::spawn(async move { activity_queue_receiver.process().await }).abort_handle();
        Self {
            activities,
            queue_sender,
            queue_task,
            workflows: Vec::new(),
            functions_to_workflows: HashMap::new(),
        }
    }

    pub async fn add_workflow_definition(
        &mut self,
        workflow_wasm_path: String,
        config: &WorkflowConfig,
    ) -> Result<Arc<Workflow>, anyhow::Error> {
        let workflow = Arc::new(
            Workflow::new_with_config(
                workflow_wasm_path,
                self.activities.clone(),
                ActivityQueueSender {
                    sender: self.queue_sender.clone(),
                },
                config,
            )
            .await?,
        );
        self.workflows.push(workflow.clone());
        for fqn in workflow.functions() {
            if let Some(old) = self
                .functions_to_workflows
                .insert(fqn.clone(), workflow.clone())
            {
                warn!("Replaced `{fqn}` from `{old}`", old = old.wasm_path);
            }
        }
        Ok(workflow)
    }

    pub async fn schedule_workflow(
        &self,
        workflow_id: &WorkflowId,
        event_history: &mut EventHistory,
        fqn: &FunctionFqn<'static>,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        // TODO: persist execution in the `scheduled` state.
        let workflow = self
            .functions_to_workflows
            .get(fqn)
            .ok_or_else(|| ExecutionError::NotFound(fqn.to_owned()))?
            .clone();

        workflow
            .execute_all(workflow_id, event_history, fqn, params)
            .await
        // TODO: persist final result
    }

    pub async fn abort(&mut self) {
        self.queue_task.abort();
        while !self.queue_task.is_finished() {
            tokio::task::yield_now().await;
        }
    }
}
