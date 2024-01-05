use crate::activity::{Activity, ActivityConfig};
use crate::event_history::{EventHistory, SupportedFunctionResult};
use crate::queue::activity_queue::{ActivityQueueReceiver, ActivityQueueSender};
use crate::workflow::{ExecutionError, Workflow, WorkflowConfig};
use crate::workflow_id::WorkflowId;
use crate::{FunctionFqn, FunctionMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;
use wasmtime::component::Val;

#[derive(Default)]
pub struct Runtime {
    functions_to_workflows: HashMap<Arc<FunctionFqn<'static>>, Arc<Workflow>>,
    functions_to_activities: HashMap<Arc<FunctionFqn<'static>>, Arc<Activity>>,
}

impl Runtime {
    pub async fn add_activity(
        &mut self,
        activity_wasm_path: String,
        config: &ActivityConfig,
    ) -> Result<(), anyhow::Error> {
        let activity = Arc::new(Activity::new_with_config(activity_wasm_path, config).await?);
        for fqn in activity.functions() {
            if let Some(old) = self
                .functions_to_activities
                .insert(fqn.clone(), activity.clone())
            {
                warn!(
                    "Replaced activity `{fqn}` from `{old}`",
                    old = old.wasm_path
                );
            }
        }
        Ok(())
    }

    pub async fn add_workflow_definition(
        &mut self,
        workflow_wasm_path: String,
        config: &WorkflowConfig,
    ) -> Result<Arc<Workflow>, anyhow::Error> {
        let workflow = Arc::new(
            Workflow::new_with_config(workflow_wasm_path, &self.functions_to_activities, config)
                .await?,
        );
        for fqn in workflow.functions() {
            if let Some(old) = self
                .functions_to_workflows
                .insert(fqn.clone(), workflow.clone())
            {
                warn!(
                    "Replaced workflow `{fqn}` from `{old}`",
                    old = old.wasm_path
                );
            }
        }
        Ok(workflow)
    }

    pub async fn schedule_workflow<W: AsRef<WorkflowId>, E: AsMut<EventHistory>>(
        &self,
        workflow_id: W,
        mut event_history: E,
        fqn: &FunctionFqn<'_>,
        params: &[Val],
    ) -> Result<SupportedFunctionResult, ExecutionError> {
        let (queue_sender, queue_receiver) = mpsc::channel(100); // FIXME
        let mut activity_queue_receiver = ActivityQueueReceiver {
            receiver: queue_receiver,
            functions_to_activities: self.functions_to_activities.clone(),
        };
        // TODO: allow cancelling this task
        tokio::spawn(async move { activity_queue_receiver.process().await }).abort_handle();
        let activity_queue_sender = ActivityQueueSender {
            sender: queue_sender,
        };
        // TODO: persist execution in the `scheduled` state.
        let workflow = self
            .functions_to_workflows
            .get(fqn)
            .ok_or_else(|| ExecutionError::NotFound(fqn.to_owned()))?
            .clone();

        workflow
            .execute_all(
                workflow_id.as_ref(),
                &activity_queue_sender,
                event_history.as_mut(),
                fqn,
                params,
            )
            .await
        // TODO: persist final result
    }

    pub fn workflow_function_metadata<'a>(
        &'a self,
        fqn: &FunctionFqn<'a>,
    ) -> Option<&'a FunctionMetadata> {
        self.functions_to_workflows
            .get(fqn)
            .and_then(|w| w.functions_to_metadata.get(fqn))
    }
}
