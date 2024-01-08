use crate::activity::{Activity, ActivityConfig};
use crate::event_history::{EventHistory, SupportedFunctionResult};
use crate::queue::activity_queue::{ActivityQueueReceiver, ActivityQueueSender};
use crate::workflow::{ExecutionError, Workflow, WorkflowConfig};
use crate::workflow_id::WorkflowId;
use crate::{FunctionFqn, FunctionMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, trace, warn};
use wasmtime::component::Val;
use wasmtime::Engine;

#[derive(Default)]
pub struct RuntimeConfig {
    pub workflow_engine_config: EngineConfig,
    pub activity_engine_config: EngineConfig,
}

pub struct EngineConfig {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(
                wasmtime::PoolingAllocationConfig::default(),
            ),
        }
    }
}

pub struct Runtime {
    functions_to_workflows: HashMap<Arc<FunctionFqn<'static>>, Arc<Workflow>>,
    functions_to_activities: HashMap<Arc<FunctionFqn<'static>>, Arc<Activity>>,
    workflow_engine: Arc<Engine>,
    activity_engine: Arc<Engine>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new_with_config(RuntimeConfig::default())
    }
}

impl Runtime {
    pub fn new_with_config(config: RuntimeConfig) -> Self {
        let workflow_engine = {
            let mut wasmtime_config = wasmtime::Config::new();
            // TODO: limit execution with fuel
            wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
            wasmtime_config.wasm_component_model(true);
            wasmtime_config.async_support(true);
            wasmtime_config.allocation_strategy(config.workflow_engine_config.allocation_strategy);
            Arc::new(Engine::new(&wasmtime_config).unwrap())
        };
        let activity_engine = {
            let mut wasmtime_config = wasmtime::Config::new();
            // TODO: limit execution with epoch_interruption
            wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
            wasmtime_config.wasm_component_model(true);
            wasmtime_config.async_support(true);
            wasmtime_config.allocation_strategy(config.activity_engine_config.allocation_strategy);
            Arc::new(Engine::new(&wasmtime_config).unwrap())
        };
        Self {
            functions_to_workflows: HashMap::default(),
            functions_to_activities: HashMap::default(),
            workflow_engine,
            activity_engine,
        }
    }

    pub async fn add_activity(
        &mut self,
        activity_wasm_path: String,
        config: &ActivityConfig,
    ) -> Result<(), anyhow::Error> {
        info!("Loading activity from \"{activity_wasm_path}\"");
        let activity = Arc::new(
            Activity::new_with_config(activity_wasm_path, config, self.activity_engine.clone())
                .await?,
        );
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
        info!("Loading workflow definition from \"{workflow_wasm_path}\"");
        let workflow = Arc::new(
            Workflow::new_with_config(
                workflow_wasm_path,
                &self.functions_to_activities,
                config,
                self.workflow_engine.clone(),
            )
            .await?,
        );
        trace!("Loaded {workflow:#?}");
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
        info!(
            "[{workflow_id}] Scheduling workflow `{fqn}`",
            workflow_id = workflow_id.as_ref()
        );
        let (queue_sender, queue_receiver) = mpsc::channel(100); // FIXME
        let mut activity_queue_receiver = ActivityQueueReceiver {
            receiver: queue_receiver,
            functions_to_activities: self.functions_to_activities.clone(),
            workflow_id: workflow_id.as_ref().clone(),
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
