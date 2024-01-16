use crate::activity::{Activity, ActivityConfig};
use crate::database::{ActivityQueueSender, Database, WorkflowEventFetcher};
use crate::workflow::{ExecutionError, Workflow, WorkflowConfig};
use crate::{FunctionFqn, FunctionMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};
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

    // TODO: builder
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

    // TODO: builder
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

    async fn process_workflows(
        functions_to_workflows: HashMap<Arc<FunctionFqn<'static>>, Arc<Workflow>>,
        fetcher: WorkflowEventFetcher,
        activity_queue_sender: ActivityQueueSender,
    ) {
        while let Some((request, oneshot_tx)) = fetcher.fetch_one().await {
            if let Some(workflow) = functions_to_workflows.get(&request.fqn) {
                let mut event_history = request.event_history.lock().await;
                // TODO: currently runs until completion. Allow persisting partial completion.
                let resp = workflow
                    .execute_all(
                        &request.workflow_id,
                        &activity_queue_sender,
                        event_history.as_mut(),
                        &request.fqn,
                        &request.params,
                    )
                    .await;
                // TODO: persist execution in the `scheduled` state.
                let _ = oneshot_tx.send(resp);
            } else {
                let err = ExecutionError::NotFound {
                    workflow_id: request.workflow_id,
                    fqn: request.fqn.clone(),
                };
                warn!("{err}");
                let _ = oneshot_tx.send(Err(err));
            }
            debug!("Runtime:::process_workflows exitting");
        }
    }

    // FIXME: cancel on drop of Runtime
    pub fn spawn(&self, database: &Database) -> tokio::task::AbortHandle {
        let process_workflows = Self::process_workflows(
            self.functions_to_workflows.clone(),
            database.workflow_event_fetcher(),
            database.activity_queue_sender(),
        );
        let process_activities = crate::queue::activity_queue::process(
            database.activity_event_fetcher(),
            self.functions_to_activities.clone(), // TODO: move here with Builder
        );
        tokio::spawn(async move {
            futures_util::join!(process_activities, process_workflows);
        })
        .abort_handle()
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
