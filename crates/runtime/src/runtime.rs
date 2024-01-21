use crate::activity::{Activity, ActivityConfig};
use crate::database::{ActivityQueueSender, Database, WorkflowEventFetcher};
use crate::error::ExecutionError;
use crate::host_activity::{self, HOST_ACTIVITY_PACKAGE};
use crate::workflow::{Workflow, WorkflowConfig};
use crate::FunctionMetadata;
use crate::{database::ActivityEventFetcher, ActivityFailed, FunctionFqn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::AbortHandle;
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

#[derive(Clone)]
pub struct RuntimeBuilder {
    workflow_engine: Arc<Engine>,
    activity_engine: Arc<Engine>,
    functions_to_workflows: HashMap<FunctionFqn, Arc<Workflow>>,
    functions_to_activities: HashMap<FunctionFqn, Arc<Activity>>,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        RuntimeBuilder::new_with_config(RuntimeConfig::default())
    }
}

impl RuntimeBuilder {
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
            workflow_engine,
            activity_engine,
            functions_to_workflows: HashMap::default(),
            functions_to_activities: HashMap::default(),
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

    pub fn build(self) -> Runtime {
        // TODO: check that no function is in host namespace, no activity-workflow collisions
        Runtime {
            functions_to_workflows: Arc::new(self.functions_to_workflows),
            functions_to_activities: Arc::new(self.functions_to_activities),
        }
    }
}

pub struct Runtime {
    functions_to_workflows: Arc<HashMap<FunctionFqn, Arc<Workflow>>>,
    functions_to_activities: Arc<HashMap<FunctionFqn, Arc<Activity>>>,
}

impl Runtime {
    pub fn workflow_function_metadata<'a>(
        &'a self,
        fqn: &FunctionFqn,
    ) -> Option<&'a FunctionMetadata> {
        self.functions_to_workflows
            .get(fqn)
            .and_then(|w| w.functions_to_metadata.get(fqn))
    }

    // TODO: add runtime+spawn id
    #[must_use]
    pub fn spawn(&self, database: &Database) -> StructuredAbortHandle {
        let process_workflows = Self::process_workflows(
            self.functions_to_workflows.clone(),
            database.workflow_event_fetcher(),
            database.activity_queue_sender(),
        );
        let process_activities = Self::process_activities(
            database.activity_event_fetcher(),
            self.functions_to_activities.clone(),
        );
        StructuredAbortHandle(
            tokio::spawn(async move {
                futures_util::join!(process_activities, process_workflows);
            })
            .abort_handle(),
        )
    }

    async fn process_workflows(
        functions_to_workflows: Arc<HashMap<FunctionFqn, Arc<Workflow>>>,
        fetcher: WorkflowEventFetcher,
        activity_queue_sender: ActivityQueueSender,
    ) {
        while let Some((request, oneshot_tx)) = fetcher.fetch_one().await {
            if let Some(workflow) = functions_to_workflows.get(&request.workflow_fqn) {
                let workflow = workflow.clone();
                let activity_queue_sender = activity_queue_sender.clone();
                tokio::spawn(async move {
                    let mut event_history = request.event_history.lock().await;
                    // TODO: cancellation
                    let resp = workflow
                        .execute_all(
                            &request.workflow_id,
                            &activity_queue_sender,
                            event_history.as_mut(),
                            &request.workflow_fqn,
                            &request.params,
                        )
                        .await;
                    let _ = oneshot_tx.send(resp);
                });
            } else {
                let err = ExecutionError::NotFound {
                    workflow_id: request.workflow_id,
                    fqn: request.workflow_fqn.clone(),
                };
                warn!("{err}");
                let _ = oneshot_tx.send(Err(err));
            }
            debug!("Runtime::process_workflows exitting");
        }
    }

    async fn process_activities(
        activity_event_fetcher: ActivityEventFetcher,
        functions_to_activities: Arc<HashMap<FunctionFqn, Arc<Activity>>>,
    ) {
        while let Some((request, resp_tx)) = activity_event_fetcher.fetch_one().await {
            if *request.activity_fqn.ifc_fqn == HOST_ACTIVITY_PACKAGE {
                host_activity::execute_host_activity(request, resp_tx).await;
            } else {
                // execute wasm activity
                match functions_to_activities.get(&request.activity_fqn) {
                    Some(activity) => {
                        let activity = activity.clone();
                        tokio::spawn(async move {
                            // TODO: cancellation
                            let _ = resp_tx.send(activity.run(&request).await);
                        });
                    }
                    None => {
                        let err = ActivityFailed::NotFound {
                            workflow_id: request.workflow_id,
                            activity_fqn: request.activity_fqn,
                        };
                        warn!("{err}");
                        let _ = resp_tx.send(Err(err));
                    }
                };
            }
        }
        debug!("Runtime::process_activities exiting");
    }
}

pub struct StructuredAbortHandle(AbortHandle);

impl StructuredAbortHandle {
    pub fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for StructuredAbortHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
