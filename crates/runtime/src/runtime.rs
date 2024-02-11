use crate::activity::{Activity, ActivityConfig};
use crate::database::{ActivityQueueSender, Database, WorkflowEventFetcher};
use crate::error::ExecutionError;
use crate::host_activity::{self, HOST_ACTIVITY_IFC};
use crate::workflow::{Workflow, WorkflowConfig};
use crate::{database::ActivityEventFetcher, ActivityFailed};
use anyhow::bail;
use concepts::{FnName, FunctionFqn, FunctionMetadata, IfcFqnName};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tracing::{debug, info, info_span, trace, warn, Instrument};
use tracing_unwrap::ResultExt;
use wasmtime::Engine;

#[derive(Default)]
#[allow(clippy::module_name_repetitions)]
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
#[allow(clippy::module_name_repetitions)]
pub struct RuntimeBuilder {
    workflow_engine: Arc<Engine>,
    activity_engine: Arc<Engine>,
    functions_to_activities: HashMap<FunctionFqn, Arc<Activity>>,
    interfaces_to_activity_function_names: HashMap<IfcFqnName, Vec<FnName>>,
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        RuntimeBuilder::new_with_config(RuntimeConfig::default())
    }
}

impl RuntimeBuilder {
    #[must_use]
    pub fn new_with_config(config: RuntimeConfig) -> Self {
        let workflow_engine = {
            let mut wasmtime_config = wasmtime::Config::new();
            // TODO: limit execution with fuel
            // Disable backtrace details to improve performance of restarting workflows.
            // TODO: Re-enable for matching interrupts with source code.
            wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable);
            wasmtime_config.wasm_component_model(true);
            wasmtime_config.async_support(true);
            wasmtime_config.allocation_strategy(config.workflow_engine_config.allocation_strategy);
            Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
        };
        let activity_engine = {
            let mut wasmtime_config = wasmtime::Config::new();
            // TODO: limit execution with epoch_interruption
            wasmtime_config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
            wasmtime_config.wasm_component_model(true);
            wasmtime_config.async_support(true);
            wasmtime_config.allocation_strategy(config.activity_engine_config.allocation_strategy);
            Arc::new(Engine::new(&wasmtime_config).unwrap_or_log())
        };
        Self {
            workflow_engine,
            activity_engine,
            functions_to_activities: HashMap::default(),
            interfaces_to_activity_function_names: HashMap::default(),
        }
    }

    #[tracing::instrument(skip_all)]
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
        let mut interfaces_to_activity_function_names = HashMap::<_, Vec<_>>::new();
        for fqn in activity.functions() {
            if let Some(old) = self.functions_to_activities.get(fqn) {
                // FIXME: leaves the builder in inconsistent state
                bail!(
                    "cannot replace activity {fqn} from `{old_path}`",
                    old_path = old.wasm_path
                );
            }
            self.functions_to_activities
                .insert(fqn.clone(), activity.clone());
            interfaces_to_activity_function_names
                .entry(fqn.ifc_fqn.clone())
                .or_default()
                .push(fqn.function_name.clone());
        }
        for (ifc_name, vec) in interfaces_to_activity_function_names {
            if self
                .interfaces_to_activity_function_names
                .get(&ifc_name)
                .is_some()
            {
                // FIXME: leaves the builder in inconsistent state
                bail!("interface {ifc_name} already exported",);
            }
            self.interfaces_to_activity_function_names
                .insert(ifc_name, vec);
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn build(
        self,
        workflow_wasm_path: String,
        config: &WorkflowConfig,
    ) -> Result<Runtime, anyhow::Error> {
        info!("Loading workflow definition from \"{workflow_wasm_path}\"");
        let workflow = Arc::new(Workflow::new_with_config(
            workflow_wasm_path,
            |ffqn| self.functions_to_activities.contains_key(ffqn),
            config,
            self.workflow_engine.clone(),
        )?);
        // TODO: check that no function is in host namespace, no activity-workflow collisions
        Ok(Runtime {
            workflow,
            functions_to_activities: Arc::new(self.functions_to_activities),
        })
    }
}

pub struct Runtime {
    workflow: Arc<Workflow>,
    functions_to_activities: Arc<HashMap<FunctionFqn, Arc<Activity>>>,
}

impl Runtime {
    #[must_use]
    pub fn workflow_function_metadata<'a>(
        &'a self,
        fqn: &FunctionFqn,
    ) -> Option<&'a FunctionMetadata> {
        self.workflow.function_metadata(fqn)
    }

    // TODO: add runtime+spawn id
    #[must_use]
    pub fn spawn(&self, database: &Database) -> StructuredAbortHandle {
        let process_workflow = Self::process_workflow(
            self.workflow.clone(),
            database.workflow_event_fetcher(),
            database.activity_queue_sender(),
        );
        let process_activities = Self::process_activities(
            database.activity_event_fetcher(),
            self.functions_to_activities.clone(),
        );
        StructuredAbortHandle(
            tokio::spawn(async move {
                futures_util::join!(process_activities, process_workflow);
            })
            .abort_handle(),
        )
    }

    #[tracing::instrument(skip_all)]
    async fn process_workflow(
        workflow: Arc<Workflow>,
        fetcher: WorkflowEventFetcher,
        activity_queue_sender: ActivityQueueSender,
    ) {
        while let Some((request, oneshot_tx)) = fetcher.fetch_one().await {
            if workflow.contains(&request.workflow_fqn) {
                let workflow = workflow.clone();
                let activity_queue_sender = activity_queue_sender.clone();
                tokio::spawn(
                    async move {
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
                    }
                    .instrument(info_span!("spawn_workflow")),
                );
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

    #[tracing::instrument(skip_all)]
    async fn process_activities(
        activity_event_fetcher: ActivityEventFetcher,
        functions_to_activities: Arc<HashMap<FunctionFqn, Arc<Activity>>>,
    ) {
        while let Some((request, resp_tx)) = activity_event_fetcher.fetch_one().await {
            if *request.activity_fqn.ifc_fqn == *HOST_ACTIVITY_IFC {
                tokio::spawn(async move {
                    debug!("Executing {activity}", activity = request.activity_fqn);
                    trace!("Executing {request:?}");
                    let _ = resp_tx.send(
                        async {
                            let res = host_activity::execute_host_activity(request).await;
                            trace!("Execution finished: {res:?}");
                            res
                        }
                        .await,
                    );
                });
            } else {
                // execute wasm activity
                if let Some(activity) = functions_to_activities.get(&request.activity_fqn) {
                    let activity = activity.clone();
                    tokio::spawn(async move {
                        debug!("Executing {activity}", activity = request.activity_fqn);
                        trace!("Executing {request:?}");
                        // TODO: cancellation
                        let _ = resp_tx.send(
                            async {
                                let res = activity.run(&request).await;
                                trace!("Execution finished: {res:?}");
                                res
                            }
                            .await,
                        );
                    });
                } else {
                    let err = ActivityFailed::NotFound {
                        workflow_id: request.workflow_id,
                        activity_fqn: request.activity_fqn,
                    };
                    warn!("{err}");
                    let _ = resp_tx.send(Err(err));
                }
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
