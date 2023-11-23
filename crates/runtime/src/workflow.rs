use crate::activity::Activities;
use crate::event_history::{
    CurrentEventHistory, Event, EventHistory, EventWrapper, HostActivity, HostFunctionError,
    WasmActivity,
};
use anyhow::Context;
use std::{fmt::Debug, sync::Arc, time::Duration};
use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Config, Engine, Store,
};

lazy_static::lazy_static! {
    static ref ENGINE: Engine = {
        let mut config = Config::new();
        // TODO: limit execution with fuel
        config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        config.wasm_component_model(true);
        config.async_support(true);
        Engine::new(&config).unwrap()
    };
}

// generate Host trait
wasmtime::component::bindgen!({
    path: "../../wit/workflow-engine/",
    async: true,
    interfaces: "import my-org:workflow-engine/host-activities;",
});

struct HostImports {
    current_event_history: CurrentEventHistory,
}

#[async_trait::async_trait]
impl my_org::workflow_engine::host_activities::Host for HostImports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event::HostActivity(HostActivity::Sleep(Duration::from_millis(millis)));
        let replay_result = self.current_event_history.exit_early_or_replay(event)?;
        assert!(replay_result.is_none());
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum ExecutionError {
    #[error("non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("handle: {0:?}")]
    Handle(EventWrapper),
    #[error("unknown error: {0:?}")]
    UnknownError(anyhow::Error),
}

async fn execute<S, T>(
    mut store: S,
    instance_pre: &wasmtime::component::InstancePre<T>,
    function_name: &str,
) -> wasmtime::Result<String>
where
    S: wasmtime::AsContextMut<Data = T>,
    T: Send,
{
    let instance = instance_pre.instantiate_async(&mut store).await?;
    let func = {
        let mut store = store.as_context_mut();
        let mut exports = instance.exports(&mut store);
        let mut exports = exports.root();
        *exports.typed_func::<(), (String,)>(function_name)?.func()
    };
    // call func
    let callee = unsafe { wasmtime::component::TypedFunc::<(), (String,)>::new_unchecked(func) };
    let (ret0,) = callee.call_async(&mut store, ()).await?;
    callee.post_return_async(&mut store).await?;
    Ok(ret0)
}

async fn replay_execute_next_step(
    execution_config: &mut ExecutionConfig<'_>,
    instance_pre: &InstancePre<HostImports>,
) -> Result<String, ExecutionError> {
    // Instantiate the component
    let mut store = Store::new(
        &ENGINE,
        HostImports {
            current_event_history: CurrentEventHistory::new(execution_config.event_history),
        },
    );
    execute(&mut store, instance_pre, execution_config.function_name)
        .await
        .map_err(|err| {
            match err
                .source()
                .and_then(|source| source.downcast_ref::<HostFunctionError>())
            {
                Some(HostFunctionError::NonDeterminismDetected(reason)) => {
                    ExecutionError::NonDeterminismDetected(reason.clone())
                }
                Some(HostFunctionError::Handle(_)) => {
                    ExecutionError::Handle(EventWrapper(Arc::new(err)))
                }
                None => ExecutionError::UnknownError(err),
            }
        })
}

async fn execute_all(
    execution_config: &mut ExecutionConfig<'_>,
    instance_pre: &InstancePre<HostImports>,
    activities: Arc<Activities>,
) -> wasmtime::Result<String> {
    loop {
        let res = replay_execute_next_step(execution_config, instance_pre).await;
        match res {
            Ok(output) => return Ok(output),
            Err(ExecutionError::Handle(event)) => {
                event
                    .handle(execution_config.event_history, activities.clone())
                    .await?;
            }
            Err(ExecutionError::NonDeterminismDetected(reason)) => {
                panic!("Non determinism detected: {reason}")
            }
            Err(ExecutionError::UnknownError(err)) => panic!("Unknown error: {err:?}"),
        }
    }
}

#[derive(Debug)]
struct ExecutionConfig<'a> {
    event_history: &'a mut EventHistory,
    function_name: &'a str,
}

pub(crate) struct Workflow {
    instance_pre: InstancePre<HostImports>,
    activities: Arc<Activities>,
}
impl Workflow {
    pub(crate) async fn new(
        wasm_path: &str,
        activities: Arc<Activities>,
    ) -> Result<Self, anyhow::Error> {
        let wasm = std::fs::read(wasm_path).with_context(|| format!("cannot open {wasm_path}"))?;
        let instance_pre = {
            let mut linker = Linker::new(&ENGINE);
            // Add workflow host functions
            my_org::workflow_engine::host_activities::add_to_linker(
                &mut linker,
                |state: &mut HostImports| state,
            )?;
            // add activities
            for (ifc_fqn, function_name) in activities.activity_functions() {
                let mut inst = linker.instance(ifc_fqn)?;
                let ifc_fqn = Arc::new(ifc_fqn.to_string());
                let function_name = Arc::new(function_name.to_string());
                inst.func_wrap(
                    &function_name.clone(),
                    move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports>, (): ()| {
                        let ifc_fqn = ifc_fqn.clone();
                        let function_name = function_name.clone();
                        let event = Event::WasmActivity(WasmActivity {
                            ifc_fqn,
                            function_name,
                        });
                        let store = store_ctx.data_mut();
                        let replay_result =
                            store.current_event_history.exit_early_or_replay(event)?;
                        let replay_result = replay_result.expect("currently hardcoded");
                        Ok((replay_result,))
                    },
                )?;
            }
            // Read and compile the wasm component
            let component = Component::from_binary(&ENGINE, &wasm)?;
            linker.instantiate_pre(&component)?
        };
        Ok(Self {
            instance_pre,
            activities,
        })
    }

    pub(crate) async fn run(
        &self,
        event_history: &mut EventHistory,
        function_name: &str,
    ) -> Result<String, anyhow::Error> {
        let mut execution_config = ExecutionConfig {
            event_history,
            function_name,
        };
        execute_all(
            &mut execution_config,
            &self.instance_pre,
            self.activities.clone(),
        )
        .await
    }
}
