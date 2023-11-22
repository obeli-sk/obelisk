use std::{fmt::Debug, sync::Arc, time::Duration};
use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Config, Engine, Store,
};

use crate::activity::Activities;

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
    world: "keep-wasmtime-bindgen-happy",
    path: "wit/host-world.wit",
    async: true,
});

#[derive(Clone, Debug, PartialEq)]
enum Event {
    Sleep(Duration),
}

#[derive(thiserror::Error, Debug)]
enum HostFunctionError {
    #[error("non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("handle: {0:?}")]
    Handle(Event),
}

struct HostImports<E: AsRef<Event>> {
    event_history: Vec<E>,
    idx: usize,
}

impl<E: AsRef<Event>> HostImports<E> {
    fn handle(&mut self, event: Event) -> Result<(), HostFunctionError> {
        match self.event_history.get(self.idx).map(AsRef::as_ref) {
            None => {
                // new event needs to be handled by the runtime
                Err(HostFunctionError::Handle(event))
            }
            Some(current) if *current == event => {
                println!("Skipping {current:?}");
                self.idx += 1;
                Ok(())
            }
            Some(other) => Err(HostFunctionError::NonDeterminismDetected(format!(
                "Expected {event:?}, got {other:?}"
            ))),
        }
    }
}

#[async_trait::async_trait]
impl my_org::my_workflow::host_activities::Host for HostImports<EventWrapper> {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event::Sleep(Duration::from_millis(millis));
        Ok(self.handle(event)?)
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

// Holds the wasmtime error in order to avoid cloning the event
#[derive(Clone)]
pub(crate) struct EventWrapper(Arc<anyhow::Error>);
impl AsRef<Event> for EventWrapper {
    fn as_ref(&self) -> &Event {
        match self
            .0
            .source()
            .expect("source must be present")
            .downcast_ref::<HostFunctionError>()
            .expect("source must be HostFunctionError")
        {
            HostFunctionError::Handle(event) => event,
            other => panic!("HostFunctionError::Handle expected, got {other:?}"),
        }
    }
}
impl Debug for EventWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
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

async fn execute_next_step(
    execution_config: &mut ExecutionConfig<'_, EventWrapper>,
    instance_pre: &InstancePre<HostImports<EventWrapper>>,
) -> Result<String, ExecutionError> {
    // Instantiate the component
    let mut store = Store::new(
        &ENGINE,
        HostImports {
            event_history: execution_config.event_history.clone(),
            idx: 0,
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
    execution_config: &mut ExecutionConfig<'_, EventWrapper>,
    instance_pre: &InstancePre<HostImports<EventWrapper>>,
) -> wasmtime::Result<String> {
    loop {
        let res = execute_next_step(execution_config, &instance_pre).await;
        match res {
            Ok(output) => return Ok(output),
            Err(ExecutionError::Handle(event)) => {
                println!("Handling {event:?}");
                match event.as_ref() {
                    Event::Sleep(duration) => tokio::time::sleep(*duration).await,
                }
                execution_config.event_history.push(event);
            }

            Err(ExecutionError::NonDeterminismDetected(reason)) => {
                panic!("Non determinism detected: {reason}")
            }
            Err(ExecutionError::UnknownError(err)) => panic!("Unknown error: {err:?}"),
        }
    }
}

#[derive(Debug)]
struct ExecutionConfig<'a, E: AsRef<Event>> {
    event_history: &'a mut Vec<E>,
    function_name: &'a str,
}

pub(crate) struct Workflow {
    instance_pre: InstancePre<HostImports<EventWrapper>>,
}
impl Workflow {
    pub(crate) async fn new(
        wasm: &[u8],
        activities: Arc<Activities>,
    ) -> Result<Self, anyhow::Error> {
        let instance_pre = {
            let mut linker = Linker::new(&ENGINE);
            // Add workflow host functions
            my_org::my_workflow::host_activities::add_to_linker(
                &mut linker,
                |state: &mut HostImports<_>| state,
            )?;
            // add activities
            for (ifc_fqn, function_name) in activities.activity_functions() {
                let mut inst = linker.instance(ifc_fqn)?;
                let ifc_fqn = Arc::new(ifc_fqn.to_string());
                let function_name = Arc::new(function_name.to_string());
                let activities = activities.clone();
                inst.func_wrap_async(
                    &function_name.clone(),
                    move |_store_ctx: wasmtime::StoreContextMut<'_, HostImports<_>>, (): ()| {
                        let activities = activities.clone();
                        let ifc_fqn = ifc_fqn.clone();
                        let function_name = function_name.clone();
                        Box::new(async move {
                            println!("Before activity {ifc_fqn}.{function_name}");
                            let r2 = activities.run(&ifc_fqn, &function_name).await?;
                            println!("After activity {ifc_fqn}.{function_name}");
                            Ok((r2,))
                            //Ok((Ok::<_, String>("".to_string()),))
                        })
                    },
                )?;
            }

            // Read and compile the wasm component
            let component = Component::from_binary(&ENGINE, wasm)?;
            linker.instantiate_pre(&component)?
        };
        Ok(Self { instance_pre })
    }

    pub(crate) async fn run(
        &self,
        event_history: &mut Vec<EventWrapper>,
        function_name: &str,
    ) -> Result<String, anyhow::Error> {
        let mut execution_config = ExecutionConfig {
            event_history,
            function_name,
        };
        execute_all(&mut execution_config, &self.instance_pre).await
    }
}
