use std::{fmt::Debug, time::Duration};
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

struct HostImports<'a, E: AsRef<Event>> {
    event_history: &'a [E],
    idx: usize,
    activities: &'a Activities,
}

impl<E: AsRef<Event>> HostImports<'_, E> {
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
impl my_org::my_workflow::host_activities::Host for HostImports<'_, EventWrapper> {
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
struct EventWrapper(anyhow::Error);
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

pub async fn execute<S, T>(
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
    instance_pre: &InstancePre<HostImports<'_, EventWrapper>>,
    activities: &Activities,
) -> Result<String, ExecutionError> {
    // Instantiate the component
    let mut store = Store::new(
        &ENGINE,
        HostImports {
            event_history: execution_config.event_history,
            idx: 0,
            activities,
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
                Some(HostFunctionError::Handle(_)) => ExecutionError::Handle(EventWrapper(err)),
                None => ExecutionError::UnknownError(err),
            }
        })
}

async fn execute_all(
    execution_config: &mut ExecutionConfig<'_, EventWrapper>,
    instance_pre: &InstancePre<HostImports<'_, EventWrapper>>,
    activities: &Activities,
) -> wasmtime::Result<String> {
    loop {
        let res = execute_next_step(execution_config, &instance_pre, activities).await;
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

pub(crate) async fn workflow_example(
    wasm: &[u8],
    function_name: &str,
    activities: &Activities,
) -> Result<(), anyhow::Error> {
    let instance_pre = {
        let mut linker = Linker::new(&ENGINE);
        // Add workflow host functions
        my_org::my_workflow::host_activities::add_to_linker(
            &mut linker,
            |state: &mut HostImports<_>| state,
        )?;
        // add activities
        {
            let mut inst = linker.instance("my-org:my-workflow/my-activity")?;
            inst.func_wrap_async(
                "send",
                move |mut store_ctx: wasmtime::StoreContextMut<'_, HostImports<_>>, (): ()| {
                    Box::new(async move {
                        let host_imports = store_ctx.data_mut();
                        let r2 = host_imports.activities.run("send").await?;
                        Ok((r2,))
                    })
                },
            )?;
        }

        // Read and compile the wasm component
        let component = Component::from_binary(&ENGINE, wasm)?;
        linker.instantiate_pre(&component)?
    };
    // Prepare ExecutionConfig
    let mut event_history = Vec::new();
    let mut execution_config = ExecutionConfig {
        event_history: &mut event_history,
        function_name,
    };
    // Execute once recording the events
    let output = execute_all(&mut execution_config, &instance_pre, activities).await?;
    println!("Finished: {output}, {execution_config:?}");
    println!();
    // Execute by replaying the event history
    let output = execute_all(&mut execution_config, &instance_pre, activities).await?;
    println!("Finished: {output}, {execution_config:?}");
    Ok(())
}
