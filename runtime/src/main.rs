use std::time::Duration;

use wasmtime::{
    self,
    component::{Component, InstancePre, Linker},
    Config, Engine, Store,
};

// generate my_org::my_workflow::host_activities::Host trait
wasmtime::component::bindgen!({
    world: "keep-wasmtime-bindgen-happy",
    path: "../wit/host-world.wit",
    async: true,
});

pub async fn execute<S, T>(
    mut store: impl wasmtime::AsContextMut<Data = T>,
    instance_pre: &wasmtime::component::InstancePre<T>,
    name: &str,
) -> wasmtime::Result<String>
where
    S: wasmtime::AsContextMut<Data = T>,
    T: Send,
{
    let instance = instance_pre.instantiate_async(&mut store).await?;
    // new
    let execute = {
        let mut store = store.as_context_mut();
        let mut exports = instance.exports(&mut store);
        let mut __exports = exports.root();
        *__exports.typed_func::<(), (String,)>(name)?.func()
    };
    // call_execute
    let callee = unsafe { wasmtime::component::TypedFunc::<(), (String,)>::new_unchecked(execute) };
    let (ret0,) = callee.call_async(&mut store, ()).await?;
    callee.post_return_async(&mut store).await?;
    Ok(ret0)
}

struct HostImports<'a> {
    event_history: &'a [Event],
    idx: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum Event {
    Sleep(Duration),
}

#[derive(thiserror::Error, Debug)]
enum HostFunctionError {
    #[error("Non deterministic execution: {0}")]
    NonDeterminismDetected(String),
    #[error("Persisting {0:?}")]
    Handle(Event),
}

#[async_trait::async_trait]
impl my_org::my_workflow::host_activities::Host for HostImports<'_> {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let event = Event::Sleep(Duration::from_millis(millis));
        match self.event_history.get(self.idx) {
            Some(current) if *current == event => {
                println!("Skipping {current:?}");
                self.idx += 1;
                Ok(())
            }
            Some(other) => {
                anyhow::bail!(HostFunctionError::NonDeterminismDetected(format!(
                    "Expected {event:?}, got {other:?}"
                )))
            }
            None => {
                // new event needs to be handled by the runtime
                anyhow::bail!(HostFunctionError::Handle(event))
            }
        }
    }
}

async fn execute_next_step(
    execution_config: &mut ExecutionConfig<'_>,
    engine: &Engine,
    instance_pre: &InstancePre<HostImports<'_>>,
) -> wasmtime::Result<String> {
    // Instantiate the component
    let mut store = Store::new(
        &engine,
        HostImports {
            event_history: execution_config.event_history,
            idx: 0,
        },
    );
    execute::<&mut Store<_>, _>(&mut store, &instance_pre, execution_config.function_name).await
}

async fn execute_all(
    execution_config: &mut ExecutionConfig<'_>,
    engine: &Engine,
    component: &Component,
    linker: &Linker<HostImports<'_>>,
) -> wasmtime::Result<String> {
    let instance_pre = linker.instantiate_pre(component)?;
    loop {
        let res = execute_next_step(execution_config, engine, &instance_pre).await;
        match res {
            Ok(output) => return Ok(output),
            Err(err)
                if err
                    .source()
                    .is_some_and(|src| src.downcast_ref::<HostFunctionError>().is_some()) =>
            {
                let source: &HostFunctionError = err.source().unwrap().downcast_ref().unwrap();
                match source {
                    HostFunctionError::Handle(event) => {
                        println!("Handling {event:?}");
                        match event {
                            Event::Sleep(duration) => tokio::time::sleep(*duration).await,
                        }
                        execution_config.event_history.push(event.to_owned());
                    }
                    HostFunctionError::NonDeterminismDetected(err) => {
                        panic!("Non determinism detected: {err}")
                    }
                }
            }
            Err(err) => panic!("Unknown error {err:?}"),
        }
    }
}

#[derive(Debug)]
struct ExecutionConfig<'a> {
    event_history: &'a mut Vec<Event>,
    function_name: &'a str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let wasm = args.next().expect("Parameters: file.wasm function_name");
    let function_name = args.next().expect("Parameters: file.wasm function_name");
    // Enable component model and async support
    let mut config = Config::new();
    config.async_support(true).wasm_component_model(true);
    // Create a wasmtime execution context
    let engine = Engine::new(&config)?;
    let mut linker = Linker::new(&engine);
    // add host functions
    my_org::my_workflow::host_activities::add_to_linker(&mut linker, |state: &mut HostImports| {
        state
    })?;
    // Read and compile the wasm component
    let component = Component::from_file(&engine, wasm)?;
    // Prepare ExecutionConfig
    let mut event_history = Vec::new();
    let mut execution_config = ExecutionConfig {
        event_history: &mut event_history,
        function_name: &function_name,
    };
    // Execute once recording the events
    let output = execute_all(&mut execution_config, &engine, &component, &linker).await?;
    println!("Finished: {output}, {execution_config:?}");
    println!();
    // Execute by replaying the event history
    let output = execute_all(&mut execution_config, &engine, &component, &linker).await?;
    println!("Finished: {output}, {execution_config:?}");
    Ok(())
}
