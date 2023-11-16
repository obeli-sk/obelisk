use std::time::Duration;

use wasmtime::{
    self,
    component::{Component, Linker},
    Config, Engine, Store,
};

wasmtime::component::bindgen!({
    world: "sleepy-workflow",
    path: "../wit/hello-world.wit",
    async: true,
});

struct Imports {
    event_history: Vec<Event>,
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
    Persist(Event),
}

#[async_trait::async_trait]
impl my_org::my_workflow::host_activities::Host for Imports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        let expected = Event::Sleep(Duration::from_millis(millis));
        match self.event_history.get(self.idx) {
            Some(current) if *current == expected => {
                println!("Skipping {current:?}");
                self.idx += 1;
                Ok(())
            }
            Some(other) => {
                anyhow::bail!(HostFunctionError::NonDeterminismDetected(format!(
                    "Expected {expected:?}, got {other:?}"
                )))
            }
            None => {
                // persist the new event and quit the current execution
                anyhow::bail!(HostFunctionError::Persist(expected))
            }
        }
    }
}

async fn execute_next_step(
    event_history: Vec<Event>,
    engine: &Engine,
    component: &Component,
    linker: &Linker<Imports>,
) -> wasmtime::Result<String> {
    // Instantiate the component
    let mut store = Store::new(
        &engine,
        Imports {
            event_history,
            idx: 0,
        },
    );
    let (workflow, _instance) =
        SleepyWorkflow::instantiate_async(&mut store, &component, linker).await?;
    workflow.call_execute(&mut store).await
}

async fn execute_all(
    event_history: &mut Vec<Event>,
    engine: &Engine,
    component: &Component,
    linker: &Linker<Imports>,
) -> wasmtime::Result<String> {
    loop {
        let res = execute_next_step(event_history.clone(), &engine, &component, &linker).await;
        match res {
            Ok(output) => return Ok(output),
            Err(err)
                if err
                    .source()
                    .is_some_and(|src| src.downcast_ref::<HostFunctionError>().is_some()) =>
            {
                let source: &HostFunctionError = err.source().unwrap().downcast_ref().unwrap();
                match source {
                    HostFunctionError::Persist(event) => {
                        // handle the event
                        match event {
                            Event::Sleep(duration) => {
                                println!("Sleeping {duration:?}");
                                tokio::time::sleep(*duration).await
                            }
                        }
                        event_history.push(event.to_owned());
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm = std::env::args().skip(1).next().expect("USAGE: demo WASM");
    // Enable component model (which isn't supported by default)
    let mut config = Config::new();
    config.async_support(true).wasm_component_model(true);
    // Create a wasmtime execution context
    let engine = Engine::new(&config)?;
    let mut linker = Linker::new(&engine);
    SleepyWorkflow::add_to_linker(&mut linker, |state: &mut Imports| state)?;
    // Read and compile the wasm component
    let component = Component::from_file(&engine, wasm)?;
    let mut event_history = Vec::new();
    let output = execute_all(&mut event_history, &engine, &component, &linker).await?;
    println!("Finished: {output}, event_history: {event_history:?}");
    println!();
    let output = execute_all(&mut event_history, &engine, &component, &linker).await?;
    println!("Finished: {output}, event_history: {event_history:?}");
    Ok(())
}
