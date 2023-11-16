use std::time::Duration;

use wasmtime::{
    self,
    component::{Component, Linker},
    Config, Engine, Store,
};

wasmtime::component::bindgen!({
    world: "example",
    path: "../wit/hello-world.wit",
    async: true,
});

#[derive(Default)]
struct Imports {
    slept: bool,
}

#[async_trait::async_trait]
impl ExampleImports for Imports {
    async fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        if !self.slept {
            println!("Sleeping for {millis}ms");
            tokio::time::sleep(Duration::from_millis(millis)).await;
            self.slept = true;
        } else {
            println!("Not sleeping");
        }
        Ok(())
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
    let mut store = Store::new(&engine, Imports::default());
    let mut linker = Linker::new(&engine);
    Example::add_to_linker(&mut linker, |state: &mut Imports| state)?;
    // Read and compile the wasm component
    let component = Component::from_file(&engine, wasm)?;
    // Instantiate the component
    let (example, _instance) = Example::instantiate_async(&mut store, &component, &linker).await?;
    let res = example.call_execute(&mut store).await?;
    println!("Got {res}");
    let res = example.call_execute(&mut store).await?;
    println!("Got {res}");
    Ok(())
}
