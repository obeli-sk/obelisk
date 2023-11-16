use std::time::Duration;

use wasmtime::{
    self,
    component::{Component, Linker},
    Config, Engine, Store,
};

wasmtime::component::bindgen!("example" in "wit/hello-world.wit");

#[derive(Default)]
struct Imports {
    slept: bool,
}
impl ExampleImports for Imports {
    fn sleep(&mut self, millis: u64) -> wasmtime::Result<()> {
        if !self.slept {
            println!("Sleeping for {millis}ms");
            std::thread::sleep(Duration::from_millis(millis));
            self.slept = true;
        } else {
            println!("Not sleeping");
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm = std::env::args().skip(1).next().expect("USAGE: demo WASM");
    // Enable component model (which isn't supported by default)
    let mut config = Config::new();
    config.wasm_component_model(true);
    // Create a wasmtime execution context
    let engine = Engine::new(&config)?;
    let mut store = Store::new(&engine, Imports::default());
    let mut linker = Linker::new(&engine);
    Example::add_to_linker(&mut linker, |state: &mut Imports| state)?;
    // Read and compile the wasm component
    let component = Component::from_file(&engine, wasm)?;
    // Instantiate a markdown instance
    let (instance, _) = Example::instantiate(&mut store, &component, &linker)?;
    let res = instance.call_execute(&mut store)?;
    println!("Got {res}");
    let res = instance.call_execute(&mut store)?;
    println!("Got {res}");
    Ok(())
}
