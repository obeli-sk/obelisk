mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut config = wasmtime::Config::new();
    // TODO: limit execution with epoch_interruption
    config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
    config.wasm_component_model(true);
    config.async_support(true);
    let engine = wasmtime::Engine::new(&config)?;

    // let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    // let workflow_function = "execute";
    // workflow::workflow_example(workflow_wasm_path, workflow_function).await?;
    let activity_wasm_path = "target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm";
    let activities = activity::Activities::new(activity_wasm_path, &engine).await?;
    let first_activity = activities
        .function_names()
        .next()
        .expect("at least one activity must be defined");
    println!("Running {first_activity}");
    let res = activities.run(&engine, first_activity).await?;
    println!("{res:?}");

    Ok(())
}
