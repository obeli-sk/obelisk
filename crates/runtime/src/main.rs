use anyhow::Context;

mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let activity_wasm_path = "target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm";
    let activities = activity::Activities::new(activity_wasm_path).await?;

    let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    let workflow_function = "execute";
    let workflow_wasm_contents = std::fs::read(workflow_wasm_path)
        .with_context(|| format!("cannot open {workflow_wasm_path}"))?;
    workflow::workflow_example(&workflow_wasm_contents, workflow_function, &activities).await?;
    Ok(())
}
