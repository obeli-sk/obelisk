mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    let workflow_function = "execute";
    workflow::workflow_example(workflow_wasm_path, workflow_function).await?;

    let activity_wasm_path = "target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm";
    let activity_function = "send";
    activity::activity_example(activity_wasm_path, activity_function).await?;
    Ok(())
}
