use std::sync::Arc;

use anyhow::Context;

mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let activity_wasm_path = "target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm";
    let activities = Arc::new(activity::Activities::new(activity_wasm_path).await?);
    dbg!(&activities);

    let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    let workflow_function = "execute";
    let workflow_wasm_contents = std::fs::read(workflow_wasm_path)
        .with_context(|| format!("cannot open {workflow_wasm_path}"))?;
    let workflow = workflow::Workflow::new(&workflow_wasm_contents, activities.clone()).await?;

    println!("Starting first workflow execution");
    let mut event_history = Vec::new();
    let res = workflow.run(&mut event_history, workflow_function).await;
    println!("Finished: {res:?}, {event_history:?}");
    println!();
    println!("Replaying");
    let res = workflow.run(&mut event_history, workflow_function).await;
    println!("Finished: {res:?}, {event_history:?}");
    Ok(())
}
