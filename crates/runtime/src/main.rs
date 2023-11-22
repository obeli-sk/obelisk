use std::{sync::Arc, time::Instant};

mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let activity_wasm_path = "target/wasm32-unknown-unknown/debug/wasm_email_provider.wasm";
    let activities = Arc::new(dbg!(activity::Activities::new(activity_wasm_path).await?));

    let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    let workflow_function = "execute";
    let workflow = workflow::Workflow::new(workflow_wasm_path, activities.clone()).await?;

    let mut event_history = Vec::new();
    {
        println!("Starting first workflow execution");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, workflow_function).await;
        println!(
            "Finished: in {ms}ms {res:?}, {event_history:?}",
            ms = timer.elapsed().as_millis()
        );
    }
    println!();
    {
        println!("Replaying");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, workflow_function).await;
        println!(
            "Finished: in {us}us {res:?}, {event_history:?}",
            us = timer.elapsed().as_micros()
        );
    }
    Ok(())
}
