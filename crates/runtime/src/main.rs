use std::{sync::Arc, time::Instant};

use crate::event_history::EventHistory;

mod activity;
mod event_history;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let workflow_function = std::env::args()
        .skip(1)
        .next()
        .expect("workflow function missing");

    let activity_wasm_path = "target/wasm32-unknown-unknown/release/wasm_email_provider.wasm";
    let activities = Arc::new(dbg!(activity::Activities::new(activity_wasm_path).await?));

    let workflow_wasm_path = "target/wasm32-unknown-unknown/debug/hello_world.wasm";
    let workflow = workflow::Workflow::new(workflow_wasm_path, activities.clone()).await?;

    let mut event_history = EventHistory::default();
    {
        println!("Starting first workflow execution");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, &workflow_function).await;
        println!(
            "Finished: in {ms}ms {res:?}",
            ms = timer.elapsed().as_millis()
        );
    }
    println!();
    {
        println!("Replaying");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, &workflow_function).await;
        println!(
            "Finished: in {us}us {res:?}",
            us = timer.elapsed().as_micros()
        );
    }
    Ok(())
}
