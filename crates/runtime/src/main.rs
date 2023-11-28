use std::{sync::Arc, time::Instant};

use crate::event_history::EventHistory;

mod activity;
mod event_history;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let timer = Instant::now();
    let mut args = std::env::args().skip(1);
    let workflow_function = args.next().expect("workflow function missing");

    let activity_wasm_path = args
        .next()
        .unwrap_or("target/wasm32-unknown-unknown/release/wasm_email_provider.wasm".to_string());
    let activities = Arc::new(dbg!(activity::Activities::new(&activity_wasm_path).await?));

    let workflow_wasm_path = args
        .next()
        .unwrap_or("target/wasm32-unknown-unknown/release/hello_world.wasm".to_string());
    let workflow = workflow::Workflow::new(&workflow_wasm_path, activities.clone()).await?;
    println!("Initialized in {duration:?}", duration = timer.elapsed());
    println!();

    let mut event_history = EventHistory(Vec::new());
    {
        println!("Starting first workflow execution");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, &workflow_function).await;
        println!(
            "Finished: in {duration:?} {res:?}, event history size: {len}",
            duration = timer.elapsed(),
            len = event_history.len()
        );
    }
    println!();
    {
        println!("Replaying");
        let timer = Instant::now();
        let res = workflow.run(&mut event_history, &workflow_function).await;
        println!(
            "Finished: in {duration:?} {res:?}",
            duration = timer.elapsed()
        );
    }
    Ok(())
}
