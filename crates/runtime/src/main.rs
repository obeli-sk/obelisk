use std::{sync::Arc, time::Instant};

use crate::event_history::EventHistory;

mod activity;
mod event_history;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut args = std::env::args().skip(1);
    let workflow_function = args.next().expect("workflow function missing");

    let activity_wasm_path = args.next().unwrap_or(
        "crates/activities/wasm-email-provider/target/wasm32-unknown-unknown/release/wasm_email_provider.wasm".to_string());
    let activities = Arc::new(dbg!(activity::Activities::new(&activity_wasm_path).await?));

    let workflow_wasm_path = args.next().unwrap_or(
        "crates/workflows/hello-world/target/wasm32-unknown-unknown/release/hello_world.wasm"
            .to_string(),
    );
    let workflow = workflow::Workflow::new(&workflow_wasm_path, activities.clone()).await?;

    let event_history = EventHistory(Vec::new());
    let event_history = {
        println!("Starting first workflow execution");
        let timer = Instant::now();
        let (res, event_history) = workflow.run(event_history, workflow_function.clone()).await;
        println!(
            "Finished: in {duration:?} {res:?}, event history size: {len}",
            duration = timer.elapsed(),
            len = event_history.len()
        );
        event_history
    };
    println!();
    {
        println!("Replaying");
        let timer = Instant::now();
        let (res, _event_history) = workflow.run(event_history, workflow_function).await;
        println!(
            "Finished: in {duration:?} {res:?}",
            duration = timer.elapsed()
        );
    }
    Ok(())
}
