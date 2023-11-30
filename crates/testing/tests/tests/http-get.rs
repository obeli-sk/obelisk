use std::{sync::Arc, time::Instant};

use runtime::{activity::Activities, event_history::EventHistory, workflow::Workflow};

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    println!("{}", test_programs_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY);
    let activities = Arc::new(dbg!(
        Activities::new(test_programs_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY.to_string()).await?
    ));
    let workflow = Workflow::new(
        test_programs_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW.to_string(),
        activities.clone(),
    )
    .await?;
    let mut event_history = EventHistory::new();
    let timer = Instant::now();
    let res = workflow
        .execute_all(
            &mut event_history,
            Some("testing:http-workflow/workflow"),
            "execute",
        )
        .await;
    println!(
        "Finished: in {duration:?} {res:?}, event history size: {len}",
        duration = timer.elapsed(),
        len = event_history.len()
    );
    Ok(())
}
