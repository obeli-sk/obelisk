use std::sync::Arc;

use runtime::{
    activity::Activities,
    event_history::{EventHistory, SupportedFunctionResult},
    workflow::Workflow,
    FunctionFqn, Runtime,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let activities = Arc::new(
        Activities::new(
            test_programs_types_activity_builder::TEST_PROGRAMS_TYPES_ACTIVITY.to_string(),
        )
        .await?,
    );
    let runtime = Arc::new(Runtime::new(activities));
    let workflow = Workflow::new(
        test_programs_types_workflow_builder::TEST_PROGRAMS_TYPES_WORKFLOW.to_string(),
        runtime,
    )
    .await?;
    let mut event_history = EventHistory::new();
    let iterations = 10;
    let param_vals = format!("[{iterations}]");
    let fqn = FunctionFqn::new("testing:types-workflow/workflow", "noopa");
    let metadata = workflow.function_metadata(&fqn).unwrap();
    let param_vals = metadata.deserialize_params(&param_vals).unwrap();
    let res = workflow
        .execute_all(&mut event_history, &fqn, &param_vals)
        .await;
    assert_eq!(res.unwrap(), SupportedFunctionResult::None);
    assert_eq!(event_history.len(), iterations as usize);
    Ok(())
}
