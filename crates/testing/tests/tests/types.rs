use std::sync::Arc;

use runtime::{
    activity::Activities,
    event_history::{EventHistory, SupportedFunctionResult},
    runtime::Runtime,
    workflow::WorkflowConfig,
    workflow_id::WorkflowId,
    FunctionFqn,
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
    let mut runtime = Runtime::new(activities);
    let workflow = runtime
        .add_workflow_definition(
            test_programs_types_workflow_builder::TEST_PROGRAMS_TYPES_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let mut event_history = EventHistory::new();
    let iterations: usize = 10;
    let param_vals = format!("[{iterations}]");
    let fqn = FunctionFqn::new("testing:types-workflow/workflow", "noopa");
    let metadata = workflow.function_metadata(&fqn).unwrap();
    let param_vals = metadata.deserialize_params(&param_vals).unwrap();
    let res = workflow
        .execute_all(
            &WorkflowId::generate(),
            &mut event_history,
            &fqn,
            &param_vals,
        )
        .await;
    assert_eq!(res.unwrap(), SupportedFunctionResult::None);
    assert_eq!(event_history.successful_activities(), iterations);
    Ok(())
}
