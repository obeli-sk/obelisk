use runtime::{
    activity::ActivityConfig,
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

    let mut runtime = Runtime::default();
    runtime
        .add_activity(
            test_programs_types_activity_builder::TEST_PROGRAMS_TYPES_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_types_workflow_builder::TEST_PROGRAMS_TYPES_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let mut event_history = EventHistory::default();
    let iterations: usize = 10;
    let param_vals = format!("[{iterations}]");
    let fqn = FunctionFqn::new("testing:types-workflow/workflow", "noopa");
    let metadata = runtime.workflow_function_metadata(&fqn).unwrap();
    let param_vals = metadata.deserialize_params(&param_vals).unwrap();
    let res = runtime
        .schedule_workflow(
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
