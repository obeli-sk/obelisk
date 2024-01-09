use runtime::{
    activity::ActivityConfig,
    event_history::{EventHistory, SupportedFunctionResult},
    runtime::Runtime,
    workflow::WorkflowConfig,
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::{Arc, Once};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;

static INIT: Once = Once::new();
fn set_up() {
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
    });
}

#[tokio::test]
async fn test_runtime_with_many_activities_and_workflows() -> Result<(), anyhow::Error> {
    set_up();

    let mut runtime = Runtime::default();
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_activity(
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;

    let runtime = Arc::new(runtime);
    let mut event_history = EventHistory::default();
    let res = runtime
        .schedule_workflow(
            &WorkflowId::generate(),
            &mut event_history,
            &FunctionFqn::new("testing:sleep-workflow/workflow", "run"),
            &[],
        )
        .await;
    res.unwrap();

    let mut event_history = EventHistory::default();
    let params = vec![Val::U8(10), Val::U32(1)];
    let res = runtime
        .schedule_workflow(
            &WorkflowId::generate(),
            &mut event_history,
            &FunctionFqn::new("testing:fibo-workflow/workflow", "fiboa"),
            &params,
        )
        .await;
    assert_eq!(res.unwrap(), SupportedFunctionResult::Single(Val::U64(89)));

    Ok(())
}
