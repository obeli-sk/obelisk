use std::sync::Arc;

use runtime::{
    activity::Activities,
    event_history::{EventHistory, SupportedFunctionResult},
    workflow::Workflow,
    FunctionFqn, Runtime,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let activities = Arc::new(
        Activities::new(
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
        )
        .await?,
    );
    let runtime = Arc::new(Runtime::new(activities));
    let workflow = Workflow::new(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
        runtime,
    )
    .await?;

    let iterations = 10;

    for workflow_function in ["fibow", "fiboa"] {
        let mut event_history = EventHistory::new();
        let params = vec![Val::U8(10), Val::U32(iterations)];
        let res = workflow
            .execute_all(
                &mut event_history,
                &FunctionFqn::new("testing:fibo-workflow/workflow", workflow_function),
                &params,
            )
            .await;
        assert_eq!(res.unwrap(), SupportedFunctionResult::Single(Val::U64(89)));
        assert_eq!(
            event_history.successful_activities(),
            if workflow_function.ends_with('a') {
                iterations as usize
            } else {
                0
            }
        );
    }
    Ok(())
}
