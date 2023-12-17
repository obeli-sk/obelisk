use std::sync::Arc;

use assert_matches::assert_matches;
use runtime::{
    activity::Activities,
    event_history::EventHistory,
    workflow::{ExecutionError, Workflow},
    FunctionFqn, Runtime,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;

const EXPECTED_ACTIVITY_CALLS: u32 = 10;

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let mut event_history = EventHistory::new();

    let fqn = FunctionFqn::new("testing:patch-workflow/workflow", "noopa");
    let param_vals = vec![Val::U32(EXPECTED_ACTIVITY_CALLS)];
    {
        let activities = Arc::new(
            Activities::new(
                test_programs_patch_activity_broken_builder::TEST_PROGRAMS_PATCH_ACTIVITY_BROKEN
                    .to_string(),
            )
            .await?,
        );
        let runtime = Arc::new(Runtime::new(activities));
        let workflow = Workflow::new(
            test_programs_patch_workflow_builder::TEST_PROGRAMS_PATCH_WORKFLOW.to_string(),
            runtime,
        )
        .await?;
        let res = workflow
            .execute_all(&mut event_history, &fqn, &param_vals)
            .await;

        assert_eq!(event_history.len(), 5);
        assert_matches!(
            res.unwrap_err(),
            ExecutionError::ActivityFailed {
                workflow_fqn,
                activity_fqn,
                reason: _,
            } if workflow_fqn == fqn &&
            activity_fqn == Arc::new(FunctionFqn::new("testing:patch/patch", "noop"))
        );
    }
    {
        let activities = Arc::new(
            Activities::new(
                test_programs_patch_activity_fixed_builder::TEST_PROGRAMS_PATCH_ACTIVITY_FIXED
                    .to_string(),
            )
            .await?,
        );
        let runtime = Arc::new(Runtime::new(activities));
        let workflow = Workflow::new(
            test_programs_patch_workflow_builder::TEST_PROGRAMS_PATCH_WORKFLOW.to_string(),
            runtime,
        )
        .await?;
        workflow
            .execute_all(&mut event_history, &fqn, &param_vals)
            .await
            .unwrap();
    }
    assert_eq!(
        u32::try_from(event_history.len()).unwrap(),
        EXPECTED_ACTIVITY_CALLS
    );
    Ok(())
}
