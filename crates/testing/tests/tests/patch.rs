use assert_matches::assert_matches;
use runtime::{
    activity::ActivityConfig,
    event_history::EventHistory,
    runtime::Runtime,
    workflow::{ExecutionError, WorkflowConfig},
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::Arc;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use wasmtime::component::Val;

const EXPECTED_ACTIVITY_CALLS: u32 = 10;

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let fqn = FunctionFqn::new("testing:patch-workflow/workflow", "noopa");
    let param_vals = vec![Val::U32(EXPECTED_ACTIVITY_CALLS)];
    let mut event_history = EventHistory::default();
    {
        let mut runtime = Runtime::new();
        runtime
            .add_activity(
                test_programs_patch_activity_broken_builder::TEST_PROGRAMS_PATCH_ACTIVITY_BROKEN
                    .to_string(),
                &ActivityConfig::default(),
            )
            .await?;
        runtime
            .add_workflow_definition(
                test_programs_patch_workflow_builder::TEST_PROGRAMS_PATCH_WORKFLOW.to_string(),
                &WorkflowConfig::default(),
            )
            .await?;
        let res = runtime
            .schedule_workflow(
                &WorkflowId::generate(),
                &mut event_history,
                &fqn,
                &param_vals,
            )
            .await;

        assert_matches!(
            res.unwrap_err(),
            ExecutionError::ActivityFailed {
                workflow_fqn,
                activity_fqn,
                reason: _,
            } if workflow_fqn == fqn &&
            activity_fqn == Arc::new(FunctionFqn::new("testing:patch/patch", "noop"))
        );
        assert_eq!(
            event_history.successful_activities(),
            usize::try_from(5).unwrap()
        );
    }
    {
        let mut runtime = Runtime::new();
        runtime
            .add_activity(
                test_programs_patch_activity_fixed_builder::TEST_PROGRAMS_PATCH_ACTIVITY_FIXED
                    .to_string(),
                &ActivityConfig::default(),
            )
            .await?;
        runtime
            .add_workflow_definition(
                test_programs_patch_workflow_builder::TEST_PROGRAMS_PATCH_WORKFLOW.to_string(),
                &WorkflowConfig::default(),
            )
            .await?;
        runtime
            .schedule_workflow(
                &WorkflowId::generate(),
                &mut event_history,
                &fqn,
                &param_vals,
            )
            .await
            .unwrap();
        assert_eq!(
            event_history.successful_activities(),
            usize::try_from(EXPECTED_ACTIVITY_CALLS).unwrap()
        );
    }
    Ok(())
}
