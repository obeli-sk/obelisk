use assert_matches::assert_matches;
use runtime::{
    activity::ActivityConfig,
    event_history::{EventHistory, SupportedFunctionResult},
    runtime::Runtime,
    workflow::{ExecutionError, WorkflowConfig},
    workflow_id::WorkflowId,
    ActivityFailed, FunctionFqn,
};
use std::sync::Arc;
use std::sync::Once;
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
async fn patch_activity() -> Result<(), anyhow::Error> {
    set_up();
    const EXPECTED_ACTIVITY_CALLS: u32 = 10;
    let fqn = FunctionFqn::new("testing:patch-workflow/workflow", "noopa");
    let param_vals = vec![Val::U32(EXPECTED_ACTIVITY_CALLS)];
    let mut event_history = EventHistory::default();
    {
        let mut runtime = Runtime::default();
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
        let workflow_id = WorkflowId::generate();
        let res = runtime
            .schedule_workflow(&workflow_id, &mut event_history, &fqn, &param_vals)
            .await;

        assert_matches!(
            res.unwrap_err(),
            ExecutionError::ActivityFailed {
                workflow_fqn,
                activity_fqn,
                reason: _,
                workflow_id: found_id
            } if workflow_fqn == fqn && found_id == workflow_id &&
            activity_fqn == Arc::new(FunctionFqn::new("testing:patch/patch", "noop"))
        );
        assert_eq!(
            event_history.successful_activities(),
            usize::try_from(5).unwrap()
        );
    }
    // Remove the failed activity from the event history.
    let mut event_history: Vec<_> = event_history.into();
    assert_matches!(
        event_history.pop().unwrap(),
        (_, _, Err(ActivityFailed { .. }))
    );
    let mut event_history = EventHistory::from(event_history);

    {
        let mut runtime = Runtime::default();
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

fn generate_history(max: u32) -> EventHistory {
    let mut event_history = Vec::new();
    let activity_fqn = Arc::new(FunctionFqn::new("testing:patch/patch", "noop"));
    for i in 0..max {
        event_history.push((
            activity_fqn.clone(),
            Arc::new(vec![Val::U32(i)]),
            Ok(SupportedFunctionResult::None),
        ));
    }
    EventHistory::from(event_history)
}

#[tokio::test]
async fn generate_event_history_matching() -> Result<(), anyhow::Error> {
    set_up();
    const EXPECTED_ACTIVITY_CALLS: u32 = 10;
    let mut event_history = generate_history(EXPECTED_ACTIVITY_CALLS);
    let fqn = FunctionFqn::new("testing:patch-workflow/workflow", "noopa");
    let param_vals = vec![Val::U32(EXPECTED_ACTIVITY_CALLS)];

    let mut runtime = Runtime::default();
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
    Ok(())
}

#[tokio::test]
async fn generate_event_history_too_big() -> Result<(), anyhow::Error> {
    set_up();
    const EXPECTED_ACTIVITY_CALLS: u32 = 10;
    let mut event_history = generate_history(EXPECTED_ACTIVITY_CALLS + 1);
    let fqn = FunctionFqn::new("testing:patch-workflow/workflow", "noopa");
    let param_vals = vec![Val::U32(EXPECTED_ACTIVITY_CALLS)];

    let mut runtime = Runtime::default();
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
    let workflow_id = WorkflowId::generate();
    let res = runtime
        .schedule_workflow(&workflow_id, &mut event_history, &fqn, &param_vals)
        .await;

    assert_matches!(
        res.unwrap_err(),
        ExecutionError::NonDeterminismDetected(found_fqn , found_id,_reason)
         if found_fqn == fqn && found_id == workflow_id
    );
    Ok(())
}
