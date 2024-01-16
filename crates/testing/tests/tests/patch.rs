use assert_matches::assert_matches;
use runtime::{
    activity::ActivityConfig,
    database::Database,
    event_history::{EventHistory, SupportedFunctionResult},
    runtime::Runtime,
    workflow::{ExecutionError, WorkflowConfig},
    workflow_id::WorkflowId,
    ActivityFailed, FunctionFqn,
};
use std::sync::Arc;
use std::sync::Once;
use tokio::sync::Mutex;
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
    let fqn = Arc::new(FunctionFqn::new("testing:patch-workflow/workflow", "noopa"));
    let param_vals = Arc::new(vec![Val::U32(EXPECTED_ACTIVITY_CALLS)]);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let database = Database::new(100, 100);
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
        let abort_handle = runtime.spawn(&database);
        let workflow_id = WorkflowId::generate();
        let res = database
            .workflow_scheduler()
            .schedule_workflow(
                workflow_id.clone(),
                event_history.clone(),
                fqn.clone(),
                param_vals.clone(),
            )
            .await;

        assert_matches!(
            res.unwrap_err(),
            ExecutionError::ActivityFailed {
                workflow_fqn,
                activity_fqn,
                reason: _,
                workflow_id: found_workflow_id,
                run_id,
            } if workflow_fqn == *fqn && found_workflow_id == workflow_id && run_id == 5 &&
            activity_fqn == Arc::new(FunctionFqn::new("testing:patch/patch", "noop"))
        );
        assert_eq!(
            event_history.lock().await.successful_activities(),
            usize::try_from(5).unwrap()
        );
        abort_handle.abort(); // FIXME
    }
    // Remove the failed activity from the event history.
    let mut event_history: Vec<_> = Arc::try_unwrap(event_history).unwrap().into_inner().into();
    assert_matches!(
        event_history.pop().unwrap(),
        (_, _, Err(ActivityFailed::Other { .. }))
    );
    let event_history = Arc::new(Mutex::new(EventHistory::from(event_history)));
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
        runtime.spawn(&database);
        database
            .workflow_scheduler()
            .schedule_workflow(
                WorkflowId::generate(),
                event_history.clone(),
                fqn,
                param_vals,
            )
            .await
            .unwrap();
        assert_eq!(
            event_history.lock().await.successful_activities(),
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
    let event_history = Arc::new(Mutex::new(generate_history(EXPECTED_ACTIVITY_CALLS)));
    let fqn = Arc::new(FunctionFqn::new("testing:patch-workflow/workflow", "noopa"));
    let param_vals = Arc::new(vec![Val::U32(EXPECTED_ACTIVITY_CALLS)]);
    let database = Database::new(100, 100);
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
    runtime.spawn(&database);
    database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history.clone(),
            fqn,
            param_vals,
        )
        .await
        .unwrap();

    assert_eq!(
        event_history.lock().await.successful_activities(),
        usize::try_from(EXPECTED_ACTIVITY_CALLS).unwrap()
    );
    Ok(())
}

#[tokio::test]
async fn generate_event_history_too_big() -> Result<(), anyhow::Error> {
    set_up();
    const EXPECTED_ACTIVITY_CALLS: u32 = 10;
    let event_history = Arc::new(Mutex::new(generate_history(EXPECTED_ACTIVITY_CALLS + 1)));
    let fqn = Arc::new(FunctionFqn::new("testing:patch-workflow/workflow", "noopa"));
    let param_vals = Arc::new(vec![Val::U32(EXPECTED_ACTIVITY_CALLS)]);
    let database = Database::new(100, 100);
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
    runtime.spawn(&database);
    let workflow_id = WorkflowId::generate();
    let res = database
        .workflow_scheduler()
        .schedule_workflow(workflow_id.clone(), event_history, fqn.clone(), param_vals)
        .await;

    assert_matches!(
        res.unwrap_err(),
        ExecutionError::NonDeterminismDetected {
            fqn: found_fqn ,
            workflow_id: found_id,
            reason,
            run_id
        }
        if found_fqn == *fqn && found_id == workflow_id && run_id == 0 && reason == "replay log was not drained"
    );
    Ok(())
}
