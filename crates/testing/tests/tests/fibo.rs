use runtime::{
    activity::ActivityConfig, database::Database, event_history::EventHistory,
    runtime::RuntimeBuilder, workflow::WorkflowConfig, workflow_id::WorkflowId, FunctionFqn,
    SupportedFunctionResult,
};
use std::sync::{Arc, Once};
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
async fn test_fibow_fiboa() -> Result<(), anyhow::Error> {
    set_up();
    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let _abort_handle = runtime.build().spawn(&database);

    let iterations = 10;

    for workflow_function in ["fibow", "fiboa"] {
        let event_history = Arc::new(Mutex::new(EventHistory::default()));
        let params = Arc::new(vec![Val::U8(10), Val::U32(iterations)]);
        let res = database
            .workflow_scheduler()
            .schedule_workflow(
                WorkflowId::generate(),
                event_history.clone(),
                FunctionFqn::new("testing:fibo-workflow/workflow", workflow_function),
                params,
            )
            .await;
        assert_eq!(res.unwrap(), SupportedFunctionResult::Single(Val::U64(89)));
        assert_eq!(
            event_history.lock().await.successful_activities(),
            if workflow_function.ends_with('a') {
                iterations as usize
            } else {
                0
            }
        );
    }
    Ok(())
}
