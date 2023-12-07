use std::sync::Arc;

use runtime::{
    activity::Activities,
    event_history::{EventHistory, SupportedFunctionResult},
    workflow::Workflow,
};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use val_json::TypeWrapper;
use wasmtime::component::Val;

#[tokio::test]
async fn test() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let activities = Arc::new(
        Activities::new(test_programs_builder::TEST_PROGRAMS_TYPES_ACTIVITY.to_string()).await?,
    );
    let workflow = Workflow::new(
        test_programs_builder::TEST_PROGRAMS_TYPES_WORKFLOW.to_string(),
        activities.clone(),
    )
    .await?;
    let mut event_history = EventHistory::new();
    let iterations = 10;
    let param_types = r#"["U8"]"#;
    let param_vals = format!("[{iterations}]");
    let param_types: Vec<TypeWrapper> = serde_json::from_str(param_types).unwrap();
    let param_vals = val_json::deserialize_sequence::<Val>(&param_vals, &param_types).unwrap();
    let res = workflow
        .execute_all(
            &mut event_history,
            "testing:types-workflow/workflow",
            "noop",
            &param_vals,
        )
        .await;
    assert_eq!(res.unwrap(), SupportedFunctionResult::None);
    assert_eq!(event_history.len(), iterations as usize);
    Ok(())
}
