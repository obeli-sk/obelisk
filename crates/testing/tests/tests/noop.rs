use rstest::rstest;
use runtime::{
    activity::ActivityConfig,
    database::Database,
    event_history::EventHistory,
    runtime::RuntimeBuilder,
    workflow::{AsyncActivityBehavior, WorkflowConfig},
    workflow_id::WorkflowId,
    FunctionFqn, SupportedFunctionResult,
};
use std::str::FromStr;
use std::{
    sync::{Arc, Once},
    time::Instant,
};
use tokio::sync::Mutex;
use tracing::{info, info_span};
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static mut VAL: Option<FlushGuard> = None;
static INIT: Once = Once::new();

fn set_up() -> FlushGuard {
    INIT.call_once(|| {
        let (chrome_layer, guard) = ChromeLayerBuilder::new()
            .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .with(chrome_layer)
            .init();
        unsafe {
            VAL = Some(guard);
        }
    });
    unsafe { VAL.take().unwrap() }
}

#[rstest]
#[tokio::test]
async fn test(
    #[values("noopa", "noopha")] function: &str,
    #[values(1, 10)] iterations: usize,
    #[values("Restart", "KeepWaiting")] activity_behavior: &str,
) -> Result<(), anyhow::Error> {
    let _guard = set_up();

    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_noop_activity_builder::TEST_PROGRAMS_NOOP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_noop_workflow_builder::TEST_PROGRAMS_NOOP_WORKFLOW.to_string(),
            &WorkflowConfig {
                async_activity_behavior: AsyncActivityBehavior::from_str(activity_behavior)
                    .unwrap(),
            },
        )
        .await?;
    let runtime = runtime.build();
    let _abort_handle = runtime.spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let param_vals = format!("[{iterations}]");
    let fqn = FunctionFqn::new("testing:types-workflow/workflow", function);
    let metadata = runtime.workflow_function_metadata(&fqn).unwrap();
    let param_vals = Arc::new(metadata.deserialize_params(&param_vals).unwrap());
    let span = info_span!("stopwatch").entered();
    let stopwatch = Instant::now();
    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history.clone(),
            fqn,
            param_vals,
        )
        .await;
    let stopwatch = stopwatch.elapsed();
    drop(span);
    info!("Finished in {} µs", stopwatch.as_micros());
    assert_eq!(res.unwrap(), SupportedFunctionResult::None);
    assert_eq!(
        event_history.lock().await.successful_activities(),
        iterations
    );
    Ok(())
}
