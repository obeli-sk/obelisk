use assert_matches::assert_matches;
use rstest::*;
use runtime::{
    activity::ActivityConfig,
    database::Database,
    error::ExecutionError,
    event_history::EventHistory,
    runtime::{EngineConfig, RuntimeBuilder, RuntimeConfig},
    workflow::{AsyncActivityBehavior, WorkflowConfig},
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::{Arc, Once};
use tokio::sync::Mutex;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static INIT: Once = Once::new();
fn set_up() {
    INIT.call_once(|| {
        tracing_subscriber::registry()
            .with(fmt::layer())
            .with(EnvFilter::from_default_env())
            .init();
    });
}

#[rstest]
#[tokio::test]
async fn test_async_activity(
    #[values("sleep-host-activity", "sleep-activity")] function: &str,
    #[values(WorkflowConfig {
        async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
    },
    WorkflowConfig {
        async_activity_behavior: AsyncActivityBehavior::Restart,
    })]
    workflow_config: WorkflowConfig,
) -> Result<(), anyhow::Error> {
    set_up();

    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &workflow_config,
        )
        .await?;
    let _abort_handle = runtime.build().spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let params = Arc::new(vec![wasmtime::component::Val::U64(0)]);
    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::new(
                COUNTER
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    .to_string(),
            ),
            event_history,
            FunctionFqn::new(
                "testing:sleep-workflow/workflow".to_string(),
                function.to_string(),
            ),
            params,
        )
        .await;
    res.unwrap();

    Ok(())
}

#[tokio::test]
async fn test_call_activity_with_version() -> Result<(), anyhow::Error> {
    set_up();

    let database = Database::new(100, 100);
    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let _abort_handle = runtime.build().spawn(&database);
    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history,
            FunctionFqn::new("testing:sleep-workflow/workflow", "run"),
            Arc::new(Vec::new()),
        )
        .await;
    res.unwrap();

    Ok(())
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
const ITERATIONS: u32 = 2;
const SLEEP_MILLIS: u64 = 1000;
const LIMIT: u32 = ITERATIONS - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitEngineKind {
    Workflow,
    Activity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display)]
enum LimitKind {
    #[display(fmt = "core")]
    Core,
    #[display(fmt = "component")]
    Component,
}
impl LimitKind {
    fn config(&self) -> EngineConfig {
        match self {
            Self::Core => {
                let mut config = wasmtime::PoolingAllocationConfig::default();
                config.total_core_instances(LIMIT);

                EngineConfig {
                    allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(config),
                }
            }
            Self::Component => {
                let mut config = wasmtime::PoolingAllocationConfig::default();
                config.total_component_instances(LIMIT);
                EngineConfig {
                    allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(config),
                }
            }
        }
    }
}

#[rstest]
#[tokio::test]
async fn test_limits(
    #[values(LimitEngineKind::Workflow, LimitEngineKind::Activity)]
    limit_engine_kind: LimitEngineKind,
    #[values(LimitKind::Core, LimitKind::Component)] limit_kind: LimitKind,
) -> Result<(), anyhow::Error> {
    set_up();
    let database = Database::new(100, 100);
    let engine_config = limit_kind.config();
    let mut runtime =
        RuntimeBuilder::new_with_config(if limit_engine_kind == LimitEngineKind::Activity {
            RuntimeConfig {
                activity_engine_config: engine_config,
                workflow_engine_config: EngineConfig::default(),
            }
        } else {
            RuntimeConfig {
                activity_engine_config: EngineConfig::default(),
                workflow_engine_config: engine_config,
            }
        });
    runtime
        .add_activity(
            test_programs_sleep_activity_builder::TEST_PROGRAMS_SLEEP_ACTIVITY.to_string(),
            &ActivityConfig::default(),
        )
        .await?;
    runtime
        .add_workflow_definition(
            test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
            &WorkflowConfig::default(),
        )
        .await?;
    let mut futures = Vec::new();
    let runtime = runtime.build();
    // Start tokio tasks to process the futures concurrently.
    let _abort_handles: Vec<_> = (0..ITERATIONS).map(|_| runtime.spawn(&database)).collect();
    // Prepare futures, last one processed should fail.
    for _ in 0..ITERATIONS {
        let workflow_scheduler = database.workflow_scheduler();
        let join_handle = async move {
            let event_history = Arc::new(Mutex::new(EventHistory::default()));
            let params = Arc::new(vec![wasmtime::component::Val::U64(SLEEP_MILLIS)]);
            workflow_scheduler
                .schedule_workflow(
                    WorkflowId::new(
                        COUNTER
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                            .to_string(),
                    ),
                    event_history,
                    FunctionFqn::new("testing:sleep-workflow/workflow", "sleep-activity"),
                    params,
                )
                .await
        };
        futures.push(join_handle);
    }
    let err = futures_util::future::join_all(futures)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap_err();

    if limit_engine_kind == LimitEngineKind::Activity {
        assert_matches!(err,
            ExecutionError::ActivityLimitReached { reason, .. }
            if reason == format!("maximum concurrent {limit_kind} instance limit of {LIMIT} reached"),
            "{:?}", match &err { ExecutionError::UnknownError{source,..} => source.to_string(), _ => "".to_string()}
        );
    } else {
        assert_matches!(err,
            ExecutionError::LimitReached { reason, .. }
            if reason == format!("maximum concurrent {limit_kind} instance limit of {LIMIT} reached"),
            "{:?}", match &err { ExecutionError::UnknownError{source,..} => source.to_string(), _ => "".to_string()}
        );
    }

    Ok(())
}
