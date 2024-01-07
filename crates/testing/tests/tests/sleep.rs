use assert_matches::assert_matches;
use runtime::{
    event_history::EventHistory,
    runtime::{EngineConfig, Runtime, RuntimeConfig},
    workflow::{AsyncActivityBehavior, ExecutionError, WorkflowConfig},
    workflow_id::WorkflowId,
    FunctionFqn,
};
use std::sync::{Arc, Once};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

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
async fn test_async_host_activity() -> Result<(), anyhow::Error> {
    set_up();

    for workflow_config in [
        WorkflowConfig {
            async_activity_behavior: AsyncActivityBehavior::KeepWaiting,
        },
        WorkflowConfig {
            async_activity_behavior: AsyncActivityBehavior::Restart,
        },
    ] {
        let mut runtime = Runtime::default();
        runtime
            .add_workflow_definition(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
                &workflow_config,
            )
            .await?;
        let runtime = Arc::new(runtime);
        let mut event_history = EventHistory::default();
        let params = vec![wasmtime::component::Val::U64(0)];
        let res = runtime
            .schedule_workflow(
                &WorkflowId::new(
                    COUNTER
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        .to_string(),
                ),
                &mut event_history,
                &FunctionFqn::new("testing:sleep-workflow/workflow", "sleep"),
                &params,
            )
            .await;
        res.unwrap();
    }
    Ok(())
}

static COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

#[tokio::test]
async fn test_limits() -> Result<(), anyhow::Error> {
    set_up();

    const ITERATIONS: u32 = 2;
    const SLEEP_MILLIS: u64 = 0;
    const LIMIT: u32 = ITERATIONS - 1;

    let mut limit_core_instances = wasmtime::PoolingAllocationConfig::default();
    limit_core_instances.total_core_instances(LIMIT);

    let mut limit_component_instances = wasmtime::PoolingAllocationConfig::default();
    limit_component_instances.total_component_instances(LIMIT);

    for (pooling_config, limit_name) in [
        (limit_core_instances, "core"),
        (limit_component_instances, "component"),
    ] {
        let mut runtime = Runtime::new_with_config(RuntimeConfig {
            workflow_engine_config: EngineConfig {
                allocation_strategy: wasmtime::InstanceAllocationStrategy::Pooling(pooling_config),
            },
            activity_engine_config: EngineConfig::default(),
        });
        runtime
            .add_workflow_definition(
                test_programs_sleep_workflow_builder::TEST_PROGRAMS_SLEEP_WORKFLOW.to_string(),
                &WorkflowConfig::default(),
            )
            .await?;
        let runtime = Arc::new(runtime);
        let mut futures = Vec::new();
        for _ in 0..ITERATIONS {
            let runtime = runtime.clone();
            let join_handle = async move {
                let mut event_history = EventHistory::default();
                let params = vec![wasmtime::component::Val::U64(SLEEP_MILLIS)];
                let res = runtime
                    .schedule_workflow(
                        &WorkflowId::new(
                            COUNTER
                                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                .to_string(),
                        ),
                        &mut event_history,
                        &FunctionFqn::new("testing:sleep-workflow/workflow", "sleep"),
                        &params,
                    )
                    .await;
                res
            };
            futures.push(join_handle);
        }
        let err = futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap_err();
        assert_matches!(err,
        ExecutionError::LimitReached(reason)
        if reason == format!("maximum concurrent {limit_name} instance limit of {LIMIT} reached"));
    }
    Ok(())
}
