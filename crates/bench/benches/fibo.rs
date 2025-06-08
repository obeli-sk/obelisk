use assert_matches::assert_matches;
use concepts::storage::DbPool;
use concepts::time::{ClockFn, Now, Sleep, TokioSleep};
use concepts::{
    ComponentId, ExecutionId, FunctionFqn, FunctionRegistry, Params, StrVariant,
    SupportedFunctionReturnValue,
};
use concepts::{
    ComponentType,
    prefixed_ulid::ExecutorId,
    storage::{CreateRequest, DbConnection, PendingState, wait_for_pending_state_fn},
};
use db_tests::Database;
use divan::{self};
use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
use executor::worker::Worker;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use utils::testing_fn_registry::TestingFnRegistry;
use utils::wasm_tools::WasmComponent;
use wasm_workers::activity::activity_worker::{ActivityConfig, ActivityWorker};
use wasm_workers::engines::{Engines, PoolingOptions};
use wasm_workers::workflow::workflow_worker::{
    JoinNextBlockingStrategy, WorkflowConfig, WorkflowWorkerCompiled,
};
use wasmtime::Engine;

fn main() {
    divan::main();
}

const TICK_SLEEP: Duration = Duration::from_millis(1);

pub(crate) fn compile_activity_with_engine(
    wasm_path: &'static str,
    engine: &Engine,
) -> (WasmComponent, ComponentId) {
    let component_id =
        ComponentId::new(ComponentType::ActivityWasm, wasm_file_name(wasm_path)).unwrap();
    (
        WasmComponent::new(wasm_path, engine, Some(component_id.component_type.into())).unwrap(),
        component_id,
    )
}

fn activity_config(component_id: ComponentId) -> ActivityConfig {
    ActivityConfig {
        component_id,
        forward_stdout: None,
        forward_stderr: None,
        env_vars: Arc::from([]),
        retry_on_err: true,
        directories_config: None,
    }
}

pub(crate) fn spawn_activity<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    wasm_path: &'static str,
    clock_fn: impl ClockFn + 'static,
    sleep: impl Sleep + 'static,
    activity_engine: Arc<Engine>,
) -> ExecutorTaskHandle {
    spawn_activity_with_config(
        db_pool,
        wasm_path,
        clock_fn,
        sleep,
        activity_config,
        activity_engine,
    )
}

pub(crate) fn spawn_activity_with_config<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    wasm_path: &'static str,
    clock_fn: impl ClockFn + 'static,
    sleep: impl Sleep + 'static,
    config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
    activity_engine: Arc<Engine>,
) -> ExecutorTaskHandle {
    let (worker, component_id) = new_activity_worker_with_config(
        wasm_path,
        activity_engine,
        clock_fn.clone(),
        sleep,
        config_fn,
    );
    let exec_config = ExecConfig {
        batch_size: 1,
        lock_expiry: Duration::from_secs(1),
        tick_sleep: TICK_SLEEP,
        component_id,
        task_limiter: None,
    };
    ExecTask::spawn_new(
        worker,
        exec_config,
        clock_fn,
        db_pool,
        ExecutorId::generate(),
    )
}

fn new_activity_worker_with_config(
    wasm_path: &'static str,
    engine: Arc<Engine>,
    clock_fn: impl ClockFn + 'static,
    sleep: impl Sleep + 'static,
    config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
) -> (Arc<dyn Worker>, ComponentId) {
    let (wasm_component, component_id) = compile_activity_with_engine(wasm_path, &engine);
    (
        Arc::new(
            ActivityWorker::new_with_config(
                wasm_component,
                config_fn(component_id.clone()),
                engine,
                clock_fn,
                sleep,
            )
            .unwrap(),
        ),
        component_id,
    )
}

pub(crate) fn spawn_activity_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    clock_fn: impl ClockFn + 'static,
    sleep: impl Sleep + 'static,
    activity_engine: Arc<Engine>,
) -> ExecutorTaskHandle {
    spawn_activity(
        db_pool,
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        clock_fn,
        sleep,
        activity_engine,
    )
}

pub(crate) fn compile_workflow_with_engine(
    wasm_path: &'static str,
    engine: &Engine,
) -> (WasmComponent, ComponentId) {
    let component_id =
        ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
    (
        WasmComponent::new(wasm_path, engine, Some(component_id.component_type.into())).unwrap(),
        component_id,
    )
}

fn spawn_workflow<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    wasm_path: &'static str,
    clock_fn: impl ClockFn + 'static,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    fn_registry: Arc<dyn FunctionRegistry>,
    workflow_engine: Arc<Engine>,
) -> ExecutorTaskHandle {
    let component_id =
        ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
    let worker = Arc::new(
        WorkflowWorkerCompiled::new_with_config(
            WasmComponent::new(
                wasm_path,
                &workflow_engine,
                Some(component_id.component_type.into()),
            )
            .unwrap(),
            WorkflowConfig {
                component_id: component_id.clone(),
                join_next_blocking_strategy,
                retry_on_trap: false,
                forward_unhandled_child_errors_in_join_set_close: false,
                backtrace_persist: false,
                stub_wasi: false,
            },
            workflow_engine,
            clock_fn.clone(),
            TokioSleep,
        )
        .unwrap()
        .link(fn_registry)
        .unwrap()
        .into_worker(db_pool.clone()),
    );
    let exec_config = ExecConfig {
        batch_size: 1,
        lock_expiry: Duration::from_secs(3),
        tick_sleep: TICK_SLEEP,
        component_id,
        task_limiter: None,
    };
    ExecTask::spawn_new(
        worker,
        exec_config,
        clock_fn,
        db_pool,
        ExecutorId::generate(),
    )
}

pub(crate) fn spawn_workflow_fibo<DB: DbConnection + 'static, P: DbPool<DB> + 'static>(
    db_pool: P,
    clock_fn: impl ClockFn + 'static,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    fn_registry: Arc<dyn FunctionRegistry>,
    workflow_engine: Arc<Engine>,
) -> ExecutorTaskHandle {
    spawn_workflow(
        db_pool,
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        clock_fn,
        join_next_blocking_strategy,
        fn_registry,
        workflow_engine,
    )
}

pub(crate) fn wasm_file_name(input: impl AsRef<Path>) -> StrVariant {
    let input = input.as_ref();
    let input = input.file_name().and_then(|name| name.to_str()).unwrap();
    let input = input.strip_suffix(".wasm").unwrap().to_string();
    StrVariant::from(input)
}

#[divan::bench(args = [100, 200, 400, 800, 1600])]
fn fiboa(bencher: divan::Bencher, args: u32) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let workspace_dir = PathBuf::from(
        std::env::var("CARGO_WORKSPACE_DIR")
            .as_deref()
            .unwrap_or("."),
    )
    .canonicalize()
    .unwrap();
    let codegen_cache = workspace_dir.join("test-codegen-cache");
    let engines =
        Arc::new(Engines::pooling(PoolingOptions::default(), Some(codegen_cache)).unwrap());
    // compile before benching
    compile_activity_with_engine(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        &engines.activity_engine,
    );
    compile_workflow_with_engine(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        &engines.workflow_engine,
    );
    bencher.bench(|| {
        let engines = engines.clone();
        rt.block_on(async move { fibo_workflow(1, args, engines).await });
    });
}

const FIBO_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
    test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
); // fiboa: func(n: u8, iterations: u32) -> u64;

async fn fibo_workflow(fibo_n: u8, iterations: u32, engines: Arc<Engines>) {
    let join_next_blocking_strategy = JoinNextBlockingStrategy::Await {
        non_blocking_event_batching: 10,
    };

    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let fn_registry = TestingFnRegistry::new_from_components(vec![
        compile_activity_with_engine(
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            &engines.activity_engine,
        ),
        compile_workflow_with_engine(
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            &engines.workflow_engine,
        ),
    ]);
    let workflow_exec_task = spawn_workflow_fibo(
        db_pool.clone(),
        Now,
        join_next_blocking_strategy,
        fn_registry,
        engines.workflow_engine.clone(),
    );
    // Create an execution.
    let execution_id = ExecutionId::generate();
    let created_at = Now.now();
    let db_connection = db_pool.connection();

    let params = Params::from_json_values(vec![json!(fibo_n), json!(iterations)]);
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: execution_id.clone(),
            ffqn: FIBO_WORKFLOW_FFQN,
            params,
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: created_at,
            retry_exp_backoff: Duration::ZERO,
            max_retries: u32::MAX,
            component_id: ComponentId::dummy_workflow(),
            scheduled_by: None,
        })
        .await
        .unwrap();

    wait_for_pending_state_fn(
        &db_connection,
        &execution_id,
        |exe_history| {
            matches!(
                exe_history.pending_state,
                PendingState::BlockedByJoinSet { .. }
            )
            .then_some(())
        },
        None,
    )
    .await
    .unwrap();

    let activity_exec_task = spawn_activity_fibo(
        db_pool.clone(),
        Now,
        TokioSleep,
        engines.activity_engine.clone(),
    );

    let res = db_connection
        .wait_for_finished_result(&execution_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(res, SupportedFunctionReturnValue::InfallibleOrResultOk(_));

    workflow_exec_task.close().await;
    activity_exec_task.close().await;
    db_pool.close().await.unwrap();
}
