fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    divan::main();
}

mod bench {
    use assert_matches::assert_matches;
    use concepts::storage::DbPool;
    use concepts::time::{ClockFn, Now, Sleep, TokioSleep};
    use concepts::{
        ComponentId, ExecutionId, FunctionFqn, FunctionRegistry, Params, StrVariant,
        SupportedFunctionReturnValue,
    };
    use concepts::{ComponentType, prefixed_ulid::ExecutorId, storage::CreateRequest};
    use db_tests::Database;
    use divan::{self};
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
    use executor::worker::Worker;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use wasm_workers::RunnableComponent;
    use wasm_workers::activity::activity_worker::{ActivityConfig, ActivityWorker};
    use wasm_workers::engines::Engines;
    use wasm_workers::testing_fn_registry::TestingFnRegistry;
    use wasm_workers::workflow::workflow_worker::{
        DEFAULT_NON_BLOCKING_EVENT_BATCHING, JoinNextBlockingStrategy, WorkflowConfig,
        WorkflowWorkerCompiled,
    };
    use wasmtime::Engine;

    const TICK_SLEEP: Duration = Duration::from_millis(1);

    pub(crate) fn compile_activity_with_engine(
        wasm_path: &'static str,
        engine: &Engine,
    ) -> (RunnableComponent, ComponentId) {
        let component_id =
            ComponentId::new(ComponentType::ActivityWasm, wasm_file_name(wasm_path)).unwrap();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
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
            fuel: None,
        }
    }

    pub(crate) fn spawn_activity(
        db_pool: Arc<dyn DbPool>,
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

    pub(crate) fn spawn_activity_with_config(
        db_pool: Arc<dyn DbPool>,
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
            executor_id: ExecutorId::generate(),
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool)
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

    pub(crate) fn spawn_activity_fibo(
        db_pool: Arc<dyn DbPool>,
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
    ) -> (RunnableComponent, ComponentId) {
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
            component_id,
        )
    }

    fn spawn_workflow(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
    ) -> ExecutorTaskHandle {
        let component_id =
            ComponentId::new(ComponentType::Workflow, wasm_file_name(wasm_path)).unwrap();
        let worker = Arc::new(
            WorkflowWorkerCompiled::new_with_config(
                RunnableComponent::new(wasm_path, &workflow_engine, component_id.component_type)
                    .unwrap(),
                WorkflowConfig {
                    component_id: component_id.clone(),
                    join_next_blocking_strategy,
                    backtrace_persist: false,
                    stub_wasi: false,
                    fuel: None,
                },
                workflow_engine,
                clock_fn.clone(),
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
            executor_id: ExecutorId::generate(),
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool)
    }

    pub(crate) fn spawn_workflow_fibo(
        db_pool: Arc<dyn DbPool>,
        clock_fn: impl ClockFn + 'static,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
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

    #[divan::bench(args = [100, 200, 400, 800])]
    fn fiboa_sqlite(bencher: divan::Bencher, args: u32) {
        fiboa_db(bencher, args, Database::Sqlite);
    }

    #[divan::bench(args = [100, 200, 400, 800])]
    fn fiboa_memory(bencher: divan::Bencher, args: u32) {
        fiboa_db(bencher, args, Database::Memory);
    }

    fn fiboa_db(bencher: divan::Bencher, iterations: u32, database: Database) {
        let tokio = Handle::current();
        let workspace_dir = PathBuf::from(
            std::env::var("CARGO_WORKSPACE_DIR")
                .as_deref()
                .unwrap_or("."),
        )
        .canonicalize()
        .unwrap();
        let codegen_cache = workspace_dir.join("test-codegen-cache");
        let engines = Arc::new(Engines::on_demand(Some(codegen_cache), false, true).unwrap()); // TODO test with pooling engine as well.

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

        let (_guard, db_pool) = tokio.block_on(async move { database.set_up().await });

        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            Now,
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
            },
            &fn_registry,
            engines.workflow_engine.clone(),
        );

        let activity_exec_task = spawn_activity_fibo(
            db_pool.clone(),
            Now,
            TokioSleep,
            engines.activity_engine.clone(),
        );

        bencher.bench(|| {
            let db_pool = db_pool.clone();
            tokio.block_on(async move { fibo_workflow(1, iterations, db_pool).await });
        });

        tokio.block_on(async move { workflow_exec_task.close().await });
        tokio.block_on(async move { activity_exec_task.close().await });

        tokio.block_on(async move { db_pool.close().await.unwrap() });
    }

    const FIBO_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
    ); // fiboa: func(n: u8, iterations: u32) -> u64;

    async fn fibo_workflow(fibo_n: u8, iterations: u32, db_pool: Arc<dyn DbPool>) {
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

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok(_));
    }
}
