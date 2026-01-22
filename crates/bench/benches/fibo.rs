fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();
    divan::main();
}

mod bench {
    use assert_matches::assert_matches;
    use concepts::storage::{DbPool, DbPoolCloseable};
    use concepts::time::{ClockFn, Now, Sleep, TokioSleep};
    use concepts::{
        ComponentId, ComponentRetryConfig, ExecutionId, FunctionFqn, FunctionRegistry, Params,
        SupportedFunctionReturnValue,
    };
    use concepts::{prefixed_ulid::ExecutorId, storage::CreateRequest};
    use db_tests::Database;
    use divan::{self};
    use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle, LockingStrategy};
    use executor::worker::Worker;
    use serde_json::json;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use wasm_workers::RunnableComponent;
    use wasm_workers::activity::activity_worker::{ActivityConfig, ActivityWorkerCompiled};
    use wasm_workers::activity::cancel_registry::CancelRegistry;
    use wasm_workers::engines::{EngineConfig, Engines, PoolingConfig};
    use wasm_workers::testing_fn_registry::TestingFnRegistry;
    use wasm_workers::workflow::deadline_tracker::DeadlineTrackerFactoryTokio;
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
        let component_id = ComponentId::dummy_activity();
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
        clock_fn: Box<dyn ClockFn>,
        sleep: impl Sleep + 'static,
        activity_engine: Arc<Engine>,
        cancel_registry: CancelRegistry,
    ) -> ExecutorTaskHandle {
        spawn_activity_with_config(
            db_pool,
            wasm_path,
            clock_fn,
            sleep,
            activity_config,
            activity_engine,
            cancel_registry,
        )
    }

    pub(crate) fn spawn_activity_with_config(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: Box<dyn ClockFn>,
        sleep: impl Sleep + 'static,
        config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
        activity_engine: Arc<Engine>,
        cancel_registry: CancelRegistry,
    ) -> ExecutorTaskHandle {
        let (worker, component_id) = new_activity_worker_with_config(
            wasm_path,
            activity_engine,
            clock_fn.clone_box(),
            sleep,
            config_fn,
            cancel_registry,
        );
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(1),
            tick_sleep: TICK_SLEEP,
            component_id,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::ZERO,
            locking_strategy: LockingStrategy::ByComponentDigest,
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool, TokioSleep)
    }

    fn new_activity_worker_with_config(
        wasm_path: &'static str,
        engine: Arc<Engine>,
        clock_fn: Box<dyn ClockFn>,
        sleep: impl Sleep + 'static,
        config_fn: impl FnOnce(ComponentId) -> ActivityConfig,
        cancel_registry: CancelRegistry,
    ) -> (Arc<dyn Worker>, ComponentId) {
        let (wasm_component, component_id) = compile_activity_with_engine(wasm_path, &engine);
        let (db_forwarder_sender, _) = mpsc::channel(1);
        (
            Arc::new(
                ActivityWorkerCompiled::new_with_config(
                    wasm_component,
                    config_fn(component_id.clone()),
                    engine,
                    clock_fn,
                    sleep,
                )
                .unwrap()
                .into_worker(
                    cancel_registry,
                    &db_forwarder_sender,
                    None, // log_storage_config
                ),
            ),
            component_id,
        )
    }

    pub(crate) fn spawn_activity_fibo(
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        sleep: impl Sleep + 'static,
        activity_engine: Arc<Engine>,
        cancel_registry: CancelRegistry,
    ) -> ExecutorTaskHandle {
        spawn_activity(
            db_pool,
            test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
            clock_fn,
            sleep,
            activity_engine,
            cancel_registry,
        )
    }

    pub(crate) fn compile_workflow_with_engine(
        wasm_path: &'static str,
        engine: &Engine,
    ) -> (RunnableComponent, ComponentId) {
        let component_id = ComponentId::dummy_workflow();
        (
            RunnableComponent::new(wasm_path, engine, component_id.component_type).unwrap(),
            component_id,
        )
    }

    fn spawn_workflow(
        db_pool: Arc<dyn DbPool>,
        wasm_path: &'static str,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
        cancel_registry: CancelRegistry,
    ) -> ExecutorTaskHandle {
        let component_id = ComponentId::dummy_workflow();
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
                    lock_extension: Duration::ZERO,
                    subscription_interruption: None,
                },
                workflow_engine,
                clock_fn.clone_box(),
            )
            .unwrap()
            .link(fn_registry.clone())
            .unwrap()
            .into_worker(
                db_pool.clone(),
                Arc::new(DeadlineTrackerFactoryTokio {
                    leeway: Duration::ZERO,
                    clock_fn: clock_fn.clone_box(),
                }),
                cancel_registry,
                None, // log_storage_config
            ),
        );
        let exec_config = ExecConfig {
            batch_size: 1,
            lock_expiry: Duration::from_secs(3),
            tick_sleep: TICK_SLEEP,
            component_id,
            task_limiter: None,
            executor_id: ExecutorId::generate(),
            retry_config: ComponentRetryConfig::ZERO,
            locking_strategy: LockingStrategy::ByComponentDigest,
        };
        ExecTask::spawn_new(worker, exec_config, clock_fn, db_pool, TokioSleep)
    }

    pub(crate) fn spawn_workflow_fibo(
        db_pool: Arc<dyn DbPool>,
        clock_fn: Box<dyn ClockFn>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: &Arc<dyn FunctionRegistry>,
        workflow_engine: Arc<Engine>,
        cancel_registry: CancelRegistry,
    ) -> ExecutorTaskHandle {
        spawn_workflow(
            db_pool,
            test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
            clock_fn,
            join_next_blocking_strategy,
            fn_registry,
            workflow_engine,
            cancel_registry,
        )
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
        let codegen_cache_dir = workspace_dir.join("test-codegen-cache");
        let engine_config = EngineConfig {
            pooling_config: PoolingConfig::OnDemand, // TODO test with pooling engine as well.
            codegen_cache_dir: Some(codegen_cache_dir),
            consume_fuel: false,
            parallel_compilation: true,
            debug: false,
        };
        let engines = Arc::new(Engines::new(engine_config).unwrap());

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

        let (_guard, db_pool, db_close) = tokio.block_on(async move { database.set_up().await });

        let cancel_registry = CancelRegistry::new();

        let workflow_exec_task = spawn_workflow_fibo(
            db_pool.clone(),
            Now.clone_box(),
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching: DEFAULT_NON_BLOCKING_EVENT_BATCHING,
            },
            &fn_registry,
            engines.workflow_engine.clone(),
            cancel_registry.clone(),
        );

        let activity_exec_task = spawn_activity_fibo(
            db_pool.clone(),
            Now.clone_box(),
            TokioSleep,
            engines.activity_engine.clone(),
            cancel_registry,
        );

        bencher.bench(|| {
            let db_pool = db_pool.clone();
            tokio.block_on(async move { fibo_workflow(1, iterations, db_pool).await });
        });

        tokio.block_on(async move { workflow_exec_task.close().await });
        tokio.block_on(async move { activity_exec_task.close().await });

        tokio.block_on(async move { db_close.close().await });
    }

    const FIBO_WORKFLOW_FFQN: FunctionFqn = FunctionFqn::new_static_tuple(
        test_programs_fibo_workflow_builder::exports::testing::fibo_workflow::workflow::FIBOA,
    ); // fiboa: func(n: u8, iterations: u32) -> u64;

    async fn fibo_workflow(fibo_n: u8, iterations: u32, db_pool: Arc<dyn DbPool>) {
        // Create an execution.
        let execution_id = ExecutionId::generate();
        let created_at = Now.now();
        let db_connection = db_pool.connection().await.unwrap();

        let params = Params::from_json_values_test(vec![json!(fibo_n), json!(iterations)]);
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id: execution_id.clone(),
                ffqn: FIBO_WORKFLOW_FFQN,
                params,
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                component_id: ComponentId::dummy_workflow(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let res = db_connection
            .wait_for_finished_result(&execution_id, None)
            .await
            .unwrap();
        assert_matches!(res, SupportedFunctionReturnValue::Ok { .. });
    }
}
