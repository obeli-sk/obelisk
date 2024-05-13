use concepts::storage::DbConnection;
use concepts::storage::{CreateRequest, DbPool};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{ExecutionId, FunctionFqn, Params};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::sync::Arc;
use std::{marker::PhantomData, time::Duration};
use utils::time::now;
use wasm_workers::activity_worker::TIMEOUT_SLEEP_UNIT;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::{
    activity_worker::{activity_engine, RecycleInstancesSetting},
    auto_worker::{AutoConfig, AutoWorker},
    workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
    EngineConfig,
};
use wasmtime::Engine;

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let enable_chrome_layer = std::env::var("CHROME_TRACE")
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default();
    let builder = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with(tracing_subscriber::EnvFilter::from_default_env());
    let _chrome_guard;
    if enable_chrome_layer {
        let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .trace_style(tracing_chrome::TraceStyle::Async)
            .build();
        _chrome_guard = guard;
        builder.with(chrome_layer).init();
    } else {
        builder.init();
    }
    std::panic::set_hook(Box::new(utils::tracing_panic_hook));

    let db_pool = SqlitePool::new("obelisk.sqlite").await.unwrap();
    let activity_engine = activity_engine(EngineConfig::default());
    let workflow_engine = workflow_engine(EngineConfig::default());

    let _epoch_ticker = EpochTicker::spawn_new(
        vec![activity_engine.weak(), workflow_engine.weak()],
        Duration::from_millis(10),
    );

    let timers_watcher = TimersWatcherTask::spawn_new(
        db_pool.connection(),
        TimersWatcherConfig {
            tick_sleep: Duration::from_millis(100),
            clock_fn: now,
        },
    )
    .unwrap();

    let wasm_tasks = vec![
        exec(
            StrVariant::Static(
                test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY,
            ),
            db_pool.clone(),
            workflow_engine.clone(),
            activity_engine.clone(),
        ),
        exec(
            StrVariant::Static(
                test_programs_http_get_workflow_builder::TEST_PROGRAMS_HTTP_GET_WORKFLOW,
            ),
            db_pool.clone(),
            workflow_engine,
            activity_engine,
        ),
    ];

    // parse input
    let mut args = std::env::args().peekable();
    args.next();
    if args.peek().is_none() {
        eprintln!("Two arguments expected: `function's fully qualified name` `parameters`");
        eprintln!("Available functions:");
        for (_, ffqns) in &wasm_tasks {
            for ffqn in ffqns {
                eprintln!("{ffqn}");
            }
        }
        return;
    }
    let ffqn = args.next().expect("first parameter must be function fully qualified name: namespace:package_name/interface_name[@version].function_name");
    let ffqn = ffqn.parse().unwrap();
    let params = args
        .next()
        .expect("second parameter must be parameters encoded as json array");
    let params = serde_json::from_str(&params).unwrap();
    let params = Params::from_json_array(params).unwrap();

    let db_connection = db_pool.connection();
    let execution_id = ExecutionId::generate();
    let created_at = now();
    db_connection
        .create(CreateRequest {
            created_at: now(),
            execution_id,
            ffqn,
            params,
            parent: None,
            scheduled_at: created_at,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 5,
        })
        .await
        .unwrap();

    let _ = db_connection
        .wait_for_finished_result(execution_id, None)
        .await
        .unwrap();

    for (task, _) in wasm_tasks {
        task.close().await;
    }
    timers_watcher.close().await;
    db_pool.close().await.unwrap();
}

fn exec<DB: DbConnection + 'static>(
    wasm_path: StrVariant,
    db_pool: impl DbPool<DB> + 'static,
    workflow_engine: Arc<Engine>,
    activity_engine: Arc<Engine>,
) -> (ExecutorTaskHandle, Vec<FunctionFqn>) {
    let config = AutoConfig {
        wasm_path,
        config_id: ConfigId::generate(),
        activity_recycled_instances: RecycleInstancesSetting::Enable,
        clock_fn: now,
        workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::Await,
        workflow_db_pool: db_pool.clone(),
        workflow_child_retry_exp_backoff: Duration::from_millis(10),
        workflow_child_max_retries: 5,
        timeout_sleep_unit: TIMEOUT_SLEEP_UNIT,
        phantom_data: PhantomData,
    };
    let worker =
        Arc::new(AutoWorker::new_with_config(config, workflow_engine, activity_engine).unwrap());
    let ffqns = worker.exported_functions().cloned().collect::<Vec<_>>();
    let exec_config = ExecConfig {
        ffqns: ffqns.clone(),
        batch_size: 10,
        lock_expiry: Duration::from_secs(1),
        tick_sleep: Duration::from_millis(1),
        clock_fn: now,
    };
    (
        ExecTask::spawn_new(worker, exec_config, db_pool, None),
        ffqns,
    )
}
