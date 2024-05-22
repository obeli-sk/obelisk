use concepts::storage::DbConnection;
use concepts::storage::{CreateRequest, DbPool};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{ExecutionId, FunctionFqn, Params, SupportedFunctionResult};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::{ExecConfig, ExecTask, ExecutorTaskHandle};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::sync::Arc;
use std::{marker::PhantomData, time::Duration};
use tracing::error;
use utils::time::now;
use val_json::wast_val::{WastVal, WastValWithType};
use wasm_workers::activity_worker::TIMEOUT_SLEEP_UNIT;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::workflow_worker::NonBlockingEventBatching;
use wasm_workers::{
    activity_worker::{activity_engine, RecycleInstancesSetting},
    auto_worker::{AutoConfig, AutoWorker},
    workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
    EngineConfig,
};
use wasmtime::Engine;

fn is_env_true(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|val| val.parse::<bool>().ok())
        .unwrap_or_default()
}

#[cfg(feature = "tokio-console")]
fn tokio_console_layer<S>() -> Option<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    if is_env_true("TOKIO_CONSOLE") {
        // Run with
        // TOKIO_CONSOLE=true RUSTFLAGS="--cfg tokio_unstable --cfg tracing_unstable" DB=mem RUST_LOG=error,runtime=trace,tokio=trace cargo run --target-dir=target/debug/tokio-console --features parallel-compilation,tokio-console > /dev/null
        use tracing_subscriber::Layer;
        Some(
            console_subscriber::spawn().with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("tokio", tracing::Level::TRACE)
                    .with_target("runtime", tracing::Level::TRACE),
            ),
        )
    } else {
        None
    }
}

#[cfg(not(feature = "tokio-console"))]
fn tokio_console_layer() -> Option<tracing::level_filters::LevelFilter> {
    None
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let _chrome_guard;
    tracing_subscriber::registry()
        .with(tokio_console_layer())
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(if is_env_true("CHROME_TRACE") {
            let (chrome_layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                .trace_style(tracing_chrome::TraceStyle::Async)
                .build();
            _chrome_guard = guard;
            Some(chrome_layer)
        } else {
            None
        })
        .init();

    std::panic::set_hook(Box::new(utils::tracing_panic_hook));

    if std::env::var("DB")
        .unwrap_or_default()
        .eq_ignore_ascii_case("mem")
    {
        let db_pool = db_mem::inmemory_dao::InMemoryPool::new();
        run(db_pool).await;
    } else {
        let db_pool = SqlitePool::new("obelisk.sqlite").await.unwrap();
        run(db_pool).await;
    }
}

#[allow(clippy::too_many_lines)]
async fn run<DB: DbConnection + 'static>(db_pool: impl DbPool<DB> + 'static) {
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
            workflow_engine.clone(),
            activity_engine.clone(),
        ),
        exec(
            StrVariant::Static(test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY),
            db_pool.clone(),
            workflow_engine.clone(),
            activity_engine.clone(),
        ),
        exec(
            StrVariant::Static(test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW),
            db_pool.clone(),
            workflow_engine,
            activity_engine,
        ),
    ];

    // parse input
    let mut args = std::env::args();
    args.next();
    let args = args.collect::<Vec<_>>();
    if args.len() == 2 {
        let ffqn = args.first().unwrap();
        let ffqn = ffqn.parse().unwrap();
        let params = args.get(1).unwrap();
        let params = serde_json::from_str(params).unwrap();
        let params = Params::from_json_array(params).unwrap();

        let db_connection = db_pool.connection();
        let execution_id = ExecutionId::generate();
        let created_at = now();
        db_connection
            .create(CreateRequest {
                created_at,
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

        let res = db_connection
            .wait_for_finished_result(execution_id, None)
            .await
            .unwrap();

        let duration = (now() - created_at).to_std().unwrap();
        match res {
            Ok(
                SupportedFunctionResult::None
                | SupportedFunctionResult::Infallible(_)
                | SupportedFunctionResult::Fallible(WastValWithType {
                    value: WastVal::Result(Ok(_)),
                    ..
                }),
            ) => {
                error!("Finished OK in {duration:?}");
            }
            _ => {
                error!("Finished with an error in {duration:?} - {res:?}");
            }
        }

        for (task, _) in wasm_tasks {
            task.close().await;
        }
        timers_watcher.close().await;
        db_pool.close().await.unwrap();
    } else if args.is_empty() {
        loop {
            tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        }
    } else {
        eprintln!("Two arguments expected: `function's fully qualified name` `parameters`");
        eprintln!("Available functions:");
        for (_, ffqns) in &wasm_tasks {
            for ffqn in ffqns {
                eprintln!("{ffqn}");
            }
        }
    }
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
        non_blocking_event_batching: NonBlockingEventBatching::Enabled,
        phantom_data: PhantomData,
    };
    let worker =
        Arc::new(AutoWorker::new_with_config(config, workflow_engine, activity_engine).unwrap());
    let ffqns = worker.exported_functions().cloned().collect::<Vec<_>>();
    let exec_config = ExecConfig {
        ffqns: ffqns.clone(),
        batch_size: 10,
        lock_expiry: Duration::from_secs(10),
        tick_sleep: Duration::from_millis(5),
        clock_fn: now,
    };
    (
        ExecTask::spawn_new(worker, exec_config, db_pool, None),
        ffqns,
    )
}
