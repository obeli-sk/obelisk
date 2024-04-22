use concepts::storage::DbConnection;
use concepts::storage::{CreateRequest, DbPool};
use concepts::{prefixed_ulid::ConfigId, StrVariant};
use concepts::{ExecutionId, FunctionFqn, Params};
use db_sqlite::sqlite_dao::SqlitePool;
use executor::executor::{ExecConfig, ExecTask};
use executor::expired_timers_watcher::{TimersWatcherConfig, TimersWatcherTask};
use executor::worker::Worker;
use std::sync::Arc;
use std::{marker::PhantomData, time::Duration};
use utils::time::now;
use wasm_workers::epoch_ticker::EpochTicker;
use wasm_workers::{
    activity_worker::{activity_engine, RecycleInstancesSetting},
    auto_worker::{AutoConfig, AutoWorker},
    workflow_worker::{workflow_engine, JoinNextBlockingStrategy},
    EngineConfig,
};
use wasmtime::component::Val;

#[tokio::main]
async fn main() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .json(),
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    std::panic::set_hook(Box::new(utils::tracing_panic_hook));
    let db_pool = SqlitePool::new("obeli-sk.sqlite").await.unwrap();
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

    let activity = test_programs_http_get_activity_builder::TEST_PROGRAMS_HTTP_GET_ACTIVITY;
    let config = AutoConfig {
        wasm_path: StrVariant::Static(activity),
        config_id: ConfigId::generate(),
        activity_recycled_instances: RecycleInstancesSetting::Enable,
        clock_fn: now,
        workflow_join_next_blocking_strategy: JoinNextBlockingStrategy::default(),
        workflow_db_pool: db_pool.clone(),
        workflow_child_retry_exp_backoff: Duration::ZERO,
        workflow_child_max_retries: 0,
        phantom_data: PhantomData,
    };
    let worker =
        Arc::new(AutoWorker::new_with_config(config, workflow_engine, activity_engine).unwrap());
    let exec_config = ExecConfig {
        ffqns: worker.supported_functions().cloned().collect(),
        batch_size: 1,
        lock_expiry: Duration::from_secs(1),
        tick_sleep: Duration::from_millis(1),
        clock_fn: now,
    };
    let exec_task = ExecTask::spawn_new(worker, exec_config, db_pool.clone(), None);

    let db_connection = db_pool.connection();
    let params = Params::from([Val::String("neverssl.com".into()), Val::String("/".into())]);
    let execution_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at: now(),
            execution_id,
            ffqn: FunctionFqn::new_static_tuple(test_programs_http_get_activity_builder::GET),
            params,
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();

    let _ = db_connection
        .wait_for_finished_result(execution_id, Some(Duration::from_secs(1)))
        .await
        .unwrap();

    exec_task.close().await;
    timers_watcher.close().await;
    db_pool.close().await.unwrap();
}
