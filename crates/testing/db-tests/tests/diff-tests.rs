#![cfg(not(madsim))] // Madsim does not work with `async-sqlite`

use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionLog;
use concepts::storage::{AppendRequest, CreateRequest};
use concepts::ExecutionId;
use concepts::Params;
use db_mem::inmemory_dao::DbTask;
use db_tests::sqlite_pool;
use db_tests_common::db_test_stubs::SOME_FFQN;
use std::time::Duration;
use test_utils::arbitrary::UnstructuredHolder;
use test_utils::set_up;
use tracing::debug;
use tracing::info;
use utils::time::now;

#[tokio::test]
async fn stochastic_proptest() {
    set_up();
    let unstructured_holder = UnstructuredHolder::new();
    let mut unstructured = unstructured_holder.unstructured();
    let execution_id = ExecutionId::generate();
    let create_req = CreateRequest {
        created_at: now(),
        execution_id,
        ffqn: SOME_FFQN,
        params: Params::default(),
        parent: None,
        scheduled_at: None,
        retry_exp_backoff: Duration::ZERO,
        max_retries: 0,
    };
    let append_requests = (0..unstructured.int_in_range(5..=10).unwrap())
        .map(|_| AppendRequest {
            event: unstructured.arbitrary().unwrap(),
            created_at: now(),
        })
        .collect::<Vec<_>>();
    info!("Creating: {create_req:?}");
    for (idx, step) in append_requests.iter().enumerate() {
        info!("{idx}: {step:?}");
    }
    let mut db_mem_task = DbTask::spawn_new(1);
    let mem_conn = db_mem_task.pool().unwrap().connection().unwrap();
    let mem_log = create_and_append(
        &mem_conn,
        execution_id,
        create_req.clone(),
        &append_requests,
    )
    .await;
    drop(mem_conn);
    db_mem_task.close().await;
    let (sqlite_conn, _guard) = sqlite_pool().await;
    let sqlite_log =
        create_and_append(&sqlite_conn, execution_id, create_req, &append_requests).await;
    sqlite_conn.close().await.unwrap();
    println!(
        "Expected:\n{expected}\nActual:\n{actual}",
        expected = serde_json::to_string(&mem_log).unwrap(),
        actual = serde_json::to_string(&sqlite_log).unwrap()
    );
    assert_eq!(mem_log.pending_state, sqlite_log.pending_state);
    assert_eq!(mem_log.events.len(), sqlite_log.events.len());
    for i in 0..mem_log.events.len() {
        assert_eq!(
            mem_log.events[i], sqlite_log.events[i],
            "Failed at {i}-th element"
        );
    }
    assert_eq!(mem_log.events, sqlite_log.events);
    assert_eq!(mem_log, sqlite_log,);
}

async fn create_and_append(
    db_connection: &impl DbConnection,
    execution_id: ExecutionId,
    create_req: CreateRequest,
    append_requests: &[AppendRequest],
) -> ExecutionLog {
    let mut version;
    // Create
    version = db_connection.create(create_req).await.unwrap();
    for event in append_requests {
        match db_connection
            .append(execution_id, Some(version.clone()), event.clone())
            .await
        {
            Ok(new_version) => version = new_version,
            Err(err) => debug!("Ignoring {err:?}"),
        }
    }
    db_connection.get(execution_id).await.unwrap()
}
