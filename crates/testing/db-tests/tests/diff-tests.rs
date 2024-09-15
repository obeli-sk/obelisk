use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionLog;
use concepts::storage::{AppendRequest, CreateRequest};
use concepts::ConfigId;
use concepts::ExecutionId;
use concepts::Params;
use db_tests::Database;
use db_tests::SOME_FFQN;
use std::time::Duration;
use test_utils::arbitrary::UnstructuredHolder;
use test_utils::set_up;
use utils::time::now;

#[tokio::test]
async fn diff_proptest() {
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
        correlation_id: None,
        scheduled_at: now(),
        retry_exp_backoff: Duration::ZERO,
        max_retries: 0,
        config_id: ConfigId::dummy(),
        return_type: None,
    };
    let mut append_requests = vec![];
    while append_requests.is_empty() {
        append_requests = (0..unstructured.int_in_range(5..=10).unwrap())
            .map(|_| AppendRequest {
                event: unstructured.arbitrary().unwrap(),
                created_at: now(),
            })
            .filter_map(|req| {
                // filter out Create requests
                if let AppendRequest {
                    event: ExecutionEventInner::Created { .. },
                    ..
                } = req
                {
                    None
                } else {
                    Some(req)
                }
            })
            .collect::<Vec<_>>();
    }
    println!("Creating: {create_req:?}");
    for (idx, step) in append_requests.iter().enumerate() {
        println!("{idx}: {step:?}");
    }
    let (_mem_guard, db_mem_pool) = Database::Memory.set_up().await;
    let mem_conn = db_mem_pool.connection();
    let mem_log = create_and_append(
        &mem_conn,
        execution_id,
        create_req.clone(),
        &append_requests,
    )
    .await;
    db_mem_pool.close().await.unwrap();
    let (_sqlite_guard, sqlite_pool) = Database::Sqlite.set_up().await;
    let sqlite_conn = sqlite_pool.connection();
    let sqlite_log =
        create_and_append(&sqlite_conn, execution_id, create_req, &append_requests).await;
    sqlite_pool.close().await.unwrap();
    println!(
        "Expected (mem):\n{expected}\nActual (sqlite):\n{actual}",
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
            .append(execution_id, version.clone(), event.clone())
            .await
        {
            Ok(new_version) => version = new_version,
            Err(err) => println!("Ignoring {err:?}"),
        }
    }
    db_connection.get(execution_id).await.unwrap()
}
