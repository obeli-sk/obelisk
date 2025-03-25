use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionLog;
use concepts::storage::{AppendRequest, CreateRequest};
use concepts::time::ClockFn as _;
use concepts::time::Now;
use concepts::ComponentId;
use concepts::ExecutionId;
use concepts::Params;
use db_tests::Database;
use db_tests::SOME_FFQN;
use std::time::Duration;
use test_utils::arbitrary::UnstructuredHolder;
use test_utils::set_up;

#[tokio::test]
async fn diff_proptest() {
    set_up();
    let unstructured_holder = UnstructuredHolder::new();
    let mut unstructured = unstructured_holder.unstructured();
    let execution_id = ExecutionId::generate();
    let create_req = CreateRequest {
        created_at: Now.now(),
        execution_id: execution_id.clone(),
        ffqn: SOME_FFQN,
        params: Params::default(),
        parent: None,
        metadata: concepts::ExecutionMetadata::empty(),
        scheduled_at: Now.now(),
        retry_exp_backoff: Duration::ZERO,
        max_retries: 0,
        component_id: ComponentId::dummy_activity(),
        scheduled_by: None,
    };
    let mut append_requests = vec![];
    while append_requests.is_empty() {
        append_requests = (0..unstructured.int_in_range(5..=10).unwrap())
            .map(|_| AppendRequest {
                event: unstructured.arbitrary().unwrap(),
                created_at: Now.now(),
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
        execution_id.clone(),
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
            .append(execution_id.clone(), version.clone(), event.clone())
            .await
        {
            Ok(new_version) => version = new_version,
            Err(err) => println!("Ignoring {err:?}"),
        }
    }
    db_connection.get(&execution_id).await.unwrap()
}

#[cfg(not(madsim))]
mod nomadsim {
    use concepts::prefixed_ulid::ExecutorId;
    use concepts::storage::DbConnection;
    use concepts::storage::DbPool;
    use concepts::storage::ExecutionEventInner;
    use concepts::storage::Version;
    use concepts::storage::{AppendRequest, CreateRequest};
    use concepts::time::ClockFn as _;
    use concepts::ComponentId;
    use concepts::ExecutionId;
    use concepts::FinishedExecutionResult;
    use concepts::Params;
    use db_tests::Database;
    use db_tests::SOME_FFQN;
    use std::sync::Arc;
    use std::time::Duration;
    use test_utils::set_up;
    use test_utils::sim_clock::SimClock;

    use val_json::wast_val::WastValWithType;

    const WVWT_RECORD_UNSORTED: &str = r#"
        {
            "type": {
                "record": {
                    "login": "string",
                    "repo": "string",
                    "description": {
                        "option": "string"
                    }
                }
            },
            "value": {
                "login": "login",
                "repo": "repo",
                "description": "description"
            }
        }
        "#;

    async fn persist_finished_event(
        db_connection: &impl DbConnection,
    ) -> (ExecutionId, Version, ExecutionEventInner) {
        const LOCK_EXPIRY: Duration = Duration::from_millis(500);
        let component_id = ComponentId::dummy_activity();
        let sim_clock = SimClock::default();
        let execution_id = ExecutionId::generate();
        let exec1 = ExecutorId::generate();

        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: component_id.clone(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        // LockPending
        let version = {
            let created_at = sim_clock.now();
            let mut locked_executions = db_connection
                .lock_pending(
                    1,
                    created_at,
                    Arc::from([SOME_FFQN]),
                    created_at,
                    component_id.clone(),
                    exec1,
                    created_at + LOCK_EXPIRY,
                )
                .await
                .unwrap();
            assert_eq!(1, locked_executions.len());
            let locked_execution = locked_executions.pop().unwrap();
            assert_eq!(Version::new(2), locked_execution.version);
            locked_execution.version
        };

        let wast_val_with_type: WastValWithType =
            serde_json::from_str(WVWT_RECORD_UNSORTED).unwrap();
        let inner = ExecutionEventInner::Finished {
            result: FinishedExecutionResult::Ok(
                concepts::SupportedFunctionReturnValue::InfallibleOrResultOk(wast_val_with_type),
            ),
            http_client_traces: None,
        };
        // Finished
        let version = {
            let req = AppendRequest {
                event: inner.clone(),
                created_at: sim_clock.now(),
            };
            db_connection
                .append(execution_id.clone(), version, req)
                .await
                .unwrap()
        };
        (execution_id, version, inner)
    }

    // Test that the sqlite database no longer uses serde_json::Value during deserialization as it
    // would sorts attributes and thus break `TypeWrapper` and `WastVal`,
    #[tokio::test]
    #[rstest::rstest]
    async fn get_execution_event_should_not_break_json_order(
        #[values(Database::Sqlite, Database::Memory)] database: Database,
    ) {
        set_up();
        let (_guard, db_pool) = database.set_up().await;
        let db_connection = db_pool.connection();

        let (execution_id, version, expected_inner) = persist_finished_event(&db_connection).await;
        let found_inner = db_connection
            .get_execution_event(&execution_id, &version)
            .await
            .unwrap()
            .event;
        assert_eq!(expected_inner, found_inner);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    // Test that the sqlite database no longer uses serde_json::Value during deserialization as it
    // would sorts attributes and thus break `TypeWrapper` and `WastVal`,
    #[tokio::test]
    async fn list_execution_events_should_not_break_json_order() {
        set_up();
        let (_guard, db_pool) = Database::Sqlite.set_up().await;
        let db_connection = db_pool.connection();

        let (execution_id, version, expected_inner) = persist_finished_event(&db_connection).await;
        let found_inner = db_connection
            .list_execution_events(&execution_id, &version, 1)
            .await
            .unwrap();
        assert_eq!(1, found_inner.len());
        let found_inner = &found_inner[0].event;
        assert_eq!(expected_inner, *found_inner);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }
}
