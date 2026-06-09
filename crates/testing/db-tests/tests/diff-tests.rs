use chrono::{DateTime, Utc};
use concepts::ComponentId;
use concepts::ComponentRetryConfig;
use concepts::ExecutionId;
use concepts::Params;
use concepts::SupportedFunctionReturnValue;
use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
use concepts::prefixed_ulid::ExecutorId;
use concepts::prefixed_ulid::RunId;
use concepts::storage::DbConnection;
use concepts::storage::DbConnectionTest;
use concepts::storage::DbPoolCloseable;
use concepts::storage::ExecutionLog;
use concepts::storage::ExecutionRequest;
use concepts::storage::HistoryEvent;
use concepts::storage::HistoryEventScheduleAt;
use concepts::storage::JoinSetRequest;
use concepts::storage::Pagination;
use concepts::storage::Version;
use concepts::storage::{AppendRequest, CreateRequest};
use concepts::time::ClockFn as _;
use concepts::time::Now;
use obeli_db_tests::Database;
use obeli_db_tests::SOME_FFQN;
use rstest::rstest;
use std::sync::Arc;
use std::time::Duration;
use test_db_macro::expand_enum_database;
use test_utils::ExecutionLogSanitized;
use test_utils::arbitrary::UnstructuredHolder;
use test_utils::set_up;
use test_utils::sim_clock::SimClock;
use tracing::warn;
use val_json::wast_val::WastValWithType;

#[tokio::test]
async fn diff_proptest() {
    set_up();
    for seed in test_utils::get_seed() {
        diff_proptest_inner(seed).await;
    }
}

async fn diff_proptest_inner(seed: u64) {
    let unstructured_holder = UnstructuredHolder::new(seed);
    let mut unstructured = unstructured_holder.unstructured();
    let execution_id = ExecutionId::generate();
    let create_req = CreateRequest {
        created_at: Now.now(),
        execution_id: execution_id.clone(),
        ffqn: SOME_FFQN,
        params: Params::empty(),
        parent: None,
        metadata: concepts::ExecutionMetadata::empty(),
        scheduled_at: Now.now(),
        component_id: ComponentId::dummy_activity(),
        deployment_id: DEPLOYMENT_ID_DUMMY,
        scheduled_by: None,
        paused: false,
    };
    let mut append_requests = vec![];
    while append_requests.is_empty() {
        append_requests = (0..unstructured.int_in_range(5..=10).unwrap())
            .map(|_| arbitrary_valid_append_request(&mut unstructured).unwrap())
            .filter_map(|req| {
                // Remove Created, Unpaused
                if let AppendRequest {
                    event: ExecutionRequest::Created { .. } | ExecutionRequest::Unpaused,
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
    // Insert Unpause after each Pause
    append_requests = append_requests
        .into_iter()
        .flat_map(|req| {
            if matches!(req.event, ExecutionRequest::Paused) {
                let created_at = req.created_at;
                vec![
                    req,
                    AppendRequest {
                        event: ExecutionRequest::Unpaused,
                        created_at,
                    },
                ]
            } else {
                vec![req]
            }
        })
        .collect();

    println!("Creating: {create_req:?}");
    for (idx, step) in append_requests.iter().enumerate() {
        println!("{idx}: {step:?}");
    }
    let (_sqlite_guard, sqlite_pool, db_sqlite_close) = Database::Sqlite.set_up().await;
    let sqlite_conn = sqlite_pool.connection_test().await.unwrap();
    let sqlite_log = ExecutionLogSanitized::from(
        create_and_append(
            sqlite_conn.as_ref(),
            execution_id.clone(),
            create_req.clone(),
            &append_requests,
        )
        .await,
    );
    db_sqlite_close.close().await;

    let (_postgres_guard, postgres_pool, db_postgres_close) = Database::Postgres.set_up().await;
    let postgres_conn = postgres_pool.connection_test().await.unwrap();
    let postgres_log = ExecutionLogSanitized::from(
        create_and_append(
            postgres_conn.as_ref(),
            execution_id,
            create_req,
            &append_requests,
        )
        .await,
    );
    drop(postgres_conn);
    db_postgres_close.close().await;
    println!(
        "Expected (sqlite):\n{expected}\nActual (postgres):\n{actual}",
        expected = serde_json::to_string(&sqlite_log).unwrap(),
        actual = serde_json::to_string(&postgres_log).unwrap()
    );
    assert_eq!(sqlite_log.pending_state, postgres_log.pending_state);
    assert_eq!(sqlite_log.events.len(), postgres_log.events.len());
    for i in 0..sqlite_log.events.len() {
        assert_eq!(
            sqlite_log.events[i], postgres_log.events[i],
            "Failed at {i}-th element"
        );
    }
    assert_eq!(sqlite_log.events, postgres_log.events);
    assert_eq!(sqlite_log, postgres_log,);
}

fn arbitrary_valid_append_request(
    unstructured: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<AppendRequest> {
    let mut req = AppendRequest {
        event: postgres_compatible(&unstructured.arbitrary()?),
        created_at: Now.now(),
    };
    normalize_timestamps(&mut req, unstructured)?;
    Ok(req)
}

fn postgres_compatible(event: &ExecutionRequest) -> ExecutionRequest {
    let serialized = serde_json::to_string(&event).expect("ExecutionRequest must serialize");
    serde_json::from_str(&serialized.replace("\\u0000", ""))
        .expect("sanitized ExecutionRequest must deserialize")
}

fn normalize_timestamps(
    req: &mut AppendRequest,
    unstructured: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<()> {
    match &mut req.event {
        ExecutionRequest::Created {
            ffqn: _,
            params: _,
            parent: _,
            scheduled_at,
            component_id: _,
            deployment_id: _,
            metadata: _,
            scheduled_by: _,
        } => {
            *scheduled_at = arbitrary_valid_datetime(unstructured)?;
        }
        ExecutionRequest::Locked(locked) => {
            locked.lock_expires_at = arbitrary_valid_datetime(unstructured)?;
        }
        ExecutionRequest::Unlocked(unlocked) => {
            unlocked.backoff_expires_at = arbitrary_valid_datetime(unstructured)?;
        }
        ExecutionRequest::TemporarilyFailed {
            backoff_expires_at,
            reason: _,
            detail: _,
            http_client_traces: _,
        }
        | ExecutionRequest::TemporarilyTimedOut {
            backoff_expires_at,
            http_client_traces: _,
        } => {
            *backoff_expires_at = arbitrary_valid_datetime(unstructured)?;
        }
        ExecutionRequest::HistoryEvent { event } => {
            normalize_history_event_timestamps(event, unstructured)?;
        }
        ExecutionRequest::ComponentUpgradeFinished {
            component_digest: _,
            deployment_id: _,
            outcome: _,
        }
        | ExecutionRequest::Finished {
            retval: _,
            http_client_traces: _,
        }
        | ExecutionRequest::Paused
        | ExecutionRequest::Unpaused => {}
    }

    Ok(())
}

fn normalize_history_event_timestamps(
    event: &mut HistoryEvent,
    unstructured: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<()> {
    match event {
        HistoryEvent::JoinSetRequest {
            join_set_id: _,
            request,
        } => match request {
            JoinSetRequest::DelayRequest {
                delay_id: _,
                expires_at,
                schedule_at,
                paused: _,
            } => {
                *expires_at = arbitrary_valid_datetime(unstructured)?;
                normalize_schedule_at(schedule_at, unstructured)?;
            }
            JoinSetRequest::ChildExecutionRequest {
                child_execution_id: _,
                target_ffqn: _,
                params: _,
                result: _,
            } => {}
        },
        HistoryEvent::JoinNext {
            join_set_id: _,
            run_expires_at,
            requested_ffqn: _,
            closing: _,
        } => {
            *run_expires_at = arbitrary_valid_datetime(unstructured)?;
        }
        HistoryEvent::Schedule {
            execution_id: _,
            schedule_at,
            result: _,
        } => {
            normalize_schedule_at(schedule_at, unstructured)?;
        }
        HistoryEvent::Persist { value: _, kind: _ }
        | HistoryEvent::JoinSetCreate { join_set_id: _ }
        | HistoryEvent::JoinNextTry {
            join_set_id: _,
            outcome: _,
        }
        | HistoryEvent::JoinNextTooMany {
            join_set_id: _,
            requested_ffqn: _,
        }
        | HistoryEvent::Stub {
            target_execution_id: _,
            retval_hash: _,
            result: _,
        } => {}
    }

    Ok(())
}

fn normalize_schedule_at(
    schedule_at: &mut HistoryEventScheduleAt,
    unstructured: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<()> {
    if matches!(schedule_at, HistoryEventScheduleAt::At(_)) {
        *schedule_at = HistoryEventScheduleAt::At(arbitrary_valid_datetime(unstructured)?);
    }
    Ok(())
}

fn arbitrary_valid_datetime(
    unstructured: &mut arbitrary::Unstructured<'_>,
) -> arbitrary::Result<DateTime<Utc>> {
    let seconds = unstructured.int_in_range(0..=4_102_444_800_i64)?;
    let micros = unstructured.int_in_range(0..=999_999_u32)?;
    Ok(DateTime::from_timestamp(seconds, micros * 1_000).unwrap())
}

async fn create_and_append(
    db_connection: &dyn DbConnectionTest,
    execution_id: ExecutionId,
    create_req: CreateRequest,
    append_requests: &[AppendRequest],
) -> ExecutionLog {
    let mut version;
    // Create
    version = db_connection.create(create_req).await.unwrap();

    for event in append_requests {
        let event = event.clone();
        let res = db_connection
            .append(execution_id.clone(), version.clone(), event)
            .await;
        match res {
            Ok(new_version) => version = new_version,
            Err(err) => warn!("Ignoring {err:?}"),
        }
    }
    db_connection.get(&execution_id).await.unwrap()
}

const WVWT_RECORD_UNSORTED: &str = r#"
        {
            "type": "record { login :string, repo: string, description: option<string> }",
            "value": {
                "login": "login",
                "repo": "repo",
                "description": "description"
            }
        }
        "#;

async fn persist_finished_event(
    db_connection: &dyn DbConnection,
) -> (
    ExecutionId,
    Version,          /* finished version */
    ExecutionRequest, /* ExecutionRequest::Finished */
) {
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
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            component_id: component_id.clone(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            scheduled_by: None,
            paused: false,
        })
        .await
        .unwrap();

    // LockPending
    let version = {
        let created_at = sim_clock.now();
        let mut locked_executions = db_connection
            .lock_pending_by_ffqns(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                DEPLOYMENT_ID_DUMMY,
                exec1,
                created_at + LOCK_EXPIRY,
                RunId::generate(),
                ComponentRetryConfig {
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: Some(0),
                },
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.pop().unwrap();
        assert_eq!(Version::new(2), locked_execution.next_version);
        locked_execution.next_version
    };

    let wast_val_with_type: WastValWithType = serde_json::from_str(WVWT_RECORD_UNSORTED).unwrap();
    let finished = ExecutionRequest::Finished {
        retval: SupportedFunctionReturnValue::Ok(Some(wast_val_with_type)),
        http_client_traces: None,
    };
    // Finished
    let version = {
        let req = AppendRequest {
            event: finished.clone(),
            created_at: sim_clock.now(),
        };
        db_connection
            .append(execution_id.clone(), version, req)
            .await
            .unwrap()
    };
    (execution_id, version, finished)
}

// Test that the database does not use `serde_json::Value` during deserialization as it
// would sorts attributes and thus break `TypeWrapper` and `WastVal`,
#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn get_execution_event_should_not_break_json_order(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();

    let (execution_id, finished_version, finished) =
        persist_finished_event(db_connection.as_ref()).await;
    let found_inner = db_connection
        .get_execution_event(&execution_id, &finished_version)
        .await
        .unwrap()
        .event;
    assert_eq!(finished, found_inner);
    drop(db_connection);
    db_close.close().await;
}

// Test that the database does not use `serde_json::Value` during deserialization as it
// would sort attributes and thus break `TypeWrapper` and `WastVal`,
#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_execution_events_should_not_break_json_order(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.external_api_conn().await.unwrap();

    let (execution_id, finished_version, finished) =
        persist_finished_event(db_connection.as_ref()).await;
    let result = db_connection
        .list_execution_events(
            &execution_id,
            Pagination::NewerThan {
                length: 1,
                cursor: finished_version.0,
                including_cursor: true,
            },
            false,
        )
        .await
        .unwrap();
    assert_eq!(Version(2), finished_version);
    assert_eq!(1, result.events.len());
    assert_eq!(Version(2), result.max_version); // created = 0, locked = 1, finished = 2
    let found_inner = &result.events[0].event;
    assert_eq!(finished, *found_inner);
    drop(db_connection);
    db_close.close().await;
}
