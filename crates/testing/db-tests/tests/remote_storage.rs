use chrono::DateTime;
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, ExecutionMetadata, Params,
    SUPPORTED_RETURN_VALUE_OK_EMPTY,
    prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId},
    storage::{AppendRequest, CreateRequest, DbPool, DbPoolCloseable, ExecutionRequest, Version},
};
use db_sqlite::sqlite_dao::SqlitePool;
use db_turso::{TransactionMode, TursoConfig};
use obeli_db_tests::{DbPoolCloseableWrapper, SOME_FFQN, turso_test_config_mode};
use std::{sync::Arc, time::Duration};

fn request(id: ExecutionId) -> CreateRequest {
    CreateRequest {
        execution_id: id,
        created_at: DateTime::UNIX_EPOCH,
        ffqn: SOME_FFQN,
        params: Params::empty(),
        parent: None,
        metadata: ExecutionMetadata::empty(),
        scheduled_at: DateTime::UNIX_EPOCH,
        component_id: ComponentId::dummy_activity(),
        deployment_id: DEPLOYMENT_ID_DUMMY,
        scheduled_by: None,
        paused: false,
        max_persisted_value_size_bytes: u64::MAX,
    }
}

#[derive(Clone, Copy, Debug)]
enum ClaimStrategy {
    Ffqns,
    Auto,
    Digest,
}

#[rstest::rstest]
#[tokio::test]
async fn turso_independent_workers_claim_and_observe_finished_result(
    #[values(ClaimStrategy::Ffqns, ClaimStrategy::Auto, ClaimStrategy::Digest)]
    strategy: ClaimStrategy,
    #[values(TransactionMode::Immediate, TransactionMode::Concurrent)] mode: TransactionMode,
) {
    test_utils::set_up();
    let (config, cleanup) = turso_test_config_mode(mode).await;
    let first = db_turso::connect(config.clone()).await.unwrap();
    let second = db_turso::connect(config).await.unwrap();
    let id = ExecutionId::generate();
    let conn = first.connection_test().await.unwrap();
    conn.create(request(id.clone())).await.unwrap();
    let claim = async |pool: &SqlitePool| {
        let conn = pool.connection().await.unwrap();
        let retry_config = ComponentRetryConfig {
            retry_exp_backoff: Duration::ZERO,
            max_retries: Some(0),
        };
        match strategy {
            ClaimStrategy::Ffqns => {
                conn.lock_pending_by_ffqns(
                    1,
                    DateTime::UNIX_EPOCH,
                    Arc::from([SOME_FFQN]),
                    DateTime::UNIX_EPOCH,
                    ComponentId::dummy_activity(),
                    DEPLOYMENT_ID_DUMMY,
                    ExecutorId::generate(),
                    DateTime::UNIX_EPOCH + Duration::from_secs(30),
                    RunId::generate(),
                    retry_config,
                )
                .await
            }
            ClaimStrategy::Auto => {
                conn.lock_pending_by_ffqns_auto(
                    1,
                    DateTime::UNIX_EPOCH,
                    Arc::from([SOME_FFQN]),
                    DateTime::UNIX_EPOCH,
                    ComponentId::dummy_activity(),
                    DEPLOYMENT_ID_DUMMY,
                    ExecutorId::generate(),
                    DateTime::UNIX_EPOCH + Duration::from_secs(30),
                    RunId::generate(),
                    retry_config,
                )
                .await
            }
            ClaimStrategy::Digest => {
                conn.lock_pending_by_component_digest(
                    1,
                    DateTime::UNIX_EPOCH,
                    &ComponentId::dummy_activity(),
                    DEPLOYMENT_ID_DUMMY,
                    DateTime::UNIX_EPOCH,
                    ExecutorId::generate(),
                    DateTime::UNIX_EPOCH + Duration::from_secs(30),
                    RunId::generate(),
                    retry_config,
                )
                .await
            }
        }
        .unwrap()
    };
    let (a, b) = tokio::join!(claim(&first), claim(&second));
    assert_eq!(a.len() + b.len(), 1);
    assert_eq!(conn.get(&id).await.unwrap().events.len(), 2);
    let reader = second.connection().await.unwrap();
    let id_reader = id.clone();
    let waiter = tokio::spawn(async move {
        reader
            .wait_for_finished_result(&id_reader, None)
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    conn.append(
        id.clone(),
        Version::new(2),
        AppendRequest {
            created_at: DateTime::UNIX_EPOCH,
            event: ExecutionRequest::Finished {
                retval: SUPPORTED_RETURN_VALUE_OK_EMPTY.clone(),
                http_client_traces: None,
            },
        },
    )
    .await
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, SUPPORTED_RETURN_VALUE_OK_EMPTY);
    second.close().await;
    DbPoolCloseableWrapper::Turso(first, cleanup).close().await;
}

#[rstest::rstest]
#[tokio::test]
async fn turso_lost_commit_is_reported_without_replaying_the_write(
    #[values(TransactionMode::Immediate, TransactionMode::Concurrent)] mode: TransactionMode,
) {
    test_utils::set_up();
    let server = db_test_server::TestServer::start().await.unwrap();
    let name = if mode == TransactionMode::Concurrent {
        "mvcc_lost_commit"
    } else {
        "lost_commit"
    };
    let config = TursoConfig {
        url: format!("{}/{name}", server.url),
        auth_token: secrecy::SecretString::from(String::new()),
        queue_capacity: 100,
        transaction_mode: mode,
        request_timeout: Duration::from_secs(5),
        metrics_threshold: None,
        namespace: String::new(),
    };
    let pool = db_turso::connect(config).await.unwrap();
    let id = ExecutionId::generate();
    server.lose_next_commit_response(name).await;
    let conn = pool.connection_test().await.unwrap();
    assert!(conn.create(request(id.clone())).await.is_err());
    let log = conn.get(&id).await.unwrap();
    assert_eq!(log.events.len(), 1);
    assert_eq!(log.events[0].version, Version::new(0));
    let other = ExecutionId::generate();
    conn.create(request(other.clone())).await.unwrap();
    assert_eq!(conn.get(&other).await.unwrap().events.len(), 1);
    pool.close().await;
}

#[rstest::rstest]
#[tokio::test]
async fn turso_independent_workers_order_responses_and_wake_pending_work(
    #[values(TransactionMode::Immediate, TransactionMode::Concurrent)] mode: TransactionMode,
    #[values(false, true)] mixed_modes: bool,
) {
    use concepts::storage::{JoinSetResponse, JoinSetResponseEvent, ResponseCursor};
    use concepts::{JoinSetId, JoinSetKind, prefixed_ulid::DelayId};
    test_utils::set_up();
    let local_mode = if mixed_modes {
        TransactionMode::Concurrent
    } else {
        mode
    };
    let (mut config, cleanup) = turso_test_config_mode(local_mode).await;
    config.transaction_mode = mode;
    let first = db_turso::connect(config.clone()).await.unwrap();
    if mixed_modes {
        config.transaction_mode = match mode {
            TransactionMode::Immediate => TransactionMode::Concurrent,
            TransactionMode::Concurrent => TransactionMode::Immediate,
        };
    }
    let second = db_turso::connect(config).await.unwrap();
    let reader = second.connection().await.unwrap();
    let pending = tokio::spawn(async move {
        reader
            .wait_for_pending_by_ffqn(
                DateTime::UNIX_EPOCH,
                Arc::from([SOME_FFQN]),
                None,
                Box::pin(std::future::pending()),
            )
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let id = ExecutionId::generate();
    let writer: Arc<dyn concepts::storage::DbConnectionTest> =
        first.connection_test().await.unwrap().into();
    writer.create(request(id.clone())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    let other: Arc<dyn concepts::storage::DbConnectionTest> =
        second.connection_test().await.unwrap().into();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..8 {
        let writer = if i % 2 == 0 {
            writer.clone()
        } else {
            other.clone()
        };
        let id = id.clone();
        tasks.spawn(async move {
            let join_set_id =
                JoinSetId::new(JoinSetKind::Named, format!("response-{i}").into()).unwrap();
            writer
                .append_response(
                    DateTime::UNIX_EPOCH,
                    id.clone(),
                    JoinSetResponseEvent {
                        event: JoinSetResponse::DelayFinished {
                            delay_id: DelayId::new(&id, &join_set_id),
                            result: Ok(()),
                        },
                        join_set_id,
                    },
                )
                .await
                .unwrap();
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let responses = other
        .subscribe_to_next_responses(&id, ResponseCursor(0), Box::pin(std::future::pending()))
        .await
        .unwrap();
    assert_eq!(responses.len(), 8);
    assert_eq!(
        responses.iter().map(|r| r.cursor.0).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    let watermark = responses.last().unwrap().cursor;
    assert!(matches!(
        other
            .subscribe_to_next_responses(&id, watermark, Box::pin(std::future::pending()))
            .await,
        Err(
            concepts::storage::SubscribeToResponsesError::SubscriptionEnded(
                concepts::storage::ResponseSubscriptionEnd::PollIntervalElapsed
            )
        )
    ));
    second.close().await;
    DbPoolCloseableWrapper::Turso(first, cleanup).close().await;
}

#[tokio::test]
async fn turso_truncated_begin_response_releases_the_global_writer_lock() {
    test_utils::set_up();
    let server = db_test_server::TestServer::start().await.unwrap();
    let config = TursoConfig {
        url: format!("{}/lost_begin", server.url),
        auth_token: secrecy::SecretString::from(String::new()),
        queue_capacity: 100,
        transaction_mode: db_turso::TransactionMode::Immediate,
        request_timeout: Duration::from_secs(5),
        metrics_threshold: None,
        namespace: String::new(),
    };
    let first = db_turso::connect(config.clone()).await.unwrap();
    let second = db_turso::connect(config).await.unwrap();
    server.truncate_next_begin_response("lost_begin").await;
    let rejected = ExecutionId::generate();
    assert!(
        first
            .connection_test()
            .await
            .unwrap()
            .create(request(rejected.clone()))
            .await
            .is_err()
    );
    let conn = second.connection_test().await.unwrap();
    let other = ExecutionId::generate();
    tokio::time::timeout(Duration::from_secs(5), conn.create(request(other.clone())))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(conn.get(&other).await.unwrap().events.len(), 1);
    assert!(conn.get(&rejected).await.is_err());
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn turso_concurrent_write_finishes_while_another_writer_is_uncommitted() {
    use secrecy::ExposeSecret as _;
    test_utils::set_up();
    let (config, cleanup) = turso_test_config_mode(TransactionMode::Concurrent).await;
    let pool = db_turso::connect(config.clone()).await.unwrap();
    let database = turso_serverless::Builder::new_remote(&config.url)
        .with_auth_token(config.auth_token.expose_secret())
        .build()
        .await
        .unwrap();
    let held = database.connect().unwrap();
    let table = format!("{}t_overlap_probe", config.namespace);
    held.execute(
        format!("CREATE TABLE {table}(id INTEGER PRIMARY KEY, value INTEGER)"),
        (),
    )
    .await
    .unwrap();
    held.execute(format!("INSERT INTO {table} VALUES(1, 0)"), ())
        .await
        .unwrap();
    held.execute("BEGIN CONCURRENT", ()).await.unwrap();
    held.execute(format!("UPDATE {table} SET value=1 WHERE id=1"), ())
        .await
        .unwrap();
    let conn = pool.connection_test().await.unwrap();
    let id = ExecutionId::generate();
    let created =
        tokio::time::timeout(Duration::from_secs(10), conn.create(request(id.clone()))).await;
    held.execute("ROLLBACK", ()).await.unwrap();
    created
        .expect("independent DAO write must finish while the other writer is uncommitted")
        .unwrap();
    assert_eq!(conn.get(&id).await.unwrap().events.len(), 1);
    let mut rows = held
        .query(format!("SELECT value FROM {table} WHERE id=1"), ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
    held.execute(format!("DROP TABLE {table}"), ())
        .await
        .unwrap();
    held.close().await.unwrap();
    DbPoolCloseableWrapper::Turso(pool, cleanup).close().await;
}

#[tokio::test]
async fn turso_concurrent_child_finishes_and_responses_commit_in_cursor_order() {
    use concepts::storage::{
        AppendEventsToExecution, AppendResponseToExecution, JoinSetResponse, ResponseCursor,
        ResponseSubscriptionEnd, SubscribeToResponsesError,
    };
    use concepts::{JoinSetId, JoinSetKind};
    test_utils::set_up();
    let (config, cleanup) = turso_test_config_mode(TransactionMode::Concurrent).await;
    let first = db_turso::connect(config.clone()).await.unwrap();
    let second = db_turso::connect(config).await.unwrap();
    let writer: Arc<dyn concepts::storage::DbConnectionTest> =
        first.connection_test().await.unwrap().into();
    let other: Arc<dyn concepts::storage::DbConnectionTest> =
        second.connection_test().await.unwrap().into();
    let parent = ExecutionId::generate();
    writer.create(request(parent.clone())).await.unwrap();
    let mut children = Vec::new();
    for i in 0..8 {
        let join_set_id = JoinSetId::new(JoinSetKind::Named, format!("child-{i}").into()).unwrap();
        let child = parent.next_level(&join_set_id);
        writer
            .create(request(ExecutionId::Derived(child.clone())))
            .await
            .unwrap();
        children.push((join_set_id, child));
    }
    let mut tasks = tokio::task::JoinSet::new();
    for (i, (join_set_id, child)) in children.into_iter().enumerate() {
        let writer = if i % 2 == 0 {
            writer.clone()
        } else {
            other.clone()
        };
        let parent = parent.clone();
        tasks.spawn(async move {
            writer
                .append_batch_respond_to_parent(
                    AppendEventsToExecution {
                        execution_id: ExecutionId::Derived(child.clone()),
                        version: Version::new(1),
                        batch: vec![AppendRequest {
                            created_at: DateTime::UNIX_EPOCH,
                            event: ExecutionRequest::Finished {
                                retval: SUPPORTED_RETURN_VALUE_OK_EMPTY.clone(),
                                http_client_traces: None,
                            },
                        }],
                    },
                    AppendResponseToExecution {
                        parent_execution_id: parent,
                        created_at: DateTime::UNIX_EPOCH,
                        join_set_id,
                        child_execution_id: child,
                        finished_version: Version::new(1),
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY.clone(),
                    },
                    DateTime::UNIX_EPOCH,
                )
                .await
                .unwrap();
        });
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cursor = ResponseCursor(0);
        while cursor.0 < 8 {
            match other
                .subscribe_to_next_responses(&parent, cursor, Box::pin(std::future::pending()))
                .await
            {
                Ok(responses) => {
                    for response in responses {
                        assert_eq!(response.cursor.0, cursor.0 + 1);
                        let JoinSetResponse::ChildExecutionFinished {
                            child_execution_id,
                            result,
                            ..
                        } = response.event.event.event
                        else {
                            panic!("expected child finish");
                        };
                        assert_eq!(result, SUPPORTED_RETURN_VALUE_OK_EMPTY);
                        let log = other
                            .get(&ExecutionId::Derived(child_execution_id))
                            .await
                            .unwrap();
                        assert_eq!(log.events.len(), 2);
                        assert!(matches!(
                            log.events[1].event,
                            ExecutionRequest::Finished { .. }
                        ));
                        cursor = response.cursor;
                    }
                }
                Err(SubscribeToResponsesError::SubscriptionEnded(
                    ResponseSubscriptionEnd::PollIntervalElapsed,
                )) => {}
                Err(error) => panic!("{error:?}"),
            }
        }
    })
    .await
    .unwrap();
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    second.close().await;
    DbPoolCloseableWrapper::Turso(first, cleanup).close().await;
}

#[rstest::rstest]
#[tokio::test]
async fn turso_confirmed_idle_abort_replays_the_whole_transaction_once(
    #[values(TransactionMode::Immediate, TransactionMode::Concurrent)] mode: TransactionMode,
    #[values(false, true)] at_commit: bool,
) {
    test_utils::set_up();
    let server = db_test_server::TestServer::start().await.unwrap();
    let name = if mode == TransactionMode::Concurrent {
        "mvcc_idle_abort"
    } else {
        "idle_abort"
    };
    let config = TursoConfig {
        url: format!("{}/{name}", server.url),
        auth_token: secrecy::SecretString::from(String::new()),
        queue_capacity: 100,
        transaction_mode: mode,
        request_timeout: Duration::from_secs(5),
        metrics_threshold: None,
        namespace: String::new(),
    };
    let pool = db_turso::connect(config).await.unwrap();
    if at_commit {
        server.expire_next_commit(name).await;
    } else {
        server
            .expire_next_statement(name, "INSERT INTO t_state")
            .await;
    }
    let conn = pool.connection_test().await.unwrap();
    let id = ExecutionId::generate();
    let version = conn.create(request(id.clone())).await.unwrap();
    assert_eq!(version, Version::new(1));
    assert_eq!(server.idle_abort_count(name).await, 1);
    let log = conn.get(&id).await.unwrap();
    assert_eq!(log.events.len(), 1);
    assert_eq!(log.events[0].version, Version::new(0));
    let other = ExecutionId::generate();
    conn.create(request(other.clone())).await.unwrap();
    assert_eq!(conn.get(&other).await.unwrap().events.len(), 1);
    pool.close().await;
}

#[rstest::rstest]
#[tokio::test]
async fn turso_statement_transport_failure_stops_before_commit(
    #[values(TransactionMode::Immediate, TransactionMode::Concurrent)] mode: TransactionMode,
    #[values(false, true)] read_only: bool,
) {
    test_utils::set_up();
    let server = db_test_server::TestServer::start().await.unwrap();
    let name = if mode == TransactionMode::Concurrent {
        "mvcc_statement_failure"
    } else {
        "statement_failure"
    };
    let config = TursoConfig {
        url: format!("{}/{name}", server.url),
        auth_token: secrecy::SecretString::from(String::new()),
        queue_capacity: 100,
        transaction_mode: mode,
        request_timeout: Duration::from_secs(5),
        metrics_threshold: None,
        namespace: String::new(),
    };
    let pool = db_turso::connect(config).await.unwrap();
    let conn = pool.connection_test().await.unwrap();
    let id = ExecutionId::generate();
    if read_only {
        conn.create(request(id.clone())).await.unwrap();
    }
    let before = server.commit_sequence_count(name).await;
    let fragment = if read_only {
        "FROM t_state"
    } else {
        "INSERT INTO t_state"
    };
    server.fail_next_statement_response(name, fragment).await;
    if read_only {
        assert!(conn.get_pending_state(&id).await.is_err());
    } else {
        assert!(conn.create(request(id.clone())).await.is_err());
    }
    assert_eq!(server.commit_sequence_count(name).await, before);
    if read_only {
        assert_eq!(conn.get(&id).await.unwrap().events.len(), 1);
    } else {
        assert!(matches!(
            conn.get(&id).await,
            Err(concepts::storage::DbErrorRead::NotFound)
        ));
    }
    pool.close().await;
}
