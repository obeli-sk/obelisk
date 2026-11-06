//! Multi-node and failure behavior of the libSQL backend against a real `sqld`.
use chrono::DateTime;
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, ExecutionMetadata, Params,
    SUPPORTED_RETURN_VALUE_OK_EMPTY,
    prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId},
    storage::{AppendRequest, CreateRequest, DbPool, DbPoolCloseable, ExecutionRequest, Version},
};
use db_libsql::{HranaClient, LibsqlPool};
use db_sqlite::sql_connection::{BatchStep, PipelineError, SqlParams, SqlPipeline, StepOutcome};
use obeli_db_tests::{SOME_FFQN, SqldServer};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

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
async fn independent_nodes_claim_once_and_observe_finished_result(
    #[values(ClaimStrategy::Ffqns, ClaimStrategy::Auto, ClaimStrategy::Digest)]
    strategy: ClaimStrategy,
) {
    test_utils::set_up();
    let server = SqldServer::start().await;
    let first = db_libsql::connect(server.config()).await.unwrap();
    let second = db_libsql::connect(server.config()).await.unwrap();
    let id = ExecutionId::generate();
    let conn = first.connection_test().await.unwrap();
    conn.create(request(id.clone())).await.unwrap();
    let claim = async |pool: &LibsqlPool| {
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
                http_response: None,
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
    first.close().await;
}

#[tokio::test]
async fn independent_nodes_order_responses_and_wake_pending_work() {
    use concepts::storage::{JoinSetResponse, JoinSetResponseEvent, ResponseCursor};
    use concepts::{JoinSetId, JoinSetKind, prefixed_ulid::DelayId};
    test_utils::set_up();
    let server = SqldServer::start().await;
    let first = db_libsql::connect(server.config()).await.unwrap();
    let second = db_libsql::connect(server.config()).await.unwrap();
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
    first.close().await;
}

/// Observes the batches of one node and loses a chosen response.
struct ObservedPipeline {
    inner: HranaClient,
    batches: Arc<AtomicUsize>,
    lose_next_commit_response: Arc<AtomicBool>,
}
impl SqlPipeline for ObservedPipeline {
    fn batch(&self, steps: &[BatchStep]) -> Result<Vec<StepOutcome>, PipelineError> {
        self.batches.fetch_add(1, Ordering::SeqCst);
        let outcomes = self.inner.batch(steps)?;
        if steps.iter().any(|step| step.sql == "COMMIT")
            && self.lose_next_commit_response.swap(false, Ordering::SeqCst)
        {
            return Err(PipelineError::Failed("connection reset".to_string()));
        }
        Ok(outcomes)
    }
}

async fn observed_pool(server: &SqldServer) -> (LibsqlPool, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let batches = Arc::new(AtomicUsize::new(0));
    let lose_next_commit_response = Arc::new(AtomicBool::new(false));
    let pool = db_libsql::connect_with(server.config(), {
        let batches = batches.clone();
        let lose_next_commit_response = lose_next_commit_response.clone();
        move |inner| {
            Box::new(ObservedPipeline {
                inner,
                batches,
                lose_next_commit_response,
            })
        }
    })
    .await
    .unwrap();
    (pool, batches, lose_next_commit_response)
}

#[tokio::test]
async fn lost_commit_response_is_reported_without_replaying_the_write() {
    test_utils::set_up();
    let server = SqldServer::start().await;
    let (pool, _, lose_next_commit_response) = observed_pool(&server).await;
    let conn = pool.connection_test().await.unwrap();
    let id = ExecutionId::generate();
    lose_next_commit_response.store(true, Ordering::SeqCst);
    assert!(conn.create(request(id.clone())).await.is_err());
    let log = conn.get(&id).await.unwrap();
    assert_eq!(log.events.len(), 1);
    assert_eq!(log.events[0].version, Version::new(0));
    let other = ExecutionId::generate();
    conn.create(request(other.clone())).await.unwrap();
    assert_eq!(conn.get(&other).await.unwrap().events.len(), 1);
    pool.close().await;
}

#[tokio::test]
async fn concurrent_creates_stack_into_far_fewer_batches() {
    const CREATES: usize = 200;
    test_utils::set_up();
    let server = SqldServer::start().await;
    let (pool, batches, _) = observed_pool(&server).await;
    let conn: Arc<dyn concepts::storage::DbConnectionTest> =
        pool.connection_test().await.unwrap().into();
    // Warm up the speculated row counts.
    conn.create(request(ExecutionId::generate())).await.unwrap();
    batches.store(0, Ordering::SeqCst);
    let ids = (0..CREATES)
        .map(|_| ExecutionId::generate())
        .collect::<Vec<_>>();
    let mut tasks = tokio::task::JoinSet::new();
    for id in ids.clone() {
        let conn = conn.clone();
        tasks.spawn(async move { conn.create(request(id)).await.unwrap() });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let used = batches.load(Ordering::SeqCst);
    println!("{CREATES} creates used {used} batches");
    assert!(used < CREATES / 4, "{CREATES} creates used {used} batches");
    for id in &ids {
        assert_eq!(conn.get(id).await.unwrap().events.len(), 1);
    }
    pool.close().await;
}

#[tokio::test]
async fn two_busy_nodes_never_lose_a_create_or_double_claim() {
    const CREATES_PER_NODE: usize = 100;
    const CLAIMERS_PER_NODE: usize = 8;
    test_utils::set_up();
    let server = SqldServer::start().await;
    let nodes = [
        db_libsql::connect(server.config()).await.unwrap(),
        db_libsql::connect(server.config()).await.unwrap(),
    ];
    let mut create_tasks = tokio::task::JoinSet::new();
    let mut created = HashSet::new();
    for node in &nodes {
        let conn: Arc<dyn concepts::storage::DbConnectionTest> =
            node.connection_test().await.unwrap().into();
        for _ in 0..CREATES_PER_NODE {
            let id = ExecutionId::generate();
            created.insert(id.clone());
            let conn = conn.clone();
            create_tasks.spawn(async move { conn.create(request(id)).await.unwrap() });
        }
    }
    let mut claims = tokio::task::JoinSet::new();
    for node in &nodes {
        for _ in 0..CLAIMERS_PER_NODE {
            let conn = node.connection().await.unwrap();
            claims.spawn(async move {
                let mut claimed = Vec::new();
                let mut idle_rounds = 0;
                while idle_rounds < 20 {
                    let locked = conn
                        .lock_pending_by_ffqns(
                            5,
                            DateTime::UNIX_EPOCH,
                            Arc::from([SOME_FFQN]),
                            DateTime::UNIX_EPOCH,
                            ComponentId::dummy_activity(),
                            DEPLOYMENT_ID_DUMMY,
                            ExecutorId::generate(),
                            DateTime::UNIX_EPOCH + Duration::from_secs(30),
                            RunId::generate(),
                            ComponentRetryConfig {
                                retry_exp_backoff: Duration::ZERO,
                                max_retries: Some(0),
                            },
                        )
                        .await
                        .unwrap();
                    idle_rounds = if locked.is_empty() {
                        idle_rounds + 1
                    } else {
                        0
                    };
                    claimed.extend(locked.into_iter().map(|locked| locked.execution_id));
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                claimed
            });
        }
    }
    while let Some(result) = create_tasks.join_next().await {
        result.unwrap();
    }
    let mut claimed = Vec::new();
    while let Some(result) = claims.join_next().await {
        claimed.extend(result.unwrap());
    }
    assert_eq!(claimed.len(), created.len());
    assert_eq!(claimed.into_iter().collect::<HashSet<_>>(), created);
    let conn = nodes[0].connection_test().await.unwrap();
    for id in &created {
        assert_eq!(conn.get(id).await.unwrap().events.len(), 2);
    }
    let [first, second] = nodes;
    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn files_above_the_server_value_limit_roundtrip_and_are_collected() {
    const SIZE: usize = 13 * 1024 * 1024;
    test_utils::set_up();
    let server = SqldServer::start().await;
    let pool = db_libsql::connect(server.config()).await.unwrap();
    let cas = pool.cas_conn().await.unwrap();
    let content = (0..SIZE)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect::<Vec<_>>();
    let digest = cas.write_blob(&content).await.unwrap();
    assert_eq!(digest, cas.write_blob(&content).await.unwrap());
    // Together their chunks exceed the size of a single server response.
    let reads = (0..6).map(|_| cas.read_blob(&digest));
    for read in futures_util::future::join_all(reads).await {
        assert_eq!(read.unwrap().unwrap(), content);
    }
    let gc = pool.cas_gc_conn().await.unwrap();
    let result = gc.gc_cas(false, 100, chrono::Utc::now()).await.unwrap();
    assert_eq!(result.deleted_blobs, 1);
    assert!(cas.read_blob(&digest).await.unwrap().is_none());
    pool.close().await;
    let probe = reqwest::Client::new()
        .post(format!("{}/v3/pipeline", server.url()))
        .json(&serde_json::json!({"requests": [
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM t_file_chunk", "want_rows": true}},
            {"type": "close"},
        ]}))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let count = &probe["results"][0]["response"]["result"]["rows"][0][0]["value"];
    assert_eq!("0", count, "{probe}");
}

#[tokio::test(flavor = "multi_thread")]
async fn migrations_run_one_transaction_each_with_foreign_keys_off() {
    use refinery::{Migration, Runner};
    test_utils::set_up();
    let server = SqldServer::start().await;
    let client = HranaClient::new(&server.config()).unwrap();
    let migration = |name: &str, sql: &str| Migration::unapplied(name, sql).unwrap();
    let parent = "CREATE TABLE parent (id INTEGER PRIMARY KEY);";
    let schema = migration(
        "V1__schema",
        &format!(
            "{parent} CREATE TABLE child (id INTEGER REFERENCES parent(id) ON DELETE CASCADE);
             INSERT INTO parent VALUES (1); INSERT INTO child VALUES (1);"
        ),
    );
    let rebuild = migration("V2__rebuild", &format!("DROP TABLE parent; {parent}"));
    let broken = migration(
        "V3__broken",
        "CREATE TABLE half_done (id INTEGER); INSERT INTO missing VALUES (1);",
    );
    tokio::task::spawn_blocking(move || {
        db_libsql::apply(&client, Runner::new(&[schema, rebuild, broken])).unwrap_err();
        let count = |sql: &str| {
            let step = BatchStep {
                sql: sql.to_owned(),
                params: SqlParams::None,
                want_rows: true,
                condition: None,
            };
            match client.batch(&[step]).unwrap().remove(0) {
                StepOutcome::Done(result) => result.rows.len(),
                StepOutcome::Error { .. } | StepOutcome::Skipped => panic!("query failed"),
            }
        };
        assert_eq!(count("SELECT id FROM child"), 1);
        assert_eq!(count("SELECT version FROM refinery_schema_history"), 2);
        assert_eq!(
            count("SELECT name FROM sqlite_master WHERE name = 'half_done'"),
            0
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn nodes_racing_on_an_empty_database_all_start_and_can_restart() {
    test_utils::set_up();
    let server = SqldServer::start().await;
    let (first, second, third) = tokio::join!(
        db_libsql::connect(server.config()),
        db_libsql::connect(server.config()),
        db_libsql::connect(server.config()),
    );
    let (first, second, third) = (first.unwrap(), second.unwrap(), third.unwrap());
    first.close().await;
    second.close().await;
    third.close().await;
    let restarted = db_libsql::connect(server.config()).await.unwrap();
    let conn = restarted.connection_test().await.unwrap();
    let id = ExecutionId::generate();
    conn.create(request(id.clone())).await.unwrap();
    assert_eq!(conn.get(&id).await.unwrap().events.len(), 1);
    restarted.close().await;
}
