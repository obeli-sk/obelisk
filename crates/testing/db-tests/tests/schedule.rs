use chrono::{DateTime, Utc};
use concepts::storage::{
    AppendRequest, CreateRequest, DbPoolCloseable, ExecutionRequest, HistoryEvent,
    HistoryEventScheduleAt, ScheduleRequestError, Version,
};
use concepts::{ComponentId, ExecutionId, ExecutionMetadata, FunctionFqn, Params};
use obeli_db_tests::Database;
use rstest::rstest;
use std::time::Duration;
use test_db_macro::expand_enum_database;

fn create_request(execution_id: ExecutionId, created_at: DateTime<Utc>) -> CreateRequest {
    CreateRequest {
        created_at,
        execution_id,
        ffqn: FunctionFqn::new_static("test:schedule/activity", "run"),
        params: Params::empty(),
        parent: None,
        scheduled_at: created_at + Duration::from_secs(60),
        component_id: ComponentId::dummy_activity(),
        deployment_id: concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY,
        metadata: ExecutionMetadata::empty(),
        scheduled_by: None,
        paused: false,
        max_persisted_value_size_bytes: u64::MAX,
    }
}

fn schedule_request(child: &CreateRequest, schedule_at: HistoryEventScheduleAt) -> AppendRequest {
    AppendRequest {
        created_at: child.created_at,
        event: ExecutionRequest::HistoryEvent {
            event: HistoryEvent::Schedule {
                execution_id: child.execution_id.clone(),
                schedule_at,
                params_hash: Some(concepts::persisted_value::compact_json_sha256(
                    &child.params,
                )),
                result: Ok(()),
            },
        },
    }
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn schedule_with_id_is_atomic_and_idempotent(database: Database) {
    test_utils::set_up();
    let (_guard, pool, close) = database.set_up().await;
    let conn = pool.connection().await.unwrap();
    let created_at = DateTime::UNIX_EPOCH + Duration::from_nanos(123_456_789);
    let child = create_request(ExecutionId::generate(), created_at);
    let mut tasks = tokio::task::JoinSet::new();
    for offset in 0..8 {
        let pool = pool.clone();
        let mut child = child.clone();
        child.created_at += Duration::from_secs(offset);
        child.scheduled_at += Duration::from_secs(offset);
        tasks.spawn(async move {
            let conn = pool.connection().await.unwrap();
            let parent = ExecutionId::generate();
            let version = conn
                .create(create_request(parent.clone(), child.created_at))
                .await
                .unwrap();
            let request =
                schedule_request(&child, HistoryEventScheduleAt::In(Duration::from_secs(60)));
            let (_, result) = conn
                .append_schedule(parent.clone(), version, request, child, vec![])
                .await
                .unwrap();
            assert_eq!(result, Ok(()));
            let log = conn.get(&parent).await.unwrap();
            assert_eq!(log.events.len(), 2);
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let original = conn
        .get_execution_event(&child.execution_id, &Version::new(0))
        .await
        .unwrap();
    let ExecutionRequest::Created(original_created) = &original.event else {
        panic!("expected Created")
    };
    let parent = ExecutionId::generate();
    let mut version = conn
        .create(create_request(parent.clone(), created_at))
        .await
        .unwrap();
    let mut retry = child.clone();
    retry.created_at += Duration::from_secs(100);
    retry.scheduled_at += Duration::from_secs(100);
    let cases = [
        (
            HistoryEventScheduleAt::At(original_created.scheduled_at),
            None,
            Ok(()),
        ),
        (
            HistoryEventScheduleAt::In(Duration::from_secs(61)),
            None,
            Err(ScheduleRequestError::Conflict),
        ),
        (
            HistoryEventScheduleAt::Now,
            None,
            Err(ScheduleRequestError::Conflict),
        ),
        (
            HistoryEventScheduleAt::At(original_created.scheduled_at + Duration::from_secs(1)),
            None,
            Err(ScheduleRequestError::Conflict),
        ),
        (
            HistoryEventScheduleAt::In(Duration::from_secs(60)),
            Some(FunctionFqn::new_static("test:schedule/activity", "other")),
            Err(ScheduleRequestError::Conflict),
        ),
    ];
    for (schedule_at, ffqn, expected) in cases {
        let mut retry = retry.clone();
        if let Some(ffqn) = ffqn {
            retry.ffqn = ffqn;
        }
        retry.scheduled_at = schedule_at.as_date_time(retry.created_at).unwrap();
        let (next, result) = conn
            .append_schedule(
                parent.clone(),
                version,
                schedule_request(&retry, schedule_at),
                retry,
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(result, expected);
        let event = conn
            .get_execution_event(&parent, &Version::new(next.0 - 1))
            .await
            .unwrap();
        assert!(
            matches!(event.event, ExecutionRequest::HistoryEvent { event: HistoryEvent::Schedule { result, .. } } if result == expected)
        );
        version = next;
    }
    let mut retry = child.clone();
    retry.params = Params::from_json_values_test(vec![serde_json::json!(1)]);
    let (next, result) = conn
        .append_schedule(
            parent.clone(),
            version,
            schedule_request(&retry, HistoryEventScheduleAt::In(Duration::from_secs(60))),
            retry,
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(result, Err(ScheduleRequestError::Conflict));
    version = next;
    let join_set =
        concepts::JoinSetId::new(concepts::JoinSetKind::Named, "derived".into()).unwrap();
    let derived = child.execution_id.next_level(&join_set);
    let mut invalid = child.clone();
    invalid.execution_id = ExecutionId::Derived(derived);
    let (_, result) = conn
        .append_schedule(
            parent,
            version,
            schedule_request(&invalid, HistoryEventScheduleAt::Now),
            invalid.clone(),
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(result, Err(ScheduleRequestError::ExecutionIdMustBeTopLevel));
    assert!(conn.get(&invalid.execution_id).await.is_err());
    assert_eq!(
        original,
        conn.get_execution_event(&child.execution_id, &Version::new(0))
            .await
            .unwrap()
    );
    drop(conn);
    close.close().await;
}
