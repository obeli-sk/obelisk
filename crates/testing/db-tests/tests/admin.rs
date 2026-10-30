use chrono::Duration;
use concepts::{
    ComponentId, ExecutionId, JoinSetId, JoinSetKind, Params, SUPPORTED_RETURN_VALUE_OK_EMPTY,
    StrVariant,
    prefixed_ulid::{DeploymentId, NodeRunId, SystemEventId},
    storage::{
        AppendRequest, CAS_GC_BATCH_SIZE_BYTES, CreateRequest, DbPoolCloseable,
        DeleteDeploymentResult, DeleteExecutionTreeResult, DeploymentFileRecord, DeploymentRecord,
        DeploymentStatus, ExecutionRequest, RetentionPolicy, SystemEvent, SystemEventCode,
        SystemEventFilter, SystemEventLevel,
    },
    time::ClockFn,
};
use obeli_db_tests::{Database, SOME_FFQN};
use rstest::rstest;
use test_db_macro::expand_enum_database;
use test_utils::{set_up, sim_clock::SimClock};

fn deployment_record(
    deployment_id: DeploymentId,
    created_at: chrono::DateTime<chrono::Utc>,
    files: Vec<DeploymentFileRecord>,
) -> DeploymentRecord {
    DeploymentRecord {
        deployment_id,
        description: None,
        digest: DeploymentRecord::compute_digest("{}"),
        created_at,
        updated_at: created_at,
        last_active_at: None,
        last_active_app_config_digest: None,
        status: DeploymentStatus::Inactive,
        deployment_toml: "{}".into(),
        obelisk_version: "test".into(),
        created_by: None,
        files,
    }
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn system_events_are_filtered_paginated_and_collected(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let admin = db_pool.admin_conn().await.unwrap();
    let mut first = SystemEvent::new(
        SystemEventCode::AdminExecutionDeleteStarted,
        None,
        None,
        serde_json::json!({"version": "test"}),
    )
    .unwrap();
    first.level = SystemEventLevel::Debug;
    first.node_run_id = "NodeRun_01M281KSH954NC9NHNMCN09S6W".parse().unwrap();
    first.app_config_digest = Some("app-config:v1:sha256:test".into());
    let first_id = first.event_id;
    admin.append_system_event(first).await.unwrap();
    assert_eq!(
        admin
            .get_system_event(first_id)
            .await
            .unwrap()
            .unwrap()
            .event_id,
        first_id
    );
    assert!(
        admin
            .get_system_event(SystemEventId::from_parts(0, 1))
            .await
            .unwrap()
            .is_none()
    );
    let mut second = SystemEvent::new(
        SystemEventCode::MaintenanceGcFailed,
        None,
        None,
        serde_json::json!({"threshold_percent": 90}),
    )
    .unwrap();
    second.node_run_id = "NodeRun_01M281KSH954NC9NHNMCN09S6X".parse().unwrap();
    let second_id = second.event_id;
    admin.append_system_event(second).await.unwrap();

    let warning = admin
        .list_system_events(SystemEventFilter {
            level: Some(SystemEventLevel::Warning),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(warning.len(), 1);
    assert_eq!(warning[0].event_id, second_id);
    let debug = admin
        .list_system_events(SystemEventFilter {
            level: Some(SystemEventLevel::Debug),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(debug.len(), 1);
    assert_eq!(debug[0].event_id, first_id);
    assert_eq!(
        debug[0].app_config_digest.as_deref(),
        Some("app-config:v1:sha256:test")
    );
    let first_run = admin
        .list_system_events(SystemEventFilter {
            node_run_id: Some(
                "NodeRun_01M281KSH954NC9NHNMCN09S6W"
                    .parse::<NodeRunId>()
                    .unwrap(),
            ),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(first_run.len(), 1);
    assert_eq!(first_run[0].event_id, first_id);
    let first_page = admin
        .list_system_events(SystemEventFilter {
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let older = admin
        .list_system_events(SystemEventFilter {
            before_event_id: Some(first_page[0].event_id),
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(older.len(), 1);
    assert_ne!(older[0].event_id, first_page[0].event_id);
    assert!([first_id, second_id].contains(&older[0].event_id));
    assert_eq!(
        admin.get_storage_status().await.unwrap().system_event_count,
        2
    );
    let cutoff = chrono::Utc::now() + Duration::seconds(1);
    let first_run_id = "NodeRun_01M281KSH954NC9NHNMCN09S6W".parse().unwrap();
    let preview = admin
        .retain_system_events(cutoff, 100, first_run_id, true)
        .await
        .unwrap();
    assert_eq!(
        preview.deleted, 1,
        "events of the kept node run must be skipped"
    );
    assert!(!preview.has_more);
    let other_run = NodeRunId::generate();
    for (limit, expected_deleted, expected_has_more) in
        [(1, 1, true), (2, 2, false), (100, 2, false)]
    {
        let preview = admin
            .retain_system_events(cutoff, limit, other_run, true)
            .await
            .unwrap();
        assert_eq!(preview.deleted, expected_deleted);
        assert_eq!(preview.has_more, expected_has_more);
        assert!(admin.get_system_event(first_id).await.unwrap().is_some());
        assert!(admin.get_system_event(second_id).await.unwrap().is_some());
    }
    let preview = admin
        .retain_system_events(chrono::DateTime::UNIX_EPOCH, 1, other_run, true)
        .await
        .unwrap();
    assert_eq!(preview.deleted, 0);
    assert!(!preview.has_more);
    let retention = admin
        .retain_system_events(cutoff, 1, other_run, false)
        .await
        .unwrap();
    assert_eq!(retention.deleted, 1);
    assert!(retention.has_more);
    assert_eq!(
        admin.get_storage_status().await.unwrap().system_event_count,
        1
    );
    let deployment_id = DeploymentId::generate();
    for _ in 0..2 {
        let event = SystemEvent::new(
            SystemEventCode::OutboundHttpDenied,
            None,
            Some(deployment_id),
            serde_json::json!({"host": "example.com"}),
        )
        .unwrap()
        .with_dedupe_key("component|GET|https|example.com|443|GlobalAllowlist");
        admin.append_system_event(event).await.unwrap();
    }
    let denials = admin
        .list_system_events(SystemEventFilter {
            code: Some(SystemEventCode::OutboundHttpDenied.as_str().to_owned()),
            deployment_id: Some(deployment_id),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(denials.len(), 1);
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn system_events_in_same_millisecond_are_sorted_by_created_at(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let admin = db_pool.admin_conn().await.unwrap();
    let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let timestamp_ms = u64::try_from(base.timestamp_millis()).unwrap();
    // Event IDs share the millisecond and have random parts ordered opposite to `created_at`.
    let mut ids = Vec::new();
    for idx in 0..3u8 {
        let mut event = SystemEvent::new(
            SystemEventCode::MaintenanceGcFailed,
            None,
            None,
            serde_json::json!({}),
        )
        .unwrap();
        event.created_at = base + Duration::microseconds(i64::from(idx) * 100);
        event.event_id = SystemEventId::from_parts(timestamp_ms, u128::from(3 - idx));
        ids.push(event.event_id);
        admin.append_system_event(event).await.unwrap();
    }
    ids.reverse();
    let list = |before_event_id, limit| {
        let admin = &admin;
        async move {
            admin
                .list_system_events(SystemEventFilter {
                    before_event_id,
                    limit,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(list(None, 100).await, ids);
    assert_eq!(list(Some(ids[0]), 1).await, ids[1..2]);
    assert_eq!(list(Some(ids[1]), 100).await, ids[2..]);
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn system_events_are_filtered_by_created_at_range(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let admin = db_pool.admin_conn().await.unwrap();
    let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .to_utc();
    let offsets = [
        Duration::zero(),
        Duration::milliseconds(500),
        Duration::seconds(1),
        Duration::milliseconds(1_250),
    ];
    let mut ids = Vec::new();
    for offset in offsets {
        let mut event = SystemEvent::new(
            SystemEventCode::MaintenanceGcFailed,
            None,
            None,
            serde_json::json!({}),
        )
        .unwrap();
        event.created_at = base + offset;
        ids.push(event.event_id);
        admin.append_system_event(event).await.unwrap();
    }
    let list = |created_from, created_to| {
        let admin = &admin;
        async move {
            let mut ids = admin
                .list_system_events(SystemEventFilter {
                    created_from,
                    created_to,
                    limit: 100,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>();
            ids.reverse();
            ids
        }
    };
    assert_eq!(
        list(Some(base + Duration::milliseconds(500)), None).await,
        ids[1..]
    );
    assert_eq!(
        list(None, Some(base + Duration::seconds(1))).await,
        ids[..2]
    );
    assert_eq!(
        list(
            Some(base + Duration::milliseconds(500)),
            Some(base + Duration::milliseconds(1_250))
        )
        .await,
        ids[1..3]
    );
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn system_event_cas_details_are_retained_with_the_event(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let cas = db_pool.cas_conn().await.unwrap();
    let content = br#"{"policy":"large"}"#.to_vec();
    let digest = concepts::cas::content_digest(&content);
    let event = SystemEvent::new(
        SystemEventCode::DeploymentPolicyApplied,
        None,
        Some(DeploymentId::generate()),
        serde_json::json!({"policy_digest": digest.to_string()}),
    )
    .unwrap()
    .with_cas_digest(digest.clone());
    let admin = db_pool.admin_conn().await.unwrap();
    admin
        .append_system_event_with_cas(event, content)
        .await
        .unwrap();

    let gc = db_pool.cas_gc_conn().await.unwrap();
    assert_eq!(
        gc.gc_cas(false, 100, chrono::Utc::now())
            .await
            .unwrap()
            .deleted_blobs,
        0
    );
    assert!(cas.contains_blob(&digest).await.unwrap());

    let cutoff = chrono::Utc::now() + Duration::seconds(1);
    let retention = admin
        .retain_system_events(
            cutoff,
            100,
            concepts::storage::initialize_node_run_id(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(retention.deleted, 0);
    assert_eq!(
        gc.gc_cas(false, 100, chrono::Utc::now())
            .await
            .unwrap()
            .deleted_blobs,
        0
    );
    assert!(cas.contains_blob(&digest).await.unwrap());

    admin
        .retain_system_events(cutoff, 100, NodeRunId::generate(), false)
        .await
        .unwrap();
    assert_eq!(
        gc.gc_cas(false, 100, chrono::Utc::now())
            .await
            .unwrap()
            .deleted_blobs,
        1
    );
    assert!(!cas.contains_blob(&digest).await.unwrap());

    drop((admin, gc, cas));
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn uploaded_blob_survives_gc_until_grace_period_ends(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let cas = db_pool.cas_conn().await.unwrap();
    let gc = db_pool.cas_gc_conn().await.unwrap();
    let uploaded_at = chrono::Utc::now();
    let digest = gc
        .upload_blob(b"uploaded".to_vec(), uploaded_at)
        .await
        .unwrap();

    let within_grace = gc.gc_cas(false, 100, uploaded_at).await.unwrap();
    assert_eq!(within_grace.orphan_blobs, 0);
    assert!(cas.contains_blob(&digest).await.unwrap());

    // Re-uploading refreshes the grace period.
    let reuploaded_at = uploaded_at + Duration::seconds(10);
    gc.upload_blob(b"uploaded".to_vec(), reuploaded_at)
        .await
        .unwrap();
    gc.gc_cas(false, 100, uploaded_at + Duration::seconds(1))
        .await
        .unwrap();
    assert!(cas.contains_blob(&digest).await.unwrap());

    let expired = gc
        .gc_cas(false, 100, reuploaded_at + Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(expired.deleted_blobs, 1);
    assert!(!cas.contains_blob(&digest).await.unwrap());

    drop((gc, cas));
    db_close.close().await;
}

async fn create_execution(
    db_pool: &dyn concepts::storage::DbPool,
    clock: &SimClock,
    deployment_id: DeploymentId,
    finished: bool,
) -> ExecutionId {
    let execution_id = ExecutionId::generate();
    let connection = db_pool.connection().await.unwrap();
    connection
        .create(CreateRequest {
            created_at: clock.now(),
            execution_id: execution_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: clock.now(),
            component_id: ComponentId::dummy_activity(),
            deployment_id,
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: u64::MAX,
        })
        .await
        .unwrap();
    if finished {
        connection
            .append(
                execution_id.clone(),
                concepts::storage::Version::new(1),
                AppendRequest {
                    created_at: clock.now(),
                    event: ExecutionRequest::Finished {
                        retval: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        http_client_traces: None,
                        http_response: None,
                    },
                },
            )
            .await
            .unwrap();
    }
    execution_id
}

async fn insert_deployment(
    db_pool: &dyn concepts::storage::DbPool,
    deployment_id: DeploymentId,
    created_at: chrono::DateTime<chrono::Utc>,
) {
    db_pool
        .external_api_conn()
        .await
        .unwrap()
        .insert_deployment_with_components(
            deployment_record(deployment_id, created_at, Vec::new()),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();
}

async fn collect_execution_garbage(admin: &dyn concepts::storage::DbAdmin) {
    for _ in 0..100 {
        if !admin.gc_executions(10).await.unwrap().has_more {
            return;
        }
    }
    panic!("execution garbage collection did not converge");
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn execution_cleanup_is_terminal_idempotent_and_bounded(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let non_terminal =
        create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), false).await;
    let admin = db_pool.admin_conn().await.unwrap();
    assert_eq!(
        admin
            .delete_execution_tree(&non_terminal, false)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::NonTerminal
    );
    assert_eq!(
        admin
            .delete_execution_tree(&non_terminal, true)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::Deleted
    );
    collect_execution_garbage(admin.as_ref()).await;

    let active_deployment = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), active_deployment, clock.now()).await;
    db_pool
        .external_api_conn()
        .await
        .unwrap()
        .activate_deployment(active_deployment, clock.now(), Some("test-app-config"))
        .await
        .unwrap();
    let active_execution =
        create_execution(db_pool.as_ref(), &clock, active_deployment, false).await;
    assert_eq!(
        admin
            .delete_execution_tree(&active_execution, true)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::ActiveDeployment
    );

    let finished_root =
        create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), true).await;
    let pending_child = ExecutionId::Derived(
        finished_root
            .next_level(&JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap()),
    );
    db_pool
        .connection()
        .await
        .unwrap()
        .create(CreateRequest {
            created_at: clock.now(),
            execution_id: pending_child.clone(),
            ffqn: SOME_FFQN,
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: clock.now(),
            component_id: ComponentId::dummy_activity(),
            deployment_id: DeploymentId::generate(),
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: u64::MAX,
        })
        .await
        .unwrap();
    assert_eq!(
        admin
            .delete_execution_tree(&finished_root, false)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::Deleted
    );
    collect_execution_garbage(admin.as_ref()).await;
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&pending_child)
            .await
            .is_err()
    );

    let older = create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), true).await;
    clock.move_time_forward(Duration::milliseconds(1).to_std().unwrap());
    let middle = create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), true).await;
    clock.move_time_forward(Duration::milliseconds(1).to_std().unwrap());
    let newer = create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), true).await;
    let result = admin
        .retain_executions(RetentionPolicy::Count(1), 1, false, false, None)
        .await
        .unwrap();
    assert_eq!(result.deleted_execution_trees, 1);
    assert!(result.has_more);
    collect_execution_garbage(admin.as_ref()).await;
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&older)
            .await
            .is_err()
    );
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&middle)
            .await
            .is_ok()
    );
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&newer)
            .await
            .is_ok()
    );
    let result = admin
        .retain_executions(RetentionPolicy::Count(1), 1, false, false, None)
        .await
        .unwrap();
    assert_eq!(result.deleted_execution_trees, 1);
    assert!(!result.has_more);
    collect_execution_garbage(admin.as_ref()).await;
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&middle)
            .await
            .is_err()
    );
    assert_eq!(
        admin.delete_execution_tree(&older, false).await.unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_cleanup_and_cas_gc_preserve_references(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let cas = db_pool.cas_conn().await.unwrap();
    let referenced_digest = cas.write_blob(b"referenced").await.unwrap();
    let orphan_digest = cas.write_blob(b"orphan").await.unwrap();
    let second_orphan_digest = cas.write_blob(b"second orphan").await.unwrap();
    let deployment_id = DeploymentId::generate();
    db_pool
        .external_api_conn()
        .await
        .unwrap()
        .insert_deployment_with_components(
            deployment_record(
                deployment_id,
                clock.now(),
                vec![DeploymentFileRecord {
                    path: "file".into(),
                    digest: referenced_digest.clone(),
                    size: 10,
                }],
            ),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();
    let execution_id = create_execution(db_pool.as_ref(), &clock, deployment_id, true).await;
    let admin = db_pool.admin_conn().await.unwrap();
    assert_eq!(
        admin
            .delete_deployment(deployment_id, false, false)
            .await
            .unwrap(),
        DeleteDeploymentResult::Referenced { execution_trees: 1 }
    );

    let dry_run = db_pool
        .cas_gc_conn()
        .await
        .unwrap()
        .gc_cas(true, 10, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(dry_run.orphan_blobs, 2);
    assert_eq!(dry_run.deleted_blobs, 0);
    assert!(cas.contains_blob(&orphan_digest).await.unwrap());
    let collected = db_pool
        .cas_gc_conn()
        .await
        .unwrap()
        .gc_cas(false, 1, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, 1);
    assert!(collected.has_more);
    assert!(cas.contains_blob(&referenced_digest).await.unwrap());
    assert_ne!(
        cas.contains_blob(&orphan_digest).await.unwrap(),
        cas.contains_blob(&second_orphan_digest).await.unwrap(),
        "one CAS GC batch must delete exactly one of the two orphan blobs"
    );
    let collected = db_pool
        .cas_gc_conn()
        .await
        .unwrap()
        .gc_cas(false, 1, chrono::Utc::now())
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, 1);
    assert!(!collected.has_more);
    assert!(!cas.contains_blob(&orphan_digest).await.unwrap());
    assert!(!cas.contains_blob(&second_orphan_digest).await.unwrap());

    assert_eq!(
        admin
            .delete_deployment(deployment_id, true, false)
            .await
            .unwrap(),
        DeleteDeploymentResult::Deleted {
            deleted_execution_trees: 1
        }
    );
    collect_execution_garbage(admin.as_ref()).await;
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&execution_id)
            .await
            .is_err()
    );
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(deployment_id)
            .await
            .unwrap()
            .is_none()
    );

    let stuck_deployment = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), stuck_deployment, clock.now()).await;
    create_execution(db_pool.as_ref(), &clock, stuck_deployment, false).await;
    assert_eq!(
        admin
            .delete_deployment(stuck_deployment, true, false)
            .await
            .unwrap(),
        DeleteDeploymentResult::ReferencedByNonTerminal { execution_trees: 1 }
    );
    assert_eq!(
        admin
            .delete_deployment(stuck_deployment, true, true)
            .await
            .unwrap(),
        DeleteDeploymentResult::Deleted {
            deleted_execution_trees: 1
        }
    );
    drop(admin);
    drop(cas);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn cas_gc_batches_are_bounded_by_bytes(database: Database) {
    set_up();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let cas = db_pool.cas_conn().await.unwrap();
    let blob_size = usize::try_from(CAS_GC_BATCH_SIZE_BYTES / 2 + 1).unwrap();
    let first_digest = cas.write_blob(&vec![1; blob_size]).await.unwrap();
    let second_digest = cas.write_blob(&vec![2; blob_size]).await.unwrap();
    let gc = db_pool.cas_gc_conn().await.unwrap();

    let first = gc.gc_cas(false, 100, chrono::Utc::now()).await.unwrap();
    assert_eq!(first.orphan_blobs, 1);
    assert_eq!(first.deleted_blobs, 1);
    assert_eq!(first.deleted_bytes, blob_size as u64);
    assert!(first.has_more);
    assert_ne!(
        cas.contains_blob(&first_digest).await.unwrap(),
        cas.contains_blob(&second_digest).await.unwrap()
    );

    let second = gc.gc_cas(false, 100, chrono::Utc::now()).await.unwrap();
    assert_eq!(second.deleted_blobs, 1);
    assert_eq!(second.deleted_bytes, blob_size as u64);
    assert!(!second.has_more);

    let oversized_size = usize::try_from(CAS_GC_BATCH_SIZE_BYTES + 1).unwrap();
    let oversized_digest = cas.write_blob(&vec![3; oversized_size]).await.unwrap();
    let oversized = gc.gc_cas(false, 100, chrono::Utc::now()).await.unwrap();
    assert_eq!(oversized.deleted_blobs, 1);
    assert_eq!(oversized.deleted_bytes, oversized_size as u64);
    assert!(!oversized.has_more);
    assert!(!cas.contains_blob(&oversized_digest).await.unwrap());

    drop((gc, cas));
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_preserves_inactive_checkpoint(database: Database) {
    set_up();
    let (_guard, pool, close) = database.set_up().await;
    let conn = pool.external_api_conn().await.unwrap();
    let admin = pool.admin_conn().await.unwrap();
    let now = chrono::Utc::now();
    let last_good = DeploymentId::generate();
    let candidate = DeploymentId::generate();
    let unused = DeploymentId::generate();
    for id in [last_good, candidate, unused] {
        insert_deployment(pool.as_ref(), id, now).await;
    }
    conn.commit_app_checkpoint(
        concepts::storage::AppCheckpoint {
            deployment_id: last_good,
            app_toml: "app_name='test'".into(),
            app_env_prefix: "V1_".into(),
        },
        now,
        "policy".into(),
    )
    .await
    .unwrap();
    conn.activate_deployment(candidate, now, Some("policy"))
        .await
        .unwrap();
    assert_eq!(
        conn.get_deployment(last_good)
            .await
            .unwrap()
            .unwrap()
            .status,
        DeploymentStatus::Inactive
    );
    for policy in [
        RetentionPolicy::Count(0),
        RetentionPolicy::CreatedAtOrAfter(now + Duration::days(1)),
    ] {
        let preview = admin
            .retain_deployments(policy, 10, true, true, true, None)
            .await
            .unwrap();
        assert_eq!(preview.deleted_deployments, 1);
    }
    let result = admin
        .retain_deployments(
            RetentionPolicy::CreatedAtOrAfter(now + Duration::days(1)),
            10,
            true,
            true,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.deleted_deployments, 1);
    assert!(conn.get_deployment(unused).await.unwrap().is_none());
    assert!(conn.get_deployment(last_good).await.unwrap().is_some());
    conn.commit_app_checkpoint(
        concepts::storage::AppCheckpoint {
            deployment_id: candidate,
            app_toml: "app_name='test'".into(),
            app_env_prefix: "V2_".into(),
        },
        now,
        "policy".into(),
    )
    .await
    .unwrap();
    let result = admin
        .retain_deployments(RetentionPolicy::Count(0), 10, true, true, false, None)
        .await
        .unwrap();
    assert_eq!(result.deleted_deployments, 1);
    assert!(conn.get_deployment(last_good).await.unwrap().is_none());
    assert!(conn.get_deployment(candidate).await.unwrap().is_some());
    drop((admin, conn));
    close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_skips_referenced_deployments(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;

    let oldest = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), oldest, clock.now()).await;
    clock.move_time_forward(Duration::milliseconds(1).to_std().unwrap());
    let older = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), older, clock.now()).await;
    clock.move_time_forward(Duration::milliseconds(1).to_std().unwrap());
    let referenced = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), referenced, clock.now()).await;
    create_execution(db_pool.as_ref(), &clock, referenced, true).await;

    let admin = db_pool.admin_conn().await.unwrap();
    let first = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, false, false, false, None)
        .await
        .unwrap();
    assert_eq!(first.deleted_deployments, 1);
    assert_eq!(first.blocked_by_execution_reference, 0);
    assert!(first.has_more);
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(oldest)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(older)
            .await
            .unwrap()
            .is_some()
    );

    let second = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, false, false, false, None)
        .await
        .unwrap();
    assert_eq!(second.deleted_deployments, 1);
    assert_eq!(second.blocked_by_execution_reference, 0);
    assert!(second.has_more);
    let third = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, false, false, false, None)
        .await
        .unwrap();
    assert_eq!(third.deleted_deployments, 0);
    assert_eq!(third.blocked_by_execution_reference, 1);
    assert!(!third.has_more);
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(referenced)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(oldest)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(older)
            .await
            .unwrap()
            .is_none()
    );
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_batches_multiple_execution_trees(
    database: Database,
    #[values(2, 3, 4, 10)] batch_size: u32,
) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let mut deployments = Vec::new();
    let mut executions = Vec::new();
    for _ in 0..3 {
        let deployment = DeploymentId::generate();
        insert_deployment(db_pool.as_ref(), deployment, clock.now()).await;
        deployments.push(deployment);
        for _ in 0..2 {
            executions.push(create_execution(db_pool.as_ref(), &clock, deployment, true).await);
        }
        clock.move_time_forward(Duration::milliseconds(1).to_std().unwrap());
    }
    let admin = db_pool.admin_conn().await.unwrap();
    let preview = admin
        .retain_deployments(
            RetentionPolicy::Count(1),
            batch_size,
            true,
            false,
            true,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        preview.deleted_execution_trees,
        u64::from(batch_size.min(4))
    );
    assert_eq!(
        preview.deleted_deployments,
        if batch_size < 4 { 1 } else { 2 }
    );
    assert_eq!(preview.has_more, batch_size < 4);
    assert_eq!(preview.blocked_non_terminal, 0);
    assert_eq!(preview.blocked_by_execution_reference, 0);
    let conn = db_pool.external_api_conn().await.unwrap();
    for deployment in &deployments {
        assert!(conn.get_deployment(*deployment).await.unwrap().is_some());
    }
    let execution_conn = db_pool.connection().await.unwrap();
    for execution in &executions {
        execution_conn.get(execution).await.unwrap();
    }
    let deleted = admin
        .retain_deployments(
            RetentionPolicy::Count(1),
            batch_size,
            true,
            false,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        deleted.deleted_execution_trees,
        preview.deleted_execution_trees
    );
    assert_eq!(deleted.deleted_deployments, preview.deleted_deployments);
    assert_eq!(deleted.has_more, preview.has_more);
    assert!(conn.get_deployment(deployments[0]).await.unwrap().is_none());
    assert_eq!(
        conn.get_deployment(deployments[1]).await.unwrap().is_some(),
        batch_size < 4
    );
    assert!(conn.get_deployment(deployments[2]).await.unwrap().is_some());
    if batch_size < 4 {
        let remainder = admin
            .retain_deployments(
                RetentionPolicy::Count(1),
                batch_size,
                true,
                false,
                false,
                None,
            )
            .await
            .unwrap();
        assert_eq!(remainder.deleted_execution_trees, u64::from(4 - batch_size));
        assert_eq!(remainder.deleted_deployments, 1);
        assert!(!remainder.has_more);
        assert!(conn.get_deployment(deployments[1]).await.unwrap().is_none());
    }
    drop((admin, conn, execution_conn));
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_skips_blocked_candidates_across_batches(
    database: Database,
    #[values(false, true)] delete_executions: bool,
    #[values(1, 2)] batch_size: u32,
    #[values(false, true)] retain_by_age: bool,
) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let mut candidates = [DeploymentId::generate(), DeploymentId::generate()];
    candidates.sort();
    let [blocked, eligible] = candidates;
    insert_deployment(db_pool.as_ref(), blocked, clock.now()).await;
    insert_deployment(db_pool.as_ref(), eligible, clock.now()).await;
    let mut blocked_executions = Vec::new();
    for _ in 0..3 {
        blocked_executions
            .push(create_execution(db_pool.as_ref(), &clock, blocked, !delete_executions).await);
        if delete_executions {
            create_execution(db_pool.as_ref(), &clock, eligible, true).await;
        }
    }
    clock.move_time_forward(std::time::Duration::from_secs(1));
    let cutoff = clock.now();
    let newest = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), newest, cutoff).await;
    let policy = if retain_by_age {
        RetentionPolicy::CreatedAtOrAfter(cutoff)
    } else {
        RetentionPolicy::Count(1)
    };
    let admin = db_pool.admin_conn().await.unwrap();
    let mut cursor = None;
    let mut deleted_deployments = 0;
    let mut deleted_trees = 0;
    let mut blocked_deployments = 0;
    for batch in 0..5 {
        let preview = admin
            .retain_deployments(policy, batch_size, delete_executions, false, true, cursor)
            .await
            .unwrap();
        let result = admin
            .retain_deployments(policy, batch_size, delete_executions, false, false, cursor)
            .await
            .unwrap();
        assert_eq!(preview, result);
        assert!(result.deleted_execution_trees <= u64::from(batch_size));
        deleted_deployments += result.deleted_deployments;
        deleted_trees += result.deleted_execution_trees;
        blocked_deployments += result.blocked_non_terminal + result.blocked_by_execution_reference;
        if !result.has_more {
            break;
        }
        assert!(batch < 4, "retention must advance past blocked candidates");
        assert!(result.next_deployment_cursor.is_some());
        cursor = result.next_deployment_cursor;
    }
    assert_eq!(deleted_deployments, 1);
    assert_eq!(deleted_trees, if delete_executions { 3 } else { 0 });
    assert_eq!(blocked_deployments, 1);
    let conn = db_pool.external_api_conn().await.unwrap();
    assert!(conn.get_deployment(blocked).await.unwrap().is_some());
    assert!(conn.get_deployment(eligible).await.unwrap().is_none());
    assert!(conn.get_deployment(newest).await.unwrap().is_some());
    let execution_conn = db_pool.connection().await.unwrap();
    for execution in blocked_executions {
        execution_conn.get(&execution).await.unwrap();
    }
    drop((admin, conn, execution_conn));
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_can_force_non_terminal_trees(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let deployment_id = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), deployment_id, clock.now()).await;
    create_execution(db_pool.as_ref(), &clock, deployment_id, false).await;

    let admin = db_pool.admin_conn().await.unwrap();
    let blocked = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, true, false, false, None)
        .await
        .unwrap();
    assert_eq!(blocked.deleted_deployments, 0);
    assert_eq!(blocked.blocked_non_terminal, 1);

    let forced = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, true, true, false, None)
        .await
        .unwrap();
    assert_eq!(forced.deleted_deployments, 1);
    assert_eq!(forced.deleted_execution_trees, 1);
    assert_eq!(forced.blocked_non_terminal, 0);

    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_retention_resumes_after_an_execution_batch(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let deployment_id = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), deployment_id, clock.now()).await;
    let first_execution = create_execution(db_pool.as_ref(), &clock, deployment_id, true).await;
    let second_execution = create_execution(db_pool.as_ref(), &clock, deployment_id, true).await;
    let admin = db_pool.admin_conn().await.unwrap();

    let first = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, true, false, false, None)
        .await
        .unwrap();
    assert_eq!(first.deleted_execution_trees, 1);
    assert_eq!(first.deleted_deployments, 0);
    assert!(first.has_more);
    assert!(
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .get_deployment(deployment_id)
            .await
            .unwrap()
            .is_some()
    );

    let second = admin
        .retain_deployments(RetentionPolicy::Count(0), 1, true, false, false, None)
        .await
        .unwrap();
    assert_eq!(second.deleted_execution_trees, 1);
    assert_eq!(second.deleted_deployments, 1);
    assert!(!second.has_more);
    assert_eq!(
        admin
            .delete_execution_tree(&first_execution, false)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );
    assert_eq!(
        admin
            .delete_execution_tree(&second_execution, false)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );

    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn execution_retention_skips_protected_trees_across_batches(
    database: Database,
    #[values(false, true)] force: bool,
    #[values(1, 3)] batch_size: u32,
    #[values(false, true)] retain_by_age: bool,
) {
    set_up();
    let clock = SimClock::new(chrono::Utc::now());
    let (_guard, db_pool, db_close) = database.set_up().await;
    let active = DeploymentId::generate();
    let inactive = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), active, clock.now()).await;
    insert_deployment(db_pool.as_ref(), inactive, clock.now()).await;
    let conn = db_pool.external_api_conn().await.unwrap();
    conn.activate_deployment(active, clock.now(), Some("test-app-config"))
        .await
        .unwrap();
    let mut protected = Vec::new();
    for _ in 0..2 {
        protected.push(create_execution(db_pool.as_ref(), &clock, active, false).await);
        clock.move_time_forward(std::time::Duration::from_secs(1));
    }
    let eligible = create_execution(db_pool.as_ref(), &clock, inactive, true).await;
    let policy = if retain_by_age {
        RetentionPolicy::CreatedAtOrAfter(clock.now() + Duration::days(1))
    } else {
        RetentionPolicy::Count(0)
    };
    let admin = db_pool.admin_conn().await.unwrap();
    let mut cursor = None;
    let mut deleted = 0;
    let mut blocked = 0;
    for batch in 0..3 {
        let preview = admin
            .retain_executions(policy, batch_size, force, true, cursor)
            .await
            .unwrap();
        let result = admin
            .retain_executions(policy, batch_size, force, false, cursor)
            .await
            .unwrap();
        assert_eq!(preview, result);
        deleted += result.deleted_execution_trees;
        blocked += result.blocked_non_terminal;
        if !result.has_more {
            break;
        }
        assert!(
            batch < 2,
            "execution retention must advance past protected trees"
        );
        assert_ne!(result.next_execution_cursor, cursor);
        cursor = result.next_execution_cursor;
    }
    assert_eq!(deleted, 1);
    assert_eq!(blocked, if force { 2 } else { 0 });
    assert_eq!(
        admin.delete_execution_tree(&eligible, false).await.unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );
    let execution_conn = db_pool.connection().await.unwrap();
    for execution in protected {
        let log = execution_conn.get(&execution).await.unwrap();
        assert!(!log.pending_state.is_finished());
    }
    drop((admin, conn, execution_conn));
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn age_retention_uses_completion_and_inactive_times(database: Database) {
    set_up();
    // State transition timestamps use the database clock. Start near wall-clock time so
    // advancing the simulated API clock also produces a meaningful retention cutoff.
    let clock = SimClock::new(chrono::Utc::now());
    let (_guard, db_pool, db_close) = database.set_up().await;
    let old_deployment = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), old_deployment, clock.now()).await;
    let old_execution = create_execution(db_pool.as_ref(), &clock, old_deployment, true).await;
    let stale_non_terminal =
        create_execution(db_pool.as_ref(), &clock, old_deployment, false).await;
    clock.move_time_forward(Duration::days(31).to_std().unwrap());
    let new_deployment = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), new_deployment, clock.now()).await;
    db_pool
        .external_api_conn()
        .await
        .unwrap()
        .activate_deployment(old_deployment, clock.now(), Some("test-app-config"))
        .await
        .unwrap();
    db_pool
        .external_api_conn()
        .await
        .unwrap()
        .activate_deployment(new_deployment, clock.now(), Some("test-app-config"))
        .await
        .unwrap();
    let cutoff = clock.now() - Duration::days(30);
    let admin = db_pool.admin_conn().await.unwrap();
    let executions = admin
        .retain_executions(
            RetentionPolicy::CreatedAtOrAfter(cutoff),
            10,
            false,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(executions.deleted_execution_trees, 1);
    assert_eq!(
        admin
            .delete_execution_tree(&old_execution, false)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );
    let forced = admin
        .retain_executions(
            RetentionPolicy::CreatedAtOrAfter(cutoff),
            10,
            true,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(forced.deleted_execution_trees, 1);
    assert_eq!(
        admin
            .delete_execution_tree(&stale_non_terminal, true)
            .await
            .unwrap(),
        DeleteExecutionTreeResult::AlreadyDeleted
    );
    let deployments = admin
        .retain_deployments(
            RetentionPolicy::CreatedAtOrAfter(cutoff),
            10,
            true,
            false,
            false,
            None,
        )
        .await
        .unwrap();
    assert_eq!(deployments.deleted_deployments, 0);

    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn deployment_cleanup_owns_mixed_tree_by_root(database: Database) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let root_deployment = DeploymentId::generate();
    let child_deployment = DeploymentId::generate();
    insert_deployment(db_pool.as_ref(), root_deployment, clock.now()).await;
    insert_deployment(db_pool.as_ref(), child_deployment, clock.now()).await;
    let root = create_execution(db_pool.as_ref(), &clock, root_deployment, true).await;
    let child = ExecutionId::Derived(
        root.next_level(&JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap()),
    );
    db_pool
        .connection()
        .await
        .unwrap()
        .create(CreateRequest {
            created_at: clock.now(),
            execution_id: child.clone(),
            ffqn: SOME_FFQN,
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: clock.now(),
            component_id: ComponentId::dummy_activity(),
            deployment_id: child_deployment,
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: u64::MAX,
        })
        .await
        .unwrap();

    let admin = db_pool.admin_conn().await.unwrap();
    assert_eq!(
        admin
            .delete_deployment(child_deployment, false, false)
            .await
            .unwrap(),
        DeleteDeploymentResult::Deleted {
            deleted_execution_trees: 0
        }
    );
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&child)
            .await
            .is_ok()
    );
    assert_eq!(
        admin
            .delete_deployment(root_deployment, true, false)
            .await
            .unwrap(),
        DeleteDeploymentResult::Deleted {
            deleted_execution_trees: 1
        }
    );
    collect_execution_garbage(admin.as_ref()).await;
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&root)
            .await
            .is_err()
    );
    assert!(
        db_pool
            .connection()
            .await
            .unwrap()
            .get(&child)
            .await
            .is_err()
    );
    drop(admin);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn count_retention_handles_ties_and_population_boundaries(
    database: Database,
    #[values(0, 1, 3, 4)] keep: u32,
) {
    set_up();
    let clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let mut executions = Vec::new();
    let mut deployments = Vec::new();
    for _ in 0..3 {
        executions.push(
            create_execution(db_pool.as_ref(), &clock, DeploymentId::generate(), true)
                .await
                .to_string(),
        );
        let deployment = DeploymentId::generate();
        insert_deployment(db_pool.as_ref(), deployment, clock.now()).await;
        deployments.push(deployment.to_string());
    }
    executions.sort();
    deployments.sort();
    let candidates = 3_usize.saturating_sub(keep as usize);
    let admin = db_pool.admin_conn().await.unwrap();
    let mut execution_cursor = None;
    let mut deployment_cursor = None;
    let mut found_executions = Vec::new();
    let mut found_deployments = Vec::new();
    for _ in 0..4 {
        let page = admin
            .retain_executions(
                RetentionPolicy::Count(keep),
                1,
                false,
                true,
                execution_cursor,
            )
            .await
            .unwrap();
        execution_cursor = page.next_execution_cursor;
        if page.deleted_execution_trees > 0 {
            found_executions.push(execution_cursor.unwrap().execution_id.to_string());
        }
        if !page.has_more {
            break;
        }
    }
    for _ in 0..4 {
        let page = admin
            .retain_deployments(
                RetentionPolicy::Count(keep),
                1,
                false,
                false,
                true,
                deployment_cursor,
            )
            .await
            .unwrap();
        deployment_cursor = page.next_deployment_cursor;
        if page.deleted_deployments > 0 {
            found_deployments.push(deployment_cursor.unwrap().deployment_id.to_string());
        }
        if !page.has_more {
            break;
        }
    }
    assert_eq!(found_executions, executions[..candidates]);
    assert_eq!(found_deployments, deployments[..candidates]);
    drop(admin);
    db_close.close().await;
}
