use chrono::Duration;
use concepts::{
    ComponentId, ExecutionId, JoinSetId, JoinSetKind, Params, SUPPORTED_RETURN_VALUE_OK_EMPTY,
    StrVariant,
    prefixed_ulid::DeploymentId,
    storage::{
        AppendRequest, CreateRequest, DbPoolCloseable, DeleteDeploymentResult,
        DeleteExecutionTreeResult, DeploymentFileRecord, DeploymentRecord, DeploymentStatus,
        ExecutionRequest,
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
        last_active_at: None,
        status: DeploymentStatus::Inactive,
        deployment_toml: "{}".into(),
        obelisk_version: "test".into(),
        created_by: None,
        files,
    }
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
        .activate_deployment(active_deployment, clock.now())
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
    let result = admin.retain_executions(1, 1, false).await.unwrap();
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
    let result = admin.retain_executions(1, 1, false).await.unwrap();
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
        .gc_cas(true, 10)
        .await
        .unwrap();
    assert_eq!(dry_run.referenced_blobs, 1);
    assert_eq!(dry_run.orphan_blobs, 2);
    assert_eq!(dry_run.deleted_blobs, 0);
    assert!(cas.contains_blob(&orphan_digest).await.unwrap());
    let collected = db_pool
        .cas_gc_conn()
        .await
        .unwrap()
        .gc_cas(false, 1)
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, 1);
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
        .gc_cas(false, 1)
        .await
        .unwrap();
    assert_eq!(collected.deleted_blobs, 1);
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
    let first = admin.retain_deployments(0, 1, false, false).await.unwrap();
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

    let second = admin.retain_deployments(0, 1, false, false).await.unwrap();
    assert_eq!(second.deleted_deployments, 1);
    assert_eq!(second.blocked_by_execution_reference, 1);
    assert!(!second.has_more);
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
