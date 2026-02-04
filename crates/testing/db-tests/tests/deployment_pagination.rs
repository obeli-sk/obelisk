//! Tests for deployment pagination via `list_deployment_states`.
//!
//! Creates multiple deployments with executions and verifies pagination works correctly.

use concepts::prefixed_ulid::ExecutorId;
use concepts::prefixed_ulid::{DeploymentId, RunId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbConnectionTest, DbPoolCloseable, DeploymentState,
    ExecutionRequest, Pagination, Version,
};
use concepts::time::ClockFn;
use concepts::{
    ComponentId, ComponentRetryConfig, ExecutionId, Params, SUPPORTED_RETURN_VALUE_OK_EMPTY,
};
use obeli_db_tests::{Database, SOME_FFQN};
use rstest::rstest;
use std::time::Duration;
use test_db_macro::expand_enum_database;
use test_utils::set_up;
use test_utils::sim_clock::SimClock;
use tracing::debug;

const DEPLOYMENT_COUNT: usize = 10;

/// Helper to create a deployment with an execution.
async fn create_deployment_with_execution(
    db_connection: &dyn DbConnection,
    deployment_id: DeploymentId,
    sim_clock: &SimClock,
) -> ExecutionId {
    let execution_id = ExecutionId::generate();
    let component_id = ComponentId::dummy_activity();

    db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id: execution_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            component_id,
            deployment_id,
            scheduled_by: None,
        })
        .await
        .unwrap();

    execution_id
}

/// Helper to lock an execution.
async fn lock_execution(
    db_connection: &dyn DbConnectionTest,
    execution_id: &ExecutionId,
    deployment_id: DeploymentId,
    sim_clock: &SimClock,
) -> Version {
    let component_id = ComponentId::dummy_activity();
    let executor_id = ExecutorId::generate();
    let run_id = RunId::generate();
    let lock_expiry = Duration::from_secs(60);

    let locked = db_connection
        .lock_one(
            sim_clock.now(),
            component_id,
            deployment_id,
            execution_id,
            run_id,
            Version::new(1),
            executor_id,
            sim_clock.now() + lock_expiry,
            ComponentRetryConfig {
                retry_exp_backoff: Duration::ZERO,
                max_retries: Some(0),
            },
        )
        .await
        .unwrap();

    assert_eq!(*execution_id, locked.execution_id);
    locked.next_version
}

/// Helper to finish an execution.
async fn finish_execution(
    db_connection: &dyn DbConnection,
    execution_id: &ExecutionId,
    version: Version,
    sim_clock: &SimClock,
) {
    db_connection
        .append(
            execution_id.clone(),
            version,
            AppendRequest {
                created_at: sim_clock.now(),
                event: ExecutionRequest::Finished {
                    result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                    http_client_traces: None,
                },
            },
        )
        .await
        .unwrap();
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_basic(database: Database) {
    if database == Database::Memory {
        // external_api_conn not implemented for in-memory DB
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create 10 deployments, each with one execution
    let mut deployment_ids: Vec<DeploymentId> = Vec::with_capacity(DEPLOYMENT_COUNT);
    for _ in 0..DEPLOYMENT_COUNT {
        let deployment_id = DeploymentId::generate();
        deployment_ids.push(deployment_id);
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Sort deployment_ids for comparison (deployments are returned sorted by ID)
    deployment_ids.sort_by_key(std::string::ToString::to_string);

    // List all deployments (OlderThan with no cursor = from newest to oldest)
    let pagination: Pagination<Option<DeploymentId>> = Pagination::OlderThan {
        length: 20,
        cursor: None,
        including_cursor: false,
    };
    let deployments = api_conn
        .list_deployment_states(sim_clock.now(), pagination)
        .await
        .unwrap();

    assert_eq!(DEPLOYMENT_COUNT, deployments.len());

    // Each deployment should have 1 pending execution
    for deployment in &deployments {
        assert_eq!(
            1, deployment.pending,
            "deployment {:?}",
            deployment.deployment_id
        );
        assert_eq!(0, deployment.locked);
        assert_eq!(0, deployment.scheduled);
        assert_eq!(0, deployment.blocked);
        assert_eq!(0, deployment.finished);
    }

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_pagination_older_than(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create 10 deployments
    let mut deployment_ids: Vec<DeploymentId> = Vec::with_capacity(DEPLOYMENT_COUNT);
    for _ in 0..DEPLOYMENT_COUNT {
        let deployment_id = DeploymentId::generate();
        deployment_ids.push(deployment_id);
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Sort deployment_ids descending (OlderThan returns in descending order)
    deployment_ids.sort_by_key(|b| std::cmp::Reverse(b.to_string()));

    // Page 1: Get first 3 deployments (oldest first in descending order = newest)
    let page1 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 3,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page1.len());
    debug!(
        "Page 1: {:?}",
        page1.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 2: Get next 3 deployments older than the last one from page 1
    let cursor = Some(page1.last().unwrap().deployment_id);
    let page2 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page2.len());
    debug!(
        "Page 2: {:?}",
        page2.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 3: Get next 3 deployments
    let cursor = Some(page2.last().unwrap().deployment_id);
    let page3 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page3.len());
    debug!(
        "Page 3: {:?}",
        page3.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 4: Get remaining deployment(s)
    let cursor = Some(page3.last().unwrap().deployment_id);
    let page4 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(1, page4.len());
    debug!(
        "Page 4: {:?}",
        page4.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Verify no duplicates across pages
    let all_ids: Vec<DeploymentId> = page1
        .iter()
        .chain(page2.iter())
        .chain(page3.iter())
        .chain(page4.iter())
        .map(|d| d.deployment_id)
        .collect();
    assert_eq!(DEPLOYMENT_COUNT, all_ids.len());

    let mut unique_ids = all_ids.clone();
    unique_ids.sort_by_key(std::string::ToString::to_string);
    unique_ids.dedup();
    assert_eq!(
        DEPLOYMENT_COUNT,
        unique_ids.len(),
        "Found duplicate deployment IDs across pages"
    );

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_pagination_newer_than(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create 10 deployments
    let mut deployment_ids: Vec<DeploymentId> = Vec::with_capacity(DEPLOYMENT_COUNT);
    for _ in 0..DEPLOYMENT_COUNT {
        let deployment_id = DeploymentId::generate();
        deployment_ids.push(deployment_id);
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Sort deployment_ids ascending (NewerThan returns in ascending order)
    deployment_ids.sort_by_key(std::string::ToString::to_string);

    // First, get all deployments to find the oldest one
    let all = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 20,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    let oldest = all.last().unwrap().deployment_id;

    // Page 1: Get first 3 deployments newer than oldest (including oldest)
    let page1 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 3,
                cursor: Some(oldest),
                including_cursor: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page1.len());
    assert_eq!(oldest, page1.first().unwrap().deployment_id);
    debug!(
        "Page 1: {:?}",
        page1.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 2: Get next 3 deployments newer than the last one from page 1
    let cursor = Some(page1.last().unwrap().deployment_id);
    let page2 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page2.len());
    debug!(
        "Page 2: {:?}",
        page2.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 3: Get next 3 deployments
    let cursor = Some(page2.last().unwrap().deployment_id);
    let page3 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(3, page3.len());
    debug!(
        "Page 3: {:?}",
        page3.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Page 4: Get remaining deployment(s)
    let cursor = Some(page3.last().unwrap().deployment_id);
    let page4 = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 3,
                cursor,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(1, page4.len());
    debug!(
        "Page 4: {:?}",
        page4.iter().map(|d| &d.deployment_id).collect::<Vec<_>>()
    );

    // Verify no duplicates across pages
    let all_ids: Vec<DeploymentId> = page1
        .iter()
        .chain(page2.iter())
        .chain(page3.iter())
        .chain(page4.iter())
        .map(|d| d.deployment_id)
        .collect();
    assert_eq!(DEPLOYMENT_COUNT, all_ids.len());

    let mut unique_ids = all_ids.clone();
    unique_ids.sort_by_key(std::string::ToString::to_string);
    unique_ids.dedup();
    assert_eq!(
        DEPLOYMENT_COUNT,
        unique_ids.len(),
        "Found duplicate deployment IDs across pages"
    );

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_with_different_execution_states(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection_test().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create deployments with various execution states
    let deployment_pending = DeploymentId::generate();
    let deployment_locked = DeploymentId::generate();
    let deployment_finished = DeploymentId::generate();
    let deployment_mixed = DeploymentId::generate();

    // Deployment with pending execution
    create_deployment_with_execution(db_connection.as_ref(), deployment_pending, &sim_clock).await;

    // Deployment with locked execution
    let exec_locked =
        create_deployment_with_execution(db_connection.as_ref(), deployment_locked, &sim_clock)
            .await;
    lock_execution(
        db_connection.as_ref(),
        &exec_locked,
        deployment_locked,
        &sim_clock,
    )
    .await;

    // Deployment with finished execution
    let exec_finished =
        create_deployment_with_execution(db_connection.as_ref(), deployment_finished, &sim_clock)
            .await;
    let version = lock_execution(
        db_connection.as_ref(),
        &exec_finished,
        deployment_finished,
        &sim_clock,
    )
    .await;
    finish_execution(db_connection.as_ref(), &exec_finished, version, &sim_clock).await;

    // Deployment with mixed states: 1 pending, 1 locked, 1 finished
    let _exec_mixed_pending =
        create_deployment_with_execution(db_connection.as_ref(), deployment_mixed, &sim_clock)
            .await;
    let exec_mixed_locked =
        create_deployment_with_execution(db_connection.as_ref(), deployment_mixed, &sim_clock)
            .await;
    let exec_mixed_finished =
        create_deployment_with_execution(db_connection.as_ref(), deployment_mixed, &sim_clock)
            .await;

    lock_execution(
        db_connection.as_ref(),
        &exec_mixed_locked,
        deployment_mixed,
        &sim_clock,
    )
    .await;

    let version = lock_execution(
        db_connection.as_ref(),
        &exec_mixed_finished,
        deployment_mixed,
        &sim_clock,
    )
    .await;
    finish_execution(
        db_connection.as_ref(),
        &exec_mixed_finished,
        version,
        &sim_clock,
    )
    .await;

    // List all deployments
    let deployments = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 20,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(4, deployments.len());

    // Find each deployment and verify its state
    let find_deployment = |id: DeploymentId| -> &DeploymentState {
        deployments.iter().find(|d| d.deployment_id == id).unwrap()
    };

    let pending_state = find_deployment(deployment_pending);
    assert_eq!(1, pending_state.pending);
    assert_eq!(0, pending_state.locked);
    assert_eq!(0, pending_state.finished);

    let locked_state = find_deployment(deployment_locked);
    assert_eq!(0, locked_state.pending);
    assert_eq!(1, locked_state.locked);
    assert_eq!(0, locked_state.finished);

    let finished_state = find_deployment(deployment_finished);
    assert_eq!(0, finished_state.pending);
    assert_eq!(0, finished_state.locked);
    assert_eq!(1, finished_state.finished);

    let mixed_state = find_deployment(deployment_mixed);
    assert_eq!(
        1, mixed_state.pending,
        "expected 1 pending in mixed deployment"
    );
    assert_eq!(
        1, mixed_state.locked,
        "expected 1 locked in mixed deployment"
    );
    assert_eq!(
        1, mixed_state.finished,
        "expected 1 finished in mixed deployment"
    );

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_including_cursor(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create 5 deployments
    let mut deployment_ids: Vec<DeploymentId> = Vec::with_capacity(5);
    for _ in 0..5 {
        let deployment_id = DeploymentId::generate();
        deployment_ids.push(deployment_id);
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Get all to find a middle deployment
    let all = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 10,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    let middle = all[2].deployment_id;

    // OlderThan including cursor should include the cursor deployment
    let with_cursor = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 10,
                cursor: Some(middle),
                including_cursor: true,
            },
        )
        .await
        .unwrap();
    assert!(
        with_cursor.iter().any(|d| d.deployment_id == middle),
        "including_cursor=true should include cursor"
    );

    // OlderThan excluding cursor should not include the cursor deployment
    let without_cursor = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 10,
                cursor: Some(middle),
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert!(
        !without_cursor.iter().any(|d| d.deployment_id == middle),
        "including_cursor=false should not include cursor"
    );

    // NewerThan including cursor should include the cursor deployment
    let with_cursor_newer = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 10,
                cursor: Some(middle),
                including_cursor: true,
            },
        )
        .await
        .unwrap();
    assert!(
        with_cursor_newer.iter().any(|d| d.deployment_id == middle),
        "including_cursor=true should include cursor (NewerThan)"
    );

    // NewerThan excluding cursor should not include the cursor deployment
    let without_cursor_newer = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 10,
                cursor: Some(middle),
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert!(
        !without_cursor_newer
            .iter()
            .any(|d| d.deployment_id == middle),
        "including_cursor=false should not include cursor (NewerThan)"
    );

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_older_then_newer_returns_all(database: Database) {
    // Simulates: user sees < 20 deployments, clicks Older (sees nothing),
    // then clicks Newer - should see all deployments again
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create 5 deployments (less than page size of 20)
    let deployment_count = 5;
    for _ in 0..deployment_count {
        let deployment_id = DeploymentId::generate();
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Initial fetch: OlderThan with no cursor - should get all 5
    let initial = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 20,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        deployment_count,
        initial.len(),
        "Initial fetch should return all deployments"
    );

    // Collect all deployment IDs from initial fetch for comparison
    let initial_ids: Vec<DeploymentId> = initial.iter().map(|d| d.deployment_id).collect();
    debug!(
        "Initial deployment IDs (newest to oldest): {:?}",
        initial_ids
    );

    // The oldest deployment is the last one in the list (OlderThan returns newest first)
    let oldest = initial.last().unwrap().deployment_id;
    let newest = initial.first().unwrap().deployment_id;
    debug!("Oldest deployment: {}", oldest);
    debug!("Newest deployment: {}", newest);

    // Click "Older": OlderThan with cursor = oldest, excluding cursor
    // Should return nothing since we're already at the oldest
    let older_page = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 20,
                cursor: Some(oldest),
                including_cursor: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        0,
        older_page.len(),
        "OlderThan from oldest should return nothing"
    );

    // Click "Newer": NewerThan with cursor = oldest, including cursor
    // Should return all deployments (oldest + all newer ones)
    let newer_page = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::NewerThan {
                length: 20,
                cursor: Some(oldest),
                including_cursor: true,
            },
        )
        .await
        .unwrap();
    let newer_ids: Vec<DeploymentId> = newer_page.iter().map(|d| d.deployment_id).collect();
    debug!("NewerThan deployment IDs: {:?}", newer_ids);
    assert_eq!(
        deployment_count,
        newer_page.len(),
        "NewerThan from oldest (including) should return all {} deployments, got {}. IDs: {:?}",
        deployment_count,
        newer_page.len(),
        newer_ids
    );

    // Verify that the first item in newer_page is the oldest (cursor)
    assert_eq!(
        oldest,
        newer_page.first().unwrap().deployment_id,
        "NewerThan should return cursor first when including_cursor=true"
    );

    // Verify all original deployments are present
    let newer_id_set: std::collections::HashSet<DeploymentId> = newer_ids.iter().copied().collect();
    for id in &initial_ids {
        assert!(
            newer_id_set.contains(id),
            "Deployment {id} from initial fetch should be in NewerThan result"
        );
    }

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_empty(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // List deployments when none exist
    let deployments = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 10,
                cursor: None,
                including_cursor: false,
            },
        )
        .await
        .unwrap();

    assert!(deployments.is_empty());

    drop(api_conn);
    db_close.close().await;
}

#[expand_enum_database]
#[rstest]
#[tokio::test]
async fn list_deployment_states_cursor_not_found(database: Database) {
    if database == Database::Memory {
        return;
    }
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, db_close) = database.set_up().await;
    let db_connection = db_pool.connection().await.unwrap();
    let api_conn = db_pool.external_api_conn().await.unwrap();

    // Create some deployments
    for _ in 0..3 {
        let deployment_id = DeploymentId::generate();
        create_deployment_with_execution(db_connection.as_ref(), deployment_id, &sim_clock).await;
    }

    // Use a non-existent deployment ID as cursor
    let fake_cursor = DeploymentId::generate();

    // Should still work - returns deployments older/newer than the fake cursor
    // (since cursor is compared by string/ULID ordering, not by existence)
    let result = api_conn
        .list_deployment_states(
            sim_clock.now(),
            Pagination::OlderThan {
                length: 10,
                cursor: Some(fake_cursor),
                including_cursor: false,
            },
        )
        .await
        .unwrap();

    // The result depends on ULID ordering - a newly generated ULID is likely
    // newer than existing ones, so OlderThan should return some/all existing deployments
    debug!("Deployments older than fake cursor: {}", result.len());

    drop(api_conn);
    drop(db_connection);
    db_close.close().await;
}
