use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, RunId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, ExecutionEventInner, ExpiredTimer, JoinSetRequest,
    JoinSetResponse, JoinSetResponseEventOuter, LockedExecution, PendingState, Version,
};
use concepts::storage::{DbErrorWrite, DbPoolCloseable};
use concepts::storage::{DbErrorWritePermanent, HistoryEvent};
use concepts::storage::{HistoryEventScheduleAt, JoinSetResponseEvent};
use concepts::time::ClockFn;
use concepts::time::Now;
use concepts::{ClosingStrategy, JoinSetId, SUPPORTED_RETURN_VALUE_OK_EMPTY};
use concepts::{ComponentId, Params, StrVariant};
use concepts::{ExecutionId, prefixed_ulid::ExecutorId};
use db_tests::Database;
use db_tests::SOME_FFQN;
use std::sync::Arc;
use std::time::Duration;
use test_utils::set_up;
use test_utils::sim_clock::SimClock;
use tracing::{debug, info};

#[tokio::test]
async fn test_expired_lock_should_be_found_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    expired_lock_should_be_found(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_expired_lock_should_be_found_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    expired_lock_should_be_found(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_append_batch_respond_to_parent_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    append_batch_respond_to_parent(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_append_batch_respond_to_parent_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    append_batch_respond_to_parent(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_pending_should_sort_by_scheduled_at_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    lock_pending_should_sort_by_scheduled_at(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_pending_should_sort_by_scheduled_at_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    lock_pending_should_sort_by_scheduled_at(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    test_lock(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    test_lock(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_lock_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_lock(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_lock_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_lock(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_delay_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_delay(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_delay_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool, _db_exec, db_close) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_delay(db_connection.as_ref(), sim_clock).await;
    db_close.close().await.unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn append_after_finish_should_not_be_possible(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    let sim_clock = SimClock::default();

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    // Create
    let component_id = ComponentId::dummy_activity();
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
    let version = lock_pending(
        &execution_id,
        sim_clock.now() + lock_expiry,
        exec1,
        &component_id,
        db_connection.as_ref(),
        &sim_clock,
    )
    .await;
    let version = {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Finish execution");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                http_client_traces: None,
            },
            created_at,
        };
        db_connection
            .append(execution_id.clone(), version, req)
            .await
            .unwrap()
    };
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Append after finish should fail");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                http_client_traces: None,
            },
            created_at,
        };
        let err = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap_err();

        let msg = assert_matches!(
            err,
            DbErrorWrite::Permanent(DbErrorWritePermanent::CannotWrite{reason, ..})
            => reason
        );
        assert_eq!("already finished", msg.as_ref());
    }
    db_close.close().await.unwrap();
}

async fn lock_pending(
    execution_id: &ExecutionId,
    lock_expires_at: DateTime<Utc>,
    executor_id: ExecutorId,
    component_id: &ComponentId,
    db_connection: &dyn DbConnection,
    sim_clock: &SimClock,
) -> Version {
    let created_at = sim_clock.now();
    info!(now = %created_at, "LockPending");
    let locked_executions = db_connection
        .lock_pending(
            1,
            created_at,
            Arc::from([SOME_FFQN]),
            created_at,
            component_id.clone(),
            executor_id,
            lock_expires_at,
            RunId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(1, locked_executions.len());
    let locked_execution = locked_executions.into_iter().next().unwrap();
    assert_eq!(*execution_id, locked_execution.execution_id);
    assert_eq!(Version::new(2), locked_execution.next_version);
    assert_eq!(0, locked_execution.params.len());
    assert_eq!(SOME_FFQN, locked_execution.ffqn);
    locked_execution.next_version
}

#[tokio::test]
#[rstest::rstest]
async fn locking_in_unlock_backoff_should_not_be_possible(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    let sim_clock = SimClock::default();

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    // Create
    let component_id = ComponentId::dummy_activity();
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

    let version = lock_pending(
        &execution_id,
        sim_clock.now() + lock_expiry,
        exec1,
        &component_id,
        db_connection.as_ref(),
        &sim_clock,
    )
    .await;

    // The Executor / Worker responds with a Unlock
    let backoff = Duration::from_millis(500);
    let backoff_expires_at = sim_clock.now() + backoff;
    let _version = {
        let created_at = sim_clock.now();
        info!(now = %created_at, "unlock");
        let req = AppendRequest {
            event: ExecutionEventInner::Unlocked {
                backoff_expires_at,
                reason: StrVariant::Static("reason"),
            },
            created_at,
        };
        db_connection
            .append(execution_id.clone(), version, req)
            .await
            .unwrap()
    };

    // Too soon
    sim_clock.move_time_forward(backoff - Duration::from_millis(100));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock while in timeout backoff");
        let locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec1,
                created_at + lock_expiry,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert!(locked_executions.is_empty());
    }

    sim_clock.move_time_to(backoff_expires_at);
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Locking exactly at `backoff_expires_at` should succeed");
        let locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec1,
                created_at + lock_expiry,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
    }
    db_close.close().await.unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn lock_extended_with_the_same_executor_should_work(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    {
        let db_connection = db_pool.connection();
        let lock_resp = lock_and_attept_to_extend(true, true, db_connection.as_ref())
            .await
            .unwrap();
        assert_eq!(Version::new(3), lock_resp.next_version);
    }
    db_close.close().await.unwrap();
}
#[tokio::test]
#[rstest::rstest]
async fn lock_extended_with_another_executor_should_fail(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    {
        let db_connection = db_pool.connection();
        lock_and_attept_to_extend(false, true, db_connection.as_ref())
            .await
            .unwrap_err();
        lock_and_attept_to_extend(true, false, db_connection.as_ref())
            .await
            .unwrap_err();
        lock_and_attept_to_extend(false, false, db_connection.as_ref())
            .await
            .unwrap_err();
    }
    db_close.close().await.unwrap();
}
async fn lock_and_attept_to_extend(
    same_executor_id: bool,
    same_run_id: bool,
    db_connection: &dyn DbConnection,
) -> Result<LockedExecution, DbErrorWrite> {
    let sim_clock = SimClock::epoch();
    let execution_id = ExecutionId::generate();
    let exec_pending = ExecutorId::generate();
    let run_pending = RunId::generate();
    let exec_extend = if same_executor_id {
        exec_pending
    } else {
        ExecutorId::generate()
    };
    let run_extend = if same_run_id {
        run_pending
    } else {
        RunId::generate()
    };

    // Create
    let component_id = ComponentId::dummy_activity();
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

    let lock_expiry = Duration::from_millis(500);

    // Lock using lock_pending
    let version = {
        let created_at = sim_clock.now();
        info!(now = %created_at, "LockPending");
        let locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec_pending,
                created_at + lock_expiry,
                run_pending,
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.into_iter().next().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(Version::new(2), locked_execution.next_version);
        assert_eq!(0, locked_execution.params.len());
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
        locked_execution.next_version
    };

    sim_clock.move_time_forward(lock_expiry); // does not matter, as long as the execution is still in Locked state.
    let created_at = sim_clock.now();
    info!(now = %created_at, "Attempt extend the lock");
    db_connection
        .lock_one(
            created_at,
            component_id.clone(),
            &execution_id,
            run_extend,
            version,
            exec_extend,
            created_at + lock_expiry,
        )
        .await
}

#[tokio::test]
#[rstest::rstest]
async fn locking_in_timeout_backoff_should_not_be_possible(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    let sim_clock = SimClock::default();

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    // Create
    let component_id = ComponentId::dummy_activity();
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
        info!(now = %created_at, "LockPending");
        let locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec1,
                created_at + lock_expiry,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.into_iter().next().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(Version::new(2), locked_execution.next_version);
        assert_eq!(0, locked_execution.params.len());
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
        locked_execution.next_version
    };
    // The Executor / Worker responds with a timeout
    sim_clock.move_time_forward(lock_expiry);
    let backoff = Duration::from_millis(300);
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Temporary timeout");
        let req = AppendRequest {
            created_at,
            event: ExecutionEventInner::TemporarilyTimedOut {
                backoff_expires_at: created_at + backoff,
                http_client_traces: None,
            },
        };

        db_connection
            .append(execution_id.clone(), version, req)
            .await
            .unwrap();
    }
    sim_clock.move_time_forward(backoff - Duration::from_millis(100));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock while in timeout backoff");
        let locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec1,
                created_at + lock_expiry,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert!(locked_executions.is_empty());
    }
    db_close.close().await.unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn lock_pending_while_nothing_is_stored_should_work(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    let sim_clock = SimClock::default();

    let exec1 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    assert!(
        db_connection
            .lock_pending(
                1,
                sim_clock.now(),
                Arc::from([SOME_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_activity(),
                exec1,
                sim_clock.now() + lock_expiry,
                RunId::generate()
            )
            .await
            .unwrap()
            .is_empty()
    );
    db_close.close().await.unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn creating_execution_twice_should_fail(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();
    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    let sim_clock = SimClock::default();

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    assert!(
        db_connection
            .lock_pending(
                1,
                sim_clock.now(),
                Arc::from([SOME_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_activity(),
                exec1,
                sim_clock.now() + lock_expiry,
                RunId::generate()
            )
            .await
            .unwrap()
            .is_empty()
    );
    // Create
    let component_id = ComponentId::dummy_activity();
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

    // Create again should fail
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
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap_err();
    db_close.close().await.unwrap();
}

#[tokio::test]
#[rstest::rstest]
async fn lock_pending_while_expired_lock_should_return_nothing(
    #[values(Database::Sqlite, Database::Memory)] database: Database,
) {
    set_up();

    let (_guard, db_pool, _db_exec, db_close) = database.set_up().await;
    let db_connection = db_pool.connection();
    lock_pending_while_expired_lock_should_return_nothing_inner(db_connection.as_ref()).await;
    db_close.close().await.unwrap();
}

async fn lock_pending_while_expired_lock_should_return_nothing_inner(
    db_connection: &dyn DbConnection,
) {
    const LOCK_EXPIRY: Duration = Duration::from_millis(500);
    let sim_clock = SimClock::default();
    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();

    // Create
    let component_id = ComponentId::dummy_activity();
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
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "LockPending");
        let mut locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                component_id.clone(),
                exec1,
                created_at + LOCK_EXPIRY,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.pop().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(Version::new(2), locked_execution.next_version);
        assert_eq!(0, locked_execution.params.len());
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
    }
    // 0 = Before lock expiry
    // 1 = Right at lock expiry
    // 2 = Some time after lock expiry
    for _ in 0..3 {
        assert!(
            db_connection
                .lock_pending(
                    1,
                    sim_clock.now(),
                    Arc::from([SOME_FFQN]),
                    sim_clock.now(),
                    ComponentId::dummy_activity(),
                    exec1,
                    sim_clock.now() + LOCK_EXPIRY,
                    RunId::generate()
                )
                .await
                .unwrap()
                .is_empty()
        );
        sim_clock.move_time_forward(LOCK_EXPIRY);
    }
}

pub async fn expired_lock_should_be_found(db_connection: &dyn DbConnection, sim_clock: SimClock) {
    const MAX_RETRIES: u32 = 1;
    const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(100);

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    // Create
    {
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: execution_id.clone(),
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: RETRY_EXP_BACKOFF,
                max_retries: MAX_RETRIES,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
    }
    // Lock pending
    let lock_duration = Duration::from_millis(500);
    {
        let mut locked_executions = db_connection
            .lock_pending(
                1,
                sim_clock.now(),
                Arc::from([SOME_FFQN]),
                sim_clock.now(),
                ComponentId::dummy_activity(),
                exec1,
                sim_clock.now() + lock_duration,
                RunId::generate(),
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.pop().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
        assert_eq!(Version::new(2), locked_execution.next_version);
    }
    // Calling `get_expired_timers` after lock expiry should return the expired execution.
    sim_clock.move_time_forward(lock_duration);
    {
        let expired_at = sim_clock.now();
        let expired = db_connection.get_expired_timers(expired_at).await.unwrap();
        assert_eq!(1, expired.len());
        let expired = &expired[0];
        let (
            found_execution_id,
            locked_at_version,
            next_version,
            already_retried_count,
            max_retries,
            retry_exp_backoff,
            parent,
        ) = assert_matches!(expired,
            ExpiredTimer::Lock { execution_id, locked_at_version, next_version, intermittent_event_count, max_retries, retry_exp_backoff, parent } =>
            (execution_id, locked_at_version, next_version, intermittent_event_count, max_retries, retry_exp_backoff, parent));
        assert_eq!(execution_id, *found_execution_id);
        assert_eq!(Version::new(1), *locked_at_version);
        assert_eq!(Version::new(2), *next_version);
        assert_eq!(0, *already_retried_count);
        assert_eq!(MAX_RETRIES, *max_retries);
        assert_eq!(RETRY_EXP_BACKOFF, *retry_exp_backoff);
        assert_eq!(None, *parent);
    }
}

pub async fn append_batch_respond_to_parent(db_connection: &dyn DbConnection, sim_clock: SimClock) {
    let parent_id = ExecutionId::generate();

    // Create parent
    let version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id: parent_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();
    // Create joinset
    let join_set_id = JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
    let mut version = db_connection
        .append(
            parent_id.clone(),
            version,
            AppendRequest {
                created_at: sim_clock.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetCreate {
                        join_set_id: join_set_id.clone(),
                        closing_strategy: ClosingStrategy::Complete,
                    },
                },
            },
        )
        .await
        .unwrap();

    let child_a = {
        let parent_log = db_connection.get(&parent_id).await.unwrap();
        assert_matches!(
            parent_log.pending_state,
            PendingState::PendingAt {
                scheduled_at
            } if scheduled_at == sim_clock.now()
        );
        // Create child 1
        let child_id = parent_id.next_level(&join_set_id);
        let child_version = db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: ExecutionId::Derived(child_id.clone()),
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        assert_eq!(Version(1), child_version);
        // Append JoinNext before receiving the response should make the parent BlockedByJoinSet
        db_connection
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinNext {
                            join_set_id: join_set_id.clone(),
                            run_expires_at: sim_clock.now(),
                            closing: false,
                            requested_ffqn: Some(SOME_FFQN),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let parent_exe = db_connection.get(&parent_id).await.unwrap();
        assert_matches!(
            parent_exe.pending_state,
            PendingState::BlockedByJoinSet { join_set_id: found_join_set_id, lock_expires_at, closing: false }
            if found_join_set_id == join_set_id && lock_expires_at == sim_clock.now()
        );

        // Append child response to unblock the parent
        db_connection
            .append_batch_respond_to_parent(
                child_id.clone(),
                sim_clock.now(),
                vec![AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::Finished {
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        http_client_traces: None,
                    },
                }],
                child_version.clone(),
                parent_id.clone(),
                JoinSetResponseEventOuter {
                    created_at: sim_clock.now(),
                    event: JoinSetResponseEvent {
                        join_set_id: join_set_id.clone(),
                        event: JoinSetResponse::ChildExecutionFinished {
                            child_execution_id: child_id.clone(),
                            finished_version: child_version, // will remain at 1.
                            result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        },
                    },
                },
            )
            .await
            .unwrap();

        let parent_exe = db_connection.get(&parent_id).await.unwrap();
        assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });
        version = parent_exe.next_version;

        child_id
    };
    let child_b = {
        // Create child 2
        let child_id = child_a.get_incremented();
        let child_version = db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: ExecutionId::Derived(child_id.clone()),
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();
        let child_resp = vec![AppendRequest {
            created_at: sim_clock.now(),
            event: ExecutionEventInner::Finished {
                result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                http_client_traces: None,
            },
        }];

        db_connection
            .append_batch_respond_to_parent(
                child_id.clone(),
                sim_clock.now(),
                child_resp,
                child_version.clone(),
                parent_id.clone(),
                JoinSetResponseEventOuter {
                    created_at: sim_clock.now(),
                    event: JoinSetResponseEvent {
                        join_set_id: join_set_id.clone(),
                        event: JoinSetResponse::ChildExecutionFinished {
                            child_execution_id: child_id.clone(),
                            finished_version: child_version,
                            result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        },
                    },
                },
            )
            .await
            .unwrap();

        // Append JoinNext after receiving the response should make the parent PendingAt
        db_connection
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinNext {
                            join_set_id: join_set_id.clone(),
                            run_expires_at: sim_clock.now(),
                            closing: false,
                            requested_ffqn: Some(SOME_FFQN),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let parent_exe = db_connection.get(&parent_id).await.unwrap();
        assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });

        child_id
    };
    let parent_exe = db_connection.get(&parent_id).await.unwrap();
    assert_eq!(2, parent_exe.responses.len());
    assert_eq!(
        *parent_exe.responses.first().unwrap(),
        JoinSetResponseEventOuter {
            created_at: sim_clock.now(),
            event: JoinSetResponseEvent {
                join_set_id: join_set_id.clone(),
                event: JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_a,
                    finished_version: Version(1),
                    result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                },
            }
        }
    );
    assert_eq!(
        *parent_exe.responses.get(1).unwrap(),
        JoinSetResponseEventOuter {
            created_at: sim_clock.now(),
            event: JoinSetResponseEvent {
                join_set_id: join_set_id.clone(),
                event: JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_b,
                    finished_version: Version(1),
                    result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                },
            }
        }
    );
    assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });
}

pub async fn lock_pending_should_sort_by_scheduled_at(
    db_connection: &dyn DbConnection,
    sim_clock: SimClock,
) {
    let created_at = sim_clock.now();
    let older_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: older_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(1));
    let newer_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newer_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(999));
    let newest_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newest_id.clone(),
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();

    let locked_ids = db_connection
        .lock_pending(
            3,
            sim_clock.now(),
            Arc::from([SOME_FFQN]),
            sim_clock.now(),
            ComponentId::dummy_activity(),
            ExecutorId::generate(),
            sim_clock.now() + Duration::from_secs(1),
            RunId::generate(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|locked| locked.execution_id)
        .collect::<Vec<_>>();

    assert_eq!(vec![older_id, newer_id, newest_id], locked_ids);
}

pub async fn test_lock(db_connection: &dyn DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
    let version = db_connection
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
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();
    // Append an event that does not change Pending state but must update the version.
    {
        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        db_connection
            .append(
                execution_id.clone(),
                version,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinSetRequest {
                            request: JoinSetRequest::DelayRequest {
                                delay_id: DelayId::new(&execution_id, &join_set_id),
                                expires_at: sim_clock.now(),
                                schedule_at: HistoryEventScheduleAt::Now,
                            },
                            join_set_id,
                        },
                    },
                },
            )
            .await
            .unwrap()
    };

    lock(
        db_connection,
        &execution_id,
        &sim_clock,
        executor_id,
        sim_clock.now() + Duration::from_millis(100), // lock expires at
    )
    .await;
}

async fn lock(
    db_connection: &dyn DbConnection,
    execution_id: &ExecutionId,
    sim_clock: &SimClock,
    executor_id: ExecutorId,
    lock_expires_at: DateTime<Utc>,
) -> Version {
    let lock_pending_res = db_connection
        .lock_pending(
            1,
            sim_clock.now(), // pending at or sooner
            Arc::from([SOME_FFQN]),
            sim_clock.now(), // created at
            ComponentId::dummy_activity(),
            executor_id,
            lock_expires_at,
            RunId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(1, lock_pending_res.len());
    let lock_pending_res = lock_pending_res.into_iter().next().unwrap();
    assert_eq!(*execution_id, lock_pending_res.execution_id);

    lock_pending_res.next_version
}

pub async fn get_expired_lock(db_connection: &dyn DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
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
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);

    let version = lock(
        db_connection,
        &execution_id,
        &sim_clock,
        executor_id,
        sim_clock.now() + lock_expiry,
    )
    .await;

    assert!(
        db_connection
            .get_expired_timers(sim_clock.now())
            .await
            .unwrap()
            .is_empty()
    );

    sim_clock.move_time_forward(lock_expiry);

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::Lock {
        execution_id,
        locked_at_version: Version::new(1),
        next_version: version,
        intermittent_event_count: 0,
        max_retries: 0,
        retry_exp_backoff: Duration::ZERO,
        parent: None,
    };
    assert_eq!(expected, actual);
}

pub async fn get_expired_delay(db_connection: &dyn DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
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
            component_id: ComponentId::dummy_activity(),
            scheduled_by: None,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);
    let version = lock(
        db_connection,
        &execution_id,
        &sim_clock,
        executor_id,
        sim_clock.now() + lock_expiry * 2, // lock expires at
    )
    .await;

    // Create joinset
    let join_set_id = JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
    let version = db_connection
        .append(
            execution_id.clone(),
            version,
            AppendRequest {
                created_at: sim_clock.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetCreate {
                        join_set_id: join_set_id.clone(),
                        closing_strategy: ClosingStrategy::Complete,
                    },
                },
            },
        )
        .await
        .unwrap();

    let delay_id = DelayId::new(&execution_id, &join_set_id);
    db_connection
        .append(
            execution_id.clone(),
            version,
            AppendRequest {
                created_at: Now.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: join_set_id.clone(),
                        request: JoinSetRequest::DelayRequest {
                            delay_id: delay_id.clone(),
                            expires_at: sim_clock.now() + lock_expiry,
                            schedule_at: HistoryEventScheduleAt::In(lock_expiry),
                        },
                    },
                },
            },
        )
        .await
        .unwrap();

    assert!(
        db_connection
            .get_expired_timers(sim_clock.now())
            .await
            .unwrap()
            .is_empty()
    );

    sim_clock.move_time_forward(lock_expiry);

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::Delay {
        execution_id: execution_id.clone(),
        join_set_id,
        delay_id,
    };
    assert_eq!(expected, actual);
}
