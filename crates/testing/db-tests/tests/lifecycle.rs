use assert_matches::assert_matches;
use concepts::prefixed_ulid::{DelayId, JoinSetId, RunId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, ExpiredTimer,
    JoinSetRequest, JoinSetResponse, JoinSetResponseEventOuter, PendingState, SpecificError,
    Version,
};
use concepts::storage::{DbPool, JoinSetResponseEvent};
use concepts::{prefixed_ulid::ExecutorId, ExecutionId};
use concepts::{storage::HistoryEvent, FinishedExecutionResult};
use concepts::{ConfigId, Params, StrVariant};
use db_tests::Database;
use db_tests::SOME_FFQN;
use std::sync::Arc;
use std::time::Duration;
use test_utils::set_up;
use test_utils::sim_clock::SimClock;
use tracing::{debug, info};
use utils::time::ClockFn;
use utils::time::Now;
use val_json::type_wrapper::TypeWrapper;

#[tokio::test]
async fn test_lifecycle_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    lifecycle(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_lifecycle_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    lifecycle(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_expired_lock_should_be_found_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    expired_lock_should_be_found(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_expired_lock_should_be_found_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    expired_lock_should_be_found(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_append_batch_respond_to_parent_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    append_batch_respond_to_parent(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_append_batch_respond_to_parent_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    append_batch_respond_to_parent(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_pending_should_sort_by_scheduled_at_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    lock_pending_should_sort_by_scheduled_at(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_lock_pending_should_sort_by_scheduled_at_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    lock_pending_should_sort_by_scheduled_at(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_lock_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    lock(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_lock_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    lock(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_lock_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_lock(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_get_expired_lock_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_lock(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[tokio::test]
async fn test_get_expired_delay_mem() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Memory.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_delay(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[cfg(not(madsim))]
#[tokio::test]
async fn test_get_expired_delay_sqlite() {
    set_up();
    let sim_clock = SimClock::default();
    let (_guard, db_pool) = Database::Sqlite.set_up().await;
    let db_connection = db_pool.connection();
    get_expired_delay(&db_connection, sim_clock).await;
    drop(db_connection);
    db_pool.close().await.unwrap();
}

#[expect(clippy::too_many_lines)]
pub async fn lifecycle(db_connection: &impl DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let exec2 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    assert!(db_connection
        .lock_pending(
            1,
            sim_clock.now(),
            Arc::from([SOME_FFQN]),
            sim_clock.now(),
            ConfigId::dummy_activity(),
            exec1,
            sim_clock.now() + lock_expiry,
        )
        .await
        .unwrap()
        .is_empty());

    let mut version;
    // Create
    let config_id = ConfigId::dummy_activity();
    db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: config_id.clone(),
            return_type: None,
            topmost_parent: execution_id,
        })
        .await
        .unwrap();

    // Create again should fail
    db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: execution_id,
        })
        .await
        .unwrap_err();

    // LockPending
    let run_id = {
        let created_at = sim_clock.now();
        info!(now = %created_at, "LockPending");
        let mut locked_executions = db_connection
            .lock_pending(
                1,
                created_at,
                Arc::from([SOME_FFQN]),
                created_at,
                config_id.clone(),
                exec1,
                created_at + lock_expiry,
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.pop().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(Version::new(2), locked_execution.version);
        assert_eq!(0, locked_execution.params.len());
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
        version = locked_execution.version;
        locked_execution.run_id
    };
    sim_clock
        .move_time_forward(Duration::from_millis(499))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Intermittent timeout");
        let req = AppendRequest {
            created_at,
            event: ExecutionEventInner::IntermittentTimeout {
                expires_at: created_at + lock_expiry,
            },
        };

        version = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap();
    }
    sim_clock
        .move_time_forward(lock_expiry - Duration::from_millis(100))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock using exec2");
        let not_yet_pending = db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                RunId::generate(),
                version.clone(),
                exec2,
                created_at + lock_expiry,
            )
            .await
            .unwrap_err();
        assert_eq!(
            DbError::Specific(SpecificError::ValidationFailed(StrVariant::Static(
                "cannot lock, not yet pending"
            ))),
            not_yet_pending
        );
    }
    // TODO: attempt to append an event requiring version without it.

    sim_clock
        .move_time_forward(Duration::from_millis(100))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Lock again using exec1");
        let (event_history, current_version) = db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                run_id,
                version,
                exec1,
                created_at + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(event_history.is_empty());
        version = current_version;
    }
    sim_clock
        .move_time_forward(Duration::from_millis(700))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock using exec2  while in a lock");
        assert!(db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                RunId::generate(),
                version.clone(),
                exec2,
                created_at + lock_expiry,
            )
            .await
            .is_err());
        // Version is not changed
    }

    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Extend lock using exec1");
        let (event_history, current_version) = db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                run_id,
                version,
                exec1,
                created_at + lock_expiry,
            )
            .await
            .unwrap();
        assert!(event_history.is_empty());
        version = current_version;
    }
    sim_clock
        .move_time_forward(Duration::from_millis(200))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Extend lock using exec1 and wrong run id should fail");
        assert!(db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                RunId::generate(),
                version.clone(),
                exec1,
                created_at + lock_expiry,
            )
            .await
            .is_err());
    }
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "persist and unlock");
        let req = AppendRequest {
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::Persist {
                    value: Vec::from("hello".as_bytes()),
                },
            },
            created_at,
        };
        version = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap();
        let req = AppendRequest {
            event: ExecutionEventInner::Unlocked,
            created_at,
        };
        version = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap();
    }
    sim_clock
        .move_time_forward(Duration::from_millis(200))
        .await;
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Lock again");
        let (event_history, current_version) = db_connection
            .lock(
                created_at,
                config_id.clone(),
                execution_id,
                RunId::generate(),
                version,
                exec1,
                created_at + lock_expiry,
            )
            .await
            .unwrap();
        assert_eq!(1, event_history.len());
        let value =
            assert_matches!(event_history.last(), Some(HistoryEvent::Persist { value }) => value );
        assert_eq!(Vec::from("hello".as_bytes()), *value);
        version = current_version;
    }
    sim_clock
        .move_time_forward(Duration::from_millis(300))
        .await;
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Finish execution");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionReturnValue::None),
            },
            created_at,
        };
        version = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap();
    }
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Append after finish should fail");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionReturnValue::None),
            },
            created_at,
        };
        let err = db_connection
            .append(execution_id, version, req)
            .await
            .unwrap_err();

        let msg = assert_matches!(
            err,
            DbError::Specific(SpecificError::ValidationFailed(StrVariant::Static(
                msg
            )))
            => msg
        );
        assert!(
            msg.contains("already finished"),
            "Message `{msg}` must contain text `already finished`"
        );
    }
}

pub async fn expired_lock_should_be_found(db_connection: &impl DbConnection, sim_clock: SimClock) {
    const MAX_RETRIES: u32 = 1;
    const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(100);

    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    // Create
    {
        db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id,
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: RETRY_EXP_BACKOFF,
                max_retries: MAX_RETRIES,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: execution_id,
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
                ConfigId::dummy_activity(),
                exec1,
                sim_clock.now() + lock_duration,
            )
            .await
            .unwrap();
        assert_eq!(1, locked_executions.len());
        let locked_execution = locked_executions.pop().unwrap();
        assert_eq!(execution_id, locked_execution.execution_id);
        assert_eq!(SOME_FFQN, locked_execution.ffqn);
        assert_eq!(Version::new(2), locked_execution.version);
    }
    // Calling `get_expired_timers` after lock expiry should return the expired execution.
    sim_clock.move_time_forward(lock_duration).await;
    {
        let expired_at = sim_clock.now();
        let expired = db_connection.get_expired_timers(expired_at).await.unwrap();
        assert_eq!(1, expired.len());
        let expired = &expired[0];
        let (
            found_execution_id,
            version,
            already_retried_count,
            max_retries,
            retry_exp_backoff,
            parent,
        ) = assert_matches!(expired,
            ExpiredTimer::Lock { execution_id, version, intermittent_event_count, max_retries, retry_exp_backoff, parent } =>
            (execution_id, version, intermittent_event_count, max_retries, retry_exp_backoff, parent));
        assert_eq!(execution_id, *found_execution_id);
        assert_eq!(Version::new(2), *version);
        assert_eq!(0, *already_retried_count);
        assert_eq!(MAX_RETRIES, *max_retries);
        assert_eq!(RETRY_EXP_BACKOFF, *retry_exp_backoff);
        assert_eq!(None, *parent);
    }
}

#[expect(clippy::too_many_lines)]
pub async fn append_batch_respond_to_parent(
    db_connection: &impl DbConnection,
    sim_clock: SimClock,
) {
    let parent_id = ExecutionId::generate();

    // Create parent
    let version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id: parent_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: parent_id,
        })
        .await
        .unwrap();
    // Create joinset
    let join_set_id = JoinSetId::generate();
    let mut version = db_connection
        .append(
            parent_id,
            version,
            AppendRequest {
                created_at: sim_clock.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSet { join_set_id },
                },
            },
        )
        .await
        .unwrap();

    let child_a = {
        let parent_exe = db_connection.get(parent_id).await.unwrap();
        assert_matches!(
            parent_exe.pending_state,
            PendingState::PendingAt {
                scheduled_at
            } if scheduled_at == sim_clock.now()
        );
        // Create child 1
        let child_id = ExecutionId::generate();
        let child_version = db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: child_id,
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: parent_id,
            })
            .await
            .unwrap();
        // Append JoinNext before receiving the response should make the parent BlockedByJoinSet
        db_connection
            .append(
                parent_id,
                version,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinNext {
                            join_set_id,
                            lock_expires_at: sim_clock.now(),
                            closing: false,
                        },
                    },
                },
            )
            .await
            .unwrap();
        let parent_exe = db_connection.get(parent_id).await.unwrap();
        assert_matches!(
            parent_exe.pending_state,
            PendingState::BlockedByJoinSet { join_set_id: found_join_set_id, lock_expires_at, closing: false }
            if found_join_set_id == join_set_id && lock_expires_at == sim_clock.now()
        );

        // Append child response to unblock the parent
        db_connection
            .append_batch_respond_to_parent(
                child_id,
                sim_clock.now(),
                vec![ExecutionEventInner::Finished {
                    result: Ok(concepts::SupportedFunctionReturnValue::None),
                }],
                child_version,
                parent_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_id,
                        result: Ok(concepts::SupportedFunctionReturnValue::None),
                    },
                },
            )
            .await
            .unwrap();

        let parent_exe = db_connection.get(parent_id).await.unwrap();
        assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });
        version = parent_exe.next_version;

        child_id
    };
    let child_b = {
        // Create child 2
        let child_id = ExecutionId::generate();
        let child_version = db_connection
            .create(CreateRequest {
                created_at: sim_clock.now(),
                execution_id: child_id,
                ffqn: SOME_FFQN,
                params: Params::default(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: sim_clock.now(),
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy_activity(),
                return_type: None,
                topmost_parent: parent_id,
            })
            .await
            .unwrap();
        let child_resp = vec![ExecutionEventInner::Finished {
            result: Ok(concepts::SupportedFunctionReturnValue::None),
        }];

        db_connection
            .append_batch_respond_to_parent(
                child_id,
                sim_clock.now(),
                child_resp,
                child_version,
                parent_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_id,
                        result: Ok(concepts::SupportedFunctionReturnValue::None),
                    },
                },
            )
            .await
            .unwrap();

        // Append JoinNext after receiving the response should make the parent PendingAt
        db_connection
            .append(
                parent_id,
                version,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinNext {
                            join_set_id,
                            lock_expires_at: sim_clock.now(),
                            closing: false,
                        },
                    },
                },
            )
            .await
            .unwrap();
        let parent_exe = db_connection.get(parent_id).await.unwrap();
        assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });

        child_id
    };
    let parent_exe = db_connection.get(parent_id).await.unwrap();
    assert_eq!(2, parent_exe.responses.len());
    assert_eq!(
        *parent_exe.responses.first().unwrap(),
        JoinSetResponseEventOuter {
            created_at: sim_clock.now(),
            event: JoinSetResponseEvent {
                join_set_id,
                event: JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_a,
                    result: Ok(concepts::SupportedFunctionReturnValue::None),
                },
            }
        }
    );
    assert_eq!(
        *parent_exe.responses.get(1).unwrap(),
        JoinSetResponseEventOuter {
            created_at: sim_clock.now(),
            event: JoinSetResponseEvent {
                join_set_id,
                event: JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_b,
                    result: Ok(concepts::SupportedFunctionReturnValue::None),
                },
            }
        }
    );
    assert_matches!(parent_exe.pending_state, PendingState::PendingAt { .. });
}

pub async fn lock_pending_should_sort_by_scheduled_at(
    db_connection: &impl DbConnection,
    sim_clock: SimClock,
) {
    let created_at = sim_clock.now();
    let older_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: older_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: older_id,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(1)).await;
    let newer_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newer_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: newer_id,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(999)).await;
    let newest_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newest_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: newest_id,
        })
        .await
        .unwrap();

    let locked_ids = db_connection
        .lock_pending(
            3,
            sim_clock.now(),
            Arc::from([SOME_FFQN]),
            sim_clock.now(),
            ConfigId::dummy_activity(),
            ExecutorId::generate(),
            sim_clock.now() + Duration::from_secs(1),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|locked| locked.execution_id)
        .collect::<Vec<_>>();

    assert_eq!(vec![older_id, newer_id, newest_id], locked_ids);
}

pub async fn lock(db_connection: &impl DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
    let version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: execution_id,
        })
        .await
        .unwrap();
    // Append an event that does not change Pending state but must update the version.
    let version = db_connection
        .append(
            execution_id,
            version,
            AppendRequest {
                created_at: sim_clock.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: JoinSetId::generate(),
                        request: JoinSetRequest::DelayRequest {
                            delay_id: DelayId::generate(),
                            expires_at: sim_clock.now(),
                        },
                    },
                },
            },
        )
        .await
        .unwrap();
    let locked_at = sim_clock.now();
    let (_, _version) = db_connection
        .lock(
            locked_at,
            ConfigId::dummy_activity(),
            execution_id,
            RunId::generate(),
            version,
            executor_id,
            locked_at + Duration::from_millis(100),
        )
        .await
        .unwrap();
}

pub async fn get_expired_lock(db_connection: &impl DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
    let version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: Some(TypeWrapper::U8),
            topmost_parent: execution_id,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);
    let (_, version) = db_connection
        .lock(
            sim_clock.now(),
            ConfigId::dummy_activity(),
            execution_id,
            RunId::generate(),
            version,
            executor_id,
            sim_clock.now() + lock_expiry,
        )
        .await
        .unwrap();

    assert!(db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap()
        .is_empty());

    sim_clock.move_time_forward(lock_expiry).await;

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::Lock {
        execution_id,
        version,
        intermittent_event_count: 0,
        max_retries: 0,
        retry_exp_backoff: Duration::ZERO,
        parent: None,
    };
    assert_eq!(expected, actual);
}

pub async fn get_expired_delay(db_connection: &impl DbConnection, sim_clock: SimClock) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
    let version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: sim_clock.now(),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
            config_id: ConfigId::dummy_activity(),
            return_type: None,
            topmost_parent: execution_id,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);
    let (_, version) = db_connection
        .lock(
            sim_clock.now(),
            ConfigId::dummy_activity(),
            execution_id,
            RunId::generate(),
            version,
            executor_id,
            sim_clock.now() + lock_expiry * 2,
        )
        .await
        .unwrap();

    let join_set_id = JoinSetId::generate();
    let delay_id = DelayId::generate();
    db_connection
        .append(
            execution_id,
            version,
            AppendRequest {
                created_at: Now.now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request: JoinSetRequest::DelayRequest {
                            delay_id,
                            expires_at: sim_clock.now() + lock_expiry,
                        },
                    },
                },
            },
        )
        .await
        .unwrap();

    assert!(db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap()
        .is_empty());

    sim_clock.move_time_forward(lock_expiry).await;

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::AsyncDelay {
        execution_id,
        join_set_id,
        delay_id,
    };
    assert_eq!(expected, actual);
}
