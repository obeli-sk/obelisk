use assert_matches::assert_matches;
use chrono::DateTime;
use concepts::prefixed_ulid::{DelayId, JoinSetId, RunId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, ExpiredTimer,
    JoinSetRequest, JoinSetResponse, SpecificError, Version,
};
use concepts::{prefixed_ulid::ExecutorId, ExecutionId};
use concepts::{storage::HistoryEvent, FinishedExecutionResult};
use concepts::{FunctionFqn, Params, StrVariant};
use std::time::Duration;
use test_utils::sim_clock::SimClock;
use tracing::{debug, info};
use utils::time::now;

pub const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");

#[allow(clippy::too_many_lines)]
pub async fn lifecycle(db_connection: &impl DbConnection) {
    let sim_clock = SimClock::new(now());
    let execution_id = ExecutionId::generate();
    let exec1 = ExecutorId::generate();
    let exec2 = ExecutorId::generate();
    let lock_expiry = Duration::from_millis(500);

    assert!(db_connection
        .lock_pending(
            1,
            sim_clock.now(),
            vec![SOME_FFQN],
            sim_clock.now(),
            exec1,
            sim_clock.now() + lock_expiry,
        )
        .await
        .unwrap()
        .is_empty());

    let mut version;
    // Create

    db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
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
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
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
                vec![SOME_FFQN],
                created_at,
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
    sim_clock.move_time_forward(Duration::from_millis(499));
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
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
    sim_clock.move_time_forward(lock_expiry - Duration::from_millis(100));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock using exec2");
        let not_yet_pending = db_connection
            .lock(
                created_at,
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

    sim_clock.move_time_forward(Duration::from_millis(100));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Lock again using exec1");
        let (event_history, current_version) = db_connection
            .lock(
                created_at,
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
    sim_clock.move_time_forward(Duration::from_millis(700));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock using exec2  while in a lock");
        assert!(db_connection
            .lock(
                created_at,
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
    sim_clock.move_time_forward(Duration::from_millis(200));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Extend lock using exec1 and wrong run id should fail");
        assert!(db_connection
            .lock(
                created_at,
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
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
        let req = AppendRequest {
            event: ExecutionEventInner::Unlocked,
            created_at,
        };
        version = db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
    sim_clock.move_time_forward(Duration::from_millis(200));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Lock again");
        let (event_history, current_version) = db_connection
            .lock(
                created_at,
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
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Cancel request");
        let req = AppendRequest {
            event: ExecutionEventInner::CancelRequest,
            created_at,
        };
        version = db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
    sim_clock.move_time_forward(Duration::from_millis(300));
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Finish execution");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionResult::None),
            },
            created_at,
        };
        version = db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Append after finish should fail");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionResult::None),
            },
            created_at,
        };
        let err = db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap_err();

        assert_eq!(
            DbError::Specific(SpecificError::ValidationFailed(StrVariant::Static(
                "already finished"
            ))),
            err
        );
    }
}

pub async fn expired_lock_should_be_found(db_connection: &impl DbConnection) {
    const MAX_RETRIES: u32 = 1;
    const RETRY_EXP_BACKOFF: Duration = Duration::from_millis(100);
    let sim_clock = SimClock::new(now());
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
                scheduled_at: None,
                retry_exp_backoff: RETRY_EXP_BACKOFF,
                max_retries: MAX_RETRIES,
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
                vec![SOME_FFQN],
                sim_clock.now(),
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
    sim_clock.move_time_forward(lock_duration);
    {
        let expired_at = sim_clock.now();
        let expired = db_connection.get_expired_timers(expired_at).await.unwrap();
        assert_eq!(1, expired.len());
        let expired = &expired[0];
        let (found_execution_id, version, already_retried_count, max_retries, retry_exp_backoff) = assert_matches!(expired,
            ExpiredTimer::Lock { execution_id, version, already_tried_count, max_retries, retry_exp_backoff } =>
            (execution_id, version, already_tried_count, max_retries, retry_exp_backoff));
        assert_eq!(execution_id, *found_execution_id);
        assert_eq!(Version::new(2), *version);
        assert_eq!(0, *already_retried_count);
        assert_eq!(MAX_RETRIES, *max_retries);
        assert_eq!(RETRY_EXP_BACKOFF, *retry_exp_backoff);
    }
}

pub async fn append_batch_respond_to_parent(db_connection: &impl DbConnection) {
    let sim_clock = SimClock::new(now());
    let parent_id = ExecutionId::generate();
    let child_id = ExecutionId::generate();
    // Create parent
    db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id: parent_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
    // Create child
    let child_version = db_connection
        .create(CreateRequest {
            created_at: sim_clock.now(),
            execution_id: child_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
    let child_resp = vec![AppendRequest {
        created_at: sim_clock.now(),
        event: ExecutionEventInner::Finished {
            result: Ok(concepts::SupportedFunctionResult::None),
        },
    }];
    let parent_add = AppendRequest {
        created_at: sim_clock.now(),
        event: ExecutionEventInner::HistoryEvent {
            event: HistoryEvent::JoinSetResponse {
                join_set_id: JoinSetId::generate(),
                response: JoinSetResponse::ChildExecutionFinished {
                    child_execution_id: child_id,
                    result: Ok(concepts::SupportedFunctionResult::None),
                },
            },
        },
    };
    db_connection
        .append_batch_respond_to_parent(
            child_resp,
            child_id,
            child_version,
            (parent_id, parent_add),
        )
        .await
        .unwrap();
}

pub async fn lock_pending_should_sort_by_scheduled_at(db_connection: &impl DbConnection) {
    let sim_clock = SimClock::new(DateTime::default());
    let created_at = sim_clock.now();
    let older_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: older_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: Some(sim_clock.now()),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(1));
    let newer_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newer_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: Some(sim_clock.now()),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();

    sim_clock.move_time_forward(Duration::from_nanos(999));
    let newest_id = ExecutionId::generate();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id: newest_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: Some(sim_clock.now()),
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();

    let locked_ids = db_connection
        .lock_pending(
            3,
            sim_clock.now(),
            vec![SOME_FFQN],
            sim_clock.now(),
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

pub async fn lock_should_delete_from_pending(db_connection: &impl DbConnection) {
    let execution_id = ExecutionId::generate();
    let executor_id = ExecutorId::generate();
    // Create
    let version = db_connection
        .create(CreateRequest {
            created_at: now(),
            execution_id,
            ffqn: SOME_FFQN,
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
    // Append an event that does not change Pending state but must update the version in the `pending` table.
    let version = db_connection
        .append(
            execution_id,
            Some(version),
            AppendRequest {
                created_at: now(),
                event: ExecutionEventInner::HistoryEvent {
                    event: HistoryEvent::JoinSetRequest {
                        join_set_id: JoinSetId::generate(),
                        request: JoinSetRequest::DelayRequest {
                            delay_id: DelayId::generate(),
                            expires_at: now(),
                        },
                    },
                },
            },
        )
        .await
        .unwrap();
    let locked_at = now();
    let (_, _version) = db_connection
        .lock(
            locked_at,
            execution_id,
            RunId::generate(),
            version,
            executor_id,
            locked_at + Duration::from_millis(100),
        )
        .await
        .unwrap();
}

pub async fn get_expired_lock(db_connection: &impl DbConnection) {
    let sim_clock = SimClock::new(DateTime::default());
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
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);
    let (_, version) = db_connection
        .lock(
            sim_clock.now(),
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

    sim_clock.move_time_forward(lock_expiry);

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::Lock {
        execution_id,
        version,
        already_tried_count: 0,
        max_retries: 0,
        retry_exp_backoff: Duration::ZERO,
    };
    assert_eq!(expected, actual);
}

pub async fn get_expired_delay(db_connection: &impl DbConnection) {
    let sim_clock = SimClock::new(DateTime::default());
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
            scheduled_at: None,
            retry_exp_backoff: Duration::ZERO,
            max_retries: 0,
        })
        .await
        .unwrap();
    let lock_expiry = Duration::from_millis(100);
    let (_, version) = db_connection
        .lock(
            sim_clock.now(),
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
    let version = db_connection
        .append(
            execution_id,
            Some(version),
            AppendRequest {
                created_at: now(),
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

    sim_clock.move_time_forward(lock_expiry);

    let mut actual = db_connection
        .get_expired_timers(sim_clock.now())
        .await
        .unwrap();
    assert_eq!(1, actual.len());
    let actual = actual.pop().unwrap();
    let expected = ExpiredTimer::AsyncDelay {
        execution_id,
        version,
        join_set_id,
        delay_id,
    };
    assert_eq!(expected, actual);
}
