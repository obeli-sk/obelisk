use concepts::prefixed_ulid::RunId;
use concepts::storage::{AppendRequest, CreateRequest, DbConnection, ExecutionEventInner, Version};
use concepts::{prefixed_ulid::ExecutorId, ExecutionId};
use concepts::{storage::HistoryEvent, FinishedExecutionResult};
use concepts::{FunctionFqn, Params};
use std::time::Duration;
use test_utils::sim_clock::SimClock;
use tracing::{debug, info};
use utils::time::now;

pub const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");

#[allow(clippy::too_many_lines)]
pub async fn lifecycle(db_connection: impl DbConnection) {
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
    sim_clock.sleep(Duration::from_millis(499));
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
    sim_clock.sleep(lock_expiry - Duration::from_millis(100));
    {
        let created_at = sim_clock.now();
        info!(now = %created_at, "Attempt to lock using exec2");
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
    sim_clock.sleep(Duration::from_millis(100));
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
                created_at + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert!(event_history.is_empty());
        version = current_version;
    }
    sim_clock.sleep(Duration::from_millis(700));
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
    sim_clock.sleep(Duration::from_millis(200));
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
        info!(now = %created_at, "Yield");
        let req = AppendRequest {
            event: ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::Yield,
            },
            created_at,
        };
        version = db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
    sim_clock.sleep(Duration::from_millis(200));
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
        assert_eq!(vec![HistoryEvent::Yield], event_history);
        version = current_version;
    }
    sim_clock.sleep(Duration::from_millis(300));
    {
        let created_at = sim_clock.now();
        debug!(now = %created_at, "Finish execution");
        let req = AppendRequest {
            event: ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionResult::None),
            },
            created_at,
        };

        db_connection
            .append(execution_id, Some(version), req)
            .await
            .unwrap();
    }
}
