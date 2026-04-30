use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{
    AppendRequest, CreateRequest, ExecutionRequest, HistoryEvent, HistoryEventScheduleAt,
};
use concepts::time::ClockFn;
use concepts::{
    ComponentId, ExecutionId, ExecutionMetadata, FunctionFqn, FunctionMetadata, Name, Params,
    StrVariant,
};
use executor::worker::{Worker, WorkerContext, WorkerResult, WorkerResultOk};
use std::sync::Arc;
use tracing::debug;

#[derive(derive_more::Debug)]
pub struct CronWorker {
    pub component_id: ComponentId,
    pub target_ffqn: FunctionFqn,
    pub target_component_id: ComponentId,
    pub params: Params,
    pub cron_schedule: CronOrOnce,
    pub deployment_id: DeploymentId,
    #[debug(skip)]
    pub db_pool: Arc<dyn concepts::storage::DbPool>,
    #[debug(skip)]
    pub clock_fn: Box<dyn ClockFn>,
}

#[derive(Debug, Clone)]
pub enum CronOrOnce {
    Cron(Box<croner::Cron>),
    Once,
}

impl CronWorker {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        component_id: ComponentId,
        target_ffqn: FunctionFqn,
        target_component_id: ComponentId,
        params: Params,
        cron_schedule: CronOrOnce,
        deployment_id: DeploymentId,
        db_pool: Arc<dyn concepts::storage::DbPool>,
        clock_fn: Box<dyn ClockFn>,
    ) -> Self {
        Self {
            component_id,
            target_ffqn,
            target_component_id,
            params,
            cron_schedule,
            deployment_id,
            db_pool,
            clock_fn,
        }
    }

    fn next_fire_time(
        &self,
        after: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, executor::worker::FatalError> {
        match &self.cron_schedule {
            CronOrOnce::Once => Ok(None),
            CronOrOnce::Cron(cron) => {
                cron.find_next_occurrence(&after, false)
                    .map(Some)
                    .map_err(|err| executor::worker::FatalError::ConstraintViolation {
                        reason: format!("cron next occurrence error: {err}").into(),
                    })
            }
        }
    }
}

/// Not read internally, only used for seed execution creation.
/// FFQN starts with `obelisk-cron-` prefix for easy execution search.
#[must_use]
pub fn cron_ffqn(target: &FunctionFqn) -> FunctionFqn {
    FunctionFqn {
        ifc_fqn: Name::new_arc(format!("obelisk-cron-{}", target.ifc_fqn).into()),
        function_name: target.function_name.clone(),
    }
}

#[async_trait]
impl Worker for CronWorker {
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        let now = self.clock_fn.now();
        let current_execution_id = ctx.execution_id.clone();
        let version = ctx.version.clone();

        let db_connection = self
            .db_pool
            .connection()
            .await
            .map_err(|e| executor::worker::WorkerError::DbError(e.into()))?;

        let scheduled_execution_id = ExecutionId::generate();
        let schedule_req = CreateRequest {
            created_at: now,
            execution_id: scheduled_execution_id.clone(),
            ffqn: self.target_ffqn.clone(),
            params: self.params.clone(),
            parent: None,
            scheduled_at: now,
            component_id: self.target_component_id.clone(),
            deployment_id: self.deployment_id,
            metadata: ExecutionMetadata::empty(),
            scheduled_by: Some(current_execution_id.clone()),
            paused: false,
        };

        // Build the history event for the schedule
        let schedule_event = AppendRequest {
            created_at: now,
            event: ExecutionRequest::HistoryEvent {
                event: HistoryEvent::Schedule {
                    execution_id: scheduled_execution_id,
                    schedule_at: HistoryEventScheduleAt::Now,
                    result: Ok(()),
                },
            },
        };

        let next_fire = self
            .next_fire_time(now)
            .map_err(|err| executor::worker::WorkerError::FatalError(err, version.clone()))?;
        let second_event = match next_fire {
            None => AppendRequest {
                created_at: now,
                event: ExecutionRequest::Finished {
                    retval: concepts::SupportedFunctionReturnValue::Ok(None),
                    http_client_traces: None,
                },
            },
            Some(next_fire) => {
                debug!(next_fire = %next_fire, "Scheduling next tick");
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::Unlocked {
                        backoff_expires_at: next_fire,
                        reason: StrVariant::Static("cron: waiting for next cron tick"),
                    },
                }
            }
        };
        let batch = vec![schedule_event, second_event];
        db_connection
            .append_batch_create_new_execution(
                now,
                batch,
                current_execution_id,
                version,
                vec![schedule_req],
                vec![], // no backtraces
            )
            .await
            .map_err(executor::worker::WorkerError::DbError)?;

        Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher)
    }

    fn exported_functions_noext(&self) -> &[FunctionMetadata] {
        &[] // registry nor other components cannot directly interact with cron worker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use concepts::component_id::COMPONENT_DIGEST_DUMMY;
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{
        AppendRequest, DbPool, ExecutionRequest, Locked, PendingState, PendingStateFinished,
        PendingStateFinishedResultKind, PendingStatePendingAt, Version,
    };
    use db_mem::inmemory_dao::InMemoryPool;
    use test_utils::sim_clock::SimClock;

    const TARGET_FFQN: FunctionFqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");

    fn make_cron_component_id() -> ComponentId {
        ComponentId::new(
            concepts::ComponentType::Cron,
            StrVariant::Static("my_schedule"),
            COMPONENT_DIGEST_DUMMY,
        )
        .unwrap()
    }

    fn make_target_component_id() -> ComponentId {
        ComponentId::dummy_activity()
    }

    fn make_worker(
        db_pool: Arc<dyn DbPool>,
        cron_schedule: CronOrOnce,
        clock_fn: SimClock,
    ) -> CronWorker {
        CronWorker::new(
            make_cron_component_id(),
            TARGET_FFQN,
            make_target_component_id(),
            Params::empty(),
            cron_schedule,
            DEPLOYMENT_ID_DUMMY,
            db_pool,
            Box::new(clock_fn),
        )
    }

    fn parse_cron(expr: &str) -> CronOrOnce {
        CronOrOnce::Cron(Box::new(croner::Cron::new(expr).parse().unwrap()))
    }

    fn make_locked_event(now: DateTime<Utc>) -> Locked {
        Locked {
            component_id: make_cron_component_id(),
            executor_id: ExecutorId::generate(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            run_id: RunId::generate(),
            lock_expires_at: now + chrono::Duration::seconds(60),
            retry_config: concepts::ComponentRetryConfig::ZERO,
        }
    }

    fn make_worker_context(
        execution_id: ExecutionId,
        version: Version,
        now: DateTime<Utc>,
    ) -> WorkerContext {
        WorkerContext {
            execution_id,
            metadata: ExecutionMetadata::empty(),
            ffqn: cron_ffqn(&TARGET_FFQN),
            params: Params::empty(),
            event_history: Vec::new(),
            responses: Vec::new(),
            version,
            can_be_retried: false,
            worker_span: tracing::info_span!("schedule_test"),
            locked_event: make_locked_event(now),
            executor_close_watcher: tokio::sync::watch::channel(false).1,
        }
    }

    /// Create a seed execution in the DB, append a Locked event,
    /// and return the `execution_id` and the version after locking (for the worker).
    async fn create_and_lock_execution(
        db_pool: &Arc<dyn DbPool>,
        now: DateTime<Utc>,
    ) -> (ExecutionId, Version) {
        let conn = db_pool.connection().await.unwrap();
        let execution_id = ExecutionId::generate();
        conn.create(CreateRequest {
            created_at: now,
            execution_id: execution_id.clone(),
            ffqn: cron_ffqn(&TARGET_FFQN),
            params: Params::empty(),
            parent: None,
            scheduled_at: now,
            component_id: make_cron_component_id(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            metadata: ExecutionMetadata::empty(),
            scheduled_by: None,
            paused: false,
        })
        .await
        .unwrap();
        // Append a Locked event at version 1
        conn.append(
            execution_id.clone(),
            Version::new(1),
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::Locked(Locked {
                    lock_expires_at: now + chrono::Duration::seconds(60),
                    component_id: make_cron_component_id(),
                    executor_id: ExecutorId::generate(),
                    run_id: RunId::generate(),
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    retry_config: concepts::ComponentRetryConfig::ZERO,
                }),
            },
        )
        .await
        .unwrap();
        // Worker receives version after Lock (version 2)
        (execution_id, Version::new(2))
    }

    // Fixed test time: 2025-01-15 10:30:00 UTC
    fn test_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap()
    }

    #[test]
    fn next_fire_time_returns_none_for_once() {
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(test_time());
        let worker = make_worker(db_pool, CronOrOnce::Once, sim_clock);
        assert!(worker.next_fire_time(test_time()).unwrap().is_none());
    }

    #[test]
    fn next_fire_time_computes_correct_time() {
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(test_time());
        let worker = make_worker(db_pool, parse_cron("0 12 * * *"), sim_clock); // daily at 12:00
        let after = Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap();
        let next = worker
            .next_fire_time(after)
            .unwrap()
            .expect("must have next fire time");
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap());
    }

    #[test]
    fn next_fire_time_wraps_to_next_day() {
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(test_time());
        let worker = make_worker(db_pool, parse_cron("0 12 * * *"), sim_clock); // daily at 12:00
        let after = Utc.with_ymd_and_hms(2025, 1, 15, 13, 0, 0).unwrap();
        let next = worker
            .next_fire_time(after)
            .unwrap()
            .expect("must have next fire time");
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 16, 12, 0, 0).unwrap());
    }

    #[tokio::test]
    async fn once_schedule_creates_child_and_finishes() {
        let now = test_time();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(now);
        let worker = make_worker(db_pool.clone(), CronOrOnce::Once, sim_clock);

        let (execution_id, version) = create_and_lock_execution(&db_pool, now).await;
        let ctx = make_worker_context(execution_id.clone(), version, now);

        let result = worker.run(ctx).await.unwrap();
        assert!(matches!(result, WorkerResultOk::DbUpdatedByWorkerOrWatcher));

        // Verify the schedule execution is now finished
        let conn = db_pool.connection().await.unwrap();
        let schedule_log = conn.get(&execution_id).await.unwrap();
        assert!(
            matches!(
                schedule_log.pending_state,
                PendingState::Finished(PendingStateFinished {
                    result_kind: PendingStateFinishedResultKind::Ok,
                    ..
                })
            ),
            "schedule must be finished with Ok, got: {:?}",
            schedule_log.pending_state,
        );
        // Events: Created(0), Locked(1), Schedule(2), Finished(3)
        assert_eq!(schedule_log.events.len(), 4);
        assert!(matches!(
            schedule_log.events[2].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::Schedule { .. }
            }
        ));
        assert!(matches!(
            schedule_log.events[3].event,
            ExecutionRequest::Finished { .. }
        ));

        // Verify the child execution was created
        if let ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::Schedule {
                    execution_id: child_id,
                    ..
                },
        } = &schedule_log.events[2].event
        {
            let child_log = conn.get(child_id).await.unwrap();
            assert!(
                matches!(child_log.pending_state, PendingState::PendingAt(_)),
                "child must be PendingAt, got: {:?}",
                child_log.pending_state,
            );
            // Verify child has the correct target FFQN
            if let ExecutionRequest::Created { ffqn, .. } = &child_log.events[0].event {
                assert_eq!(*ffqn, TARGET_FFQN);
            } else {
                panic!("first event of child must be Created");
            }
        } else {
            panic!("third event must be Schedule");
        }
    }

    #[tokio::test]
    async fn recurring_schedule_creates_child_and_reschedules() {
        let now = test_time();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(now);
        let worker = make_worker(db_pool.clone(), parse_cron("0 * * * *"), sim_clock); // every hour

        let (execution_id, version) = create_and_lock_execution(&db_pool, now).await;
        let ctx = make_worker_context(execution_id.clone(), version, now);

        let result = worker.run(ctx).await.unwrap();
        assert!(matches!(result, WorkerResultOk::DbUpdatedByWorkerOrWatcher));

        // Verify the schedule execution is now PendingAt (waiting for next tick)
        let conn = db_pool.connection().await.unwrap();
        let schedule_log = conn.get(&execution_id).await.unwrap();
        assert!(
            matches!(
                schedule_log.pending_state,
                PendingState::PendingAt(PendingStatePendingAt { .. })
            ),
            "schedule must be PendingAt for next tick, got: {:?}",
            schedule_log.pending_state,
        );
        // Events: Created(0), Locked(1), Schedule(2), Unlocked(3)
        assert_eq!(schedule_log.events.len(), 4);
        assert!(matches!(
            schedule_log.events[2].event,
            ExecutionRequest::HistoryEvent {
                event: HistoryEvent::Schedule { .. }
            }
        ));
        assert!(matches!(
            schedule_log.events[3].event,
            ExecutionRequest::Unlocked { .. }
        ));

        // Verify the child execution was created with correct FFQN
        if let ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::Schedule {
                    execution_id: child_id,
                    ..
                },
        } = &schedule_log.events[2].event
        {
            let child_log = conn.get(child_id).await.unwrap();
            if let ExecutionRequest::Created { ffqn, .. } = &child_log.events[0].event {
                assert_eq!(*ffqn, TARGET_FFQN);
            } else {
                panic!("first event of child must be Created");
            }
        } else {
            panic!("third event must be Schedule");
        }
    }

    #[tokio::test]
    async fn recurring_schedule_next_tick_is_in_the_future() {
        // Use a fixed time: 2025-01-15 10:30:00 UTC
        // With cron "0 * * * *" (every hour), next tick should be 11:00:00
        let now = test_time();
        let db_pool: Arc<dyn DbPool> = Arc::new(InMemoryPool::new());
        let sim_clock = SimClock::new(now);
        let worker = make_worker(db_pool.clone(), parse_cron("0 * * * *"), sim_clock); // every hour

        let (execution_id, version) = create_and_lock_execution(&db_pool, now).await;
        let ctx = make_worker_context(execution_id.clone(), version, now);
        worker.run(ctx).await.unwrap();

        // Check that the schedule's PendingAt time is the expected next hour
        let conn = db_pool.connection().await.unwrap();
        let schedule_log = conn.get(&execution_id).await.unwrap();
        if let PendingState::PendingAt(PendingStatePendingAt { scheduled_at, .. }) =
            &schedule_log.pending_state
        {
            let expected_next = Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap();
            assert_eq!(
                *scheduled_at, expected_next,
                "next tick must be {expected_next}, got {scheduled_at}"
            );
        } else {
            panic!("expected PendingAt, got: {:?}", schedule_log.pending_state);
        }
    }
}
