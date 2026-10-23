use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbErrorRead, ExecutionRequest, HistoryEvent,
    HistoryEventScheduleAt, Unlocked, Version,
};
use concepts::time::ClockFn;
use concepts::{
    ComponentId, ExecutionId, ExecutionMetadata, FunctionFqn, FunctionMetadata, Name, Params,
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
    pub overlap: CronOverlap,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CronOverlap {
    /// Skip the tick while the previously scheduled execution is not finished.
    #[default]
    Skip,
    Allow,
}

impl CronWorker {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        component_id: ComponentId,
        target_ffqn: FunctionFqn,
        target_component_id: ComponentId,
        params: Params,
        cron_schedule: CronOrOnce,
        overlap: CronOverlap,
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
            overlap,
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

async fn previous_unfinished(
    event_history: &[(HistoryEvent, Version)],
    db_connection: &dyn DbConnection,
) -> Result<Option<ExecutionId>, executor::worker::WorkerError> {
    let Some(previous) = event_history
        .iter()
        .rev()
        .find_map(|(event, _)| match event {
            HistoryEvent::Schedule { execution_id, .. } => Some(execution_id),
            _ => None,
        })
    else {
        return Ok(None);
    };
    match db_connection.get_pending_state(previous).await {
        Ok(state) if state.pending_state.is_finished() => Ok(None),
        Ok(_) => Ok(Some(previous.clone())),
        // A removed execution must not block the cron forever.
        Err(DbErrorRead::NotFound) => Ok(None),
        Err(DbErrorRead::Generic(err)) => Err(executor::worker::WorkerError::DbError(err.into())),
    }
}

#[async_trait]
impl Worker for CronWorker {
    async fn run(&self, ctx: WorkerContext) -> WorkerResult {
        let now = self.clock_fn.now();
        let current_execution_id = ctx.execution_id.clone();
        let version = ctx.version.clone();
        let max_persisted_value_size_bytes = ctx
            .metadata
            .max_persisted_value_size_bytes()
            .unwrap_or(u64::MAX);

        let db_connection = self
            .db_pool
            .connection()
            .await
            .map_err(|e| executor::worker::WorkerError::DbError(e.into()))?;

        let next_fire = self
            .next_fire_time(now)
            .map_err(|err| executor::worker::WorkerError::FatalError(err, version.clone()))?;

        if self.overlap == CronOverlap::Skip
            && let Some(next_fire) = next_fire
            && let Some(running) =
                previous_unfinished(&ctx.event_history, db_connection.as_ref()).await?
        {
            debug!(%running, %next_fire, "Skipping tick, previous execution is not finished");
            db_connection
                .append(
                    current_execution_id,
                    version,
                    AppendRequest {
                        created_at: now,
                        event: ExecutionRequest::Unlocked(Unlocked {
                            unlocked_at: next_fire,
                            reason: format!("cron tick skipped, `{running}` is not finished")
                                .into(),
                        }),
                    },
                )
                .await
                .map_err(executor::worker::WorkerError::DbError)?;
            return Ok(WorkerResultOk::DbUpdatedByWorkerOrWatcher);
        }

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
            max_persisted_value_size_bytes,
        };

        // Build the history event for the schedule
        let schedule_event = AppendRequest {
            created_at: now,
            event: ExecutionRequest::HistoryEvent {
                event: HistoryEvent::Schedule {
                    execution_id: scheduled_execution_id,
                    schedule_at: HistoryEventScheduleAt::Now,
                    params_hash: Some(concepts::persisted_value::compact_json_sha256(&self.params)),
                    result: Ok(()),
                },
            },
        };

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
                // Unlocked transition must be valid - execution was just locked
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::Unlocked(Unlocked {
                        unlocked_at: next_fire,
                        reason: "cron".into(),
                    }),
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
    use concepts::StrVariant;
    use concepts::component_id::{ComponentDigest, Digest};
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{
        AppendRequest, Created, DbPool, ExecutionRequest, Locked, PendingState,
        PendingStateFinished, PendingStateFinishedResultKind, PendingStatePendingAt, Version,
    };
    use test_utils::sim_clock::SimClock;

    const TARGET_FFQN: FunctionFqn = FunctionFqn::new_static("test:pkg/ifc", "do-work");
    const MAX_PERSISTED_VALUE_SIZE_BYTES: u64 = 12_345;

    fn make_cron_component_id() -> ComponentId {
        ComponentId::new(
            concepts::ComponentType::Cron,
            StrVariant::Static("my_schedule"),
            ComponentDigest(Digest([1; 32])), // distinct from the target's dummy digest
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
        make_worker_with_overlap(db_pool, cron_schedule, CronOverlap::default(), clock_fn)
    }

    fn make_worker_with_overlap(
        db_pool: Arc<dyn DbPool>,
        cron_schedule: CronOrOnce,
        overlap: CronOverlap,
        clock_fn: SimClock,
    ) -> CronWorker {
        CronWorker::new(
            make_cron_component_id(),
            TARGET_FFQN,
            make_target_component_id(),
            Params::empty(),
            cron_schedule,
            overlap,
            DEPLOYMENT_ID_DUMMY,
            db_pool,
            Box::new(clock_fn),
        )
    }

    fn parse_cron(expr: &str) -> CronOrOnce {
        CronOrOnce::Cron(Box::new(expr.parse::<croner::Cron>().unwrap()))
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
        let locked_event = make_locked_event(now);
        WorkerContext {
            execution_id,
            metadata: ExecutionMetadata::empty()
                .with_max_persisted_value_size_bytes(MAX_PERSISTED_VALUE_SIZE_BYTES),
            component_digest: locked_event.component_id.component_digest.clone(),
            ffqn: cron_ffqn(&TARGET_FFQN),
            params: Params::empty(),
            event_history: Vec::new(),
            responses: Vec::new(),
            parent: None,
            version,
            can_be_retried: false,
            worker_span: tracing::info_span!("schedule_test"),
            locked_event,
            execution_interrupt_watcher: tokio::sync::watch::channel(false).1,
            instance_permit: None,
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
            max_persisted_value_size_bytes: u64::MAX,
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

    #[tokio::test]
    async fn next_fire_time_returns_none_for_once() {
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let sim_clock = SimClock::new(test_time());
        let worker = make_worker(db_pool, CronOrOnce::Once, sim_clock);
        assert!(worker.next_fire_time(test_time()).unwrap().is_none());
    }

    #[tokio::test]
    async fn next_fire_time_computes_correct_time() {
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let sim_clock = SimClock::new(test_time());
        let worker = make_worker(db_pool, parse_cron("0 12 * * *"), sim_clock); // daily at 12:00
        let after = Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap();
        let next = worker
            .next_fire_time(after)
            .unwrap()
            .expect("must have next fire time");
        assert_eq!(next, Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap());
    }

    #[tokio::test]
    async fn next_fire_time_wraps_to_next_day() {
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
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
    async fn once_schedule_inherits_persisted_value_limit() {
        let now = test_time();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
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
            if let ExecutionRequest::Created(Created {
                ffqn,
                max_persisted_value_size_bytes,
                ..
            }) = &child_log.events[0].event
            {
                assert_eq!(*ffqn, TARGET_FFQN);
                assert_eq!(
                    *max_persisted_value_size_bytes,
                    MAX_PERSISTED_VALUE_SIZE_BYTES
                );
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
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
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
            ExecutionRequest::Unlocked(_)
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
            if let ExecutionRequest::Created(Created { ffqn, .. }) = &child_log.events[0].event {
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
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
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

    async fn create_seed(db_pool: &Arc<dyn DbPool>, now: DateTime<Utc>) -> ExecutionId {
        let execution_id = ExecutionId::generate();
        db_pool
            .connection()
            .await
            .unwrap()
            .create(CreateRequest {
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
                max_persisted_value_size_bytes: u64::MAX,
            })
            .await
            .unwrap();
        execution_id
    }

    async fn lock(
        db_pool: &Arc<dyn DbPool>,
        component_id: &ComponentId,
        now: DateTime<Utc>,
    ) -> concepts::storage::LockedExecution {
        let mut locked = db_pool
            .connection()
            .await
            .unwrap()
            .lock_pending_by_component_digest(
                1,
                now,
                component_id,
                DEPLOYMENT_ID_DUMMY,
                now,
                ExecutorId::generate(),
                now + chrono::Duration::seconds(60),
                RunId::generate(),
                concepts::ComponentRetryConfig::ZERO,
            )
            .await
            .unwrap();
        assert_eq!(1, locked.len());
        locked.pop().unwrap()
    }

    /// Lock the seed through the DB like the executor does and run one tick.
    async fn tick(worker: &CronWorker, db_pool: &Arc<dyn DbPool>, now: DateTime<Utc>) {
        let locked = lock(db_pool, &make_cron_component_id(), now).await;
        let mut ctx = make_worker_context(locked.execution_id, locked.next_version, now);
        ctx.event_history = locked.event_history;
        worker.run(ctx).await.unwrap();
    }

    async fn scheduled_children(
        db_pool: &Arc<dyn DbPool>,
        execution_id: &ExecutionId,
    ) -> Vec<ExecutionId> {
        let log = db_pool
            .connection()
            .await
            .unwrap()
            .get(execution_id)
            .await
            .unwrap();
        log.events
            .into_iter()
            .filter_map(|event| match event.event {
                ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::Schedule { execution_id, .. },
                } => Some(execution_id),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn skip_overlap_waits_for_previous_execution_to_finish() {
        let t0 = test_time();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let sim_clock = SimClock::new(t0);
        let worker = make_worker_with_overlap(
            db_pool.clone(),
            parse_cron("0 * * * *"),
            CronOverlap::Skip,
            sim_clock.clone(),
        );
        let seed = create_seed(&db_pool, t0).await;

        tick(&worker, &db_pool, t0).await;
        let children = scheduled_children(&db_pool, &seed).await;
        assert_eq!(1, children.len());

        sim_clock.move_time_to(t1);
        tick(&worker, &db_pool, t1).await;
        assert_eq!(children, scheduled_children(&db_pool, &seed).await);
        let conn = db_pool.connection().await.unwrap();
        let seed_log = conn.get(&seed).await.unwrap();
        assert!(
            matches!(
                seed_log.pending_state,
                PendingState::PendingAt(PendingStatePendingAt { scheduled_at, .. }) if scheduled_at == t2
            ),
            "skipped tick must reschedule, got: {:?}",
            seed_log.pending_state
        );

        let child = lock(&db_pool, &make_target_component_id(), t1).await;
        conn.append(
            child.execution_id,
            child.next_version,
            AppendRequest {
                created_at: t1,
                event: ExecutionRequest::Finished {
                    retval: concepts::SupportedFunctionReturnValue::Ok(None),
                    http_client_traces: None,
                },
            },
        )
        .await
        .unwrap();

        sim_clock.move_time_to(t2);
        tick(&worker, &db_pool, t2).await;
        assert_eq!(2, scheduled_children(&db_pool, &seed).await.len());
    }

    #[tokio::test]
    async fn allow_overlap_schedules_while_previous_execution_is_running() {
        let t0 = test_time();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 15, 11, 0, 0).unwrap();
        let (_guard, db_pool, _db_close) = db_tests::Database::Sqlite.set_up().await;
        let sim_clock = SimClock::new(t0);
        let worker = make_worker_with_overlap(
            db_pool.clone(),
            parse_cron("0 * * * *"),
            CronOverlap::Allow,
            sim_clock.clone(),
        );
        let seed = create_seed(&db_pool, t0).await;

        tick(&worker, &db_pool, t0).await;
        sim_clock.move_time_to(t1);
        tick(&worker, &db_pool, t1).await;
        assert_eq!(2, scheduled_children(&db_pool, &seed).await.len());
    }
}
