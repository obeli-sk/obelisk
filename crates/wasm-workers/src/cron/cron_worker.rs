use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::DeploymentId;
use concepts::storage::{
    AppendRequest, CreateRequest, CronAdvanceRequest, DbConnection, DbErrorRead, ExecutionRequest,
    HistoryEvent, HistoryEventScheduleAt, Unlocked,
};
use concepts::time::ClockFn;
use concepts::{
    ComponentId, ExecutionId, ExecutionMetadata, FunctionFqn, FunctionMetadata, Name, Params,
};
use executor::worker::{Worker, WorkerContext, WorkerResult, WorkerResultOk};
use std::sync::Arc;
use tracing::debug;

const TRACKER_EVENT_WATERMARK: u32 = 1_000;

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

impl CronOrOnce {
    pub fn next_occurrence(
        &self,
        after: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, croner::errors::CronError> {
        match self {
            CronOrOnce::Once => Ok(None),
            CronOrOnce::Cron(cron) => cron.find_next_occurrence(&after, false).map(Some),
        }
    }
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
        self.cron_schedule.next_occurrence(after).map_err(|err| {
            executor::worker::FatalError::ConstraintViolation {
                reason: format!("cron next occurrence error: {err}").into(),
            }
        })
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
    previous: Option<&ExecutionId>,
    db_connection: &dyn DbConnection,
) -> Result<Option<ExecutionId>, executor::worker::WorkerError> {
    let Some(previous) = previous else {
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

        let record = db_connection
            .get_cron(&self.component_id.component_digest)
            .await
            .map_err(|err| executor::worker::WorkerError::DbError(err.into()))?;
        let running = if self.overlap == CronOverlap::Skip && next_fire.is_some() {
            previous_unfinished(
                record.last_target_execution_id.as_ref(),
                db_connection.as_ref(),
            )
            .await?
        } else {
            None
        };
        let rollover =
            next_fire.is_some() && version.0.saturating_add(2) >= TRACKER_EVENT_WATERMARK;
        let mut batch = Vec::new();
        let target = if running.is_none() {
            let execution_id = ExecutionId::generate();
            batch.push(AppendRequest {
                created_at: now,
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::Schedule {
                        execution_id: execution_id.clone(),
                        schedule_at: HistoryEventScheduleAt::Now,
                        params_hash: Some(concepts::persisted_value::compact_json_sha256(
                            &self.params,
                        )),
                        result: Ok(()),
                    },
                },
            });
            Some(CreateRequest {
                created_at: now,
                execution_id,
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
            })
        } else {
            debug!(
                ?running,
                ?next_fire,
                "Skipping tick, previous execution is not finished"
            );
            None
        };
        let successor = next_fire
            .filter(|_| rollover)
            .map(|scheduled_at| CreateRequest {
                created_at: now,
                execution_id: ExecutionId::generate(),
                ffqn: cron_ffqn(&self.target_ffqn),
                params: Params::empty(),
                parent: None,
                scheduled_at,
                component_id: self.component_id.clone(),
                deployment_id: self.deployment_id,
                metadata: ctx.metadata,
                scheduled_by: None,
                paused: false,
                max_persisted_value_size_bytes,
            });
        let tracking_execution_id = match next_fire {
            Some(_) if rollover => successor.as_ref().map(|r| r.execution_id.clone()),
            Some(_) => Some(current_execution_id.clone()),
            None => None,
        };
        let event = match next_fire {
            Some(unlocked_at) if !rollover => ExecutionRequest::Unlocked(Unlocked {
                unlocked_at,
                reason: running.map_or_else(
                    || "cron".into(),
                    |running| format!("cron tick skipped, `{running}` is not finished").into(),
                ),
            }),
            _ => ExecutionRequest::Finished {
                retval: concepts::SupportedFunctionReturnValue::Ok(None),
                http_client_traces: None,
                http_response: None,
            },
        };
        batch.push(AppendRequest {
            created_at: now,
            event,
        });
        db_connection
            .advance_cron(CronAdvanceRequest {
                cron_digest: self.component_id.component_digest.clone(),
                execution_id: current_execution_id,
                version,
                current_time: now,
                batch,
                target,
                successor,
                tracking_execution_id,
            })
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

    fn tracker_request(execution_id: ExecutionId, now: DateTime<Utc>) -> CreateRequest {
        CreateRequest {
            created_at: now,
            execution_id,
            ffqn: cron_ffqn(&TARGET_FFQN),
            params: Params::empty(),
            parent: None,
            scheduled_at: now,
            component_id: make_cron_component_id(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            metadata: ExecutionMetadata::empty(),
            scheduled_by: None,
            paused: false,
            max_persisted_value_size_bytes: MAX_PERSISTED_VALUE_SIZE_BYTES,
        }
    }

    async fn collect_executions(db_pool: &Arc<dyn DbPool>) {
        let admin = db_pool.admin_conn().await.unwrap();
        while admin.gc_executions(10, None).await.unwrap().has_more {}
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cron_advance_rolls_back_and_rejects_stale_tracker(
        #[values(db_tests::Database::Sqlite, db_tests::Database::Postgres)]
        database: db_tests::Database,
    ) {
        use concepts::storage::DbPoolCloseable as _;
        let (_guard, db_pool, close) = database.set_up().await;
        let now = test_time();
        let seed = create_seed(&db_pool, now).await;
        let locked = lock(&db_pool, &make_cron_component_id(), now).await;
        let conn = db_pool.connection().await.unwrap();
        let digest = make_cron_component_id().component_digest;
        let original = conn.get_cron(&digest).await.unwrap();
        let mut target = tracker_request(seed.clone(), now);
        target.component_id = make_target_component_id();
        target.ffqn = TARGET_FFQN;
        let mut request = CronAdvanceRequest {
            cron_digest: digest.clone(),
            execution_id: seed.clone(),
            version: locked.next_version,
            current_time: now,
            batch: vec![AppendRequest {
                created_at: now,
                event: ExecutionRequest::Finished {
                    retval: concepts::SupportedFunctionReturnValue::Ok(None),
                    http_client_traces: None,
                    http_response: None,
                },
            }],
            target: Some(target),
            successor: None,
            tracking_execution_id: None,
        };
        assert!(conn.advance_cron(request.clone()).await.is_err());
        assert_eq!(conn.get_cron(&digest).await.unwrap(), original);
        let log = conn.get(&seed).await.unwrap();
        assert!(!log.pending_state.is_finished());
        assert_eq!(log.events.len(), 2);
        request.target.as_mut().unwrap().execution_id = ExecutionId::generate();
        conn.advance_cron(request.clone()).await.unwrap();
        let submitted = conn.get_cron(&digest).await.unwrap();
        assert_eq!(
            submitted.last_target_execution_id,
            request.target.as_ref().map(|r| r.execution_id.clone())
        );
        let stale_target = ExecutionId::generate();
        request.target.as_mut().unwrap().execution_id = stale_target.clone();
        assert!(conn.advance_cron(request).await.is_err());
        assert_eq!(conn.get_cron(&digest).await.unwrap(), submitted);
        assert!(conn.get(&stale_target).await.is_err());
        drop(conn);
        close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cron_registration_is_atomic(
        #[values(db_tests::Database::Sqlite, db_tests::Database::Postgres)]
        database: db_tests::Database,
    ) {
        use concepts::storage::DbPoolCloseable as _;
        let (_guard, db_pool, close) = database.set_up().await;
        let mut tasks = tokio::task::JoinSet::new();
        let requests: Vec<_> = (0..8)
            .map(|_| tracker_request(ExecutionId::generate(), test_time()))
            .collect();
        for request in requests.clone() {
            let db_pool = db_pool.clone();
            tasks.spawn(async move {
                db_pool
                    .connection()
                    .await
                    .unwrap()
                    .register_cron(request, false)
                    .await
                    .unwrap()
            });
        }
        let conn = db_pool.connection().await.unwrap();
        let mut records = Vec::new();
        while let Some(result) = tasks.join_next().await {
            records.push(result.unwrap());
        }
        assert!(records.iter().all(|record| record == &records[0]));
        let id = records[0].tracking_execution_id.as_ref().unwrap();
        assert!(id.get_top_level().timestamp_part() > 0);
        for request in requests {
            assert_eq!(
                conn.get(&request.execution_id).await.is_ok(),
                &request.execution_id == id
            );
        }
        drop(conn);
        close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn once_stays_submitted_after_gc_and_registration(
        #[values(db_tests::Database::Sqlite, db_tests::Database::Postgres)]
        database: db_tests::Database,
    ) {
        use concepts::storage::{DbPoolCloseable as _, DeleteExecutionTreeResult};
        let (_guard, db_pool, close) = database.set_up().await;
        let now = test_time();
        let worker = make_worker(db_pool.clone(), CronOrOnce::Once, SimClock::new(now));
        let seed = create_seed(&db_pool, now).await;
        tick(&worker, &db_pool, now).await;
        let conn = db_pool.connection().await.unwrap();
        let digest = make_cron_component_id().component_digest;
        let record = conn.get_cron(&digest).await.unwrap();
        assert_eq!(record.tracking_execution_id, None);
        assert_eq!(record.last_submitted_at, Some(now));
        let admin = db_pool.admin_conn().await.unwrap();
        assert_eq!(
            admin.delete_execution_tree(&seed, false).await.unwrap(),
            DeleteExecutionTreeResult::Deleted
        );
        let target = lock(&db_pool, &make_target_component_id(), now).await;
        conn.append(
            target.execution_id.clone(),
            target.next_version,
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::Finished {
                    retval: concepts::SupportedFunctionReturnValue::Ok(None),
                    http_client_traces: None,
                    http_response: None,
                },
            },
        )
        .await
        .unwrap();
        admin
            .delete_execution_tree(&target.execution_id, false)
            .await
            .unwrap();
        collect_executions(&db_pool).await;
        assert!(conn.get(&seed).await.is_err());
        assert!(conn.get(&target.execution_id).await.is_err());
        let request = tracker_request(ExecutionId::generate(), now + chrono::Duration::days(1));
        assert_eq!(
            conn.register_cron(request.clone(), true).await.unwrap(),
            record
        );
        assert!(conn.get(&request.execution_id).await.is_err());
        drop(admin);
        drop(conn);
        close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cron_adopts_legacy_tracker_on_later_registration(
        #[values(db_tests::Database::Sqlite, db_tests::Database::Postgres)]
        database: db_tests::Database,
        #[values(false, true)] once: bool,
        #[values(false, true)] submitted: bool,
    ) {
        use concepts::storage::{
            DbPoolCloseable as _, DeleteDeploymentResult, DeleteExecutionTreeResult,
            DeploymentRecord, DeploymentStatus, RetentionPolicy,
        };
        let (_guard, db_pool, close) = database.set_up().await;
        let now = test_time();
        let digest = make_cron_component_id().component_digest;
        let legacy_id = ExecutionId::deterministic_at_unix_epoch(&digest.0.0);
        db_pool
            .external_api_conn()
            .await
            .unwrap()
            .insert_deployment_with_components(
                DeploymentRecord {
                    deployment_id: DEPLOYMENT_ID_DUMMY,
                    description: None,
                    digest: DeploymentRecord::compute_digest("{}"),
                    created_at: now,
                    updated_at: now,
                    last_active_at: None,
                    last_active_app_config_digest: None,
                    status: DeploymentStatus::Inactive,
                    deployment_toml: "{}".into(),
                    obelisk_version: "0.43.0-rc.11".into(),
                    created_by: None,
                    files: vec![],
                },
                vec![],
                vec![],
                vec![],
            )
            .await
            .unwrap();
        let conn = db_pool.connection().await.unwrap();
        let version = conn
            .create(tracker_request(legacy_id.clone(), now))
            .await
            .unwrap();
        let target = ExecutionId::generate();
        let submitted_at = now + chrono::Duration::seconds(1);
        if submitted {
            let version = conn
                .append(
                    legacy_id.clone(),
                    version,
                    AppendRequest {
                        created_at: submitted_at,
                        event: ExecutionRequest::Locked(make_locked_event(now)),
                    },
                )
                .await
                .unwrap();
            conn.append_batch(
                now,
                vec![
                    AppendRequest {
                        created_at: submitted_at,
                        event: ExecutionRequest::HistoryEvent {
                            event: HistoryEvent::Schedule {
                                execution_id: target.clone(),
                                schedule_at: HistoryEventScheduleAt::Now,
                                params_hash: None,
                                result: Ok(()),
                            },
                        },
                    },
                    AppendRequest {
                        created_at: submitted_at,
                        event: if once {
                            ExecutionRequest::Finished {
                                retval: concepts::SupportedFunctionReturnValue::Ok(None),
                                http_client_traces: None,
                                http_response: None,
                            }
                        } else {
                            ExecutionRequest::Unlocked(Unlocked {
                                unlocked_at: now + chrono::Duration::hours(1),
                                reason: "cron".into(),
                            })
                        },
                    },
                ],
                legacy_id.clone(),
                version,
            )
            .await
            .unwrap();
        }
        let original = conn.get(&legacy_id).await.unwrap();
        let admin = db_pool.admin_conn().await.unwrap();
        assert_eq!(
            admin.delete_execution_tree(&legacy_id, true).await.unwrap(),
            DeleteExecutionTreeResult::CronTracker
        );
        let retained = admin
            .retain_executions(RetentionPolicy::Count(0), 10, true, false, None)
            .await
            .unwrap();
        assert_eq!(retained.deleted_execution_trees, 0);
        assert!(matches!(
            admin
                .delete_deployment(DEPLOYMENT_ID_DUMMY, true, true)
                .await
                .unwrap(),
            DeleteDeploymentResult::Referenced { .. }
        ));
        for dry_run in [true, false] {
            let retained = admin
                .retain_deployments(RetentionPolicy::Count(0), 10, true, true, dry_run, None)
                .await
                .unwrap();
            assert_eq!(retained.deleted_deployments, 0);
            assert_eq!(retained.deleted_execution_trees, 0);
        }
        collect_executions(&db_pool).await;
        let mut unrelated = tracker_request(ExecutionId::generate(), now);
        unrelated.component_id = ComponentId::new(
            concepts::ComponentType::Cron,
            StrVariant::Static("other"),
            ComponentDigest(Digest([2; 32])),
        )
        .unwrap();
        conn.register_cron(unrelated, false).await.unwrap();
        let request = tracker_request(ExecutionId::generate(), now + chrono::Duration::days(1));
        let record = conn.register_cron(request.clone(), once).await.unwrap();
        assert_eq!(record.created_at, now);
        assert_eq!(record.last_submitted_at, submitted.then_some(submitted_at));
        assert_eq!(record.last_target_execution_id, submitted.then_some(target));
        assert_eq!(
            record.tracking_execution_id,
            (!(once && submitted)).then_some(legacy_id.clone())
        );
        let adopted = conn.get(&legacy_id).await.unwrap();
        assert_eq!(adopted.events, original.events);
        assert_eq!(adopted.pending_state, original.pending_state);
        assert!(conn.get(&request.execution_id).await.is_err());
        if once && submitted {
            assert_eq!(
                admin
                    .delete_execution_tree(&legacy_id, false)
                    .await
                    .unwrap(),
                DeleteExecutionTreeResult::Deleted
            );
            collect_executions(&db_pool).await;
        }
        assert_eq!(conn.register_cron(request, once).await.unwrap(), record);
        drop(admin);
        drop(conn);
        close.close().await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn cron_rollover_preserves_overlap_after_gc(
        #[values(db_tests::Database::Sqlite, db_tests::Database::Postgres)]
        database: db_tests::Database,
        #[values(CronOverlap::Skip, CronOverlap::Allow)] overlap: CronOverlap,
    ) {
        use concepts::storage::{DbPoolCloseable as _, DeleteExecutionTreeResult};
        let (_guard, db_pool, close) = database.set_up().await;
        let mut now = test_time();
        let clock = SimClock::new(now);
        let worker = make_worker_with_overlap(
            db_pool.clone(),
            parse_cron("* * * * * *"),
            overlap,
            clock.clone(),
        );
        let seed = create_seed(&db_pool, now).await;
        let conn = db_pool.connection().await.unwrap();
        let digest = make_cron_component_id().component_digest;
        let mut record = conn.get_cron(&digest).await.unwrap();
        for _ in 0..TRACKER_EVENT_WATERMARK {
            tick(&worker, &db_pool, now).await;
            record = conn.get_cron(&digest).await.unwrap();
            now += chrono::Duration::seconds(1);
            clock.move_time_to(now);
            if record.tracking_execution_id.as_ref() != Some(&seed) {
                break;
            }
        }
        let successor = record.tracking_execution_id.clone().unwrap();
        assert_ne!(successor, seed);
        let old = conn.get(&seed).await.unwrap();
        assert!(old.pending_state.is_finished());
        assert!(old.events.len() <= TRACKER_EVENT_WATERMARK as usize + 2);
        let next = conn.get(&successor).await.unwrap();
        assert_eq!(next.events.len(), 1);
        assert!(
            matches!(next.pending_state, PendingState::PendingAt(PendingStatePendingAt { scheduled_at, .. }) if scheduled_at == now)
        );
        let admin = db_pool.admin_conn().await.unwrap();
        assert_eq!(
            admin.delete_execution_tree(&seed, false).await.unwrap(),
            DeleteExecutionTreeResult::Deleted
        );
        assert_eq!(
            admin.delete_execution_tree(&successor, true).await.unwrap(),
            DeleteExecutionTreeResult::CronTracker
        );
        collect_executions(&db_pool).await;
        assert!(conn.get(&seed).await.is_err());
        tick(&worker, &db_pool, now).await;
        let after_tick = conn.get_cron(&digest).await.unwrap();
        if overlap == CronOverlap::Skip {
            assert_eq!(
                after_tick.last_target_execution_id,
                record.last_target_execution_id
            );
            assert_eq!(after_tick.last_submitted_at, record.last_submitted_at);
            let target = lock(&db_pool, &make_target_component_id(), now).await;
            conn.append(
                target.execution_id.clone(),
                target.next_version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::Finished {
                        retval: concepts::SupportedFunctionReturnValue::Ok(None),
                        http_client_traces: None,
                        http_response: None,
                    },
                },
            )
            .await
            .unwrap();
            admin
                .delete_execution_tree(&target.execution_id, false)
                .await
                .unwrap();
            collect_executions(&db_pool).await;
            now += chrono::Duration::seconds(1);
            clock.move_time_to(now);
            tick(&worker, &db_pool, now).await;
            assert_ne!(
                conn.get_cron(&digest)
                    .await
                    .unwrap()
                    .last_target_execution_id,
                record.last_target_execution_id
            );
        } else {
            assert_ne!(
                after_tick.last_target_execution_id,
                record.last_target_execution_id
            );
            assert_eq!(after_tick.last_submitted_at, Some(now));
        }
        drop(admin);
        drop(conn);
        close.close().await;
    }

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
        conn.register_cron(tracker_request(execution_id.clone(), now), false)
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
        db_pool
            .connection()
            .await
            .unwrap()
            .register_cron(tracker_request(ExecutionId::generate(), now), false)
            .await
            .unwrap()
            .tracking_execution_id
            .unwrap()
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
                    http_response: None,
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
