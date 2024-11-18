//! Append only database containing executions and their state changes - execution journal.
//! Current [`PendingState`] can be obtained by reading last (few) events.
//!
//! When inserting, the row in the journal must contain a version that must be equal
//! to the current number of events in the journal. First change with the expected version wins.
use self::index::JournalsIndex;
use crate::journal::ExecutionJournal;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ExecutorId, JoinSetId, RunId};
use concepts::storage::{
    AppendBatchResponse, AppendRequest, AppendResponse, ClientError, CreateRequest, DbConnection,
    DbConnectionError, DbError, DbPool, ExecutionEvent, ExecutionEventInner, ExecutionLog,
    ExecutionWithState, ExpiredTimer, JoinSetResponseEventOuter, LockPendingResponse, LockResponse,
    LockedExecution, Pagination, SpecificError, Version, VersionType,
};
use concepts::storage::{JoinSetResponseEvent, PendingState};
use concepts::{ConfigId, ExecutionId, FinishedExecutionResult, FunctionFqn, StrVariant};
use hashbrown::{HashMap, HashSet};
use itertools::Either;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use tracing::instrument;

pub struct InMemoryDbConnection(Arc<tokio::sync::Mutex<DbHolder>>);

#[async_trait]
impl DbConnection for InMemoryDbConnection {
    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        self.0.lock().await.create(req).map_err(DbError::Specific)
    }

    #[instrument(skip_all)]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
        Ok(self.0.lock().await.lock_pending(
            batch_size,
            pending_at_or_sooner,
            &ffqns,
            created_at,
            &config_id,
            executor_id,
            lock_expires_at,
        ))
    }

    #[instrument(skip_all)]
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        Ok(self.0.lock().await.get_expired_timers(at))
    }

    #[instrument(skip_all, %execution_id)]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        self.0
            .lock()
            .await
            .lock(
                created_at,
                config_id,
                execution_id,
                run_id,
                version,
                executor_id,
                lock_expires_at,
            )
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append(
        &self,
        execution_id: ExecutionId,
        appending_version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        self.0
            .lock()
            .await
            .append(req.created_at, &execution_id, appending_version, req.event)
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_batch(
        &self,
        _created_at: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        appending_version: Version,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .lock()
            .await
            .append_batch(batch, &execution_id, appending_version)
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_batch_create_new_execution(
        &self,
        _current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .lock()
            .await
            .append_batch_create_child(batch, &execution_id, version, child_req)
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_batch_respond_to_parent(
        &self,
        execution_id: ExecutionId,
        _created_at: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<AppendBatchResponse, DbError> {
        self.0
            .lock()
            .await
            .append_batch_respond_to_parent(
                &execution_id,
                batch,
                version,
                &parent_execution_id,
                parent_response_event,
            )
            .map_err(DbError::Specific)
    }

    #[cfg(feature = "test")]
    #[instrument(skip_all, %execution_id)]
    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbError> {
        self.0
            .lock()
            .await
            .get(execution_id)
            .map_err(DbError::Specific)
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbError> {
        let execution_log = self.0.lock().await.get(execution_id)?;
        Ok(execution_log
            .events
            .get(usize::try_from(version.0).unwrap())
            .cloned()
            .ok_or(SpecificError::NotFound)?)
    }

    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbError> {
        let either = self
            .0
            .lock()
            .await
            .subscribe_to_next_responses(execution_id, start_idx)?;
        // unlock
        match either {
            Either::Left(resp) => Ok(resp),
            Either::Right(receiver) => receiver
                .await
                .map(|it| vec![it])
                .map_err(|_| DbError::Connection(DbConnectionError::RecvError)),
        }
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbError> {
        self.0
            .lock()
            .await
            .append_response(
                &execution_id,
                JoinSetResponseEventOuter {
                    created_at,
                    event: response_event,
                },
            )
            .map_err(DbError::Specific)
    }

    async fn subscribe_to_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        max_wait: Duration,
    ) {
        let either = self
            .0
            .lock()
            .await
            .subscribe_to_pending(pending_at_or_sooner, &ffqns);
        // unlock
        match either {
            Either::Left(()) => {} // Got results imediately
            Either::Right(mut receiver) => {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    _ = receiver.recv() => {} // Got results eventually
                    () = tokio::time::sleep(max_wait) => {} // Timeout
                }
            }
        }
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout: Option<Duration>,
    ) -> Result<FinishedExecutionResult, ClientError> {
        let execution_log = {
            let fut = async move {
                loop {
                    let execution_log = self
                        .0
                        .lock()
                        .await
                        .get(execution_id)
                        .map_err(DbError::Specific)?;
                    if execution_log.pending_state.is_finished() {
                        return Ok(execution_log);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            if let Some(timeout) = timeout {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    res = fut => res,
                    () = tokio::time::sleep(timeout) => Err(ClientError::Timeout)
                }
            } else {
                fut.await
            }
        }?;
        Ok(execution_log
            .into_finished_result()
            .expect("pending state was checked"))
    }

    async fn get_pending_state(&self, execution_id: &ExecutionId) -> Result<PendingState, DbError> {
        Ok(self
            .0
            .lock()
            .await
            .get(execution_id)
            .map_err(DbError::Specific)?
            .pending_state)
    }

    async fn list_executions(
        &self,
        _ffqn: Option<FunctionFqn>,
        _pagination: Pagination<DateTime<Utc>>,
    ) -> Result<Vec<ExecutionWithState>, DbError> {
        unimplemented!("only needed for gRPC")
    }

    async fn list_execution_events(
        &self,
        _execution_id: &ExecutionId,
        _since: &Version,
        _max_length: VersionType,
    ) -> Result<Vec<ExecutionEvent>, DbError> {
        unimplemented!("only needed for gRPC")
    }
}

mod index {
    use super::{BTreeMap, DateTime, ExecutionId, HashMap, HashSet, JoinSetId, PendingState, Utc};
    use crate::journal::ExecutionJournal;
    use concepts::prefixed_ulid::DelayId;
    use concepts::storage::{HistoryEvent, JoinSetRequest, JoinSetResponse};
    use tracing::trace;

    #[derive(Debug, Default)]
    pub(super) struct JournalsIndex {
        pending_scheduled: BTreeMap<DateTime<Utc>, HashSet<ExecutionId>>,
        pending_scheduled_rev: HashMap<ExecutionId, DateTime<Utc>>,
        #[expect(clippy::type_complexity)]
        // All open JoinSet Delays and Locks
        timers: BTreeMap<DateTime<Utc>, HashMap<ExecutionId, Option<(JoinSetId, DelayId)>>>,
        timers_rev: HashMap<ExecutionId, Vec<DateTime<Utc>>>,
    }

    impl JournalsIndex {
        pub(super) fn fetch_pending<'a>(
            &self,
            journals: &'a BTreeMap<ExecutionId, ExecutionJournal>,
            batch_size: usize,
            expiring_at_or_before: DateTime<Utc>,
            ffqns: &[concepts::FunctionFqn],
        ) -> Vec<(&'a ExecutionJournal, DateTime<Utc>)> {
            let mut pending = self
                .pending_scheduled
                .range(..=expiring_at_or_before)
                .flat_map(|(scheduled_at, ids)| {
                    ids.iter()
                        .map(|id| (journals.get(id).unwrap(), *scheduled_at))
                })
                .collect::<Vec<_>>();
            // filter by ffqn
            pending.retain(|(journal, _)| ffqns.contains(journal.ffqn()));
            pending.truncate(batch_size);
            pending
        }

        pub(super) fn fetch_expired(
            &self,
            at: DateTime<Utc>,
        ) -> impl Iterator<Item = (ExecutionId, Option<(JoinSetId, DelayId)>)> + '_ {
            self.timers
                .range(..=at)
                .flat_map(|(_scheduled_at, id_map)| id_map.iter())
                .map(|(id, is_async_delay)| (id.clone(), *is_async_delay))
        }

        fn purge(&mut self, execution_id: &ExecutionId) {
            // Remove the ID from the index (if exists)
            if let Some(schedule) = self.pending_scheduled_rev.remove(execution_id) {
                let ids = self.pending_scheduled.get_mut(&schedule).unwrap();
                ids.remove(execution_id);
            }
            if let Some(schedules) = self.timers_rev.remove(execution_id) {
                for schedule in schedules {
                    let ids = self.timers.get_mut(&schedule).unwrap();
                    ids.remove(execution_id);
                }
            }
        }

        pub(super) fn update(&mut self, journal: &mut ExecutionJournal) {
            let execution_id = &journal.execution_id;
            self.purge(execution_id);
            // Add it again if needed
            match journal.pending_state {
                PendingState::PendingAt { scheduled_at } => {
                    self.pending_scheduled
                        .entry(scheduled_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.pending_scheduled_rev
                        .insert(execution_id.clone(), scheduled_at);
                }
                PendingState::Locked {
                    lock_expires_at, ..
                } => {
                    self.timers
                        .entry(lock_expires_at)
                        .or_default()
                        .insert(execution_id.clone(), None);
                    self.timers_rev
                        .entry(execution_id.clone())
                        .or_default()
                        .push(lock_expires_at);
                }
                PendingState::BlockedByJoinSet { .. } | PendingState::Finished { .. } => {}
            }
            // Add all open async timers
            let mut delay_req_resp = journal
                .event_history()
                .filter_map(|e| match e {
                    HistoryEvent::JoinSetRequest {
                        join_set_id,
                        request:
                            JoinSetRequest::DelayRequest {
                                delay_id,
                                expires_at,
                            },
                    } => Some(((join_set_id, delay_id), expires_at)),
                    _ => None,
                })
                .collect::<HashMap<_, _>>();
            // Keep only open
            for responded in journal.responses.iter().filter_map(|e| {
                if let JoinSetResponse::DelayFinished { delay_id } = e.event.event {
                    Some((e.event.join_set_id, delay_id))
                } else {
                    None
                }
            }) {
                delay_req_resp.remove(&responded);
            }

            for ((join_set_id, delay_id), expires_at) in delay_req_resp {
                self.timers
                    .entry(expires_at)
                    .or_default()
                    .insert(execution_id.clone(), Some((join_set_id, delay_id)));
                self.timers_rev
                    .entry(execution_id.clone())
                    .or_default()
                    .push(expires_at);
            }
            trace!("Journal index updated: {self:?}");
        }
    }
}

#[derive(Clone)]
pub struct InMemoryPool(Arc<tokio::sync::Mutex<DbHolder>>, Arc<AtomicBool>);

impl Default for InMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryPool {
    #[must_use]
    pub fn new() -> Self {
        Self(
            Arc::new(tokio::sync::Mutex::new(DbHolder {
                journals: BTreeMap::default(),
                index: JournalsIndex::default(),
                ffqn_to_pending_subscription: hashbrown::HashMap::default(),
            })),
            Arc::new(AtomicBool::default()),
        )
    }
}

#[async_trait]
impl DbPool<InMemoryDbConnection> for InMemoryPool {
    fn connection(&self) -> InMemoryDbConnection {
        InMemoryDbConnection(self.0.clone())
    }

    fn is_closing(&self) -> bool {
        self.1.load(Ordering::Relaxed)
    }

    async fn close(&self) -> Result<(), DbError> {
        let res = self
            .1
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);
        if res.is_err() {
            return Err(DbError::Specific(SpecificError::GenericError(
                StrVariant::Static("already closed"),
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct DbHolder {
    journals: BTreeMap<ExecutionId, ExecutionJournal>,
    index: JournalsIndex,
    ffqn_to_pending_subscription: hashbrown::HashMap<FunctionFqn, mpsc::Sender<()>>,
}

impl DbHolder {
    #[expect(clippy::too_many_arguments)]
    fn lock_pending(
        &mut self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: &[FunctionFqn],
        created_at: DateTime<Utc>,
        config_id: &ConfigId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> LockPendingResponse {
        let pending =
            self.index
                .fetch_pending(&self.journals, batch_size, pending_at_or_sooner, ffqns);
        let mut payload = Vec::with_capacity(pending.len());
        for (journal, scheduled_at) in pending {
            let item = LockedExecution {
                execution_id: journal.execution_id().clone(),
                metadata: journal.metadata().clone(),
                version: journal.version(), // updated later
                ffqn: journal.ffqn().clone(),
                params: journal.params(),
                event_history: Vec::default(), // updated later
                responses: journal.responses.clone(),
                scheduled_at,
                retry_exp_backoff: journal.retry_exp_backoff(),
                max_retries: journal.max_retries(),
                run_id: RunId::generate(),
                parent: journal.parent(),
                intermittent_event_count: journal.intermittent_event_count(),
            };
            payload.push(item);
        }
        // Lock, update the version and event history.
        for row in &mut payload {
            let (new_event_history, new_version) = self
                .lock(
                    created_at,
                    config_id.clone(),
                    &row.execution_id,
                    row.run_id,
                    row.version.clone(),
                    executor_id,
                    lock_expires_at,
                )
                .expect("must be lockable within the same transaction");
            row.version = new_version;
            row.event_history.extend(new_event_history);
        }
        payload
    }

    fn create(&mut self, req: CreateRequest) -> Result<AppendResponse, SpecificError> {
        if self.journals.contains_key(&req.execution_id) {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "execution already exists with the same id",
            )));
        }
        let subscription = self.ffqn_to_pending_subscription.get(&req.ffqn);
        let scheduled_at = req.scheduled_at;
        let created_at = req.created_at;
        let mut journal = ExecutionJournal::new(req);
        let version = journal.version();
        self.index.update(&mut journal);
        let old_val = self.journals.insert(journal.execution_id.clone(), journal);
        assert!(
            old_val.is_none(),
            "journals cannot contain the new execution"
        );
        if scheduled_at <= created_at {
            if let Some(subscription) = subscription {
                let _ = subscription.try_send(());
            }
        }
        Ok(version)
    }

    #[expect(clippy::too_many_arguments)]
    fn lock(
        &mut self,
        created_at: DateTime<Utc>,
        config_id: ConfigId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, SpecificError> {
        let event = ExecutionEventInner::Locked {
            config_id,
            executor_id,
            lock_expires_at,
            run_id,
        };
        self.append(created_at, execution_id, version, event)
            .map(|_| {
                let journal = self.journals.get(execution_id).unwrap();
                (journal.event_history().collect(), journal.version())
            })
    }

    fn append(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: &ExecutionId,
        appending_version: Version,
        event: ExecutionEventInner,
    ) -> Result<AppendResponse, SpecificError> {
        // Disallow `Created` event
        if let ExecutionEventInner::Created { .. } = event {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        // Check version
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(SpecificError::NotFound);
        };
        let expected_version = journal.version();
        if appending_version != expected_version {
            return Err(SpecificError::VersionMismatch {
                appending_version,
                expected_version,
            });
        }
        let new_version = journal.append(created_at, event)?;
        self.index.update(journal);
        if matches!(journal.pending_state, PendingState::PendingAt { .. }) {
            if let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn()) {
                let _ = subscription.try_send(());
            }
        }
        Ok(new_version)
    }

    fn get(&mut self, execution_id: &ExecutionId) -> Result<ExecutionLog, SpecificError> {
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(SpecificError::NotFound);
        };
        Ok(journal.as_execution_log())
    }

    fn get_expired_timers(&mut self, at: DateTime<Utc>) -> Vec<ExpiredTimer> {
        let expired = self.index.fetch_expired(at);
        let mut vec = Vec::new();
        for (execution_id, is_async_timer) in expired {
            let journal = self.journals.get(&execution_id).unwrap();
            vec.push(match is_async_timer {
                Some((join_set_id, delay_id)) => ExpiredTimer::AsyncDelay {
                    execution_id,
                    join_set_id,
                    delay_id,
                },
                None => ExpiredTimer::Lock {
                    execution_id: journal.execution_id().clone(),
                    version: journal.version(),
                    max_retries: journal.max_retries(),
                    intermittent_event_count: journal.intermittent_event_count(),
                    retry_exp_backoff: journal.retry_exp_backoff(),
                    parent: journal.parent(),
                },
            });
        }
        vec
    }

    fn append_batch(
        &mut self,
        batch: Vec<AppendRequest>,
        execution_id: &ExecutionId,
        mut appending_version: Version,
    ) -> Result<AppendBatchResponse, SpecificError> {
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(SpecificError::NotFound);
        };
        let truncate_len = journal.len();
        for append_request in batch {
            let expected_version = journal.version();
            if appending_version != expected_version {
                // Rollback
                journal.truncate_and_update_pending_state(truncate_len);
                self.index.update(journal);
                return Err(SpecificError::VersionMismatch {
                    appending_version,
                    expected_version,
                });
            }
            match journal.append(append_request.created_at, append_request.event) {
                Ok(new_version) => {
                    appending_version = new_version;
                }
                Err(err) => {
                    // Rollback
                    journal.truncate_and_update_pending_state(truncate_len);
                    self.index.update(journal);
                    return Err(err);
                }
            }
        }
        let version = journal.version();
        self.index.update(journal);
        if matches!(journal.pending_state, PendingState::PendingAt { .. }) {
            if let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn()) {
                let _ = subscription.try_send(());
            }
        }
        Ok(version)
    }

    fn append_batch_create_child(
        &mut self,
        batch: Vec<AppendRequest>,
        execution_id: &ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, SpecificError> {
        let parent_version = self.append_batch(batch, execution_id, version)?;
        for child_req in child_req {
            self.create(child_req)?;
        }
        Ok(parent_version)
    }

    fn append_batch_respond_to_parent(
        &mut self,
        execution_id: &ExecutionId,
        batch: Vec<AppendRequest>,
        version: Version,
        parent_execution_id: &ExecutionId,
        parent_response_event: JoinSetResponseEventOuter,
    ) -> Result<Version, SpecificError> {
        let child_version = self.append_batch(batch, execution_id, version)?;
        self.append_response(parent_execution_id, parent_response_event)?;
        Ok(child_version)
    }

    fn append_response(
        &mut self,
        execution_id: &ExecutionId,
        response_event: JoinSetResponseEventOuter,
    ) -> Result<(), SpecificError> {
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(SpecificError::NotFound);
        };
        journal.append_response(response_event.created_at, response_event.event);
        self.index.update(journal);
        if matches!(journal.pending_state, PendingState::PendingAt { .. }) {
            if let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn()) {
                let _ = subscription.try_send(());
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    fn subscribe_to_next_responses(
        &mut self,
        execution_id: &ExecutionId,
        start_idx: usize,
    ) -> Result<
        Either<Vec<JoinSetResponseEventOuter>, oneshot::Receiver<JoinSetResponseEventOuter>>,
        DbError,
    > {
        debug!("next_response");
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbError::Specific(SpecificError::NotFound));
        };
        let res_len = journal.responses.len();
        if res_len > start_idx {
            Ok(Either::Left(
                journal.responses.iter().skip(start_idx).cloned().collect(),
            ))
        } else {
            assert_eq!(
                start_idx,
                res_len,
                "next_responses: invalid `start_idx`({start_idx}) must be >= responses length ({res_len})"
            );
            let (sender, receiver) = oneshot::channel();
            journal.response_subscriber = Some(sender);
            Ok(Either::Right(receiver))
        }
    }

    fn subscribe_to_pending(
        &mut self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: &[FunctionFqn],
    ) -> Either<(), mpsc::Receiver<()>> {
        if !self
            .index
            .fetch_pending(&self.journals, 1, pending_at_or_sooner, ffqns)
            .is_empty()
        {
            return Either::Left(());
        }
        let (sender, receiver) = mpsc::channel(1);
        for ffqn in ffqns {
            self.ffqn_to_pending_subscription
                .insert(ffqn.clone(), sender.clone());
        }
        Either::Right(receiver)
    }
}
