//! Append only database containing executions and their state changes - execution journal.
//! Current [`PendingState`] can be obtained by reading last (few) events.
//!
//! When inserting, the row in the journal must contain a version that must be equal
//! to the current number of events in the journal. First change with the expected version wins.
use self::index::JournalsIndex;
use crate::journal::ExecutionJournal;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ExecutorId, RunId};
use concepts::storage::{
    AppendBatchResponse, AppendEventsToExecution, AppendRequest, AppendResponse,
    AppendResponseToExecution, BacktraceFilter, BacktraceInfo, CreateRequest, DbConnection,
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWritePermanent,
    DbExecutor, DbPool, DbPoolCloseable, ExecutionEvent, ExecutionEventInner,
    ExecutionListPagination, ExecutionLog, ExecutionWithState, ExpiredDelay, ExpiredLock,
    ExpiredTimer, HistoryEvent, JoinSetResponseEventOuter, LockPendingResponse, LockedExecution,
    Pagination, ResponseWithCursor, Version, VersionType,
};
use concepts::storage::{JoinSetResponseEvent, PendingState};
use concepts::{ComponentId, ExecutionId, FunctionFqn};
use concepts::{JoinSetId, SupportedFunctionReturnValue};
use hashbrown::{HashMap, HashSet};
use itertools::Either;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use tracing::instrument;

pub struct InMemoryDbConnection(Arc<std::sync::Mutex<DbHolder>>);

#[async_trait]
impl DbExecutor for InMemoryDbConnection {
    #[instrument(skip_all)]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
    ) -> Result<LockPendingResponse, DbErrorGeneric> {
        Ok(self.0.lock().unwrap().lock_pending(
            batch_size,
            pending_at_or_sooner,
            &ffqns,
            created_at,
            &component_id,
            executor_id,
            lock_expires_at,
            run_id,
        ))
    }

    #[instrument(skip_all, %execution_id)]
    async fn lock_one(
        &self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockedExecution, DbErrorWrite> {
        let (next_version, event_history) = self.0.lock().unwrap().lock(
            created_at,
            component_id,
            execution_id,
            run_id,
            version,
            executor_id,
            lock_expires_at,
        )?;
        let db_holder_guard = self.0.lock().unwrap();
        let journal = db_holder_guard
            .journals
            .get(execution_id)
            .expect("must exist as already locked");
        Ok(LockedExecution {
            execution_id: journal.execution_id().clone(),
            metadata: journal.metadata().clone(),
            next_version,
            ffqn: journal.ffqn().clone(),
            params: journal.params(),
            event_history,
            responses: journal.responses.clone(),
            retry_exp_backoff: journal.retry_exp_backoff(),
            max_retries: journal.max_retries(),
            run_id,
            parent: journal.parent(),
            intermittent_event_count: journal.temporary_event_count(),
        })
    }

    #[instrument(skip_all, %execution_id)]
    async fn append(
        &self,
        execution_id: ExecutionId,
        appending_version: Version,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbErrorWrite> {
        self.0
            .lock()
            .unwrap()
            .append(req.created_at, &execution_id, appending_version, req.event)
    }

    #[instrument(skip_all)]
    async fn append_batch_respond_to_parent(
        &self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
        _current_time: DateTime<Utc>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.0
            .lock()
            .unwrap()
            .append_batch_respond_to_parent(events, response)
    }

    async fn wait_for_pending(
        &self,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Arc<[FunctionFqn]>,
        timeout_fut: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) {
        let either = {
            let mut guard = self.0.lock().unwrap();
            guard.subscribe_to_pending(pending_at_or_sooner, &ffqns)
        };
        // unlocked now
        match either {
            Either::Left(()) => {} // Got results immediately
            Either::Right(mut receiver) => {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    _ = receiver.recv() => {} // Got results eventually
                    () = timeout_fut => {} // Timeout
                }
            }
        }
    }
}

#[async_trait]
impl DbConnection for InMemoryDbConnection {
    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        self.0.lock().unwrap().create(req)
    }

    #[instrument(skip_all)]
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbErrorGeneric> {
        Ok(self.0.lock().unwrap().get_expired_timers(at))
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_batch(
        &self,
        _created_at: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        appending_version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.0
            .lock()
            .unwrap()
            .append_batch(batch, &execution_id, appending_version)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_batch_create_new_execution(
        &self,
        _current_time: DateTime<Utc>,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        self.0
            .lock()
            .unwrap()
            .append_batch_create_child(batch, &execution_id, version, child_req)
    }

    #[cfg(feature = "test")]
    #[instrument(skip_all, %execution_id)]
    async fn get(&self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbErrorRead> {
        self.0.lock().unwrap().get(execution_id)
    }

    async fn get_execution_event(
        &self,
        execution_id: &ExecutionId,
        version: &Version,
    ) -> Result<ExecutionEvent, DbErrorRead> {
        let execution_log = self.0.lock().unwrap().get(execution_id)?;
        Ok(execution_log
            .events
            .get(usize::try_from(version.0).unwrap())
            .cloned()
            .ok_or(DbErrorRead::NotFound)?)
    }

    async fn subscribe_to_next_responses(
        &self,
        execution_id: &ExecutionId,
        start_idx: usize,
        interrupt_after: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Result<Vec<JoinSetResponseEventOuter>, DbErrorReadWithTimeout> {
        let either = {
            let mut guard = self.0.lock().unwrap();
            guard.subscribe_to_next_responses(execution_id, start_idx)?
        };
        // unlocked now
        match either {
            Either::Left(resp) => Ok(resp),
            Either::Right(receiver) => {
                tokio::select! {
                    res = receiver => res
                    .map(|it| vec![it])
                    .map_err(|_| DbErrorReadWithTimeout::from(DbErrorGeneric::Close)),

                    () = interrupt_after => Err(DbErrorReadWithTimeout::Timeout),
                }
            }
        }
    }

    #[instrument(skip_all, %execution_id)]
    async fn append_response(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        response_event: JoinSetResponseEvent,
    ) -> Result<(), DbErrorWrite> {
        self.0.lock().unwrap().append_response(
            &execution_id,
            JoinSetResponseEventOuter {
                created_at,
                event: response_event,
            },
        )
    }

    async fn wait_for_finished_result(
        &self,
        execution_id: &ExecutionId,
        timeout_fut: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ) -> Result<SupportedFunctionReturnValue, DbErrorReadWithTimeout> {
        let execution_log = {
            let fut = async move {
                loop {
                    let execution_log = {
                        let mut guard = self.0.lock().unwrap();
                        guard.get(execution_id)?
                    };
                    if execution_log.pending_state.is_finished() {
                        return Ok(execution_log);
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };

            if let Some(timeout_fut) = timeout_fut {
                tokio::select! { // future's liveness: Dropping the loser immediately.
                    res = fut => res,
                    () = timeout_fut => Err(DbErrorReadWithTimeout::Timeout)
                }
            } else {
                fut.await
            }
        }?;
        Ok(execution_log
            .into_finished_result()
            .expect("pending state was checked"))
    }

    async fn get_pending_state(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<PendingState, DbErrorRead> {
        Ok(self.0.lock().unwrap().get(execution_id)?.pending_state)
    }

    async fn append_backtrace(&self, _append: BacktraceInfo) -> Result<(), DbErrorWrite> {
        // noop, backtrace functionality is for reporting only and its absence should not affect the system.
        Ok(())
    }

    async fn append_backtrace_batch(&self, _batch: Vec<BacktraceInfo>) -> Result<(), DbErrorWrite> {
        // noop, backtrace functionality is for reporting only and its absence should not affect the system.
        Ok(())
    }

    async fn get_backtrace(
        &self,
        _execution_id: &ExecutionId,
        _filter: BacktraceFilter,
    ) -> Result<BacktraceInfo, DbErrorRead> {
        unimplemented!("only needed for gRPC")
    }

    async fn list_executions(
        &self,
        _ffqn: Option<FunctionFqn>,
        _top_level_only: bool,
        _pagination: ExecutionListPagination,
    ) -> Result<Vec<ExecutionWithState>, DbErrorGeneric> {
        unimplemented!("only needed for gRPC")
    }

    async fn list_execution_events(
        &self,
        _execution_id: &ExecutionId,
        _since: &Version,
        _max_length: VersionType,
        _include_backtrace_id: bool,
    ) -> Result<Vec<ExecutionEvent>, DbErrorRead> {
        unimplemented!("only needed for gRPC")
    }

    async fn list_responses(
        &self,
        _execution_id: &ExecutionId,
        _pagination: Pagination<u32>,
    ) -> Result<Vec<ResponseWithCursor>, DbErrorRead> {
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
        ) -> Vec<(&'a ExecutionJournal, DateTime<Utc> /* scheduled at */)> {
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
                .map(|(id, is_async_delay)| (id.clone(), is_async_delay.clone()))
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
                                ..
                            },
                    } => Some(((join_set_id, delay_id), expires_at)),
                    _ => None,
                })
                .collect::<HashMap<_, _>>();
            // Keep only open
            for responded in journal.responses.iter().filter_map(|e| {
                if let JoinSetResponse::DelayFinished { delay_id } = &e.event.event {
                    Some((e.event.join_set_id.clone(), delay_id.clone()))
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
                    .insert(execution_id.clone(), Some((join_set_id.clone(), delay_id)));
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
pub struct InMemoryPool(Arc<std::sync::Mutex<DbHolder>>, Arc<AtomicBool>);

impl Default for InMemoryPool {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryPool {
    #[must_use]
    pub fn new() -> Self {
        Self(
            Arc::new(std::sync::Mutex::new(DbHolder {
                journals: BTreeMap::default(),
                index: JournalsIndex::default(),
                ffqn_to_pending_subscription: hashbrown::HashMap::default(),
            })),
            Arc::new(AtomicBool::default()),
        )
    }

    #[must_use]
    pub fn db_executor(&self) -> Arc<dyn DbExecutor> {
        Arc::new(InMemoryDbConnection(self.0.clone()))
    }
}

#[async_trait]
impl DbPoolCloseable for InMemoryPool {
    async fn close(self) {
        self.1
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("impossible to close the db twice");
    }
}

#[async_trait]
impl DbPool for InMemoryPool {
    fn connection(&self) -> Box<dyn DbConnection> {
        Box::new(InMemoryDbConnection(self.0.clone()))
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
        component_id: &ComponentId,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        run_id: RunId,
    ) -> LockPendingResponse {
        let pending =
            self.index
                .fetch_pending(&self.journals, batch_size, pending_at_or_sooner, ffqns);
        let mut resp = Vec::with_capacity(pending.len());
        for (journal, _scheduled_at) in pending {
            let row = LockedExecution {
                execution_id: journal.execution_id().clone(),
                metadata: journal.metadata().clone(),
                next_version: journal.version(), // updated later
                ffqn: journal.ffqn().clone(),
                params: journal.params(),
                event_history: Vec::default(), // updated later
                responses: journal.responses.clone(),
                retry_exp_backoff: journal.retry_exp_backoff(),
                max_retries: journal.max_retries(),
                run_id,
                parent: journal.parent(),
                intermittent_event_count: journal.temporary_event_count(),
            };
            resp.push(row);
        }
        // Lock, update the version and event history.
        for row in &mut resp {
            let (next_version, new_event_history) = self
                .lock(
                    created_at,
                    component_id.clone(),
                    &row.execution_id,
                    row.run_id,
                    row.next_version.clone(),
                    executor_id,
                    lock_expires_at,
                )
                .expect("must be lockable within the same transaction");
            row.next_version = next_version;
            row.event_history.extend(new_event_history);
        }
        resp
    }

    fn create(&mut self, req: CreateRequest) -> Result<AppendResponse, DbErrorWrite> {
        if self.journals.contains_key(&req.execution_id) {
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::CannotWrite {
                    reason: "execution already exists with the same id".into(),
                    expected_version: None,
                },
            ));
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
        if scheduled_at <= created_at
            && let Some(subscription) = subscription
        {
            let _ = subscription.try_send(());
        }
        Ok(version)
    }

    #[expect(clippy::too_many_arguments)]
    fn lock(
        &mut self,
        created_at: DateTime<Utc>,
        component_id: ComponentId,
        execution_id: &ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<(Version /* next version */, Vec<HistoryEvent>), DbErrorWrite> {
        let event = ExecutionEventInner::Locked {
            component_id,
            executor_id,
            lock_expires_at,
            run_id,
        };
        self.append(created_at, execution_id, version, event)
            .map(|next_version| {
                let journal = self.journals.get(execution_id).unwrap();
                (next_version, journal.event_history().collect())
            })
    }

    #[expect(clippy::needless_pass_by_value)]
    fn append(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: &ExecutionId,
        appending_version: Version,
        event: ExecutionEventInner,
    ) -> Result<AppendResponse, DbErrorWrite> {
        // Disallow `Created` event
        if let ExecutionEventInner::Created { .. } = event {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        // Check version
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbErrorWrite::NotFound);
        };
        let expected_version = journal.version();
        if appending_version != expected_version {
            return Err(DbErrorWrite::Permanent(
                DbErrorWritePermanent::CannotWrite {
                    reason: "version conflict".into(),
                    expected_version: Some(expected_version),
                },
            ));
        }
        let next_version = journal.append(created_at, event)?;
        self.index.update(journal);
        if matches!(journal.pending_state, PendingState::PendingAt { .. })
            && let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn())
        {
            let _ = subscription.try_send(());
        }
        Ok(next_version)
    }

    fn get(&mut self, execution_id: &ExecutionId) -> Result<ExecutionLog, DbErrorRead> {
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbErrorRead::NotFound);
        };
        Ok(journal.as_execution_log())
    }

    fn get_expired_timers(&mut self, at: DateTime<Utc>) -> Vec<ExpiredTimer> {
        let expired = self.index.fetch_expired(at);
        let mut vec = Vec::new();
        for (execution_id, is_delay) in expired {
            let journal = self.journals.get(&execution_id).unwrap();
            vec.push(if let Some((join_set_id, delay_id)) = is_delay {
                let delay = ExpiredDelay {
                    execution_id,
                    join_set_id,
                    delay_id,
                };
                ExpiredTimer::Delay(delay)
            } else {
                let lock = ExpiredLock {
                    execution_id: journal.execution_id().clone(),
                    locked_at_version: journal
                        .execution_events
                        .iter()
                        .enumerate()
                        .rfind(|(_idx, outer)| {
                            matches!(outer.event, ExecutionEventInner::Locked { .. })
                        })
                        .map(|(idx, _)| Version::new(VersionType::try_from(idx).unwrap()))
                        .expect("must have been locked"),
                    next_version: journal.version(),
                    max_retries: journal.max_retries(),
                    intermittent_event_count: journal.temporary_event_count(),
                    retry_exp_backoff: journal.retry_exp_backoff(),
                    parent: journal.parent(),
                };
                ExpiredTimer::Lock(lock)
            });
        }
        vec
    }

    fn append_batch(
        &mut self,
        batch: Vec<AppendRequest>,
        execution_id: &ExecutionId,
        mut appending_version: Version,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        assert!(!batch.is_empty(), "Empty batch request");
        if batch.iter().any(|append_request| {
            matches!(append_request.event, ExecutionEventInner::Created { .. })
        }) {
            panic!("Cannot append `Created` event - use `create` instead");
        }
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbErrorWrite::NotFound);
        };
        let truncate_len = journal.len();
        for append_request in batch {
            let expected_version = journal.version();
            if appending_version != expected_version {
                // Rollback
                journal.truncate_and_update_pending_state(truncate_len);
                self.index.update(journal);
                return Err(DbErrorWrite::Permanent(
                    DbErrorWritePermanent::CannotWrite {
                        reason: "version conflict".into(),
                        expected_version: Some(expected_version),
                    },
                ));
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
        if matches!(journal.pending_state, PendingState::PendingAt { .. })
            && let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn())
        {
            let _ = subscription.try_send(());
        }
        Ok(version)
    }

    fn append_batch_create_child(
        &mut self,
        batch: Vec<AppendRequest>,
        execution_id: &ExecutionId,
        version: Version,
        child_req: Vec<CreateRequest>,
    ) -> Result<AppendBatchResponse, DbErrorWrite> {
        let parent_version = self.append_batch(batch, execution_id, version)?;
        for child_req in child_req {
            self.create(child_req)?;
        }
        Ok(parent_version)
    }

    fn append_batch_respond_to_parent(
        &mut self,
        events: AppendEventsToExecution,
        response: AppendResponseToExecution,
    ) -> Result<Version, DbErrorWrite> {
        let child_version =
            self.append_batch(events.batch, &events.execution_id, events.version)?;
        self.append_response(
            &response.parent_execution_id,
            response.parent_response_event,
        )?;
        Ok(child_version)
    }

    fn append_response(
        &mut self,
        execution_id: &ExecutionId,
        response_event: JoinSetResponseEventOuter,
    ) -> Result<(), DbErrorWrite> {
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbErrorWrite::NotFound);
        };
        journal.append_response(response_event.created_at, response_event.event);
        self.index.update(journal);
        if matches!(journal.pending_state, PendingState::PendingAt { .. })
            && let Some(subscription) = self.ffqn_to_pending_subscription.get(journal.ffqn())
        {
            let _ = subscription.try_send(());
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
        DbErrorReadWithTimeout,
    > {
        debug!("next_response");
        let Some(journal) = self.journals.get_mut(execution_id) else {
            return Err(DbErrorReadWithTimeout::DbErrorRead(DbErrorRead::NotFound));
        };
        let res_len = journal.responses.len();
        if res_len > start_idx {
            Ok(Either::Left(
                journal.responses.iter().skip(start_idx).cloned().collect(),
            ))
        } else {
            assert_eq!(
                start_idx, res_len,
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
