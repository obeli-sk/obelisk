//! Append only database containing executions and their state changes - execution journal.
//! Current execution state can be obtained by getting the last (non-async-response)
//! state from the execution journal.
//!
//! When inserting, the row in the journal must contain a version that must be equal
//! to the current number of events in the journal. First change with the expected version wins.
use self::index::JournalsIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{ExecutorId, JoinSetId, RunId};
use concepts::storage::journal::{ExecutionJournal, PendingState};
use concepts::storage::{
    AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, AppendTxResponse,
    CreateRequest, DbConnection, DbConnectionError, DbError, DbPool, ExecutionEventInner,
    ExecutionLog, ExpiredTimer, LockPendingResponse, LockResponse, LockedExecution, SpecificError,
    Version,
};
use concepts::{ExecutionId, FunctionFqn, StrVariant};
use derivative::Derivative;
use hashbrown::{HashMap, HashSet};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    task::AbortHandle,
};
use tracing::{debug, info, instrument, trace, warn, Instrument, Level};
use tracing::{error, info_span};

pub struct InMemoryDbConnection(mpsc::Sender<DbRequest>);

#[async_trait]
impl DbConnection for InMemoryDbConnection {
    #[instrument(skip_all)]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbConnectionError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::LockPending {
            batch_size,
            pending_at_or_sooner,
            ffqns,
            created_at,
            executor_id,
            lock_expires_at,
            resp_sender,
        });
        self.0
            .send(request)
            .await
            .map_err(|_| DbConnectionError::SendError)?;
        resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)
    }

    #[instrument(skip_all)]
    async fn get_expired_timers(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<ExpiredTimer>, DbConnectionError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::GetExpiredTimers { at, resp_sender });
        self.0
            .send(request)
            .await
            .map_err(|_| DbConnectionError::SendError)?;
        resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)
    }

    #[instrument(skip_all, %execution_id)]
    async fn lock(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
            created_at,
            execution_id,
            run_id,
            version,
            executor_id,
            lock_expires_at,
            resp_sender,
        });
        self.0
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn append(
        &self,
        execution_id: ExecutionId,
        version: Option<Version>,
        req: AppendRequest,
    ) -> Result<AppendResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Append {
            execution_id,
            version,
            req,
            resp_sender,
        });
        self.0
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all)]
    async fn append_batch(
        &self,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Option<Version>,
    ) -> Result<AppendBatchResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::AppendBatch {
            batch,
            execution_id,
            version,
            resp_sender,
        });
        self.0
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all)]
    async fn append_tx(
        &self,
        items: Vec<(AppendBatch, ExecutionId, Option<Version>)>,
    ) -> Result<AppendTxResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::AppendTx { items, resp_sender });
        self.0
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(DbError::Specific)
    }

    #[instrument(skip_all, %execution_id)]
    async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Get {
            execution_id,
            resp_sender,
        });
        self.0
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(DbError::Specific)
    }
}

mod index {
    use super::{
        BTreeMap, BTreeSet, DateTime, ExecutionId, HashMap, HashSet, JoinSetId, PendingState, Utc,
    };
    use concepts::prefixed_ulid::DelayId;
    use concepts::storage::journal::ExecutionJournal;
    use concepts::storage::{HistoryEvent, JoinSetRequest, JoinSetResponse};
    use tracing::trace;

    #[derive(Debug, Default)]
    pub(super) struct JournalsIndex {
        pending: BTreeSet<ExecutionId>,
        pending_scheduled: BTreeMap<DateTime<Utc>, HashSet<ExecutionId>>,
        pending_scheduled_rev: HashMap<ExecutionId, DateTime<Utc>>,
        #[allow(clippy::type_complexity)]
        locked: BTreeMap<DateTime<Utc>, HashMap<ExecutionId, Option<(JoinSetId, DelayId)>>>,
        locked_rev: HashMap<ExecutionId, Vec<DateTime<Utc>>>,
    }

    impl JournalsIndex {
        pub(super) fn fetch_pending<'a>(
            &self,
            journals: &'a BTreeMap<ExecutionId, ExecutionJournal>,
            batch_size: usize,
            expiring_at_or_before: DateTime<Utc>,
            ffqns: &[concepts::FunctionFqn],
        ) -> Vec<(&'a ExecutionJournal, Option<DateTime<Utc>>)> {
            let mut pending = self
                .pending
                .iter()
                .map(|id| (journals.get(id).unwrap(), None))
                .collect::<Vec<_>>();
            pending.extend(
                self.pending_scheduled
                    .range(..=expiring_at_or_before)
                    .flat_map(|(scheduled_at, ids)| {
                        ids.iter()
                            .map(|id| (journals.get(id).unwrap(), Some(*scheduled_at)))
                    }),
            );
            // filter by ffqn
            pending.retain(|(journal, _)| ffqns.contains(journal.ffqn()));
            pending.truncate(batch_size);
            pending
        }

        pub(super) fn fetch_expired(
            &self,
            at: DateTime<Utc>,
        ) -> impl Iterator<Item = (ExecutionId, Option<(JoinSetId, DelayId)>)> + '_ {
            self.locked
                .range(..=at)
                .flat_map(|(_scheduled_at, id_map)| id_map.iter())
                .map(|(id, is_async_delay)| (*id, *is_async_delay))
        }

        pub(super) fn update(
            &mut self,
            execution_id: ExecutionId,
            journals: &BTreeMap<ExecutionId, ExecutionJournal>,
        ) {
            // Remove the ID from the index (if exists)
            self.pending.remove(&execution_id);
            if let Some(schedule) = self.pending_scheduled_rev.remove(&execution_id) {
                let ids = self.pending_scheduled.get_mut(&schedule).unwrap();
                ids.remove(&execution_id);
            }
            if let Some(schedules) = self.locked_rev.remove(&execution_id) {
                for schedule in schedules {
                    let ids = self.locked.get_mut(&schedule).unwrap();
                    ids.remove(&execution_id);
                }
            }
            if let Some(journal) = journals.get(&execution_id) {
                // Add it again if needed
                match journal.pending_state {
                    PendingState::PendingNow => {
                        self.pending.insert(execution_id);
                    }
                    PendingState::PendingAt(expires_at) => {
                        self.pending_scheduled
                            .entry(expires_at)
                            .or_default()
                            .insert(execution_id);
                        self.pending_scheduled_rev.insert(execution_id, expires_at);
                    }
                    PendingState::Locked {
                        lock_expires_at, ..
                    } => {
                        self.locked
                            .entry(lock_expires_at)
                            .or_default()
                            .insert(execution_id, None);
                        self.locked_rev
                            .entry(execution_id)
                            .or_default()
                            .push(lock_expires_at);
                    }
                    PendingState::BlockedByJoinSet { .. } | PendingState::Finished => {}
                }
                // Add all open async timers
                let delay_req_resp = journal
                    .event_history()
                    .filter_map(|e| match e {
                        HistoryEvent::JoinSetRequest {
                            join_set_id,
                            request:
                                JoinSetRequest::DelayRequest {
                                    delay_id,
                                    expires_at,
                                },
                        } => Some(((join_set_id, delay_id), Some(expires_at))),
                        HistoryEvent::JoinSetResponse {
                            join_set_id,
                            response: JoinSetResponse::DelayFinished { delay_id },
                        } => Some(((join_set_id, delay_id), None)),
                        _ => None,
                    })
                    .collect::<HashMap<_, _>>()
                    .into_iter()
                    // Request must predate the response, so value with Some(expires_at) is an open timer
                    .filter_map(|((join_set_id, delay_id), expires_at)| {
                        expires_at.map(|expires_at| (join_set_id, delay_id, expires_at))
                    });
                for (join_set_id, delay_id, expires_at) in delay_req_resp {
                    self.locked
                        .entry(expires_at)
                        .or_default()
                        .insert(execution_id, Some((join_set_id, delay_id)));
                    self.locked_rev
                        .entry(execution_id)
                        .or_default()
                        .push(expires_at);
                }
            } // else do nothing - rolling back creation
            trace!("Journal index updated: {self:?}");
        }
    }
}

#[derive(Debug, derive_more::Display)]
enum DbRequest {
    General(GeneralRequest),
    ExecutionSpecific(ExecutionSpecificRequest),
}

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(derive_more::Display)]
enum ExecutionSpecificRequest {
    #[display(fmt = "Lock(`{executor_id}`)")]
    Lock {
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<LockResponse, SpecificError>>,
    },
    #[display(fmt = "Insert({req})")]
    Append {
        execution_id: ExecutionId,
        version: Option<Version>,
        req: AppendRequest,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendResponse, SpecificError>>,
    },
    #[display(fmt = "AppendBatch")]
    AppendBatch {
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Option<Version>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendBatchResponse, SpecificError>>,
    },
    #[display(fmt = "Get")]
    Get {
        execution_id: ExecutionId,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<ExecutionLog, SpecificError>>,
    },
}

#[derive(derive_more::Display, Derivative)]
#[derivative(Debug)]
enum GeneralRequest {
    #[display(fmt = "LockPending(`{executor_id}`, {ffqns:?})")]
    LockPending {
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<LockPendingResponse>,
    },
    #[display(fmt = "GetExpiredTimers(`{at}`)")]
    GetExpiredTimers {
        at: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Vec<ExpiredTimer>>,
    },
    #[display(fmt = "AppendTx")]
    AppendTx {
        items: Vec<(AppendBatch, ExecutionId, Option<Version>)>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendTxResponse, SpecificError>>,
    },
}

pub struct DbTaskHandle {
    client_to_store_req_sender: Option<mpsc::Sender<DbRequest>>,
    abort_handle: AbortHandle,
}

impl DbTaskHandle {
    #[deprecated]
    #[must_use]
    pub fn connection(&self) -> Option<impl DbConnection> {
        self.client_to_store_req_sender
            .as_ref()
            .cloned()
            .map(InMemoryDbConnection)
    }

    pub fn pool(&self) -> Option<InMemoryPool> {
        self.client_to_store_req_sender
            .as_ref()
            .cloned()
            .map(InMemoryPool)
    }

    pub async fn close(&mut self) {
        self.client_to_store_req_sender.take();
        trace!("Gracefully closing");
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl Drop for DbTaskHandle {
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

#[derive(Clone)]
pub struct InMemoryPool(mpsc::Sender<DbRequest>);

impl DbPool<InMemoryDbConnection> for InMemoryPool {
    #[allow(refining_impl_trait)]
    fn connection(&self) -> Result<InMemoryDbConnection, DbConnectionError> {
        Ok(InMemoryDbConnection(self.0.clone()))
    }
}

#[derive(Debug, Default)]
pub struct DbTask {
    journals: BTreeMap<ExecutionId, ExecutionJournal>,
    index: JournalsIndex,
}

impl ExecutionSpecificRequest {
    fn execution_id(&self) -> ExecutionId {
        match self {
            Self::Lock { execution_id, .. }
            | Self::Append { execution_id, .. }
            | Self::AppendBatch { execution_id, .. }
            | Self::Get { execution_id, .. } => *execution_id,
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum DbTickResponse {
    LockPending {
        payload: LockPendingResponse,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<LockPendingResponse>,
    },
    Lock {
        payload: Result<LockResponse, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<LockResponse, SpecificError>>,
    },
    AppendResult {
        payload: Result<AppendResponse, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendResponse, SpecificError>>,
    },
    AppendBatchResult {
        payload: Result<AppendBatchResponse, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendBatchResponse, SpecificError>>,
    },
    Get {
        payload: Result<ExecutionLog, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<ExecutionLog, SpecificError>>,
    },
    GetExpiredTimers {
        payload: Vec<ExpiredTimer>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Vec<ExpiredTimer>>,
    },
    AppendTxResult {
        payload: Result<AppendTxResponse, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendTxResponse, SpecificError>>,
    },
}

impl DbTickResponse {
    fn send_response(self) -> Result<(), ()> {
        match self {
            Self::LockPending {
                resp_sender,
                payload,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::Lock {
                resp_sender,
                payload,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::AppendResult {
                resp_sender,
                payload,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::AppendBatchResult {
                resp_sender,
                payload,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::Get {
                payload,
                resp_sender,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::GetExpiredTimers {
                payload,
                resp_sender,
            } => resp_sender.send(payload).map_err(|_| ()),
            Self::AppendTxResult {
                payload,
                resp_sender,
            } => resp_sender.send(payload).map_err(|_| ()),
        }
    }
}

impl DbTask {
    pub fn spawn_new(rpc_capacity: usize) -> DbTaskHandle {
        let (client_to_store_req_sender, mut client_to_store_receiver) =
            mpsc::channel::<DbRequest>(rpc_capacity);
        let abort_handle = tokio::spawn(
            async move {
                info!("Spawned inmemory db task");
                let mut task = Self {
                    journals: BTreeMap::default(),
                    index: JournalsIndex::default(),
                };
                while let Some(request) = client_to_store_receiver.recv().await {
                    let resp = task.tick(request).send_response();
                    if resp.is_err() {
                        debug!("Failed to send back the response");
                    }
                }
            }
            .instrument(info_span!("inmemory_db_task")),
        )
        .abort_handle();
        DbTaskHandle {
            abort_handle,
            client_to_store_req_sender: Some(client_to_store_req_sender),
        }
    }

    #[cfg(any(test, feature = "test"))]
    #[must_use]
    pub fn new() -> Self {
        Self {
            journals: BTreeMap::default(),
            index: JournalsIndex::default(),
        }
    }

    #[instrument(skip_all)]
    pub(crate) fn tick(&mut self, request: DbRequest) -> DbTickResponse {
        if tracing::enabled!(Level::TRACE)
            || matches!(
                request,
                DbRequest::General(GeneralRequest::LockPending { .. })
                    | DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Get { .. })
            )
        {
            match &request {
                DbRequest::General(..) => trace!("Received {request:?}"),
                DbRequest::ExecutionSpecific(specific) => trace!(
                    execution_id = %specific.execution_id(),
                    "Received {request:?}"
                ),
            }
        } else {
            match &request {
                DbRequest::General(..) => {}
                DbRequest::ExecutionSpecific(specific) => debug!(
                    execution_id = %specific.execution_id(),
                    "Received {request}"
                ),
            }
        }
        let resp = match request {
            DbRequest::ExecutionSpecific(request) => self.handle_specific(request),
            DbRequest::General(request) => self.handle_general(request),
        };
        trace!("Responding with {resp:?}");
        resp
    }

    fn handle_general(&mut self, request: GeneralRequest) -> DbTickResponse {
        match request {
            GeneralRequest::LockPending {
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_id,
                lock_expires_at,
                resp_sender,
            } => self.lock_pending(
                batch_size,
                pending_at_or_sooner,
                &ffqns,
                created_at,
                executor_id,
                lock_expires_at,
                resp_sender,
            ),
            GeneralRequest::GetExpiredTimers {
                at: before,
                resp_sender,
            } => DbTickResponse::GetExpiredTimers {
                payload: self.get_expired_timers(before),
                resp_sender,
            },
            GeneralRequest::AppendTx { items, resp_sender } => DbTickResponse::AppendTxResult {
                payload: self.append_tx(items),
                resp_sender,
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_pending(
        &mut self,
        batch_size: usize,
        expiring_before: DateTime<Utc>,
        ffqns: &[FunctionFqn],
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
        resp_sender: oneshot::Sender<LockPendingResponse>,
    ) -> DbTickResponse {
        let pending = self
            .index
            .fetch_pending(&self.journals, batch_size, expiring_before, ffqns);
        let mut payload = Vec::with_capacity(pending.len());
        for (journal, scheduled_at) in pending {
            let item = LockedExecution {
                execution_id: journal.execution_id(),
                version: journal.version(), // updated later
                ffqn: journal.ffqn().clone(),
                params: journal.params(),
                event_history: Vec::default(), // updated later
                scheduled_at,
                retry_exp_backoff: journal.retry_exp_backoff(),
                max_retries: journal.max_retries(),
                run_id: RunId::generate(),
            };
            payload.push(item);
        }
        // Lock, update the version and event history.
        for row in &mut payload {
            let (new_event_history, new_version) = self
                .lock(
                    created_at,
                    row.execution_id,
                    row.run_id,
                    row.version.clone(),
                    executor_id,
                    lock_expires_at,
                )
                .expect("must be lockable within the same transaction");
            row.version = new_version;
            row.event_history.extend(new_event_history);
        }
        DbTickResponse::LockPending {
            payload,
            resp_sender,
        }
    }

    #[instrument(skip_all, fields(execution_id = %request.execution_id()))]
    fn handle_specific(&mut self, request: ExecutionSpecificRequest) -> DbTickResponse {
        match request {
            ExecutionSpecificRequest::Append {
                execution_id,
                version,
                req,
                resp_sender,
            } => DbTickResponse::AppendResult {
                resp_sender,
                payload: self.append(req.created_at, execution_id, version, req.event),
            },
            ExecutionSpecificRequest::Lock {
                created_at,
                execution_id,
                run_id,
                version,
                executor_id,
                lock_expires_at,
                resp_sender,
            } => DbTickResponse::Lock {
                resp_sender,
                payload: self.lock(
                    created_at,
                    execution_id,
                    run_id,
                    version,
                    executor_id,
                    lock_expires_at,
                ),
            },
            ExecutionSpecificRequest::AppendBatch {
                batch: append,
                execution_id,
                version,
                resp_sender,
            } => DbTickResponse::AppendBatchResult {
                payload: self.append_batch(append, execution_id, version),
                resp_sender,
            },
            ExecutionSpecificRequest::Get {
                execution_id,
                resp_sender,
            } => DbTickResponse::Get {
                payload: self.get(execution_id),
                resp_sender,
            },
        }
    }

    fn create(&mut self, req: CreateRequest) -> Result<AppendResponse, SpecificError> {
        if self.journals.contains_key(&req.execution_id) {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "execution is already initialized",
            )));
        }
        let journal = ExecutionJournal::new(CreateRequest {
            created_at: req.created_at,
            execution_id: req.execution_id,
            ffqn: req.ffqn,
            params: req.params,
            parent: req.parent,
            scheduled_at: req.scheduled_at,
            retry_exp_backoff: req.retry_exp_backoff,
            max_retries: req.max_retries,
        });
        let version = journal.version();
        let old_val = self.journals.insert(req.execution_id, journal);
        assert!(
            old_val.is_none(),
            "journals cannot contain the new execution"
        );
        self.index.update(req.execution_id, &self.journals);
        Ok(version)
    }

    fn lock(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        run_id: RunId,
        version: Version,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse, SpecificError> {
        let event = ExecutionEventInner::Locked {
            executor_id,
            lock_expires_at,
            run_id,
        };
        self.append(created_at, execution_id, Some(version), event)
            .map(|_| {
                let journal = self.journals.get(&execution_id).unwrap();
                (journal.event_history().collect(), journal.version())
            })
    }

    fn append(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: ExecutionId,
        version: Option<Version>,
        event: ExecutionEventInner,
    ) -> Result<AppendResponse, SpecificError> {
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        } = event
        {
            return if version.is_none() {
                self.create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn,
                    params,
                    parent,
                    scheduled_at,
                    retry_exp_backoff,
                    max_retries,
                })
            } else {
                info!("Wrong version");
                Err(SpecificError::VersionMismatch)
            };
        }
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(SpecificError::NotFound);
        };
        if let Some(version) = version {
            if version != journal.version() {
                return Err(SpecificError::VersionMismatch);
            }
        } else if !event.appendable_without_version() {
            return Err(SpecificError::VersionMismatch);
        }
        let version = journal.append(created_at, event)?;
        self.index.update(execution_id, &self.journals);
        Ok(version)
    }

    fn get(&mut self, execution_id: ExecutionId) -> Result<ExecutionLog, SpecificError> {
        let Some(journal) = self.journals.get_mut(&execution_id) else {
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
                Some((join_set_id, delay_id)) => ExpiredTimer::AsyncTimer {
                    execution_id,
                    version: journal.version(),
                    join_set_id,
                    delay_id,
                },
                None => ExpiredTimer::Lock {
                    execution_id: journal.execution_id(),
                    version: journal.version(),
                    max_retries: journal.max_retries(),
                    already_retried_count: journal.already_retried_count(),
                    retry_exp_backoff: journal.retry_exp_backoff(),
                },
            });
        }
        vec
    }

    fn append_batch(
        &mut self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        mut version: Option<Version>,
    ) -> Result<AppendBatchResponse, SpecificError> {
        if batch.is_empty() {
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "empty batch request",
            )));
        }
        let mut begin_idx = 0;
        if let Some((
            ExecutionEventInner::Created {
                ffqn,
                params,
                parent,
                scheduled_at,
                retry_exp_backoff,
                max_retries,
            },
            created_at,
        )) = batch.first().map(|req| (&req.event, req.created_at))
        {
            if version.is_none() {
                version = Some(self.create(CreateRequest {
                    created_at,
                    execution_id,
                    ffqn: ffqn.clone(),
                    params: params.clone(),
                    parent: *parent,
                    scheduled_at: *scheduled_at,
                    retry_exp_backoff: *retry_exp_backoff,
                    max_retries: *max_retries,
                })?);
                begin_idx = 1;
            } else {
                warn!("Version should not be passed to `Created` event");
                return Err(SpecificError::VersionMismatch);
            };
        }
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(SpecificError::NotFound);
        };
        let truncate_len_or_delete = if begin_idx == 1 {
            None
        } else {
            Some(journal.len())
        };
        for req in batch.into_iter().skip(begin_idx) {
            match &version {
                None => {
                    if !req.event.appendable_without_version() {
                        self.rollback(truncate_len_or_delete, execution_id);
                        return Err(SpecificError::VersionMismatch);
                    }
                }
                Some(version) => {
                    if *version != journal.version() {
                        self.rollback(truncate_len_or_delete, execution_id);
                        return Err(SpecificError::VersionMismatch);
                    }
                }
            }
            match journal
                .append(req.created_at, req.event)
                .map(|ok| (ok, version))
            {
                Ok((new_version, Some(_))) => {
                    version = Some(new_version);
                }
                Ok((_, None)) => {
                    // Do not allow appending events that require version
                    version = None;
                }
                Err(err) => {
                    self.rollback(truncate_len_or_delete, execution_id);
                    return Err(err);
                }
            }
        }
        let version = journal.version();
        self.index.update(execution_id, &self.journals);
        Ok(version)
    }

    fn rollback(&mut self, truncate_len_or_delete: Option<usize>, execution_id: ExecutionId) {
        if let Some(truncate_len) = truncate_len_or_delete {
            self.journals
                .get_mut(&execution_id)
                .unwrap()
                .truncate(truncate_len);
        } else {
            self.journals.remove(&execution_id).unwrap();
        }
        self.index.update(execution_id, &self.journals);
    }

    fn append_tx(
        &mut self,
        items: Vec<(AppendBatch, ExecutionId, Option<Version>)>,
    ) -> Result<AppendTxResponse, SpecificError> {
        let mut versions = Vec::with_capacity(items.len());
        for (idx, (batch, execution_id, version)) in items.into_iter().enumerate() {
            match self.append_batch(batch, execution_id, version) {
                Ok(version) => versions.push(version),
                Err(err) if idx == 0 => return Err(err),
                Err(err) => {
                    // not needed (yet) for its use case - appending to parent and child executions
                    error!("Cannot rollback, got error on index {idx}, {execution_id} - {err:?}");
                    unimplemented!("transaction rollback is not implemented")
                }
            }
        }
        Ok(versions)
    }
}

#[cfg(test)]
pub mod tick {
    use super::{
        async_trait, instrument, oneshot, AppendBatchResponse, AppendRequest, AppendResponse,
        DateTime, DbConnection, DbConnectionError, DbError, DbRequest, DbTask, DbTickResponse,
        ExecutionId, ExecutionLog, ExecutionSpecificRequest, ExecutorId, ExpiredTimer, FunctionFqn,
        GeneralRequest, LockPendingResponse, LockResponse, RunId, Utc, Version,
    };
    use assert_matches::assert_matches;
    use concepts::storage::AppendBatch;
    use std::sync::Arc;

    #[derive(Clone)]
    // FIXME: pub(crate)
    pub struct TickBasedDbConnection {
        pub db_task: Arc<std::sync::Mutex<DbTask>>,
    }

    #[async_trait]
    impl DbConnection for TickBasedDbConnection {
        #[instrument(skip_all)]
        async fn lock_pending(
            &self,
            batch_size: usize,
            pending_at_or_sooner: DateTime<Utc>,
            ffqns: Vec<FunctionFqn>,
            created_at: DateTime<Utc>,
            executor_id: ExecutorId,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<LockPendingResponse, DbConnectionError> {
            let request = DbRequest::General(GeneralRequest::LockPending {
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_id,
                lock_expires_at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            Ok(assert_matches!(response, DbTickResponse::LockPending {  payload, .. } => payload))
        }

        #[instrument(skip_all)]
        async fn get_expired_timers(
            &self,
            at: DateTime<Utc>,
        ) -> Result<Vec<ExpiredTimer>, DbConnectionError> {
            let request = DbRequest::General(GeneralRequest::GetExpiredTimers {
                at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            Ok(
                assert_matches!(response, DbTickResponse::GetExpiredTimers { payload, .. } => payload),
            )
        }

        async fn append_tx(
            &self,
            items: Vec<(AppendBatch, ExecutionId, Option<Version>)>,
        ) -> Result<Vec<AppendBatchResponse>, DbError> {
            let request = DbRequest::General(GeneralRequest::AppendTx {
                items,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            assert_matches!(response, DbTickResponse::AppendTxResult { payload, .. } => payload)
                .map_err(DbError::Specific)
        }

        async fn lock(
            &self,
            created_at: DateTime<Utc>,
            execution_id: ExecutionId,
            run_id: RunId,
            version: Version,
            executor_id: ExecutorId,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<LockResponse, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
                created_at,
                execution_id,
                run_id,
                version,
                executor_id,
                lock_expires_at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            assert_matches!(response, DbTickResponse::Lock { payload, .. } => payload)
                .map_err(DbError::Specific)
        }

        async fn append(
            &self,
            execution_id: ExecutionId,
            version: Option<Version>,
            req: AppendRequest,
        ) -> Result<AppendResponse, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Append {
                execution_id,
                version,
                req,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            assert_matches!(response, DbTickResponse::AppendResult { payload, .. } => payload)
                .map_err(DbError::Specific)
        }

        async fn append_batch(
            &self,
            batch: Vec<AppendRequest>,
            execution_id: ExecutionId,
            version: Option<Version>,
        ) -> Result<AppendBatchResponse, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::AppendBatch {
                batch,
                execution_id,
                version,
                resp_sender: oneshot::channel().0,
            });

            let response = self.db_task.lock().unwrap().tick(request);
            assert_matches!(response, DbTickResponse::AppendBatchResult { payload, .. } => payload)
                .map_err(DbError::Specific)
        }

        async fn get(&self, execution_id: ExecutionId) -> Result<ExecutionLog, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Get {
                execution_id,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap().tick(request);
            assert_matches!(response, DbTickResponse::Get { payload, .. } => payload)
                .map_err(DbError::Specific)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use self::tick::TickBasedDbConnection;
    use super::*;
    use assert_matches::assert_matches;
    use concepts::Params;
    use concepts::{prefixed_ulid::ExecutorId, ExecutionId};
    use concepts::{storage::HistoryEvent, FinishedExecutionError, FinishedExecutionResult};
    use std::{
        ops::Deref,
        sync::Arc,
        time::{Duration, Instant},
    };
    use test_utils::arbitrary::UnstructuredHolder;
    use test_utils::{env_or_default, sim_clock::SimClock};
    use tracing::info;
    use utils::time::now;

    fn set_up() {
        test_utils::set_up();
    }

    const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("pkg/ifc", "fn");

    #[tokio::test]
    async fn close() {
        set_up();
        let mut task = DbTask::spawn_new(1);
        task.close().await;
    }

    #[tokio::test]
    async fn lifecycle_tick_based() {
        set_up();
        let db_connection = TickBasedDbConnection {
            db_task: Arc::new(std::sync::Mutex::new(DbTask::new())),
        };
        lifecycle(db_connection).await;
    }

    #[tokio::test]
    async fn lifecycle_task_based() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection().unwrap();
        lifecycle(db_connection).await;
        db_task.close().await;
    }

    #[allow(clippy::too_many_lines)]
    async fn lifecycle(db_connection: impl DbConnection) {
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
        {
            let created_at = sim_clock.now();
            db_connection
                .create(CreateRequest {
                    created_at,
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
        }
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

    #[tokio::test]
    async fn expired_lock_should_be_found_tick_based() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        expired_lock_should_be_found(db_connection).await;
    }

    #[tokio::test]
    async fn expired_lock_should_be_found_task_based() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection().unwrap();
        expired_lock_should_be_found(db_connection).await;
        db_task.close().await;
    }

    async fn expired_lock_should_be_found(db_connection: impl DbConnection) {
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
        sim_clock.sleep(lock_duration);
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
            ) = assert_matches!(expired,
                ExpiredTimer::Lock { execution_id, version, already_retried_count, max_retries, retry_exp_backoff } =>
                (execution_id, version, already_retried_count, max_retries, retry_exp_backoff));
            assert_eq!(execution_id, *found_execution_id);
            assert_eq!(Version::new(2), *version);
            assert_eq!(0, *already_retried_count);
            assert_eq!(MAX_RETRIES, *max_retries);
            assert_eq!(RETRY_EXP_BACKOFF, *retry_exp_backoff);
        }
    }

    #[tokio::test]
    async fn stochastic_proptest() {
        set_up();
        let unstructured_holder = UnstructuredHolder::new();
        let mut unstructured = unstructured_holder.unstructured();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        let execution_id = ExecutionId::generate();
        let mut version;
        // Create
        version = db_connection
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

        let events = unstructured.int_in_range(5..=10).unwrap();
        for _ in 0..events {
            let req = AppendRequest {
                event: unstructured.arbitrary().unwrap(),
                created_at: now(),
            };
            match db_connection
                .append(execution_id, Some(version.clone()), req)
                .await
            {
                Ok(new_version) => version = new_version,
                Err(err) => debug!("Ignoring {err:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn perf_lock_pending_parallel() {
        const EXECUTIONS: usize = 100_000;
        const BATCH_SIZE: usize = 100_000;
        const TASKS: usize = 1;

        test_utils::set_up();
        let executions = env_or_default("EXECUTIONS", EXECUTIONS);
        let batch_size = env_or_default("BATCH_SIZE", BATCH_SIZE);
        let tasks = env_or_default("TASKS", TASKS);

        let mut db_task = DbTask::spawn_new(1);
        let db_pool = db_task.pool().unwrap();

        let stopwatch = Instant::now();
        let created_at = now();
        let ffqn = SOME_FFQN;
        let append_req = AppendRequest {
            created_at,
            event: ExecutionEventInner::Created {
                ffqn: ffqn.clone(),
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            },
        };
        warn!(
            "Created {executions} append entries in {:?}",
            stopwatch.elapsed()
        );
        let stopwatch = Instant::now();
        let db_connection = db_pool.connection().unwrap();
        for _ in 0..executions {
            db_connection
                .append_batch(vec![append_req.clone()], ExecutionId::generate(), None)
                .await
                .unwrap();
        }
        warn!(
            "Appended {executions} executions in {:?}",
            stopwatch.elapsed()
        );

        // spawn executors
        let exec_id = ExecutorId::generate();
        let mut exec_tasks = Vec::with_capacity(tasks);
        for _ in 0..tasks {
            let db_connection = db_pool.connection().unwrap();
            let task = tokio::spawn(async move {
                let target = executions / tasks;
                let mut locked = Vec::with_capacity(target);
                while locked.len() < target {
                    let now = now();
                    let locked_now = db_connection
                        .lock_pending(
                            batch_size,
                            now,
                            vec![SOME_FFQN],
                            now,
                            exec_id,
                            now + Duration::from_secs(1),
                        )
                        .await
                        .unwrap();
                    locked.extend(locked_now.into_iter());
                }
                locked
            });
            exec_tasks.push(task);
        }
        for task_handle in exec_tasks {
            let _locked_vec = task_handle.await.unwrap();
        }
        warn!("Finished in {} ms", stopwatch.elapsed().as_millis());
        drop(db_connection);
        drop(db_pool);
        db_task.close().await;
    }

    #[tokio::test]
    async fn append_batch_should_rollback_creation() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };

        let execution_id = ExecutionId::generate();
        let created_at = now();
        let batch = vec![
            AppendRequest {
                created_at,
                event: ExecutionEventInner::Created {
                    ffqn: SOME_FFQN,
                    params: Params::default(),
                    parent: None,
                    scheduled_at: None,
                    retry_exp_backoff: Duration::ZERO,
                    max_retries: 0,
                },
            },
            AppendRequest {
                created_at,
                event: ExecutionEventInner::Finished {
                    result: Err(FinishedExecutionError::PermanentTimeout),
                },
            },
            AppendRequest {
                created_at,
                event: ExecutionEventInner::Finished {
                    result: Err(FinishedExecutionError::PermanentTimeout),
                },
            },
        ];
        // invalid creation
        let err = db_connection
            .append_batch(batch.clone(), execution_id, None)
            .await
            .unwrap_err();
        assert_matches!(
            err,
            DbError::Specific(SpecificError::ValidationFailed(reason))
            if reason.deref() == "already finished"
        );
        let err = db_connection.get(execution_id).await.unwrap_err();
        assert_eq!(DbError::Specific(SpecificError::NotFound), err);
        // Split into [created], [finished,finished]
        let (first, rest) = batch.split_first().unwrap();
        let version = db_connection
            .append_batch(vec![first.clone()], execution_id, None)
            .await
            .unwrap();
        // this should be rolled back
        let err = db_connection
            .append_batch(rest.to_vec(), execution_id, Some(version.clone()))
            .await
            .unwrap_err();
        assert_matches!(
            err,
            DbError::Specific(SpecificError::ValidationFailed(reason))
            if reason.deref() == "already finished"
        );
        let created = db_connection.get(execution_id).await.unwrap();
        assert_eq!(version, created.version);
        assert_eq!(PendingState::PendingNow, created.pending_state);
    }
}
