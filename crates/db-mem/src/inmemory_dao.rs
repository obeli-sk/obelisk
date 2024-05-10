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
use concepts::storage::PendingState;
use concepts::storage::{
    AppendBatch, AppendBatchResponse, AppendRequest, AppendResponse, CreateRequest, DbConnection,
    DbConnectionError, DbError, DbPool, ExecutionEventInner, ExecutionLog, ExpiredTimer,
    LockPendingResponse, LockResponse, LockedExecution, SpecificError, Version,
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
    #[instrument(skip_all, fields(execution_id = %req.execution_id))]
    async fn create(&self, req: CreateRequest) -> Result<AppendResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request =
            DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Create { req, resp_sender });
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
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_id: ExecutorId,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse, DbError> {
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
        Ok(resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)?)
    }

    #[instrument(skip_all)]
    async fn get_expired_timers(&self, at: DateTime<Utc>) -> Result<Vec<ExpiredTimer>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::GetExpiredTimers { at, resp_sender });
        self.0
            .send(request)
            .await
            .map_err(|_| DbConnectionError::SendError)?;
        Ok(resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)?)
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

    #[instrument(skip_all, %execution_id)]
    async fn append_batch(
        &self,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
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

    #[instrument(skip_all, %execution_id)]
    async fn append_batch_create_child(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        child_req: CreateRequest,
    ) -> Result<AppendBatchResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::AppendBatchCreateChild {
            batch,
            execution_id,
            version,
            child_req,
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
    async fn append_batch_respond_to_parent(
        &self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
    ) -> Result<AppendBatchResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::AppendBatchRespondToParent {
            batch,
            execution_id,
            version,
            parent,
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
    use crate::journal::ExecutionJournal;
    use concepts::prefixed_ulid::DelayId;
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
                    PendingState::PendingAt { scheduled_at } => {
                        self.pending_scheduled
                            .entry(scheduled_at)
                            .or_default()
                            .insert(execution_id);
                        self.pending_scheduled_rev
                            .insert(execution_id, scheduled_at);
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
    #[display(fmt = "Create")]
    Create {
        req: CreateRequest,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendResponse, SpecificError>>,
    },
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
    #[display(fmt = "Append({req})")]
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
        version: Version,
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
    #[display(fmt = "AppendBatchCreateChild")]
    AppendBatchCreateChild {
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        child_req: CreateRequest,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendBatchResponse, SpecificError>>,
    },
    #[display(fmt = "AppendBatchRespondToParent")]
    AppendBatchRespondToParent {
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        parent: (ExecutionId, AppendRequest),
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendBatchResponse, SpecificError>>,
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
            .clone()
            .map(InMemoryDbConnection)
    }

    pub fn pool(&self) -> Option<InMemoryPool> {
        self.client_to_store_req_sender.clone().map(InMemoryPool)
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

#[async_trait]
impl DbPool<InMemoryDbConnection> for InMemoryPool {
    fn connection(&self) -> InMemoryDbConnection {
        InMemoryDbConnection(self.0.clone())
    }

    async fn close(&self) -> Result<(), StrVariant> {
        Ok(())
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
            Self::Create { req, .. } => req.execution_id,
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
    AppendBatchCreateChildResult {
        payload: Result<AppendBatchResponse, SpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendBatchResponse, SpecificError>>,
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
            Self::AppendBatchCreateChildResult {
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
            GeneralRequest::AppendBatchCreateChild {
                batch,
                execution_id,
                version,
                child_req,
                resp_sender,
            } => DbTickResponse::AppendBatchCreateChildResult {
                payload: self.append_batch_create_child(batch, execution_id, version, child_req),
                resp_sender,
            },
            GeneralRequest::AppendBatchRespondToParent {
                batch,
                execution_id,
                version,
                parent,
                resp_sender,
            } => DbTickResponse::AppendBatchResult {
                payload: self.append_batch_respond_to_parent(batch, execution_id, version, parent),
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
            ExecutionSpecificRequest::Create { req, resp_sender } => DbTickResponse::AppendResult {
                resp_sender,
                payload: self.create(req),
            },
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
        appending_version: Option<Version>,
        event: ExecutionEventInner,
    ) -> Result<AppendResponse, SpecificError> {
        // Disallow `Created` event
        if let ExecutionEventInner::Created { .. } = event {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "Cannot append `Created` event - use `create` instead",
            )));
        }
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(SpecificError::NotFound);
        };
        if let Some(appending_version) = appending_version {
            let expected_version = journal.version();
            if appending_version != expected_version {
                return Err(SpecificError::VersionMismatch {
                    appending_version,
                    expected_version,
                });
            }
        } else if !event.appendable_without_version() {
            return Err(SpecificError::VersionMissing);
        }
        let new_version = journal.append(created_at, event)?;
        self.index.update(execution_id, &self.journals);
        Ok(new_version)
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
                Some((join_set_id, delay_id)) => ExpiredTimer::AsyncDelay {
                    execution_id,
                    version: journal.version(),
                    join_set_id,
                    delay_id,
                },
                None => ExpiredTimer::Lock {
                    execution_id: journal.execution_id(),
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
        batch: AppendBatch,
        execution_id: ExecutionId,
        mut appending_version: Version,
    ) -> Result<AppendBatchResponse, SpecificError> {
        if batch.is_empty() {
            error!("Empty batch request");
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "empty batch request",
            )));
        }
        if batch
            .iter()
            .any(|event| matches!(event.event, ExecutionEventInner::Created { .. }))
        {
            error!("Cannot append `Created` event - use `create` instead");
            return Err(SpecificError::ValidationFailed(StrVariant::Static(
                "Cannot append `Created` event - use `create` instead",
            )));
        }

        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(SpecificError::NotFound);
        };
        let truncate_len_or_delete = journal.len();
        for req in batch {
            let expected_version = journal.version();
            if appending_version != expected_version {
                self.rollback(truncate_len_or_delete, execution_id);
                return Err(SpecificError::VersionMismatch {
                    appending_version,
                    expected_version,
                });
            }
            match journal.append(req.created_at, req.event) {
                Ok(new_version) => {
                    appending_version = new_version;
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

    fn rollback(&mut self, truncate_len: usize, execution_id: ExecutionId) {
        self.journals
            .get_mut(&execution_id)
            .unwrap()
            .truncate(truncate_len);
        self.index.update(execution_id, &self.journals);
    }

    fn append_batch_create_child(
        &mut self,
        batch: AppendBatch,
        execution_id: ExecutionId,
        version: Version,
        child_req: CreateRequest,
    ) -> Result<AppendBatchResponse, SpecificError> {
        let parent_version = self.append_batch(batch, execution_id, version)?;
        self.create(child_req)?;
        Ok(parent_version)
    }

    fn append_batch_respond_to_parent(
        &mut self,
        batch: Vec<AppendRequest>,
        execution_id: ExecutionId,
        version: Version,
        (parent_exe, parent_req): (ExecutionId, AppendRequest),
    ) -> Result<Version, SpecificError> {
        let child_version = self.append_batch(batch, execution_id, version)?;
        self.append(parent_req.created_at, parent_exe, None, parent_req.event)?;
        Ok(child_version)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use db_tests_common::db_test_stubs;
    use test_utils::set_up;

    #[tokio::test]
    async fn lifecycle() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::lifecycle(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn expired_lock_should_be_found() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::expired_lock_should_be_found(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn append_batch_respond_to_parent() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::append_batch_respond_to_parent(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn lock_pending_should_sort_by_scheduled_at() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::lock_pending_should_sort_by_scheduled_at(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn lock_should_delete_from_pending() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::lock_should_delete_from_pending(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn get_expired_lock() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::get_expired_lock(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }

    #[tokio::test]
    async fn get_expired_delay() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.pool().unwrap().connection();
        db_test_stubs::get_expired_delay(&db_connection).await;
        drop(db_connection);
        db_task.close().await;
    }
}
