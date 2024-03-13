//! Append only database containing executions and their state changes - execution journal.
//! Current execution state can be obtained by getting the last (non-async-response)
//! state from the execution journal.
//!
//! When inserting, the row in the journal must contain a version that must be equal
//! to the current number of events in the journal. First change with the expected version wins.
use self::index::JournalsIndex;
use super::{journal::ExecutionJournal, ExecutionEventInner, ExecutorName};
use super::{AppendBatchResponse, AppendRequest, CleanupExpiredLocks, LockedExecution};
use crate::storage::journal::PendingState;
use crate::storage::{
    AppendResponse, DbConnection, DbConnectionError, DbError, ExecutionHistory,
    LockPendingResponse, LockResponse, RowSpecificError, Version,
};
use crate::FinishedExecutionError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
use derivative::Derivative;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::AbortHandle,
};
use tracing::info_span;
use tracing::{debug, info, instrument, trace, warn, Instrument, Level};
use tracing_unwrap::{OptionExt, ResultExt};

#[derive(Clone)]
struct InMemoryDbConnection<ID: ExecutionId> {
    client_to_store_req_sender: mpsc::Sender<DbRequest<ID>>,
}

#[async_trait]
impl<ID: ExecutionId> DbConnection<ID> for InMemoryDbConnection<ID> {
    #[instrument(skip_all)]
    async fn lock_pending(
        &self,
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockPendingResponse<ID>, DbConnectionError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::LockPending {
            batch_size,
            pending_at_or_sooner,
            ffqns,
            created_at,
            executor_name,
            lock_expires_at,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbConnectionError::SendError)?;
        resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)
    }

    #[instrument(skip_all)]
    async fn cleanup_expired_locks(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CleanupExpiredLocks, DbConnectionError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::CleanupExpiredLocks { now, resp_sender });
        self.client_to_store_req_sender
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
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse<ID>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
            created_at,
            execution_id,
            version,
            executor_name,
            lock_expires_at,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(|err| DbError::RowSpecific(err))
    }

    #[instrument(skip_all, %execution_id)]
    async fn append(
        &self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<AppendResponse, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Append {
            created_at,
            execution_id,
            version,
            event,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(|err| DbError::RowSpecific(err))
    }

    #[instrument(skip_all)]
    async fn append_batch(
        &self,
        append: Vec<AppendRequest<ID>>,
    ) -> Result<AppendBatchResponse, DbConnectionError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::AppendBatch {
            append,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbConnectionError::SendError)?;
        resp_receiver
            .await
            .map_err(|_| DbConnectionError::RecvError)
    }

    #[instrument(skip_all, %execution_id)]
    async fn get(&self, execution_id: ID) -> Result<ExecutionHistory<ID>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Get {
            execution_id,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::SendError))?;
        resp_receiver
            .await
            .map_err(|_| DbError::Connection(DbConnectionError::RecvError))?
            .map_err(|err| DbError::RowSpecific(err))
    }
}

mod index {
    use super::*;

    #[derive(Debug)]
    pub(super) struct JournalsIndex<ID: ExecutionId> {
        pending: BTreeSet<ID>,
        pending_scheduled: BTreeMap<DateTime<Utc>, BTreeSet<ID>>,
        pending_scheduled_rev: HashMap<ID, DateTime<Utc>>,
        locked: BTreeMap<DateTime<Utc>, BTreeSet<ID>>,
        locked_rev: HashMap<ID, DateTime<Utc>>,
    }

    impl<ID: ExecutionId> JournalsIndex<ID> {
        pub(super) fn fetch_pending<'a>(
            &self,
            journals: &'a HashMap<ID, ExecutionJournal<ID>>,
            batch_size: usize,
            expiring_at_or_before: DateTime<Utc>,
            ffqns: Vec<concepts::FunctionFqn>,
        ) -> Vec<(&'a ExecutionJournal<ID>, Option<DateTime<Utc>>)> {
            let mut pending = self
                .pending
                .iter()
                .map(|id| (journals.get(id).unwrap_or_log(), None))
                .collect::<Vec<_>>();
            pending.extend(
                self.pending_scheduled
                    .range(..=expiring_at_or_before)
                    .flat_map(|(scheduled_at, ids)| {
                        ids.iter().map(|id| {
                            (journals.get(id).unwrap_or_log(), Some(scheduled_at.clone()))
                        })
                    }),
            );
            // filter by ffqn
            pending.retain(|(journal, _)| ffqns.contains(journal.ffqn()));
            pending.truncate(batch_size);
            pending
        }

        pub(super) fn fetch_expired(&self, now: DateTime<Utc>) -> Vec<ID> {
            self.locked
                .range(..=now)
                .flat_map(|(_scheduled_at, ids)| ids)
                .cloned()
                .collect()
        }

        pub(super) fn update(
            &mut self,
            execution_id: ID,
            journals: &HashMap<ID, ExecutionJournal<ID>>,
        ) {
            // Remove the ID from the index (if exists)
            self.pending.remove(&execution_id);
            if let Some(schedule) = self.pending_scheduled_rev.remove(&execution_id) {
                let ids = self.pending_scheduled.get_mut(&schedule).unwrap_or_log();
                ids.remove(&execution_id);
            }
            if let Some(schedule) = self.locked_rev.remove(&execution_id) {
                let ids = self.locked.get_mut(&schedule).unwrap();
                ids.remove(&execution_id);
            }
            let journal = journals.get(&execution_id).unwrap_or_log();
            // Add it again if needed
            match journal.pending_state {
                PendingState::PendingNow => {
                    self.pending.insert(execution_id);
                }
                PendingState::PendingAt(expires_at) => {
                    self.pending_scheduled
                        .entry(expires_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.pending_scheduled_rev.insert(execution_id, expires_at);
                }
                PendingState::Locked {
                    lock_expires_at, ..
                } => {
                    self.locked
                        .entry(lock_expires_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.locked_rev.insert(execution_id, lock_expires_at);
                }
                PendingState::BlockedByJoinSet | PendingState::Finished => {}
            }
        }
    }

    impl<T: ExecutionId> Default for JournalsIndex<T> {
        fn default() -> Self {
            Self {
                pending: Default::default(),
                pending_scheduled: Default::default(),
                pending_scheduled_rev: Default::default(),
                locked: Default::default(),
                locked_rev: Default::default(),
            }
        }
    }
}

#[derive(Debug, derive_more::Display)]
enum DbRequest<ID: ExecutionId> {
    General(GeneralRequest<ID>),
    ExecutionSpecific(ExecutionSpecificRequest<ID>),
}

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(derive_more::Display)]
enum ExecutionSpecificRequest<ID: ExecutionId> {
    #[display(fmt = "Lock(`{executor_name}`)")]
    Lock {
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<LockResponse<ID>, RowSpecificError>>,
    },
    #[display(fmt = "Insert({event})")]
    Append {
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendResponse, RowSpecificError>>,
    },
    #[display(fmt = "Get")]
    Get {
        execution_id: ID,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<ExecutionHistory<ID>, RowSpecificError>>,
    },
}

#[derive(derive_more::Display, Derivative)]
#[derivative(Debug)]
enum GeneralRequest<ID: ExecutionId> {
    #[display(fmt = "LockPending(`{executor_name}`, {ffqns:?})")]
    LockPending {
        batch_size: usize,
        pending_at_or_sooner: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<LockPendingResponse<ID>>,
    },
    #[display(fmt = "CleanupExpiredLocks(`{now}`)")]
    CleanupExpiredLocks {
        now: DateTime<Utc>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<CleanupExpiredLocks>,
    },
    #[display(fmt = "AppendBatch")]
    AppendBatch {
        append: Vec<AppendRequest<ID>>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<AppendBatchResponse>,
    },
}

pub struct DbTaskHandle<ID: ExecutionId> {
    client_to_store_req_sender: Option<mpsc::Sender<DbRequest<ID>>>,
    abort_handle: AbortHandle,
}

impl<ID: ExecutionId> DbTaskHandle<ID> {
    pub fn as_db_connection(&self) -> Option<impl DbConnection<ID>> {
        self.client_to_store_req_sender
            .as_ref()
            .map(|sender| InMemoryDbConnection {
                client_to_store_req_sender: sender.clone(),
            })
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

impl<ID: ExecutionId> Drop for DbTaskHandle<ID> {
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

#[derive(Debug)]
pub struct DbTask<ID: ExecutionId> {
    journals: HashMap<ID, ExecutionJournal<ID>>,
    index: JournalsIndex<ID>,
}

impl<ID: ExecutionId> ExecutionSpecificRequest<ID> {
    fn execution_id(&self) -> &ID {
        match self {
            Self::Lock { execution_id, .. } => execution_id,
            Self::Append { execution_id, .. } => execution_id,
            Self::Get { execution_id, .. } => execution_id,
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum DbTickResponse<ID: ExecutionId> {
    LockPending {
        payload: LockPendingResponse<ID>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<LockPendingResponse<ID>>,
    },
    Lock {
        payload: Result<LockResponse<ID>, RowSpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<LockResponse<ID>, RowSpecificError>>,
    },
    AppendResult {
        payload: Result<AppendResponse, RowSpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<AppendResponse, RowSpecificError>>,
    },
    AppendBatchResult {
        payload: AppendBatchResponse,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<AppendBatchResponse>,
    },
    Get {
        payload: Result<ExecutionHistory<ID>, RowSpecificError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<ExecutionHistory<ID>, RowSpecificError>>,
    },
    CleanupExpiredLocks {
        payload: CleanupExpiredLocks,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<CleanupExpiredLocks>,
    },
}

impl<ID: ExecutionId> DbTickResponse<ID> {
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
            Self::CleanupExpiredLocks {
                payload,
                resp_sender,
            } => resp_sender.send(payload).map_err(|_| ()),
        }
    }
}

impl<ID: ExecutionId> DbTask<ID> {
    pub fn spawn_new(rpc_capacity: usize) -> DbTaskHandle<ID> {
        let (client_to_store_req_sender, mut client_to_store_receiver) =
            mpsc::channel::<DbRequest<ID>>(rpc_capacity);
        let abort_handle = tokio::spawn(
            async move {
                info!("Spawned inmemory db task");
                let mut task = Self::new();
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

    pub(crate) fn new() -> Self {
        Self {
            journals: Default::default(),
            index: JournalsIndex::default(),
        }
    }

    #[instrument(skip_all)]
    pub(crate) fn tick(&mut self, request: DbRequest<ID>) -> DbTickResponse<ID> {
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
                DbRequest::General(..) => debug!("Received {request}"),
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

    fn handle_general(&mut self, request: GeneralRequest<ID>) -> DbTickResponse<ID> {
        match request {
            GeneralRequest::LockPending {
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_name,
                lock_expires_at,
                resp_sender,
            } => self.lock_pending(
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_name,
                lock_expires_at,
                resp_sender,
            ),
            GeneralRequest::CleanupExpiredLocks { now, resp_sender } => {
                self.cleanup_expired_locks(now, resp_sender)
            }
            GeneralRequest::AppendBatch {
                append,
                resp_sender,
            } => self.append_batch(append, resp_sender),
        }
    }

    fn lock_pending(
        &mut self,
        batch_size: usize,
        expiring_before: DateTime<Utc>,
        ffqns: Vec<FunctionFqn>,
        created_at: DateTime<Utc>,
        executor_name: Arc<String>,
        lock_expires_at: DateTime<Utc>,
        resp_sender: oneshot::Sender<LockPendingResponse<ID>>,
    ) -> DbTickResponse<ID> {
        let pending = self
            .index
            .fetch_pending(&self.journals, batch_size, expiring_before, ffqns);
        let mut payload = Vec::with_capacity(pending.len());
        for (journal, scheduled_at) in pending {
            let item = LockedExecution {
                execution_id: journal.execution_id().clone(),
                version: journal.version(), // updated later
                ffqn: journal.ffqn().clone(),
                params: journal.params(),
                event_history: Vec::default(), // updated later
                scheduled_at: scheduled_at,
                retry_exp_backoff: journal.retry_exp_backoff(),
                max_retries: journal.max_retries(),
            };
            payload.push(item);
        }
        // Lock, update the version and event history.
        for row in payload.iter_mut() {
            let (new_event_history, new_version) = self
                .lock(
                    created_at,
                    row.execution_id.clone(),
                    row.version,
                    executor_name.clone(),
                    lock_expires_at,
                )
                .expect_or_log("must be lockable within the same transaction");
            row.version = new_version;
            row.event_history.extend(new_event_history);
        }
        DbTickResponse::LockPending {
            payload,
            resp_sender,
        }
    }

    #[instrument(skip_all, fields(execution_id = %request.execution_id()))]
    fn handle_specific(&mut self, request: ExecutionSpecificRequest<ID>) -> DbTickResponse<ID> {
        match request {
            ExecutionSpecificRequest::Append {
                created_at,
                execution_id,
                version,
                event,
                resp_sender,
            } => DbTickResponse::AppendResult {
                resp_sender,
                payload: self.append(created_at, execution_id, version, event),
            },
            ExecutionSpecificRequest::Lock {
                created_at,
                execution_id,
                version,
                executor_name,
                lock_expires_at,
                resp_sender,
            } => DbTickResponse::Lock {
                resp_sender,
                payload: self.lock(
                    created_at,
                    execution_id,
                    version,
                    executor_name,
                    lock_expires_at,
                ),
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

    fn create(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        scheduled_at: Option<DateTime<Utc>>,
        parent: Option<ID>,
        retry_exp_backoff: Duration,
        max_retries: u32,
    ) -> Result<AppendResponse, RowSpecificError> {
        if self.journals.contains_key(&execution_id) {
            return Err(RowSpecificError::ValidationFailed(Cow::Borrowed(
                "execution is already initialized",
            )));
        }
        let journal = ExecutionJournal::new(
            execution_id.clone(),
            ffqn,
            params,
            scheduled_at,
            parent,
            created_at,
            retry_exp_backoff,
            max_retries,
        );
        let version = journal.version();
        self.journals
            .insert(execution_id.clone(), journal)
            .expect_none_or_log("journals cannot contain the new execution");
        self.index.update(execution_id, &self.journals);
        Ok(version)
    }

    fn lock(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<LockResponse<ID>, RowSpecificError> {
        let event = ExecutionEventInner::Locked {
            executor_name,
            lock_expires_at,
        };
        self.append(created_at, execution_id.clone(), version, event)
            .map(|_| {
                let journal = self.journals.get(&execution_id).unwrap_or_log();
                (journal.event_history(), journal.version())
            })
    }

    fn append(
        &mut self,
        created_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<AppendResponse, RowSpecificError> {
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
            retry_exp_backoff,
            max_retries,
        } = event
        {
            return if version == 0 {
                self.create(
                    created_at,
                    execution_id,
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                    retry_exp_backoff,
                    max_retries,
                )
            } else {
                info!("Wrong version");
                return Err(RowSpecificError::VersionMismatch);
            };
        }
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(RowSpecificError::NotFound);
        };
        if version != journal.version() {
            return Err(RowSpecificError::VersionMismatch);
        }
        journal.append(created_at, event)?;
        let version = journal.version();
        self.index.update(execution_id, &self.journals);
        Ok(version)
    }

    fn get(&mut self, execution_id: ID) -> Result<ExecutionHistory<ID>, RowSpecificError> {
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(RowSpecificError::NotFound);
        };
        Ok(journal.as_execution_history())
    }

    fn cleanup_expired_locks(
        &mut self,
        now: DateTime<Utc>,
        resp_sender: oneshot::Sender<CleanupExpiredLocks>,
    ) -> DbTickResponse<ID> {
        let expired = self.index.fetch_expired(now);
        let len = expired.len();
        for execution_id in expired {
            let journal = self.journals.get(&execution_id).unwrap();

            let event = if let Some(duration) = journal.can_be_retried_after() {
                let expires_at = now + duration;
                debug!("Retrying execution with expired lock after {duration:?} at {expires_at}");
                ExecutionEventInner::IntermittentTimeout { expires_at }
            } else {
                info!("Marking execution with expired lock as permanently timed out");
                ExecutionEventInner::Finished {
                    result: Err(FinishedExecutionError::PermanentTimeout),
                }
            };
            self.append(
                now,
                journal.execution_id().clone(),
                journal.version(),
                event,
            )
            .expect_or_log("must be lockable within the same transaction");
            self.index.update(execution_id, &self.journals);
        }
        DbTickResponse::CleanupExpiredLocks {
            payload: len,
            resp_sender,
        }
    }

    fn append_batch(
        &mut self,
        append: Vec<AppendRequest<ID>>,
        resp_sender: oneshot::Sender<AppendBatchResponse>,
    ) -> DbTickResponse<ID> {
        let payload = append
            .into_iter()
            .map(|req| self.append(req.created_at, req.execution_id, req.version, req.event))
            .collect();
        DbTickResponse::AppendBatchResult {
            payload,
            resp_sender,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        storage::{ExecutionEvent, HistoryEvent},
        FinishedExecutionResult,
    };
    use assert_matches::assert_matches;
    use concepts::{prefixed_ulid::WorkflowId, FunctionFqnStr};
    use std::time::{Duration, Instant};
    use test_utils::env_or_default;
    use tokio::time::sleep;
    use tracing::info;
    use tracing_unwrap::ResultExt;
    use utils::time::now;

    #[derive(Clone)]
    pub(crate) struct TickBasedDbConnection<ID: ExecutionId> {
        pub(crate) db_task: Arc<std::sync::Mutex<DbTask<ID>>>,
    }

    #[async_trait]
    impl<ID: ExecutionId> DbConnection<ID> for TickBasedDbConnection<ID> {
        #[instrument(skip_all)]
        async fn lock_pending(
            &self,
            batch_size: usize,
            pending_at_or_sooner: DateTime<Utc>,
            ffqns: Vec<FunctionFqn>,
            created_at: DateTime<Utc>,
            executor_name: ExecutorName,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<LockPendingResponse<ID>, DbConnectionError> {
            let request = DbRequest::General(GeneralRequest::LockPending {
                batch_size,
                pending_at_or_sooner,
                ffqns,
                created_at,
                executor_name,
                lock_expires_at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(request);
            Ok(assert_matches!(response, DbTickResponse::LockPending {  payload, .. } => payload))
        }

        #[instrument(skip_all)]
        async fn cleanup_expired_locks(
            &self,
            now: DateTime<Utc>,
        ) -> Result<CleanupExpiredLocks, DbConnectionError> {
            let request = DbRequest::General(GeneralRequest::CleanupExpiredLocks {
                now,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(request);
            Ok(
                assert_matches!(response, DbTickResponse::CleanupExpiredLocks { payload, .. } => payload),
            )
        }

        async fn lock(
            &self,
            created_at: DateTime<Utc>,
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            lock_expires_at: DateTime<Utc>,
        ) -> Result<LockResponse<ID>, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
                created_at,
                execution_id,
                version,
                executor_name,
                lock_expires_at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(request);
            assert_matches!(response, DbTickResponse::Lock { payload, .. } => payload)
                .map_err(|err| DbError::RowSpecific(err))
        }

        async fn append(
            &self,
            created_at: DateTime<Utc>,
            execution_id: ID,
            version: Version,
            event: ExecutionEventInner<ID>,
        ) -> Result<AppendResponse, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Append {
                created_at,
                execution_id,
                version,
                event,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(request);
            assert_matches!(response, DbTickResponse::AppendResult { payload, .. } => payload)
                .map_err(|err| DbError::RowSpecific(err))
        }

        async fn append_batch(
            &self,
            append: Vec<AppendRequest<ID>>,
        ) -> Result<AppendBatchResponse, DbConnectionError> {
            let request = DbRequest::General(GeneralRequest::AppendBatch {
                append,
                resp_sender: oneshot::channel().0,
            });

            let response = self.db_task.lock().unwrap_or_log().tick(request);
            Ok(
                assert_matches!(response, DbTickResponse::AppendBatchResult { payload, .. } => payload),
            )
        }

        async fn get(&self, execution_id: ID) -> Result<ExecutionHistory<ID>, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Get {
                execution_id,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(request);
            assert_matches!(response, DbTickResponse::Get { payload, .. } => payload)
                .map_err(|err| DbError::RowSpecific(err))
        }
    }

    fn set_up() {
        test_utils::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn close() {
        set_up();
        let mut task = DbTask::<WorkflowId>::spawn_new(1);
        task.close().await;
    }

    #[tokio::test]
    async fn stochastic_lifecycle_tick_based() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        lifecycle(db_connection).await;
    }

    #[tokio::test]
    async fn stochastic_lifecycle_task_based() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let client_to_store_req_sender = db_task.client_to_store_req_sender.clone().unwrap_or_log();
        let db_connection = InMemoryDbConnection {
            client_to_store_req_sender,
        };
        lifecycle(db_connection).await;
        db_task.close().await;
    }

    async fn lifecycle(db_connection: impl DbConnection<WorkflowId> + Sync) {
        let execution_id = WorkflowId::generate();
        let exec1 = Arc::new("exec1".to_string());
        let exec2 = Arc::new("exec2".to_string());
        let lock_expiry = Duration::from_millis(500);
        assert!(db_connection
            .lock_pending(
                1,
                now(),
                vec![SOME_FFQN.to_owned()],
                now(),
                exec1.clone(),
                now() + lock_expiry,
            )
            .await
            .unwrap_or_log()
            .is_empty());

        let mut version;
        // Create
        {
            db_connection
                .create(
                    now(),
                    execution_id.clone(),
                    SOME_FFQN.to_owned(),
                    Params::default(),
                    None,
                    None,
                    Duration::ZERO,
                    0,
                )
                .await
                .unwrap_or_log();
        }
        // LockPending
        sleep(Duration::from_secs(1)).await;
        {
            info!("LockPending: {}", now());
            let mut locked_executions = db_connection
                .lock_pending(
                    1,
                    now(),
                    vec![SOME_FFQN.to_owned()],
                    now(),
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log();
            assert_eq!(1, locked_executions.len());
            let locked_execution = locked_executions.pop().unwrap();
            assert_eq!(execution_id, locked_execution.execution_id);
            assert_eq!(2, locked_execution.version);
            assert_eq!(0, locked_execution.params.len());
            assert_eq!(SOME_FFQN.to_owned(), locked_execution.ffqn);
            version = locked_execution.version;
        }
        // intermittent timeout
        sleep(Duration::from_millis(499)).await;
        {
            info!("Intermittent timeout: {}", now());
            let event = ExecutionEventInner::IntermittentTimeout {
                expires_at: now() + lock_expiry,
            };

            version = db_connection
                .append(now(), execution_id.clone(), version, event)
                .await
                .unwrap_or_log();
        }
        // Attempt to lock while in a timeout
        sleep(lock_expiry - Duration::from_millis(100)).await;
        {
            info!("Attempt to lock using exec2: {}", now());
            assert!(db_connection
                .lock(
                    now(),
                    execution_id.clone(),
                    version,
                    exec2.clone(),
                    now() + lock_expiry,
                )
                .await
                .is_err());
            // Version is not changed
        }
        // Lock using exec1
        sleep(Duration::from_millis(100)).await;
        {
            info!("Extend lock using exec1: {}", now());
            let (event_history, current_version) = db_connection
                .lock(
                    now(),
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + Duration::from_secs(1),
                )
                .await
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version = current_version;
        }
        sleep(Duration::from_millis(700)).await;
        // Attempt to lock using exec2 while in a lock
        {
            info!("Attempt to lock using exec2: {}", now());
            assert!(db_connection
                .lock(
                    now(),
                    execution_id.clone(),
                    version,
                    exec2.clone(),
                    now() + lock_expiry,
                )
                .await
                .is_err());
            // Version is not changed
        }
        // Extend the lock using exec1
        {
            info!("Extend lock using exec1: {}", now());
            let (event_history, current_version) = db_connection
                .lock(
                    now(),
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version = current_version;
        }
        // Yield
        sleep(Duration::from_millis(200)).await;
        {
            info!("Yield: {}", now());

            let event = ExecutionEventInner::HistoryEvent {
                event: HistoryEvent::Yield,
            };
            version = db_connection
                .append(now(), execution_id.clone(), version, event)
                .await
                .unwrap_or_log();
        }
        // Lock again
        sleep(Duration::from_millis(200)).await;
        {
            info!("Lock again: {}", now());
            let (event_history, current_version) = db_connection
                .lock(
                    now(),
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log();
            assert_eq!(1, event_history.len());
            assert_eq!(vec![HistoryEvent::Yield], event_history);
            version = current_version;
        }
        // Finish
        sleep(Duration::from_millis(300)).await;
        {
            info!("Finish: {}", now());
            let event = ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionResult::None),
            };

            db_connection
                .append(now(), execution_id.clone(), version, event)
                .await
                .unwrap_or_log();
        }
    }

    #[tokio::test]
    async fn stochastic_lock_expired_means_permanent_timeout_tick_based() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        lock_expired_means_timeout(db_connection).await;
    }

    #[tokio::test]
    async fn stochastic_lock_expired_means_permanent_timeout_task_based() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let client_to_store_req_sender = db_task.client_to_store_req_sender.clone().unwrap_or_log();
        let db_connection = InMemoryDbConnection {
            client_to_store_req_sender,
        };
        lock_expired_means_timeout(db_connection).await;
        db_task.close().await;
    }

    async fn lock_expired_means_timeout(db_connection: impl DbConnection<WorkflowId> + Sync) {
        let execution_id = WorkflowId::generate();
        let exec1 = Arc::new("exec1".to_string());
        // Create
        let retry_exp_backoff = Duration::from_millis(100);
        {
            db_connection
                .create(
                    now(),
                    execution_id.clone(),
                    SOME_FFQN.to_owned(),
                    Params::default(),
                    None,
                    None,
                    retry_exp_backoff,
                    1,
                )
                .await
                .unwrap_or_log();
        }
        // Lock pending
        let lock_duration = Duration::from_millis(500);
        {
            let mut locked_executions = db_connection
                .lock_pending(
                    1,
                    now(),
                    vec![SOME_FFQN.to_owned()],
                    now(),
                    exec1.clone(),
                    now() + lock_duration,
                )
                .await
                .unwrap_or_log();
            assert_eq!(1, locked_executions.len());
            let locked_execution = locked_executions.pop().unwrap();
            assert_eq!(execution_id, locked_execution.execution_id);
            assert_eq!(SOME_FFQN.to_owned(), locked_execution.ffqn);
            assert_eq!(2, locked_execution.version);
        }
        // Calling `cleanup_expired_locks` after lock expiry should result in intermittent timeout.
        sleep(lock_duration).await;
        {
            let cleanup_at = now();
            let expired = db_connection
                .cleanup_expired_locks(cleanup_at)
                .await
                .unwrap();
            assert_eq!(1, expired);
            let execution_history = db_connection.get(execution_id.clone()).await.unwrap();
            let timeout_expiry = cleanup_at + retry_exp_backoff;
            assert_eq!(
                PendingState::PendingAt(timeout_expiry),
                execution_history.pending_state
            );
            let last_event = execution_history.last_event();
            assert_matches!(
                last_event,
                ExecutionEvent {
                    event: ExecutionEventInner::IntermittentTimeout {
                        expires_at,
                    },
                    ..
                } if *expires_at == timeout_expiry
            );
        }
        // Lock again
        sleep(retry_exp_backoff).await;
        {
            let mut locked_executions = db_connection
                .lock_pending(
                    1,
                    now(),
                    vec![SOME_FFQN.to_owned()],
                    now(),
                    exec1.clone(),
                    now() + lock_duration,
                )
                .await
                .unwrap_or_log();
            assert_eq!(1, locked_executions.len());
            let locked_execution = locked_executions.pop().unwrap();
            assert_eq!(execution_id, locked_execution.execution_id);
            assert_eq!(SOME_FFQN.to_owned(), locked_execution.ffqn);
            assert_eq!(4, locked_execution.version);
        }
        // Calling `cleanup_expired_locks` after lock expiry should result in permanent timeout.
        sleep(lock_duration).await;
        let expired = db_connection.cleanup_expired_locks(now()).await.unwrap();
        assert_eq!(1, expired);
        let execution_history = db_connection.get(execution_id).await.unwrap();
        assert_eq!(PendingState::Finished, execution_history.pending_state);
        let last_event = execution_history.last_event();
        assert_matches!(
            last_event,
            ExecutionEvent {
                event: ExecutionEventInner::Finished {
                    result: Err(crate::FinishedExecutionError::PermanentTimeout)
                },
                ..
            }
        );
    }

    #[tokio::test]
    async fn stochastic_proptest() {
        set_up();

        let raw_data: Vec<u8> = {
            let len = madsim::rand::random::<u16>() as usize;
            let mut raw_data = Vec::with_capacity(len);
            while raw_data.len() < len {
                raw_data.push(madsim::rand::random::<u8>());
            }
            raw_data
        };
        let mut unstructured = arbitrary::Unstructured::new(&raw_data);

        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        let execution_id = WorkflowId::generate();
        let mut version;
        // Create
        {
            version = db_connection
                .create(
                    now(),
                    execution_id.clone(),
                    SOME_FFQN.to_owned(),
                    Params::default(),
                    None,
                    None,
                    Duration::ZERO,
                    0,
                )
                .await
                .unwrap_or_log();
        }
        let events = unstructured.int_in_range(5..=10).unwrap_or_log();
        for _ in 0..events {
            let event: ExecutionEventInner<WorkflowId> = unstructured.arbitrary().unwrap_or_log();
            match db_connection
                .append(now(), execution_id.clone(), version, event)
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
        let client_to_store_req_sender = db_task.client_to_store_req_sender.clone().unwrap_or_log();
        let db_connection = InMemoryDbConnection {
            client_to_store_req_sender,
        };

        let stopwatch = Instant::now();
        let append = {
            let now = now();
            let ffqn = SOME_FFQN.to_owned();
            (0..executions)
                .map(|_| AppendRequest {
                    created_at: now,
                    execution_id: WorkflowId::generate(),
                    version: Version::default(),
                    event: ExecutionEventInner::Created {
                        ffqn: ffqn.clone(),
                        params: Params::default(),
                        parent: None,
                        scheduled_at: None,
                        retry_exp_backoff: Duration::ZERO,
                        max_retries: 0,
                    },
                })
                .collect()
        };
        warn!(
            "Created {executions} append entries in {:?}",
            stopwatch.elapsed()
        );
        let stopwatch = Instant::now();
        db_connection.append_batch(append).await.unwrap();
        warn!(
            "Appended {executions} executions in {:?}",
            stopwatch.elapsed()
        );

        // spawn executors
        let exec_name = Arc::new("exec".to_string());
        let exec_tasks = (0..tasks)
            .map(|_| {
                let db_connection = db_connection.clone();
                let exec_name = exec_name.clone();
                tokio::spawn(async move {
                    let target = executions / tasks;
                    let mut locked = Vec::with_capacity(target);
                    while locked.len() < target {
                        let now = now();
                        let locked_now = db_connection
                            .lock_pending(
                                batch_size,
                                now,
                                vec![SOME_FFQN.to_owned()],
                                now,
                                exec_name.clone(),
                                now + Duration::from_secs(1),
                            )
                            .await
                            .unwrap();
                        locked.extend(locked_now.into_iter());
                    }
                    locked
                })
            })
            .collect::<Vec<_>>();
        for task_handle in exec_tasks {
            let _locked_vec = task_handle.await.unwrap();
        }
        warn!("Finished in {} ms", stopwatch.elapsed().as_millis());
        drop(db_connection);
        db_task.close().await;
    }
}
