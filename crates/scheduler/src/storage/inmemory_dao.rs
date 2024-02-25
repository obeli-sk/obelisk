//! Append only database containing executions and their state changes - execution journal.
//! Current execution state can be obtained by getting the last (non-async-response)
//! state from the execution journal.
//!
//! When inserting, the row in the journal must contain a version that must be equal
//! to the current number of events in the journal. First change with the expected version wins.
//!
//! There can be many schedulers and executors operating on the same execution.
//!
//! Schedulers subscribe to pending executions using `FetchPending` with a list of
//! fully qualified function names that are supported.

use crate::{
    time::now,
    worker::{DbConnection, DbError, DbWriteError},
    FinishedExecutionResult,
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
use derivative::Derivative;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::AbortHandle,
};
use tracing::{debug, error, info, instrument, trace, warn, Level};
use tracing_unwrap::{OptionExt, ResultExt};

use self::{
    api::{DbRequest, DbTickRequest, ExecutionSpecificRequest, GeneralRequest, Version},
    index::{JournalsIndex, PotentiallyPending},
};

pub type ExecutorName = Arc<String>;

#[derive(Clone, Debug, derive_more::Display)]
#[display(fmt = "{event}")]
struct ExecutionEvent<ID: ExecutionId> {
    created_at: DateTime<Utc>,
    event: ExecutionEventInner<ID>,
}

#[derive(Clone, Debug, derive_more::Display)]
pub(crate) enum ExecutionEventInner<ID: ExecutionId> {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new `FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    #[display(fmt = "Created({ffqn})")]
    Created {
        // execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        parent: Option<ID>,
        scheduled_at: Option<DateTime<Utc>>,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    #[display(fmt = "Locked")]
    Locked {
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentFailure")]
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        reason: String,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    #[display(fmt = "IntermittentTimeout")]
    IntermittentTimeout { expires_at: DateTime<Utc> },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    #[display(fmt = "Finished")]
    Finished { result: FinishedExecutionResult<ID> },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    #[display(fmt = "CancelRequest")]
    CancelRequest,

    #[display(fmt = "EventHistory({event})")]
    EventHistory { event: EventHistory<ID> },
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display)]
pub(crate) enum EventHistory<ID: ExecutionId> {
    // Created by the executor holding last lock.
    // Interpreted as lock being ended.
    Yield,
    #[display(fmt = "Persist")]
    // Created by the executor holding last lock.
    // Does not block the execution
    Persist {
        value: Vec<u8>,
    },
    // Created by the executor holding last lock.
    // Does not block the execution
    JoinSet {
        joinset_id: ID,
    },
    // Created by an executor
    // Processed by a scheduler
    // Later followed by DelayFinished
    #[display(fmt = "DelayedUntilAsyncRequest({joinset_id})")]
    DelayedUntilAsyncRequest {
        joinset_id: ID,
        delay_id: ID,
        expires_at: DateTime<Utc>,
    },
    // Created by an executor
    // Processed by a scheduler - new execution must be scheduled
    // Immediately followed by ChildExecutionRequested
    #[display(fmt = "ChildExecutionAsyncRequest({joinset_id})")]
    ChildExecutionAsyncRequest {
        joinset_id: ID,
        child_execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
    },
    // Created by a scheduler right after.
    // Processed by other schedulers
    ChildExecutionRequested {
        child_execution_id: ID,
    },
    // Created by the executor.
    // Executor continues without blocking.
    JoinNextFetched {
        joinset_id: ID,
    },
    #[display(fmt = "EventHistoryAsyncResponse({joinset_id})")]
    EventHistoryAsyncResponse {
        joinset_id: ID,
        response: EventHistoryAsyncResponse<ID>,
    },
    // Created by the executor.
    // Execution is blocked, until the next response of the
    // joinset arrives. After that, a scheduler issues `WaitingForExecutor`.
    JoinNextBlocking {
        joinset_id: ID,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum EventHistoryAsyncResponse<ID: ExecutionId> {
    // Created by a scheduler sometime after DelayedUntilAsyncRequest.
    DelayFinishedAsyncResponse {
        delay_id: ID,
    },
    // Created by a scheduler sometime after ChildExecutionRequested.
    ChildExecutionAsyncResponse {
        child_execution_id: ID,
        result: FinishedExecutionResult<ID>,
    },
}

#[derive(Debug)]
struct ExecutionJournal<ID: ExecutionId> {
    execution_id: ID,
    events: VecDeque<ExecutionEvent<ID>>,
}

impl<ID: ExecutionId> ExecutionJournal<ID> {
    fn new(
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        scheduled_at: Option<DateTime<Utc>>,
        parent: Option<ID>,
        created_at: DateTime<Utc>,
    ) -> Self {
        let event = ExecutionEvent {
            event: ExecutionEventInner::Created {
                ffqn,
                params,
                scheduled_at,
                parent,
            },
            created_at,
        };
        Self {
            execution_id,
            events: VecDeque::from([event]),
        }
    }

    fn len(&self) -> usize {
        self.events.len()
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.events.iter().rev().next().unwrap_or_log().created_at
    }

    fn ffqn(&self) -> &FunctionFqn {
        match self.events.get(0) {
            Some(ExecutionEvent {
                event: ExecutionEventInner::Created { ffqn, .. },
                ..
            }) => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    fn version(&self) -> Version {
        self.events.len()
    }

    fn id(&self) -> &ID {
        &self.execution_id
    }

    fn validate_push(
        &mut self,
        event: ExecutionEventInner<ID>,
        created_at: DateTime<Utc>,
    ) -> Result<(), DbWriteError> {
        if let ExecutionEventInner::Locked {
            executor_name,
            expires_at,
        } = &event
        {
            if *expires_at <= created_at {
                return Err(DbWriteError::ValidationFailed("invalid expiry date"));
            }
            match index::potentially_pending(self) {
                PotentiallyPending::PendingNow => {}
                PotentiallyPending::PendingAfterExpiry(pending_start)
                    if pending_start <= created_at => {}
                PotentiallyPending::Locked {
                    executor_name: locked_by,
                    expires_at,
                } => {
                    if *executor_name == locked_by {
                        // we allow extending the lock
                    } else if expires_at <= created_at {
                        // we allow locking after the old lock expired
                    } else {
                        return Err(DbWriteError::ValidationFailed("already locked"));
                    }
                }
                _not_pending => {
                    return Err(DbWriteError::ValidationFailed("already locked"));
                }
            }
        }
        self.events.push_back(ExecutionEvent { event, created_at });
        Ok(())
    }

    fn event_history(&self) -> Vec<EventHistory<ID>> {
        self.events
            .iter()
            .filter_map(|event| {
                if let ExecutionEventInner::EventHistory { event: eh, .. } = &event.event {
                    Some(eh.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

pub(crate) mod api {
    use crate::worker::DbWriteError;

    use super::{EventHistory, ExecutionEventInner, ExecutorName};
    use chrono::{DateTime, Utc};
    use concepts::{ExecutionId, FunctionFqn, Params};
    use derivative::Derivative;
    use tokio::sync::oneshot;

    pub(crate) type Version = usize;

    #[derive(Debug)]
    pub(crate) struct DbTickRequest<ID: ExecutionId> {
        pub(crate) request: DbRequest<ID>,
        pub(crate) received_at: DateTime<Utc>,
    }

    #[derive(Debug)]
    pub(crate) enum DbRequest<ID: ExecutionId> {
        General(GeneralRequest<ID>),
        ExecutionSpecific(ExecutionSpecificRequest<ID>),
    }

    #[derive(Derivative)]
    #[derivative(Debug)]
    #[derive(derive_more::Display)]
    pub(crate) enum ExecutionSpecificRequest<ID: ExecutionId> {
        #[display(fmt = "Create")]
        Create {
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<ID>,
            #[derivative(Debug = "ignore")]
            resp_sender: oneshot::Sender<Result<(), DbWriteError>>,
        },
        #[display(fmt = "Lock")]
        Lock {
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
            #[derivative(Debug = "ignore")]
            resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, DbWriteError>>,
        },
        #[display(fmt = "Insert({event})")]
        Insert {
            execution_id: ID,
            version: Version,
            event: ExecutionEventInner<ID>,
            #[derivative(Debug = "ignore")]
            resp_sender: oneshot::Sender<Result<(), DbWriteError>>,
        },
    }

    #[derive(Derivative)]
    #[derivative(Debug)]
    pub(crate) enum GeneralRequest<ID: ExecutionId> {
        FetchPending {
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            created_since: Option<DateTime<Utc>>,
            ffqns: Vec<FunctionFqn>,
            #[derivative(Debug = "ignore")]
            resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
        },
    }
}

mod index {
    use super::{EventHistory, ExecutionEventInner, ExecutionJournal, ExecutorName};
    use crate::storage::inmemory_dao::ExecutionEvent;
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use tracing::error;
    use tracing_unwrap::OptionExt;

    #[derive(Debug)]
    pub(super) struct JournalsIndex<ID: ExecutionId> {
        pending: BTreeSet<ID>,
        pending_scheduled: BTreeMap<DateTime<Utc>, BTreeSet<ID>>,
        pending_scheduled_rev: HashMap<ID, DateTime<Utc>>,
    }

    impl<ID: ExecutionId> JournalsIndex<ID> {
        pub(super) fn fetch_pending<'a>(
            &self,
            journals: &'a HashMap<ID, ExecutionJournal<ID>>,
            received_at: DateTime<Utc>,
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            created_since: Option<DateTime<Utc>>,
            ffqns: Vec<concepts::FunctionFqn>,
        ) -> Vec<(&'a ExecutionJournal<ID>, Option<DateTime<Utc>>)> {
            let mut pending = self
                .pending
                .iter()
                .map(|id| (journals.get(id).unwrap_or_log(), None))
                .collect::<Vec<_>>();
            pending.extend(self.pending_scheduled.range(..expiring_before).flat_map(
                |(scheduled_at, ids)| {
                    ids.iter()
                        .map(|id| (journals.get(id).unwrap_or_log(), Some(scheduled_at.clone())))
                },
            ));
            // filter by currently pending
            pending.retain(|(journal, _)| match potentially_pending(journal) {
                PotentiallyPending::PendingNow => true,
                PotentiallyPending::PendingAfterExpiry(pending_after)
                | PotentiallyPending::Locked {
                    expires_at: pending_after,
                    ..
                } => pending_after < received_at,
                PotentiallyPending::PendingAfterExternalEvent => false,
                PotentiallyPending::NotPending => {
                    error!("Expected pending event in {journal:?}");
                    false
                }
            });
            // filter by ffqn
            pending.retain(|(journal, _)| ffqns.contains(journal.ffqn()));
            // filter out older than `created_since`
            if let Some(created_since) = created_since {
                pending.retain(|(journal, _)| journal.created_at() >= created_since);
            }
            pending.truncate(batch_size);
            pending
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
            let journal = journals.get(&execution_id).unwrap_or_log();
            // Add it again if needed
            match potentially_pending(journal) {
                PotentiallyPending::PendingNow => {
                    self.pending.insert(execution_id);
                }
                PotentiallyPending::PendingAfterExpiry(expires_at)
                | PotentiallyPending::Locked { expires_at, .. } => {
                    self.pending_scheduled
                        .entry(expires_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.pending_scheduled_rev.insert(execution_id, expires_at);
                }
                PotentiallyPending::PendingAfterExternalEvent | PotentiallyPending::NotPending => {}
            }
        }
    }

    impl<T: ExecutionId> Default for JournalsIndex<T> {
        fn default() -> Self {
            Self {
                pending: Default::default(),
                pending_scheduled: Default::default(),
                pending_scheduled_rev: Default::default(),
            }
        }
    }

    #[derive(Debug)]
    pub(super) enum PotentiallyPending {
        PendingNow,
        Locked {
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
        },
        PendingAfterExpiry(DateTime<Utc>),
        PendingAfterExternalEvent,
        NotPending,
    }

    pub(super) fn potentially_pending<ID: ExecutionId>(
        journal: &ExecutionJournal<ID>,
    ) -> PotentiallyPending {
        match last_event_excluding_async_responses(journal) {
            (
                _,
                ExecutionEvent {
                    event:
                        ExecutionEventInner::Created {
                            scheduled_at: None, ..
                        },
                    ..
                },
            ) => PotentiallyPending::PendingNow,
            (
                _,
                ExecutionEvent {
                    event:
                        ExecutionEventInner::Created {
                            scheduled_at: Some(scheduled_at),
                            ..
                        },
                    ..
                },
            ) => PotentiallyPending::PendingAfterExpiry(scheduled_at.clone()),
            (
                _,
                ExecutionEvent {
                    event:
                        ExecutionEventInner::Locked {
                            executor_name,
                            expires_at,
                        },
                    ..
                },
            ) => PotentiallyPending::Locked {
                executor_name: executor_name.clone(),
                expires_at: *expires_at,
            },
            (
                _,
                ExecutionEvent {
                    event: ExecutionEventInner::IntermittentFailure { expires_at, .. },
                    ..
                },
            ) => PotentiallyPending::PendingAfterExpiry(*expires_at),
            (
                _,
                ExecutionEvent {
                    event: ExecutionEventInner::IntermittentTimeout { expires_at, .. },
                    ..
                },
            ) => PotentiallyPending::PendingAfterExpiry(*expires_at),
            (
                _,
                ExecutionEvent {
                    event:
                        ExecutionEventInner::EventHistory {
                            event: EventHistory::Yield { .. },
                            ..
                        },
                    ..
                },
            ) => PotentiallyPending::PendingNow,
            (
                idx,
                ExecutionEvent {
                    event:
                        ExecutionEventInner::EventHistory {
                            event:
                                EventHistory::JoinNextBlocking {
                                    joinset_id: expected_join_set_id,
                                    ..
                                },
                            ..
                        },
                    ..
                },
            ) => {
                // pending if this event is followed by an async response
                if journal
                    .events
                    .iter()
                    .skip(idx + 1)
                    .find(|event| {
                        matches!(event, ExecutionEvent {
                            event:
                                ExecutionEventInner::EventHistory{event:
                                    EventHistory::EventHistoryAsyncResponse { joinset_id, .. },
                                .. },
                        .. }
                        if joinset_id == expected_join_set_id)
                    })
                    .is_some()
                {
                    PotentiallyPending::PendingNow
                } else {
                    PotentiallyPending::PendingAfterExternalEvent
                }
            }
            _ => PotentiallyPending::NotPending,
        }
    }

    fn last_event_excluding_async_responses<ID: ExecutionId>(
        journal: &ExecutionJournal<ID>,
    ) -> (usize, &ExecutionEvent<ID>) {
        journal
            .events
            .iter()
            .enumerate()
            .rev()
            .find(|(_idx, event)| {
                !matches!(
                    event,
                    ExecutionEvent {
                        event: ExecutionEventInner::EventHistory {
                            event: EventHistory::EventHistoryAsyncResponse { .. },
                            ..
                        },
                        ..
                    }
                )
            })
            .expect_or_log("must contain at least one non-async-response event")
    }
}

pub(crate) struct DbTaskHandle<ID: ExecutionId> {
    client_to_store_req_sender: Option<mpsc::Sender<DbRequest<ID>>>,
    abort_handle: AbortHandle,
}

impl<ID: ExecutionId> DbTaskHandle<ID> {
    pub fn sender(&self) -> Option<mpsc::Sender<DbRequest<ID>>> {
        self.client_to_store_req_sender.clone()
    }

    pub async fn close(&mut self) {
        self.client_to_store_req_sender.take();
        debug!("Gracefully closing");
        #[cfg(not(madsim))]
        while !self.abort_handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        info!("Gracefully closed");
    }
}

impl<ID: ExecutionId> Drop for DbTaskHandle<ID> {
    fn drop(&mut self) {
        #[cfg(not(madsim))]
        {
            if self.abort_handle.is_finished() {
                // https://github.com/madsim-rs/madsim/issues/191
                return;
            }
            warn!("Aborting the database task");
        }
        self.abort_handle.abort();
    }
}

#[derive(Debug)]
pub(crate) struct DbTask<ID: ExecutionId> {
    journals: HashMap<ID, ExecutionJournal<ID>>,
    index: JournalsIndex<ID>,
}

impl<ID: ExecutionId> ExecutionSpecificRequest<ID> {
    fn execution_id(&self) -> &ID {
        match self {
            ExecutionSpecificRequest::Create { execution_id, .. } => execution_id,
            ExecutionSpecificRequest::Lock { execution_id, .. } => execution_id,
            ExecutionSpecificRequest::Insert { execution_id, .. } => execution_id,
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum DbTickResponse<ID: ExecutionId> {
    FetchPending {
        pending_executions: Vec<(ID, Version, Option<DateTime<Utc>>)>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
    },
    Lock {
        result: Result<Vec<EventHistory<ID>>, DbWriteError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, DbWriteError>>,
    },
    PersistResult {
        result: Result<(), DbWriteError>,
        #[derivative(Debug = "ignore")]
        resp_sender: oneshot::Sender<Result<(), DbWriteError>>,
    },
}

impl<ID: ExecutionId> DbTickResponse<ID> {
    fn send_response(self) -> Result<(), ()> {
        match self {
            DbTickResponse::FetchPending {
                resp_sender,
                pending_executions: message,
            } => resp_sender.send(message).map_err(|_| ()),
            DbTickResponse::Lock {
                resp_sender,
                result,
            } => resp_sender.send(result).map_err(|_| ()),
            DbTickResponse::PersistResult {
                resp_sender,
                result,
            } => resp_sender.send(result).map_err(|_| ()),
        }
    }
}

impl<ID: ExecutionId> DbTask<ID> {
    pub fn new_spawn(rpc_capacity: usize) -> DbTaskHandle<ID> {
        let (client_to_store_req_sender, mut client_to_store_receiver) =
            mpsc::channel::<DbRequest<ID>>(rpc_capacity);
        let abort_handle = tokio::spawn(async move {
            let mut task = Self::new();
            while let Some(request) = client_to_store_receiver.recv().await {
                let resp = task
                    .tick(DbTickRequest {
                        request,
                        received_at: now(),
                    })
                    .send_response();
                if resp.is_err() {
                    debug!("Failed to send back the response");
                }
            }
            info!("Database task finished");
        })
        .abort_handle();

        DbTaskHandle {
            abort_handle,
            client_to_store_req_sender: Some(client_to_store_req_sender),
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            journals: HashMap::default(),
            index: JournalsIndex::default(),
        }
    }

    pub(crate) fn tick(&mut self, request: DbTickRequest<ID>) -> DbTickResponse<ID> {
        let DbTickRequest {
            request,
            received_at,
        } = request;
        match request {
            DbRequest::ExecutionSpecific(request) => self.handle_specific(request, received_at),
            DbRequest::General(request) => self.handle_general(request, received_at),
        }
    }

    fn handle_general(
        &mut self,
        request: GeneralRequest<ID>,
        received_at: DateTime<Utc>,
    ) -> DbTickResponse<ID> {
        trace!("Received General event {request:?} at `{received_at}`");
        match request {
            GeneralRequest::FetchPending {
                batch_size,
                expiring_before,
                created_since,
                ffqns,
                resp_sender,
            } => self.fetch_pending(
                received_at,
                batch_size,
                expiring_before,
                created_since,
                ffqns,
                resp_sender,
            ),
        }
    }

    fn fetch_pending(
        &self,
        received_at: DateTime<Utc>,
        batch_size: usize,
        expiring_before: DateTime<Utc>,
        created_since: Option<DateTime<Utc>>,
        ffqns: Vec<FunctionFqn>,
        resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
    ) -> DbTickResponse<ID> {
        let pending = self.index.fetch_pending(
            &self.journals,
            received_at,
            batch_size,
            expiring_before,
            created_since,
            ffqns,
        );
        let pending = pending
            .into_iter()
            .map(|(journal, scheduled_at)| (journal.id().clone(), journal.version(), scheduled_at))
            .collect();
        DbTickResponse::FetchPending {
            resp_sender,
            pending_executions: pending,
        }
    }

    #[instrument(skip_all, fields(execution_id = %request.execution_id()))]
    fn handle_specific(
        &mut self,
        request: ExecutionSpecificRequest<ID>,
        received_at: DateTime<Utc>,
    ) -> DbTickResponse<ID> {
        if tracing::enabled!(Level::TRACE) {
            trace!("Received {request:?} at `{received_at}`");
        } else {
            debug!("Received {request} at `{received_at}`");
        }
        let resp = match request {
            ExecutionSpecificRequest::Create {
                execution_id,
                ffqn,
                params,
                scheduled_at,
                parent,
                resp_sender,
            } => DbTickResponse::PersistResult {
                resp_sender,
                result: self.create(
                    received_at,
                    execution_id,
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                ),
            },
            ExecutionSpecificRequest::Insert {
                execution_id,
                version,
                event,
                resp_sender,
            } => DbTickResponse::PersistResult {
                resp_sender,
                result: self.insert(received_at, execution_id, version, event),
            },
            ExecutionSpecificRequest::Lock {
                execution_id,
                version,
                executor_name,
                expires_at,
                resp_sender,
            } => self.lock(
                received_at,
                execution_id,
                version,
                executor_name,
                expires_at,
                resp_sender,
            ),
        };

        if tracing::enabled!(Level::TRACE) {
            trace!("Responded with {resp:?}");
        } else {
            match &resp {
                DbTickResponse::FetchPending {
                    pending_executions, ..
                } => debug!("Fethed {} executions", pending_executions.len()),
                DbTickResponse::Lock { result: Ok(eh), .. } => {
                    debug!("Lock succeded with {} events", eh.len())
                }
                DbTickResponse::Lock {
                    result: Err(err), ..
                } => debug!("Lock failed: {err:?}"),
                DbTickResponse::PersistResult { result, .. } => {
                    debug!("Persist result: {result:?}")
                }
            };
        }

        resp
    }

    fn create(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        scheduled_at: Option<DateTime<Utc>>,
        parent: Option<ID>,
    ) -> Result<(), DbWriteError> {
        if self.journals.contains_key(&execution_id) {
            return Err(DbWriteError::ValidationFailed(
                "execution is already initialized",
            ));
        }
        let journal = ExecutionJournal::new(
            execution_id.clone(),
            ffqn,
            params,
            scheduled_at,
            parent,
            received_at,
        );
        self.journals
            .insert(execution_id.clone(), journal)
            .expect_none_or_log("journals cannot contain the new execution");
        self.index.update(execution_id, &self.journals);
        Ok(())
    }

    fn lock(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, DbWriteError>>,
    ) -> DbTickResponse<ID> {
        let event = ExecutionEventInner::Locked {
            executor_name,
            expires_at,
        };

        match self.insert(received_at, execution_id.clone(), version, event) {
            Ok(()) => {
                let event_history = self
                    .journals
                    .get(&execution_id)
                    .unwrap_or_log()
                    .event_history();
                DbTickResponse::Lock {
                    resp_sender,
                    result: Ok(event_history),
                }
            }
            Err(err) => DbTickResponse::Lock {
                resp_sender,
                result: Err(err),
            },
        }
    }

    fn insert(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<(), DbWriteError> {
        if let ExecutionEventInner::Created {
            ffqn,
            params,
            parent,
            scheduled_at,
        } = event
        {
            return if version == 0 {
                self.create(
                    received_at,
                    execution_id,
                    ffqn,
                    params,
                    scheduled_at,
                    parent,
                )
            } else {
                warn!("Wrong version");
                return Err(DbWriteError::VersionMismatch);
            };
        }
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            return Err(DbWriteError::NotFound);
        };
        if version != journal.version() {
            return Err(DbWriteError::VersionMismatch);
        }
        journal.validate_push(event, received_at)?;
        self.index.update(execution_id, &self.journals);
        Ok(())
    }
}

#[derive(Clone)]
struct InMemoryDbConnection<ID: ExecutionId> {
    client_to_store_req_sender: mpsc::Sender<DbRequest<ID>>,
}

#[async_trait]
impl<ID: ExecutionId> DbConnection<ID> for InMemoryDbConnection<ID> {
    #[instrument(skip_all)]
    async fn fetch_pending(
        &self,
        batch_size: usize,
        expiring_before: DateTime<Utc>,
        created_since: Option<DateTime<Utc>>,
        ffqns: Vec<FunctionFqn>,
    ) -> Result<Vec<(ID, Version, Option<DateTime<Utc>>)>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::General(GeneralRequest::FetchPending {
            batch_size,
            expiring_before,
            created_since,
            ffqns,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::SendError)?;
        resp_receiver.await.map_err(|_| DbError::RecvError)
    }

    #[instrument(skip_all, %execution_id)]
    async fn lock(
        &self,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
    ) -> Result<Result<Vec<EventHistory<ID>>, DbWriteError>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
            execution_id,
            version,
            executor_name,
            expires_at,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::SendError)?;
        resp_receiver.await.map_err(|_| DbError::RecvError)
    }

    #[instrument(skip_all, %execution_id)]
    async fn insert(
        &self,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<Result<(), DbWriteError>, DbError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Insert {
            execution_id,
            version,
            event,
            resp_sender,
        });
        self.client_to_store_req_sender
            .send(request)
            .await
            .map_err(|_| DbError::SendError)?;
        resp_receiver.await.map_err(|_| DbError::RecvError)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr};
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::info;

    #[derive(Clone)]
    pub(crate) struct TickBasedDbConnection<ID: ExecutionId> {
        pub(crate) db_task: Arc<std::sync::Mutex<DbTask<ID>>>,
    }

    #[async_trait]
    impl<ID: ExecutionId> DbConnection<ID> for TickBasedDbConnection<ID> {
        async fn fetch_pending(
            &self,
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            created_since: Option<DateTime<Utc>>,
            ffqns: Vec<FunctionFqn>,
        ) -> Result<Vec<(ID, Version, Option<DateTime<Utc>>)>, DbError> {
            let request = DbRequest::General(GeneralRequest::FetchPending {
                batch_size,
                expiring_before,
                created_since,
                ffqns,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(DbTickRequest {
                request,
                received_at: now(),
            });
            Ok(
                assert_matches!(response, DbTickResponse::FetchPending {  pending_executions, .. } => pending_executions),
            )
        }

        async fn lock(
            &self,
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
        ) -> Result<Result<Vec<EventHistory<ID>>, DbWriteError>, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
                execution_id,
                version,
                executor_name,
                expires_at,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(DbTickRequest {
                request,
                received_at: now(),
            });
            Ok(assert_matches!(response, DbTickResponse::Lock { result, .. } => result))
        }

        async fn insert(
            &self,
            execution_id: ID,
            version: Version,
            event: ExecutionEventInner<ID>,
        ) -> Result<Result<(), DbWriteError>, DbError> {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Insert {
                execution_id,
                version,
                event,
                resp_sender: oneshot::channel().0,
            });
            let response = self.db_task.lock().unwrap_or_log().tick(DbTickRequest {
                request,
                received_at: now(),
            });
            Ok(assert_matches!(response, DbTickResponse::PersistResult { result, .. } => result))
        }
    }

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn db_workflow_tick_based() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        db_workflow(db_connection).await;
    }

    #[tokio::test]
    async fn db_workflow_mpsc_based() {
        set_up();
        let db_task = DbTask::new_spawn(1);
        let client_to_store_req_sender = db_task.sender().unwrap_or_log();
        let db_connection = InMemoryDbConnection {
            client_to_store_req_sender,
        };
        db_workflow(db_connection).await;
    }

    #[tokio::test]
    async fn close() {
        set_up();
        let mut task = DbTask::<WorkflowId>::new_spawn(1);
        task.close().await;
    }

    async fn db_workflow(db_connection: impl DbConnection<WorkflowId>) {
        let execution_id = WorkflowId::generate();
        assert!(db_connection
            .fetch_pending(
                1,
                now() + Duration::from_secs(1),
                None,
                vec![SOME_FFQN.to_owned()],
            )
            .await
            .unwrap_or_log()
            .is_empty());
        let exec1 = Arc::new("exec1".to_string());
        let exec2 = Arc::new("exec2".to_string());
        let lock_expiry = Duration::from_millis(500);
        let mut version = 0;
        // Create
        {
            let event = ExecutionEventInner::Created {
                ffqn: SOME_FFQN.to_owned(),
                params: Params::default(),
                parent: None,
                scheduled_at: None,
            };
            db_connection
                .insert(execution_id.clone(), version, event)
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            version += 1;
        }
        // FetchPending
        sleep(Duration::from_secs(1)).await;
        {
            info!("FetchPending: {}", now());
            let pending = db_connection
                .fetch_pending(
                    1,
                    now() + Duration::from_secs(1),
                    None,
                    vec![SOME_FFQN.to_owned()],
                )
                .await
                .unwrap_or_log();
            assert_eq!(1, pending.len());
            assert_eq!((execution_id.clone(), 1_usize, None), pending[0]);
        }
        // Lock
        sleep(Duration::from_secs(1)).await;
        {
            info!("Lock: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            assert!(db_connection
                .fetch_pending(
                    1,
                    now() + Duration::from_secs(1),
                    None,
                    vec![SOME_FFQN.to_owned()],
                )
                .await
                .unwrap_or_log()
                .is_empty());
            version += 1;
        }
        // lock expired, another executor issues Lock
        sleep(lock_expiry).await;
        {
            info!("Lock after expiry: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version += 1;
        }
        // intermittent timeout
        sleep(Duration::from_millis(499)).await;
        {
            info!("Intermittent timeout: {}", now());
            let event = ExecutionEventInner::IntermittentTimeout {
                expires_at: now() + lock_expiry,
            };

            db_connection
                .insert(execution_id.clone(), version, event)
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(db_connection
                .fetch_pending(
                    1,
                    now() + Duration::from_secs(1),
                    None,
                    vec![SOME_FFQN.to_owned()],
                )
                .await
                .unwrap_or_log()
                .is_empty());
            version += 1;
        }
        // Attempt to lock while in a timeout with exec2
        sleep(Duration::from_millis(300)).await;
        {
            info!("Attempt to lock using exec2: {}", now());
            assert!(db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec2.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .is_err());
            // Version is not changed
        }
        // Lock using exec1
        sleep(Duration::from_millis(700)).await;
        {
            info!("Extend lock using exec1: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + Duration::from_secs(1),
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version += 1;
        }
        sleep(Duration::from_millis(700)).await;
        // Extend the lock using exec1
        {
            info!("Extend lock using exec1: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version += 1;
        }
        // Yield
        sleep(Duration::from_millis(200)).await;
        {
            info!("Yield: {}", now());

            let event = ExecutionEventInner::EventHistory {
                event: EventHistory::Yield,
            };
            db_connection
                .insert(execution_id.clone(), version, event)
                .await
                .unwrap_or_log()
                .unwrap_or_log();

            version += 1;
        }
        // Lock again
        sleep(Duration::from_millis(200)).await;
        {
            info!("Lock again: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec1.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert_eq!(1, event_history.len());
            assert_eq!(vec![EventHistory::Yield], event_history);
            version += 1;
        }
        // Finish
        sleep(Duration::from_millis(300)).await;
        {
            info!("Finish: {}", now());
            let event = ExecutionEventInner::Finished {
                result: FinishedExecutionResult::Ok(concepts::SupportedFunctionResult::None),
            };

            db_connection
                .insert(execution_id.clone(), version, event)
                .await
                .unwrap_or_log()
                .unwrap_or_log();
        }
    }
}
