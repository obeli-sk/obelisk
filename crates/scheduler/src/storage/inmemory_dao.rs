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
    worker::{DbConnection, DbError},
    FinishedExecutionResult,
};
use assert_matches::assert_matches;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, instrument, trace, warn, Level};
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
        execution_id: ID,
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
                execution_id,
                ffqn,
                params,
                scheduled_at,
                parent,
            },
            created_at,
        };
        Self {
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
        let ExecutionEventInner::Created { execution_id, .. } =
            &self.events.get(0).unwrap_or_log().event
        else {
            error!("First event must be `Created`");
            panic!("first event must be `Created`");
        };
        execution_id
    }

    fn validate_push(
        &mut self,
        event: ExecutionEventInner<ID>,
        created_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        if let ExecutionEventInner::Locked { .. } = event {
            match index::potentially_pending(self) {
                PotentiallyPending::PendingNow => {}
                PotentiallyPending::PendingAfterExpiry(pending_start)
                    if pending_start <= created_at => {}
                other => {
                    warn!("Cannot be locked: {other:?}");
                    return Err(());
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
    use super::{EventHistory, ExecutionEventInner, ExecutorName};
    use chrono::{DateTime, Utc};
    use concepts::{ExecutionId, FunctionFqn, Params};
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

    #[derive(Debug, derive_more::Display)]
    pub(crate) enum ExecutionSpecificRequest<ID: ExecutionId> {
        #[display(fmt = "Create")]
        Create {
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<ID>,
            resp_sender: oneshot::Sender<Result<(), ()>>,
        },
        #[display(fmt = "Lock")]
        Lock {
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
            resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
        },
        #[display(fmt = "Insert({event})")]
        Insert {
            execution_id: ID,
            version: Version,
            event: ExecutionEventInner<ID>,
            resp_sender: oneshot::Sender<Result<(), ()>>,
        },
    }

    #[derive(Debug)]
    pub(crate) enum GeneralRequest<ID: ExecutionId> {
        FetchPending {
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            created_since: Option<DateTime<Utc>>,
            ffqns: Vec<FunctionFqn>,
            resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
        },
    }
}

mod index {
    use super::{EventHistory, ExecutionEventInner, ExecutionJournal};
    use crate::storage::inmemory_dao::ExecutionEvent;
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
                PotentiallyPending::PendingAfterExpiry(pending_after) => {
                    pending_after < received_at
                }
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
                PotentiallyPending::PendingAfterExpiry(expires_at) => {
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
                    event: ExecutionEventInner::Locked { expires_at, .. },
                    ..
                },
            ) => PotentiallyPending::PendingAfterExpiry(*expires_at),
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

#[derive(Debug)]
pub(crate) enum DbTickResponse<ID: ExecutionId> {
    FetchPending {
        resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
        message: Vec<(ID, Version, Option<DateTime<Utc>>)>,
    },
    Lock {
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
        result: Result<Vec<EventHistory<ID>>, ()>,
    },
    PersistResult {
        resp_sender: oneshot::Sender<Result<(), ()>>,
        result: Result<(), ()>,
    },
}

impl<ID: ExecutionId> DbTickResponse<ID> {
    pub(crate) fn send(self) -> Result<(), ()> {
        match self {
            DbTickResponse::FetchPending {
                resp_sender,
                message,
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
    pub(crate) fn new(rpc_capacity: usize) -> (Self, mpsc::Sender<DbRequest<ID>>) {
        let (client_to_store_req_sender, client_to_store_receiver) =
            mpsc::channel::<DbRequest<ID>>(rpc_capacity);

        let task = Self {
            journals: HashMap::default(),
            index: JournalsIndex::default(),
        };
        (task, client_to_store_req_sender)
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
            message: pending,
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
        match request {
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
        }
    }

    fn create(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        scheduled_at: Option<DateTime<Utc>>,
        parent: Option<ID>,
    ) -> Result<(), ()> {
        if self.journals.contains_key(&execution_id) {
            debug!("Execution is already initialized");
            return Err(());
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
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
    ) -> DbTickResponse<ID> {
        let event = ExecutionEventInner::Locked {
            executor_name,
            expires_at,
        };

        if self
            .insert(received_at, execution_id.clone(), version, event)
            .is_ok()
        {
            let event_history = self
                .journals
                .get(&execution_id)
                .unwrap_or_log()
                .event_history();
            DbTickResponse::Lock {
                resp_sender,
                result: Ok(event_history),
            }
        } else {
            DbTickResponse::Lock {
                resp_sender,
                result: Err(()),
            }
        }
    }

    fn insert(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        event: ExecutionEventInner<ID>,
    ) -> Result<(), ()> {
        if let ExecutionEventInner::Created {
            execution_id,
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
                return Err(());
            };
        }
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            warn!("Not found");
            return Err(());
        };
        if version != journal.version() {
            warn!("Wrong version");
            return Err(());
        }

        if journal.validate_push(event, received_at).is_err() {
            warn!("Validation failed");
            return Err(());
        }
        self.index.update(execution_id, &self.journals);
        Ok(())
    }
}

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
    ) -> Result<Result<Vec<EventHistory<ID>>, ()>, DbError> {
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
    ) -> Result<Result<(), ()>, DbError> {
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
mod tests {
    use super::*;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr};
    use std::time::Duration;
    use tracing::info;

    struct TickBasedDbConnection<ID: ExecutionId> {
        db_task: Arc<std::sync::Mutex<DbTask<ID>>>,
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
            Ok(assert_matches!(response, DbTickResponse::FetchPending {  message, .. } => message))
        }

        async fn lock(
            &self,
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
        ) -> Result<Result<Vec<EventHistory<ID>>, ()>, DbError> {
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
        ) -> Result<Result<(), ()>, DbError> {
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
    async fn lock_batch() {
        set_up();
        let (db_task, _request_sender) = DbTask::new(1);
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
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
        let exec = Arc::new("exec1".to_string());
        let lock_expiry = Duration::from_millis(500);
        let mut version = 0;
        // Create
        {
            let event = ExecutionEventInner::Created {
                execution_id: execution_id.clone(),
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
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
            info!("Now: {}", now());
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
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
            info!("Now: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec.clone(),
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
        {
            tokio::time::sleep(lock_expiry).await;
            info!("Now: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version += 1;
        }
        // intermittent timeout
        {
            tokio::time::sleep(Duration::from_millis(499)).await;
            info!("Now: {}", now());
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
        // Attempt to lock while in a timeout
        {
            tokio::time::sleep(Duration::from_millis(300)).await;
            info!("Now: {}", now());
            assert!(db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .is_err());
            // Version is not changed
        }
        // Lock again
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            info!("Now: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec.clone(),
                    now() + lock_expiry,
                )
                .await
                .unwrap_or_log()
                .unwrap_or_log();
            assert!(event_history.is_empty());
            version += 1;
        }
        // Yield
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            info!("Now: {}", now());

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
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            info!("Now: {}", now());
            let event_history = db_connection
                .lock(
                    execution_id.clone(),
                    version,
                    exec.clone(),
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
        {
            tokio::time::sleep(Duration::from_millis(300)).await;
            info!("Now: {}", now());
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
