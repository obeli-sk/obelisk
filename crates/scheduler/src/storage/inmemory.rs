//! Append only database containing executions and their state changes.
//! Current execution state can be obtained by a materialized view that listens
//! to all changes.
//! External listeners can subscribe to execution changes using selectors
//! in a work-stealing fashion, meaning change will be delivered to a single
//! listener. If the change is not processed within an expiry duration,
//! the listener will be invalidated and the change will be delivered to another
//! listener.
//!
//! Each row is keyed by execution id and version that is incremented by 1 on
//! every new change. First change with the expected version wins.
//!
//! There can be many schedulers and executors operating on the same execution.
//!
//! Executors subscribe to changes by a list of supported `ffqn`s.
//! Schedulers subscribe to changes belonging to them, e.g. `Requested`
//!
//! For any given tuple (execution_id, version), first write with the next version wins.

use crate::FinishedExecutionResult;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, Params};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, info_span, instrument, trace, warn, Instrument};
use tracing_unwrap::{OptionExt, ResultExt};

use self::{
    api::{DbRequest, DbTickRequest, ExecutionSpecificRequest, GeneralRequest, Version},
    index::{JournalsIndex, PotentiallyPending},
};

type ExecutorName = Arc<String>;

#[derive(Clone, Debug)]
enum ExecutionEvent<ID: ExecutionId> {
    /// Created by an external system or a scheduler when requesting a child execution or
    /// an executor when continuing as new (`FinishedExecutionError`::`ContinueAsNew`,`CancelledWithNew` .
    // After optional expiry(`scheduled_at`) interpreted as pending.
    Created {
        execution_id: ID,
        ffqn: FunctionFqn,
        params: Params,
        parent: Option<ID>,
        scheduled_at: Option<DateTime<Utc>>,
        created_at: DateTime<Utc>,
    },
    // Created by an executor.
    // Either immediately followed by an execution request by an executor or
    // after expiry immediately followed by WaitingForExecutor by a scheduler.
    Locked {
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    IntermittentFailure {
        expires_at: DateTime<Utc>,
        reason: String,
        created_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler.
    // After expiry interpreted as pending.
    IntermittentTimeout {
        expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    },
    // Created by the executor holding last lock.
    // Processed by a scheduler if a parent execution needs to be notified,
    // also when
    Finished {
        result: FinishedExecutionResult<ID>,
        created_at: DateTime<Utc>,
    },
    // Created by an external system or a scheduler during a race.
    // Processed by the executor holding the last Lock.
    // Imediately followed by Finished by a scheduler.
    CancelRequest {
        created_at: DateTime<Utc>,
    },

    EventHistory {
        event: EventHistory<ID>,
        created_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone)]
enum EventHistory<ID: ExecutionId> {
    // Created by the executor holding last lock.
    // Interpreted as pending.
    Yield {},
    // Created by the executor holding last lock.
    // Does not block the execution
    JoinSet {
        joinset_id: ID,
    },
    // Created by an executor
    // Processed by a scheduler
    // Later followed by DelayFinished
    DelayedUntilAsyncRequest {
        joinset_id: ID,
        delay_id: ID,
        expires_at: DateTime<Utc>,
    },
    // Created by an executor
    // Processed by a scheduler - new execution must be scheduled
    // Immediately followed by ChildExecutionRequested
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

#[derive(Clone, Debug)]
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
        let event = ExecutionEvent::Created {
            execution_id,
            ffqn,
            params,
            scheduled_at,
            parent,
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
        match self.events.iter().rev().next().unwrap_or_log() {
            ExecutionEvent::Created { created_at, .. } => *created_at,
            ExecutionEvent::Locked { created_at, .. } => *created_at,
            ExecutionEvent::IntermittentFailure { created_at, .. } => *created_at,
            ExecutionEvent::IntermittentTimeout { created_at, .. } => *created_at,
            ExecutionEvent::Finished { created_at, .. } => *created_at,
            ExecutionEvent::CancelRequest { created_at } => *created_at,
            ExecutionEvent::EventHistory { created_at, .. } => *created_at,
        }
    }

    fn ffqn(&self) -> &FunctionFqn {
        match self.events.get(0) {
            Some(ExecutionEvent::Created { ffqn, .. }) => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    fn version(&self) -> Version {
        self.events.len()
    }

    fn id(&self) -> &ID {
        let ExecutionEvent::Created { execution_id, .. } = self.events.get(0).unwrap_or_log()
        else {
            error!("First event must be `Created`");
            panic!("first event must be `Created`");
        };
        execution_id
    }
}

mod api {
    use super::{EventHistory, ExecutorName};
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

    #[derive(Debug)]
    pub(crate) enum ExecutionSpecificRequest<ID: ExecutionId> {
        Create {
            execution_id: ID,
            ffqn: FunctionFqn,
            params: Params,
            scheduled_at: Option<DateTime<Utc>>,
            parent: Option<ID>,
            resp_sender: oneshot::Sender<Result<(), ()>>, // PersistResult
        },
        Lock {
            execution_id: ID,
            version: Version,
            executor_name: ExecutorName,
            expires_at: DateTime<Utc>,
            resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
        },
        IntermittentTimeout {
            execution_id: ID,
            version: Version,
            expires_at: DateTime<Utc>,
            resp_sender: oneshot::Sender<Result<(), ()>>,
        },
    }

    #[derive(Debug)]
    pub(crate) enum GeneralRequest<ID: ExecutionId> {
        FetchPending {
            batch_size: usize,
            expiring_before: DateTime<Utc>,
            created_since: Option<DateTime<Utc>>,
            resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
            ffqns: Vec<FunctionFqn>,
        },
    }
}

mod index {
    use super::{EventHistory, ExecutionJournal};
    use crate::storage::inmemory::ExecutionEvent;
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use tracing::error;
    use tracing_unwrap::OptionExt;

    #[derive(Debug)]
    pub(crate) struct JournalsIndex<ID: ExecutionId> {
        pending: HashSet<ID>,
        pending_scheduled: BTreeMap<DateTime<Utc>, HashSet<ID>>,
        pending_scheduled_rev: HashMap<ID, DateTime<Utc>>,
    }

    impl<ID: ExecutionId> JournalsIndex<ID> {
        pub(crate) fn fetch_pending<'a>(
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
            pending.retain(|(journal, _)| is_pending(journal, received_at));
            // filter by ffqn
            pending.retain(|(journal, _)| ffqns.contains(journal.ffqn()));
            // filter out older than `created_since`
            if let Some(created_since) = created_since {
                pending.retain(|(journal, _)| journal.created_at() >= created_since);
            }
            pending.truncate(batch_size);
            pending
        }

        pub(crate) fn update(
            &mut self,
            execution_id: ID,
            journals: &HashMap<ID, ExecutionJournal<ID>>,
        ) {
            // Remove the ID first (if exists)
            self.pending.remove(&execution_id);
            if let Some(schedule) = self.pending_scheduled_rev.remove(&execution_id) {
                let ids = self.pending_scheduled.get_mut(&schedule).unwrap_or_log();
                ids.remove(&execution_id);
            }
            let journal = journals.get(&execution_id).unwrap_or_log();
            match last_event_excluding_async_responses(journal) {
                (
                    0,
                    ExecutionEvent::Created {
                        scheduled_at: None, ..
                    },
                ) => {
                    self.pending.insert(execution_id);
                }
                (
                    0,
                    ExecutionEvent::Created {
                        scheduled_at: Some(scheduled_at),
                        ..
                    },
                ) => {
                    self.pending_scheduled
                        .entry(*scheduled_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.pending_scheduled_rev
                        .insert(execution_id, *scheduled_at);
                }
                (_, ExecutionEvent::Locked { expires_at, .. }) => {
                    self.pending_scheduled
                        .entry(*expires_at)
                        .or_default()
                        .insert(execution_id.clone());
                    self.pending_scheduled_rev.insert(execution_id, *expires_at);
                }
                (_idx, event) => panic!("Not implemented yet: {event:?}"),
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
    pub(crate) enum PotentiallyPending {
        PendingNow,
        PendingAfterExpiry(DateTime<Utc>),
        PendingAfterExternalEvent,
    }

    pub(crate) fn is_pending<ID: ExecutionId>(
        journal: &ExecutionJournal<ID>,
        now: DateTime<Utc>,
    ) -> bool {
        match assert_pending_get_expires_at(journal) {
            PotentiallyPending::PendingNow => true,
            PotentiallyPending::PendingAfterExpiry(pending_after) => pending_after < now,
            PotentiallyPending::PendingAfterExternalEvent => false,
        }
    }

    /// Asserts that the journal is in potentially pending state, returning the expiry if any.
    pub(crate) fn assert_pending_get_expires_at<ID: ExecutionId>(
        journal: &ExecutionJournal<ID>,
    ) -> PotentiallyPending {
        match last_event_excluding_async_responses(journal) {
            (
                _,
                ExecutionEvent::Created {
                    scheduled_at: None, ..
                },
            ) => PotentiallyPending::PendingNow,
            (
                _,
                ExecutionEvent::Created {
                    scheduled_at: Some(scheduled_at),
                    ..
                },
            ) => PotentiallyPending::PendingAfterExpiry(scheduled_at.clone()),
            (_, ExecutionEvent::Locked { expires_at, .. }) => {
                PotentiallyPending::PendingAfterExpiry(*expires_at)
            }
            (_, ExecutionEvent::IntermittentFailure { expires_at, .. }) => {
                PotentiallyPending::PendingAfterExpiry(*expires_at)
            }
            (_, ExecutionEvent::IntermittentTimeout { expires_at, .. }) => {
                PotentiallyPending::PendingAfterExpiry(*expires_at)
            }
            (
                _,
                ExecutionEvent::EventHistory {
                    event: EventHistory::Yield { .. },
                    ..
                },
            ) => PotentiallyPending::PendingNow,
            (
                idx,
                ExecutionEvent::EventHistory {
                    event:
                        EventHistory::JoinNextBlocking {
                            joinset_id: expected_join_set_id,
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
                        matches!(event, ExecutionEvent::EventHistory{event:
                        EventHistory::EventHistoryAsyncResponse { joinset_id, .. }, ..}
                        if joinset_id == expected_join_set_id)
                    })
                    .is_some()
                {
                    PotentiallyPending::PendingNow
                } else {
                    PotentiallyPending::PendingAfterExternalEvent
                }
            }
            other => {
                error!("Expected pending event, got {other:?}");
                panic!("expected pending event, got {other:?}");
            }
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
            .find(|(idx, event)| {
                !matches!(
                    event,
                    ExecutionEvent::EventHistory {
                        event: EventHistory::EventHistoryAsyncResponse { .. },
                        ..
                    }
                )
            })
            .expect_or_log("must contain at least one non-async-response event")
    }
}

#[derive(Debug)]
struct DbTask<ID: ExecutionId> {
    journals: HashMap<ID, ExecutionJournal<ID>>,
    index: JournalsIndex<ID>,
}

impl<ID: ExecutionId> ExecutionSpecificRequest<ID> {
    fn execution_id(&self) -> &ID {
        match self {
            ExecutionSpecificRequest::Create { execution_id, .. } => execution_id,
            ExecutionSpecificRequest::Lock { execution_id, .. } => execution_id,
            ExecutionSpecificRequest::IntermittentTimeout { execution_id, .. } => execution_id,
        }
    }
}

#[derive(Debug)]
enum TickResponse<ID: ExecutionId> {
    FetchPending {
        resp_sender: oneshot::Sender<Vec<(ID, Version, Option<DateTime<Utc>>)>>,
        message: Vec<(ID, Version, Option<DateTime<Utc>>)>,
    },
    Lock {
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
        result: Result<Vec<EventHistory<ID>>, ()>,
    },
    ScheduleNextTickAt(DateTime<Utc>),
    PersistResult {
        resp_sender: oneshot::Sender<Result<(), ()>>,
        result: Result<(), ()>,
    },
}

impl<ID: ExecutionId> DbTask<ID> {
    fn new(rpc_capacity: usize) -> (Self, mpsc::Sender<DbRequest<ID>>) {
        let (client_to_store_req_sender, client_to_store_receiver) =
            mpsc::channel::<DbRequest<ID>>(rpc_capacity);

        let task = Self {
            journals: HashMap::default(),
            index: JournalsIndex::default(),
        };
        (task, client_to_store_req_sender)
    }

    fn tick(&mut self, request: DbTickRequest<ID>) -> TickResponse<ID> {
        let DbTickRequest {
            request,
            received_at,
        } = request;
        trace!("Tick {request:?}");
        match request {
            DbRequest::ExecutionSpecific(request) => self.handle_specific(request, received_at),
            DbRequest::General(request) => self.handle_general(request, received_at),
        }
    }

    fn handle_general(
        &mut self,
        request: GeneralRequest<ID>,
        received_at: DateTime<Utc>,
    ) -> TickResponse<ID> {
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
    ) -> TickResponse<ID> {
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
        TickResponse::FetchPending {
            resp_sender,
            message: pending,
        }
    }

    #[instrument(skip_all, fields(execution_id = %request.execution_id()))]
    fn handle_specific(
        &mut self,
        request: ExecutionSpecificRequest<ID>,
        received_at: DateTime<Utc>,
    ) -> TickResponse<ID> {
        match request {
            ExecutionSpecificRequest::Create {
                ffqn,
                params,
                scheduled_at,
                parent,
                execution_id,
                resp_sender,
            } => self.create(
                received_at,
                execution_id,
                ffqn,
                params,
                scheduled_at,
                parent,
                resp_sender,
            ),
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
            ExecutionSpecificRequest::IntermittentTimeout {
                execution_id,
                version,
                expires_at,
                resp_sender,
            } => self.intermittent_timeout(
                received_at,
                execution_id,
                version,
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
        resp_sender: oneshot::Sender<Result<(), ()>>,
    ) -> TickResponse<ID> {
        if self.journals.contains_key(&execution_id) {
            debug!("Execution is already initialized");
            return TickResponse::PersistResult {
                resp_sender,
                result: Err(()),
            };
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
        // Materialize views
        self.index.update(execution_id, &self.journals);
        TickResponse::PersistResult {
            resp_sender,
            result: Ok(()),
        }
    }

    fn lock(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        executor_name: ExecutorName,
        expires_at: DateTime<Utc>,
        resp_sender: oneshot::Sender<Result<Vec<EventHistory<ID>>, ()>>,
    ) -> TickResponse<ID> {
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            warn!("Not found");
            return TickResponse::Lock {
                resp_sender,
                result: Err(()),
            };
        };
        if version != journal.version() {
            warn!("Wrong version");
            return TickResponse::Lock {
                resp_sender,
                result: Err(()),
            };
        }

        match index::assert_pending_get_expires_at(journal) {
            PotentiallyPending::PendingNow => {}
            PotentiallyPending::PendingAfterExpiry(pending_after)
                if pending_after < received_at => {}
            other => {
                warn!("Not pending yet: {other:?}");
                return TickResponse::Lock {
                    resp_sender,
                    result: Err(()),
                };
            }
        }
        let event = ExecutionEvent::Locked {
            executor_name,
            created_at: received_at,
            expires_at,
        };
        journal.events.push_back(event);

        let event_history = journal
            .events
            .iter()
            .filter_map(|event| {
                if let ExecutionEvent::EventHistory { event: eh, .. } = event {
                    Some(eh.clone())
                } else {
                    None
                }
            })
            .collect();
        // Materialize views
        self.index.update(execution_id, &self.journals);

        TickResponse::Lock {
            resp_sender,
            result: Ok(event_history),
        }
    }

    fn intermittent_timeout(
        &mut self,
        received_at: DateTime<Utc>,
        execution_id: ID,
        version: Version,
        expires_at: DateTime<Utc>,
        resp_sender: oneshot::Sender<Result<(), ()>>,
    ) -> TickResponse<ID> {
        // Check version
        let Some(journal) = self.journals.get_mut(&execution_id) else {
            warn!("Not found");
            return TickResponse::PersistResult {
                resp_sender,
                result: Err(()),
            };
        };
        if version != journal.version() {
            warn!("Wrong version");
            return TickResponse::PersistResult {
                resp_sender,
                result: Err(()),
            };
        }
        let event = ExecutionEvent::IntermittentTimeout {
            expires_at,
            created_at: received_at,
        };
        journal.events.push_back(event);
        TickResponse::PersistResult {
            resp_sender,
            result: Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use chrono::{NaiveDate, TimeZone};
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr};

    static INIT: std::sync::Once = std::sync::Once::new();
    fn set_up() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().without_time())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[test]
    fn lock_batch() {
        set_up();
        let (mut db_task, _request_sender) = DbTask::new(1);
        let execution_id = WorkflowId::generate();
        let mut created_at = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )
            .unwrap();

        info!("Now: {created_at}");
        // 1. Create
        {
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Create {
                ffqn: SOME_FFQN.to_owned(),
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                execution_id: execution_id.clone(),
                resp_sender: oneshot::channel().0,
            });
            let actual = db_task.tick(DbTickRequest {
                request,
                received_at: created_at,
            });
            assert_matches!(actual, TickResponse::PersistResult { result: Ok(()), .. });
        }
        // 2. FetchPending
        {
            created_at += Duration::from_secs(1);
            info!("Now: {created_at}");
            let request = DbRequest::General(GeneralRequest::FetchPending {
                batch_size: 1,
                resp_sender: oneshot::channel().0,
                expiring_before: created_at + Duration::from_secs(1),
                created_since: None,
                ffqns: vec![SOME_FFQN.to_owned()],
            });
            let actual = db_task.tick(DbTickRequest {
                request,
                received_at: created_at,
            });
            let pending = assert_matches!(
                actual,
                TickResponse::FetchPending {
                    message,
                    ..
                } => message
            );
            assert_eq!(1, pending.len());
            assert_eq!((execution_id.clone(), 1_usize, None), pending[0]);
        }
        // 3. Lock
        let lock_expiry = Duration::from_millis(500);
        {
            created_at += Duration::from_secs(1);
            info!("Now: {created_at}");
            let exec = Arc::new("exec1".to_string());
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
                execution_id: execution_id.clone(),
                version: 1,
                executor_name: exec.clone(),
                expires_at: created_at + lock_expiry,
                resp_sender: oneshot::channel().0,
            });
            let actual = db_task.tick(DbTickRequest {
                request,
                received_at: created_at,
            });
            assert_matches!(actual, TickResponse::Lock { result: Ok(event_history), .. } if event_history.is_empty());
        }
        // 4. lock expired, another executor issues Lock
        {
            created_at += lock_expiry + Duration::from_millis(1);
            info!("Now: {created_at}");
            let exec = Arc::new("exec2".to_string());
            let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Lock {
                execution_id: execution_id.clone(),
                version: 2,
                executor_name: exec.clone(),
                expires_at: created_at + lock_expiry,
                resp_sender: oneshot::channel().0,
            });
            let actual = db_task.tick(DbTickRequest {
                request,
                received_at: created_at,
            });
            assert_matches!(actual, TickResponse::Lock { result: Ok(event_history), .. } if event_history.is_empty());
        }
        // 5. intermittent timeout
        {
            created_at += Duration::from_millis(499);
            info!("Now: {created_at}");
            let request =
                DbRequest::ExecutionSpecific(ExecutionSpecificRequest::IntermittentTimeout {
                    execution_id,
                    version: 3,
                    expires_at: created_at + lock_expiry,
                    resp_sender: oneshot::channel().0,
                });
            let actual = db_task.tick(DbTickRequest {
                request,
                received_at: created_at,
            });
            assert_matches!(actual, TickResponse::PersistResult { result: Ok(()), .. });
        }
    }
}
