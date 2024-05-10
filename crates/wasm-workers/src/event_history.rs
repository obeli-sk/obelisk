use crate::workflow_ctx::FunctionError;
use crate::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, DbPool, ExecutionEventInner,
    JoinSetResponse, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, trace};
use utils::time::ClockFn;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

const DB_POLL_SLEEP: Duration = Duration::from_millis(50);

#[derive(PartialEq, Eq, Clone, Copy)]
enum ProcessingStatus {
    UnprocessedRequest,
    UnprocessedResponse,
    Processed,
}

#[cfg_attr(test, derive(Clone))]
pub(crate) struct EventHistory {
    execution_id: ExecutionId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    execution_deadline: DateTime<Utc>,
    child_retry_exp_backoff: Duration,
    child_max_retries: u32,
    events: Vec<(HistoryEvent, ProcessingStatus)>,
    start_from_idx: usize,
}

impl EventHistory {
    pub(crate) fn new(
        execution_id: ExecutionId,
        events: impl IntoIterator<
            Item = HistoryEvent,
            IntoIter = impl ExactSizeIterator<Item = HistoryEvent>,
        >,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        execution_deadline: DateTime<Utc>,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
    ) -> Self {
        let events = events.into_iter();
        EventHistory {
            execution_id,
            events: {
                let mut vec = Vec::with_capacity(events.len());
                Self::append_events(&mut vec, events);
                vec
            },
            join_next_blocking_strategy,
            execution_deadline,
            child_retry_exp_backoff,
            child_max_retries,
            start_from_idx: 0,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn replay_or_interrupt<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_pool: &impl DbPool<DB>,
        version: &mut Version,
        clock_fn: &impl ClockFn,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        #[derive(derive_more::Display)]
        enum PollVariant {
            JoinNextChild(JoinSetId),
            JoinNextDelay(JoinSetId),
        }
        impl PollVariant {
            fn join_set_id(&self) -> JoinSetId {
                match self {
                    PollVariant::JoinNextChild(join_set_id)
                    | PollVariant::JoinNextDelay(join_set_id) => *join_set_id,
                }
            }
            fn as_key(&self) -> EventHistoryKey {
                match self {
                    PollVariant::JoinNextChild(join_set_id) => EventHistoryKey::JoinNextChild {
                        join_set_id: *join_set_id,
                    },
                    PollVariant::JoinNextDelay(join_set_id) => EventHistoryKey::JoinNextDelay {
                        join_set_id: *join_set_id,
                    },
                }
            }
        }
        trace!("replay_or_interrupt: {event_call:?}");
        if let Some(accept_resp) = self.find_matching_atomic(&event_call)? {
            return Ok(accept_resp);
        }
        // not found in the history, persisting the request
        let created_at = clock_fn();
        let poll_variant = match &event_call {
            // Blocking calls can be polled for JoinSetResponse
            EventCall::BlockingChildExecutionRequest { join_set_id, .. }
            | EventCall::BlockingChildJoinNext { join_set_id } => {
                Some(PollVariant::JoinNextChild(*join_set_id))
            }
            EventCall::BlockingDelayRequest { join_set_id, .. } => {
                Some(PollVariant::JoinNextDelay(*join_set_id))
            }
            EventCall::CreateJoinSet { .. } | EventCall::StartAsync { .. } => None, // continue the execution
        };

        let db_connection = db_pool.connection();
        let lock_expires_at =
            if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                created_at
            } else {
                self.execution_deadline
            };
        let poll_variant = match poll_variant {
            None => {
                // TODO: Add speculative batching (avoid writing non-blocking responses immediately) to improve performance
                let cloned_non_blocking = event_call.clone();
                let history_events = event_call
                    .append_to_db(
                        &db_connection,
                        created_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                Self::append_events(&mut self.events, history_events);
                return Ok(self
                    .find_matching_atomic(&cloned_non_blocking)?
                    .expect("non-blocking EventCall must return some response"));
            }
            Some(poll_variant) => {
                let keys = event_call.as_keys();
                let history_events = event_call
                    .append_to_db(
                        &db_connection,
                        created_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                Self::append_events(&mut self.events, history_events);
                let keys_len = keys.len();
                for (idx, key) in keys.into_iter().enumerate() {
                    let res = self.process_event_by_key(key)?;
                    if idx == keys_len - 1 {
                        if let Some(res) = res {
                            // Last key was replayed.
                            return Ok(res);
                        }
                    }
                }
                // TODO: if start_index was at top, it should move forward to n - 1
                poll_variant
            }
        };

        if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Await {
            debug!(join_set_id = %poll_variant.join_set_id(),  "Waiting for {poll_variant}");
            while clock_fn() < self.execution_deadline {
                if self.fetch_update(&db_connection, version).await?.is_some() {
                    if let Some(accept_resp) = self.process_event_by_key(poll_variant.as_key())? {
                        debug!(join_set_id = %poll_variant.join_set_id(), "Got result");
                        return Ok(accept_resp);
                    }
                }
                tokio::time::sleep(DB_POLL_SLEEP).await;
            }
        }
        match poll_variant {
            PollVariant::JoinNextChild(_) => Err(FunctionError::ChildExecutionRequest),
            PollVariant::JoinNextDelay(_) => Err(FunctionError::DelayRequest),
        }
    }

    fn append_events(
        events: &mut Vec<(HistoryEvent, ProcessingStatus)>,
        history_events: impl IntoIterator<Item = HistoryEvent>,
    ) {
        events.extend(history_events.into_iter().map(|event| {
            let resp = event.is_response();
            (
                event,
                if resp {
                    ProcessingStatus::UnprocessedResponse
                } else {
                    ProcessingStatus::UnprocessedRequest
                },
            )
        }));
    }

    fn find_matching_atomic(
        &mut self,
        event_call: &EventCall,
    ) -> Result<Option<SupportedFunctionResult>, FunctionError> {
        let keys = event_call.as_keys();
        assert!(!keys.is_empty());
        let mut last = None;
        for (idx, key) in keys.into_iter().enumerate() {
            last = self.process_event_by_key(key)?;
            if last.is_none() {
                assert_eq!(
                    idx, 0,
                    "EventCall must be processed in an all or nothing fashion"
                );
                return Ok(None);
            }
        }
        Ok(Some(last.unwrap()))
    }

    fn next_unprocessed_request(&self) -> Option<(usize, &(HistoryEvent, ProcessingStatus))> {
        self.events
            .iter()
            .enumerate()
            .skip(self.start_from_idx)
            .find(|(_, (_, status))| *status == ProcessingStatus::UnprocessedRequest)
    }

    fn mark_next_unprocessed_response(&mut self, join_set_id: JoinSetId) -> Option<&HistoryEvent> {
        if let Some(idx) = self
            .events
            .iter()
            .enumerate()
            .skip(self.start_from_idx)
            .find_map(|(idx, (event, status))| match (status, event) {
                (
                    ProcessingStatus::UnprocessedResponse,
                    HistoryEvent::JoinSetResponse {
                        join_set_id: found, ..
                    },
                ) if *found == join_set_id => Some(idx),
                _ => None,
            })
        {
            self.events[idx].1 = ProcessingStatus::Processed;
            Some(&self.events[idx].0)
        } else {
            None
        }
    }

    #[allow(clippy::too_many_lines)]
    fn process_event_by_key(
        &mut self,
        key: EventHistoryKey,
    ) -> Result<Option<SupportedFunctionResult>, FunctionError> {
        let Some((found_idx, (found_request_event, _))) = self.next_unprocessed_request() else {
            return Ok(None);
        };
        trace!("Finding match for {key:?}, {found_request_event:?}",);
        match (key, found_request_event) {
            (
                EventHistoryKey::CreateJoinSet { join_set_id },
                HistoryEvent::JoinSet {
                    join_set_id: found_join_set_id,
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "Matched JoinSet");
                self.events[found_idx].1 = ProcessingStatus::Processed;
                // if this is a [`EventCall::CreateJoinSet`] , return join set id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(join_set_id.to_string()),
                })))
            }
            (
                EventHistoryKey::ChildExecutionRequest {
                    join_set_id,
                    child_execution_id: execution_id,
                    ..
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                },
            ) if join_set_id == *found_join_set_id && execution_id == *child_execution_id => {
                trace!(%child_execution_id,
                %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.events[found_idx].1 = ProcessingStatus::Processed;
                // if this is a [`EventCall::StartAsync`] , return execution id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(execution_id.to_string()),
                })))
            }
            (
                EventHistoryKey::DelayRequest {
                    join_set_id,
                    delay_id,
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id: found_delay_id,
                            expires_at: _,
                        },
                },
            ) if join_set_id == *found_join_set_id && delay_id == *found_delay_id => {
                trace!(%delay_id, %join_set_id, "Matched JoinSetRequest::DelayRequest");
                self.events[found_idx].1 = ProcessingStatus::Processed;
                // return delay id
                Ok(Some(SupportedFunctionResult::Infallible(WastValWithType {
                    r#type: TypeWrapper::String,
                    value: WastVal::String(delay_id.to_string()),
                })))
            }
            (
                EventHistoryKey::JoinNextChild { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(
                %join_set_id, "Peeked at JoinNext - Child");
                self.events[found_idx].1 = ProcessingStatus::Processed;
                // TODO: Add support for conditions on JoinNext, skip unrelated responses
                match self.mark_next_unprocessed_response(join_set_id) {
                    Some(HistoryEvent::JoinSetResponse {
                        response:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result,
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & ChildExecutionFinished");
                        match result {
                            Ok(result) => Ok(Some(result.clone())),
                            Err(err) => {
                                error!(%child_execution_id,
                                    %join_set_id,
                                    "Child execution finished with an execution error, failing the parent");
                                Err(FunctionError::ChildExecutionError(err.clone()))
                            }
                        }
                    }
                    None => {
                        // no progress, still at JoinNext,
                        self.events[found_idx].1 = ProcessingStatus::UnprocessedRequest;
                        Ok(None)
                    }
                    _ => unreachable!(),
                }
            }
            (
                EventHistoryKey::JoinNextDelay { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(
                    %join_set_id, "Peeked at JoinNext - Delay");
                match self.mark_next_unprocessed_response(join_set_id) {
                    Some(HistoryEvent::JoinSetResponse {
                        response:
                            JoinSetResponse::DelayFinished {
                                delay_id: _, // Currently only a single blocking delay is supported, no need to match the id
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & DelayFinished");
                        self.events[found_idx].1 = ProcessingStatus::Processed;
                        Ok(Some(SupportedFunctionResult::None))
                    }
                    _ => Ok(None), // no progress, still at JoinNext
                }
            }
            (key, found) => Err(FunctionError::NonDeterminismDetected(StrVariant::Arc(
                Arc::from(format!(
                    "unexpected key {key:?} not matching {found:?} at index {found_idx}",
                )),
            ))),
        }
    }

    async fn fetch_update<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        version: &mut Version,
    ) -> Result<Option<()>, DbError> {
        let exec_log = db_connection.get(self.execution_id).await?;
        let event_history = exec_log.event_history().collect::<Vec<_>>();
        let events_len = self.events.len();
        if event_history.len() > events_len {
            Self::append_events(&mut self.events, event_history.into_iter().skip(events_len));
            *version = exec_log.version;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum EventCall {
    CreateJoinSet {
        join_set_id: JoinSetId,
    },
    StartAsync {
        ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
        params: Params,
    },
    BlockingChildJoinNext {
        join_set_id: JoinSetId,
    },
    /// combines [`Self::CreateJoinSet`] [`Self::StartAsync`] [`Self::BlockingChildJoinNext`]
    BlockingChildExecutionRequest {
        ffqn: FunctionFqn,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
        params: Params,
    },
    BlockingDelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
        expires_at_if_new: DateTime<Utc>,
    },
}

#[derive(Debug)]
enum EventHistoryKey {
    CreateJoinSet {
        join_set_id: JoinSetId,
    },
    ChildExecutionRequest {
        join_set_id: JoinSetId,
        child_execution_id: ExecutionId,
    },
    DelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
    },
    JoinNextChild {
        join_set_id: JoinSetId,
        // TODO: Add wakeup conditions: ffqn (used by function_await), execution id
    },
    JoinNextDelay {
        join_set_id: JoinSetId,
    },
    // TODO: JoinNextAny
}

impl EventCall {
    fn as_keys(&self) -> Vec<EventHistoryKey> {
        match self {
            EventCall::CreateJoinSet { join_set_id } => {
                vec![EventHistoryKey::CreateJoinSet {
                    join_set_id: *join_set_id,
                }]
            }
            EventCall::StartAsync {
                join_set_id,
                child_execution_id,
                ..
            } => vec![EventHistoryKey::ChildExecutionRequest {
                join_set_id: *join_set_id,
                child_execution_id: *child_execution_id,
            }],
            EventCall::BlockingChildJoinNext { join_set_id } => {
                vec![EventHistoryKey::JoinNextChild {
                    join_set_id: *join_set_id,
                }]
            }
            EventCall::BlockingChildExecutionRequest {
                join_set_id,
                child_execution_id,
                ..
            } => vec![
                EventHistoryKey::CreateJoinSet {
                    join_set_id: *join_set_id,
                },
                EventHistoryKey::ChildExecutionRequest {
                    join_set_id: *join_set_id,
                    child_execution_id: *child_execution_id,
                },
                EventHistoryKey::JoinNextChild {
                    join_set_id: *join_set_id,
                },
            ],
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                ..
            } => vec![
                EventHistoryKey::CreateJoinSet {
                    join_set_id: *join_set_id,
                },
                EventHistoryKey::DelayRequest {
                    join_set_id: *join_set_id,
                    delay_id: *delay_id,
                },
                EventHistoryKey::JoinNextDelay {
                    join_set_id: *join_set_id,
                },
            ],
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn append_to_db(
        self,
        db_connection: &impl DbConnection,
        created_at: DateTime<Utc>,
        lock_expires_at: DateTime<Utc>,
        execution_id: ExecutionId,
        version: &mut Version,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
    ) -> Result<Vec<HistoryEvent>, DbError> {
        match self {
            EventCall::CreateJoinSet { join_set_id } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "CreateJoinSet: Creating new JoinSet");
                *version = db_connection
                    .append(execution_id, Some(version.clone()), join_set)
                    .await?;
                Ok(history_events)
            }
            EventCall::StartAsync {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                history_events.push(event.clone());
                let child_exec_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%child_execution_id, %join_set_id, "StartAsync: appending ChildExecutionRequest");
                let child = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: None,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };

                *version = db_connection
                    .append_batch_create_child(
                        vec![child_exec_req],
                        execution_id,
                        version.clone(),
                        child,
                    )
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingChildJoinNext { join_set_id } => {
                let mut history_events = Vec::with_capacity(1);
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "BlockingChildJoinNext: appending JoinNext");
                *version = db_connection
                    .append(execution_id, Some(version.clone()), join_next)
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingChildExecutionRequest {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                history_events.push(event.clone());
                let child_exec_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%child_execution_id, %join_set_id, "BlockingChildExecutionRequest: Appending JoinSet,ChildExecutionRequest,JoinNext");
                let child = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn: ffqn.clone(),
                    params: params.clone(),
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: None,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };
                *version = db_connection
                    .append_batch_create_child(
                        vec![join_set, child_exec_req, join_next],
                        execution_id,
                        version.clone(),
                        child,
                    )
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                expires_at_if_new,
            } => {
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at: expires_at_if_new,
                    },
                };
                history_events.push(event.clone());
                let delay_req = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%delay_id, %join_set_id, "BlockingDelayRequest: appending JoinSet,DelayRequest,JoinNext");
                *version = db_connection
                    .append_batch(
                        vec![join_set, delay_req, join_next],
                        execution_id,
                        version.clone(),
                    )
                    .await?;
                Ok(history_events)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::JoinSetId;
    use concepts::storage::{
        AppendRequest, DbConnection, DbError, ExecutionEventInner, HistoryEvent, JoinSetResponse,
        Version,
    };
    use concepts::storage::{CreateRequest, DbPool};
    use concepts::{ExecutionId, Params, SupportedFunctionResult};
    use db_mem::inmemory_dao::DbTask;
    use test_utils::sim_clock::SimClock;
    use tracing::info;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    use crate::event_history::{EventCall, EventHistory};
    use crate::workflow_ctx::tests::MOCK_FFQN;
    use crate::workflow_ctx::FunctionError;
    use crate::workflow_worker::JoinNextBlockingStrategy;

    async fn load_event_history(
        db_connection: &impl DbConnection,
        execution_id: ExecutionId,
        execution_deadline: DateTime<Utc>,
    ) -> Result<(EventHistory, Version), DbError> {
        let exec_log = db_connection.get(execution_id).await?;
        let event_history = EventHistory::new(
            execution_id,
            exec_log.event_history().collect::<Vec<_>>(),
            JoinNextBlockingStrategy::default(),
            execution_deadline,
            Duration::ZERO,
            0,
        );
        Ok((event_history, exec_log.version))
    }

    #[tokio::test]
    async fn regular_join_next_child() {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let mut db_task = DbTask::spawn_new(10);
        let db_pool = db_task.pool().expect("must be open");

        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (event_history, version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        let blocking_join_first = |mut event_history: EventHistory, mut version: Version| {
            let sim_clock = sim_clock.clone();
            let db_pool = db_pool.clone();
            async move {
                event_history
                    .replay_or_interrupt(
                        EventCall::CreateJoinSet { join_set_id },
                        &db_pool,
                        &mut version,
                        &sim_clock.get_clock_fn(),
                    )
                    .await
                    .unwrap();

                event_history
                    .replay_or_interrupt(
                        EventCall::StartAsync {
                            ffqn: MOCK_FFQN,
                            join_set_id,
                            child_execution_id,
                            params: Params::default(),
                        },
                        &db_pool,
                        &mut version,
                        &sim_clock.get_clock_fn(),
                    )
                    .await
                    .unwrap();
                event_history
                    .replay_or_interrupt(
                        EventCall::BlockingChildJoinNext { join_set_id },
                        &db_pool,
                        &mut version,
                        &sim_clock.get_clock_fn(),
                    )
                    .await
            }
        };

        assert_matches!(
            blocking_join_first(event_history, version)
                .await
                .unwrap_err(),
            FunctionError::ChildExecutionRequest
        );
        db_connection
            .append(
                execution_id,
                None,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinSetResponse {
                            join_set_id,
                            response: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result: Ok(SupportedFunctionResult::None),
                            },
                        },
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (event_history, version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        blocking_join_first(event_history, version).await.unwrap();

        drop(db_connection);
        drop(db_pool);
        db_task.close().await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn start_async_respond_then_join_next() {
        const CHILD_RESP: SupportedFunctionResult =
            SupportedFunctionResult::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });

        async fn start_async<DB: DbConnection>(
            event_history: &mut EventHistory,
            version: &mut Version,
            sim_clock: &SimClock,
            db_pool: &impl DbPool<DB>,
            join_set_id: JoinSetId,
            child_execution_id: ExecutionId,
        ) {
            event_history
                .replay_or_interrupt(
                    EventCall::CreateJoinSet { join_set_id },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
                .unwrap();
            event_history
                .replay_or_interrupt(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id,
                        params: Params::default(),
                    },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
                .unwrap();
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let mut db_task = DbTask::spawn_new(10);
        let db_pool = db_task.pool().expect("must be open");

        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        start_async(
            &mut event_history,
            &mut version,
            &sim_clock,
            &db_pool,
            join_set_id,
            child_execution_id,
        )
        .await;

        // append child response before issuing join_next
        db_connection
            .append(
                execution_id,
                None,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinSetResponse {
                            join_set_id,
                            response: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result: Ok(CHILD_RESP),
                            },
                        },
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        start_async(
            &mut event_history,
            &mut version,
            &sim_clock,
            &db_pool,
            join_set_id,
            child_execution_id,
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .replay_or_interrupt(
                EventCall::BlockingChildJoinNext { join_set_id },
                &db_pool,
                &mut version,
                &sim_clock.get_clock_fn(),
            )
            .await
            .unwrap();
        assert_eq!(CHILD_RESP, res);

        drop(db_connection);
        drop(db_pool);
        db_task.close().await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn create_two_non_blocking_childs_then_two_join_nexts() {
        const KID_A: SupportedFunctionResult =
            SupportedFunctionResult::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });
        const KID_B: SupportedFunctionResult =
            SupportedFunctionResult::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(2),
            });

        async fn blocking_join_first<DB: DbConnection>(
            event_history: &mut EventHistory,
            version: &mut Version,
            sim_clock: &SimClock,
            db_pool: &impl DbPool<DB>,
            join_set_id: JoinSetId,
            child_execution_id_a: ExecutionId,
            child_execution_id_b: ExecutionId,
        ) -> Result<SupportedFunctionResult, FunctionError> {
            event_history
                .replay_or_interrupt(
                    EventCall::CreateJoinSet { join_set_id },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
                .unwrap();
            event_history
                .replay_or_interrupt(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id: child_execution_id_a,
                        params: Params::default(),
                    },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
                .unwrap();
            event_history
                .replay_or_interrupt(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id: child_execution_id_b,
                        params: Params::default(),
                    },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
                .unwrap();
            event_history
                .replay_or_interrupt(
                    EventCall::BlockingChildJoinNext { join_set_id },
                    db_pool,
                    version,
                    &sim_clock.get_clock_fn(),
                )
                .await
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let mut db_task = DbTask::spawn_new(10);
        let db_pool = db_task.pool().expect("must be open");

        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::default(),
                parent: None,
                scheduled_at: None,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id_a = ExecutionId::generate();
        let child_execution_id_b = ExecutionId::generate();

        assert_matches!(
            blocking_join_first(
                &mut event_history,
                &mut version,
                &sim_clock,
                &db_pool,
                join_set_id,
                child_execution_id_a,
                child_execution_id_b
            )
            .await
            .unwrap_err(),
            FunctionError::ChildExecutionRequest
        );
        // append two responses
        db_connection
            .append(
                execution_id,
                None,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinSetResponse {
                            join_set_id,
                            response: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: child_execution_id_a,
                                result: Ok(KID_A),
                            },
                        },
                    },
                },
            )
            .await
            .unwrap();
        db_connection
            .append(
                execution_id,
                None,
                AppendRequest {
                    created_at: sim_clock.now(),
                    event: ExecutionEventInner::HistoryEvent {
                        event: HistoryEvent::JoinSetResponse {
                            join_set_id,
                            response: JoinSetResponse::ChildExecutionFinished {
                                child_execution_id: child_execution_id_b,
                                result: Ok(KID_B),
                            },
                        },
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) =
            load_event_history(&db_connection, execution_id, sim_clock.now())
                .await
                .unwrap();

        let res = blocking_join_first(
            &mut event_history,
            &mut version,
            &sim_clock,
            &db_pool,
            join_set_id,
            child_execution_id_a,
            child_execution_id_b,
        )
        .await
        .unwrap();
        assert_eq!(KID_A, res);

        // second child result should be found
        let res = event_history
            .replay_or_interrupt(
                EventCall::BlockingChildJoinNext { join_set_id },
                &db_pool,
                &mut version,
                &sim_clock.get_clock_fn(),
            )
            .await
            .unwrap();
        assert_eq!(KID_B, res);

        drop(db_connection);
        drop(db_pool);
        db_task.close().await;
    }
}
