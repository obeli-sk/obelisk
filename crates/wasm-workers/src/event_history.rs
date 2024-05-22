use crate::event_history::ProcessingStatus::Processed;
use crate::event_history::ProcessingStatus::Unprocessed;
use crate::workflow_ctx::FunctionError;
use crate::workflow_worker::JoinNextBlockingStrategy;
use crate::workflow_worker::NonBlockingEventBatching;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse,
    JoinSetResponseEvent, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionResult};
use executor::worker::WorkerResult;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, trace};
use utils::time::ClockFn;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

const MAX_NON_BLOCKING_CACHE_SIZE: usize = 50;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum ProcessingStatus {
    Unprocessed,
    Processed,
}

#[allow(clippy::struct_field_names)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct EventHistory<C: ClockFn> {
    execution_id: ExecutionId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    execution_deadline: DateTime<Utc>,
    child_retry_exp_backoff: Duration,
    child_max_retries: u32,
    event_history: Vec<(HistoryEvent, ProcessingStatus)>,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    non_blocking_event_batch: Option<Vec<NonBlockingCache>>,
    clock_fn: C,
    timeout_error: Arc<std::sync::Mutex<WorkerResult>>,
    // TODO: optimize using start_from_idx: usize,
}

#[cfg_attr(test, derive(Clone))]
enum NonBlockingCache {
    StartAsync {
        batch: Vec<ExecutionEventInner>,
        version: Version,
        child_req: CreateRequest,
    },
}

impl<C: ClockFn> EventHistory<C> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        execution_deadline: DateTime<Utc>,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
        non_blocking_event_batching: NonBlockingEventBatching,
        clock_fn: C,
        timeout_error: Arc<std::sync::Mutex<WorkerResult>>,
    ) -> Self {
        EventHistory {
            execution_id,
            event_history: event_history
                .into_iter()
                .map(|event| (event, Unprocessed))
                .collect(),
            responses: responses
                .into_iter()
                .map(|event| (event, Unprocessed))
                .collect(),
            join_next_blocking_strategy,
            execution_deadline,
            child_retry_exp_backoff,
            child_max_retries,
            non_blocking_event_batch: match non_blocking_event_batching {
                NonBlockingEventBatching::Disabled => None,
                NonBlockingEventBatching::Enabled => Some(Vec::new()),
            },
            clock_fn,
            timeout_error,
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn replay_or_interrupt<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_connection: &DB,
        version: &mut Version,
    ) -> Result<SupportedFunctionResult, FunctionError> {
        trace!("replay_or_interrupt: {event_call:?}");
        if let Some(accept_resp) = self.find_matching_atomic(&event_call)? {
            return Ok(accept_resp);
        }
        // not found in the history, persisting the request
        let called_at = (self.clock_fn)();
        let lock_expires_at =
            if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                called_at
            } else {
                self.execution_deadline
            };
        let poll_variant = match event_call.poll_variant() {
            None => {
                // events that cannot block
                // TODO: Add speculative batching (avoid writing non-blocking responses immediately) to improve performance
                let cloned_non_blocking = event_call.clone();
                let history_events = self
                    .append_to_db(
                        event_call,
                        db_connection,
                        called_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                self.event_history
                    .extend(history_events.into_iter().map(|event| (event, Unprocessed)));
                return Ok(self
                    .find_matching_atomic(&cloned_non_blocking)?
                    .expect("non-blocking EventCall must return the response"));
            }
            Some(poll_variant) => {
                let keys = event_call.as_keys();
                let history_events = self
                    .append_to_db(
                        event_call,
                        db_connection,
                        called_at,
                        lock_expires_at,
                        self.execution_id,
                        version,
                        self.child_retry_exp_backoff,
                        self.child_max_retries,
                    )
                    .await?;
                self.event_history
                    .extend(history_events.into_iter().map(|event| (event, Unprocessed)));
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
                // FIXME: perf: if start_from_index was at top, it should move forward to n - 1
                poll_variant
            }
        };
        if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Await {
            // JoinNext was written, wait for next response.
            let join_set_id = poll_variant.join_set_id();
            debug!(%join_set_id,  "Waiting for {poll_variant:?}");
            *self.timeout_error.lock().unwrap() = poll_variant.as_worker_result();
            let key = poll_variant.as_key();

            // Subscribe to the next response.
            loop {
                let next_responses = db_connection
                    .subscribe_to_next_responses(self.execution_id, self.responses.len())
                    .await?;
                trace!("Got next responses {next_responses:?}");
                self.responses.extend(
                    next_responses
                        .into_iter()
                        .map(|outer| (outer.event, Unprocessed)),
                );
                trace!("All responses: {:?}", self.responses);
                if let Some(accept_resp) = self.process_event_by_key(key)? {
                    debug!(join_set_id = %poll_variant.join_set_id(), "Got result");
                    return Ok(accept_resp);
                }
            }
        } else {
            debug!(join_set_id = %poll_variant.join_set_id(),  "Interrupting on {poll_variant:?}");
            Err(poll_variant.as_function_error())
        }
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
                    "EventCall must be processed in an all or be not found from the beginning"
                );
                return Ok(None);
            }
        }
        Ok(Some(last.unwrap()))
    }

    fn next_unprocessed_request(&self) -> Option<(usize, &(HistoryEvent, ProcessingStatus))> {
        // TODO: Remove ProcessingStatus from return value
        self.event_history
            .iter()
            .enumerate()
            .find(|(_, (_, status))| *status == Unprocessed)
    }

    fn mark_next_unprocessed_response(
        &mut self,
        parent_event_idx: usize, // needs to be marked as Processed as well
        join_set_id: JoinSetId,
    ) -> Option<&JoinSetResponseEvent> {
        if let Some(idx) = self
            .responses
            .iter()
            .enumerate()
            .find_map(|(idx, (event, status))| match (status, event) {
                (
                    Unprocessed,
                    JoinSetResponseEvent {
                        join_set_id: found, ..
                    },
                ) if *found == join_set_id => Some(idx),
                _ => None,
            })
        {
            self.event_history[parent_event_idx].1 = Processed;
            self.responses[idx].1 = Processed;
            Some(&self.responses[idx].0)
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
        trace!("Finding match for {key:?}, [{found_idx}] {found_request_event:?}");
        match (key, found_request_event) {
            (
                EventHistoryKey::CreateJoinSet { join_set_id },
                HistoryEvent::JoinSet {
                    join_set_id: found_join_set_id,
                },
            ) if join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "Matched JoinSet");
                self.event_history[found_idx].1 = Processed;
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
                trace!(%child_execution_id, %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.event_history[found_idx].1 = Processed;
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
                self.event_history[found_idx].1 = Processed;
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
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
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
                    None => Ok(None), // no progress, still at JoinNext
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
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
                            JoinSetResponse::DelayFinished {
                                delay_id: _, // Currently only a single blocking delay is supported, no need to match the id
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & DelayFinished");
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

    async fn flush_non_blocking_event_cache_if_full<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        created_at: DateTime<Utc>,
    ) -> Result<(), DbError> {
        if self
            .non_blocking_event_batch
            .as_ref()
            .map(std::vec::Vec::len)
            .unwrap_or_default()
            >= MAX_NON_BLOCKING_CACHE_SIZE
        {
            self.flush_non_blocking_event_cache(db_connection, created_at)
                .await
        } else {
            Ok(())
        }
    }

    async fn flush_non_blocking_event_cache<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        created_at: DateTime<Utc>,
    ) -> Result<(), DbError> {
        match &mut self.non_blocking_event_batch {
            Some(non_blocking_event_batch) if !non_blocking_event_batch.is_empty() => {
                let mut batches = Vec::with_capacity(non_blocking_event_batch.len());
                let mut childs = Vec::with_capacity(non_blocking_event_batch.len());
                let mut first_version = None;
                for non_blocking in non_blocking_event_batch.drain(..) {
                    match non_blocking {
                        NonBlockingCache::StartAsync {
                            batch,
                            version,
                            child_req,
                        } => {
                            if first_version.is_none() {
                                first_version.replace(version);
                            }
                            childs.push(child_req);
                            batches.extend(batch);
                        }
                    }
                }
                db_connection
                    .append_batch_create_child(
                        created_at,
                        batches,
                        self.execution_id,
                        first_version.unwrap(),
                        childs,
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn append_to_db<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_connection: &DB,
        created_at: DateTime<Utc>,
        lock_expires_at: DateTime<Utc>,
        execution_id: ExecutionId,
        version: &mut Version,
        child_retry_exp_backoff: Duration,
        child_max_retries: u32,
    ) -> Result<Vec<HistoryEvent>, DbError> {
        match event_call {
            EventCall::CreateJoinSet { join_set_id } => {
                let event = HistoryEvent::JoinSet { join_set_id };
                let history_events = vec![event.clone()];
                let join_set = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "CreateJoinSet: Creating new JoinSet");
                *version = db_connection
                    .append(execution_id, version.clone(), join_set)
                    .await?;
                Ok(history_events)
            }
            EventCall::StartAsync {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                let history_events = vec![event.clone()];
                let child_exec_req = ExecutionEventInner::HistoryEvent { event };
                debug!(%child_execution_id, %join_set_id, "StartAsync: appending ChildExecutionRequest");
                let child_req = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: created_at,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };
                *version =
                    if let Some(non_blocking_event_batch) = &mut self.non_blocking_event_batch {
                        non_blocking_event_batch.push(NonBlockingCache::StartAsync {
                            batch: vec![child_exec_req],
                            version: version.clone(),
                            child_req,
                        });
                        self.flush_non_blocking_event_cache_if_full(db_connection, created_at)
                            .await?;
                        Version::new(version.0 + 1)
                    } else {
                        db_connection
                            .append_batch_create_child(
                                created_at,
                                vec![child_exec_req],
                                execution_id,
                                version.clone(),
                                vec![child_req],
                            )
                            .await?
                    };
                Ok(history_events)
            }
            EventCall::BlockingChildJoinNext { join_set_id } => {
                self.flush_non_blocking_event_cache(db_connection, created_at)
                    .await?;
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                let history_events = vec![event.clone()];
                let join_next = AppendRequest {
                    created_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "BlockingChildJoinNext: appending JoinNext");
                *version = db_connection
                    .append(execution_id, version.clone(), join_next)
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingChildExecutionRequest {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                self.flush_non_blocking_event_cache(db_connection, created_at)
                    .await?;
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = ExecutionEventInner::HistoryEvent { event };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                };
                history_events.push(event.clone());
                let child_exec_req = ExecutionEventInner::HistoryEvent { event };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = ExecutionEventInner::HistoryEvent { event };
                debug!(%child_execution_id, %join_set_id, "BlockingChildExecutionRequest: Appending JoinSet,ChildExecutionRequest,JoinNext");
                let child = CreateRequest {
                    created_at,
                    execution_id: child_execution_id,
                    ffqn: ffqn.clone(),
                    params: params.clone(),
                    parent: Some((execution_id, join_set_id)),
                    scheduled_at: created_at,
                    retry_exp_backoff: child_retry_exp_backoff,
                    max_retries: child_max_retries,
                };
                *version = db_connection
                    .append_batch_create_child(
                        created_at,
                        vec![join_set, child_exec_req, join_next],
                        execution_id,
                        version.clone(),
                        vec![child],
                    )
                    .await?;
                Ok(history_events)
            }
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                expires_at_if_new,
            } => {
                self.flush_non_blocking_event_cache(db_connection, created_at)
                    .await?;
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSet { join_set_id };
                history_events.push(event.clone());
                let join_set = ExecutionEventInner::HistoryEvent { event };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at: expires_at_if_new,
                    },
                };
                history_events.push(event.clone());
                let delay_req = ExecutionEventInner::HistoryEvent { event };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                history_events.push(event.clone());
                let join_next = ExecutionEventInner::HistoryEvent { event };
                debug!(%delay_id, %join_set_id, "BlockingDelayRequest: appending JoinSet,DelayRequest,JoinNext");
                *version = db_connection
                    .append_batch(
                        created_at,
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

#[derive(Debug)]
enum PollVariant {
    JoinNextChild(JoinSetId),
    JoinNextDelay(JoinSetId),
}
impl PollVariant {
    fn join_set_id(&self) -> JoinSetId {
        match self {
            PollVariant::JoinNextChild(join_set_id) | PollVariant::JoinNextDelay(join_set_id) => {
                *join_set_id
            }
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

    fn as_function_error(&self) -> FunctionError {
        match self {
            PollVariant::JoinNextChild(_) => FunctionError::ChildExecutionRequest,
            PollVariant::JoinNextDelay(_) => FunctionError::DelayRequest,
        }
    }

    fn as_worker_result(&self) -> WorkerResult {
        match self {
            PollVariant::JoinNextChild(_) => WorkerResult::ChildExecutionRequest,
            PollVariant::JoinNextDelay(_) => WorkerResult::DelayRequest,
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

impl EventCall {
    fn poll_variant(&self) -> Option<PollVariant> {
        match &self {
            // Blocking calls can be polled for JoinSetResponse
            EventCall::BlockingChildExecutionRequest { join_set_id, .. }
            | EventCall::BlockingChildJoinNext { join_set_id } => {
                Some(PollVariant::JoinNextChild(*join_set_id))
            }
            EventCall::BlockingDelayRequest { join_set_id, .. } => {
                Some(PollVariant::JoinNextDelay(*join_set_id))
            }
            EventCall::CreateJoinSet { .. } | EventCall::StartAsync { .. } => None, // continue the execution
        }
    }
}

#[derive(Debug, Clone, Copy)]
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
}

#[cfg(test)]
mod tests {
    use crate::event_history::{EventCall, EventHistory};
    use crate::workflow_ctx::tests::MOCK_FFQN;
    use crate::workflow_ctx::FunctionError;
    use crate::workflow_worker::{JoinNextBlockingStrategy, NonBlockingEventBatching};
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::JoinSetId;
    use concepts::storage::{CreateRequest, DbPool};
    use concepts::storage::{
        DbConnection, DbError, JoinSetResponse, JoinSetResponseEvent, Version,
    };
    use concepts::{ExecutionId, Params, SupportedFunctionResult};
    use db_tests::Database;
    use executor::worker::{WorkerError, WorkerResult};
    use std::sync::Arc;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::info;
    use utils::time::ClockFn;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    async fn load_event_history<C: ClockFn>(
        db_connection: &impl DbConnection,
        execution_id: ExecutionId,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
    ) -> Result<(EventHistory<C>, Version), DbError> {
        let exec_log = db_connection.get(execution_id).await?;
        let event_history = EventHistory::new(
            execution_id,
            exec_log.event_history().collect(),
            exec_log
                .responses
                .into_iter()
                .map(|event| event.event)
                .collect(),
            JoinNextBlockingStrategy::default(),
            execution_deadline,
            Duration::ZERO,
            0,
            NonBlockingEventBatching::default(),
            clock_fn,
            Arc::new(std::sync::Mutex::new(WorkerResult::Err(
                WorkerError::IntermittentTimeout,
            ))),
        );
        Ok((event_history, exec_log.version))
    }

    #[tokio::test]
    async fn regular_join_next_child() {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up(sim_clock.get_clock_fn()).await;

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
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (event_history, version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        let blocking_join_first = |mut event_history: EventHistory<_>, mut version: Version| {
            let db_pool = db_pool.clone();
            async move {
                event_history
                    .replay_or_interrupt(
                        EventCall::CreateJoinSet { join_set_id },
                        &db_pool.connection(),
                        &mut version,
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
                        &db_pool.connection(),
                        &mut version,
                    )
                    .await
                    .unwrap();
                event_history
                    .replay_or_interrupt(
                        EventCall::BlockingChildJoinNext { join_set_id },
                        &db_pool.connection(),
                        &mut version,
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
            .append_response(
                sim_clock.now(),
                execution_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id,
                        result: Ok(SupportedFunctionResult::None),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (event_history, version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        blocking_join_first(event_history, version).await.unwrap();

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn start_async_respond_then_join_next() {
        const CHILD_RESP: SupportedFunctionResult =
            SupportedFunctionResult::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });

        async fn start_async<DB: DbConnection, C: ClockFn>(
            event_history: &mut EventHistory<C>,
            version: &mut Version,
            db_pool: &impl DbPool<DB>,
            join_set_id: JoinSetId,
            child_execution_id: ExecutionId,
        ) {
            event_history
                .replay_or_interrupt(
                    EventCall::CreateJoinSet { join_set_id },
                    &db_pool.connection(),
                    version,
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
                    &db_pool.connection(),
                    version,
                )
                .await
                .unwrap();
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up(sim_clock.get_clock_fn()).await;

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
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        start_async(
            &mut event_history,
            &mut version,
            &db_pool,
            join_set_id,
            child_execution_id,
        )
        .await;

        // append child response before issuing join_next
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id,
                        result: Ok(CHILD_RESP),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        start_async(
            &mut event_history,
            &mut version,
            &db_pool,
            join_set_id,
            child_execution_id,
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .replay_or_interrupt(
                EventCall::BlockingChildJoinNext { join_set_id },
                &db_pool.connection(),
                &mut version,
            )
            .await
            .unwrap();
        assert_eq!(CHILD_RESP, res);

        drop(db_connection);
        db_pool.close().await.unwrap();
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

        async fn blocking_join_first<DB: DbConnection, C: ClockFn>(
            event_history: &mut EventHistory<C>,
            version: &mut Version,
            db_pool: &impl DbPool<DB>,
            join_set_id: JoinSetId,
            child_execution_id_a: ExecutionId,
            child_execution_id_b: ExecutionId,
        ) -> Result<SupportedFunctionResult, FunctionError> {
            event_history
                .replay_or_interrupt(
                    EventCall::CreateJoinSet { join_set_id },
                    &db_pool.connection(),
                    version,
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
                    &db_pool.connection(),
                    version,
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
                    &db_pool.connection(),
                    version,
                )
                .await
                .unwrap();
            event_history
                .replay_or_interrupt(
                    EventCall::BlockingChildJoinNext { join_set_id },
                    &db_pool.connection(),
                    version,
                )
                .await
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up(sim_clock.get_clock_fn()).await;

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
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        let join_set_id = JoinSetId::generate();
        let child_execution_id_a = ExecutionId::generate();
        let child_execution_id_b = ExecutionId::generate();

        assert_matches!(
            blocking_join_first(
                &mut event_history,
                &mut version,
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
            .append_response(
                sim_clock.now(),
                execution_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_a,
                        result: Ok(KID_A),
                    },
                },
            )
            .await
            .unwrap();
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_b,
                        result: Ok(KID_B),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
        )
        .await
        .unwrap();

        let res = blocking_join_first(
            &mut event_history,
            &mut version,
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
                &db_pool.connection(),
                &mut version,
            )
            .await
            .unwrap();
        assert_eq!(KID_B, res);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }
}
