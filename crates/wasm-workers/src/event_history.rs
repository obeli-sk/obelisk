use crate::event_history::ProcessingStatus::Processed;
use crate::event_history::ProcessingStatus::Unprocessed;
use crate::workflow_ctx::WorkflowFunctionError;
use crate::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::HistoryEventScheduledAt;
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse,
    JoinSetResponseEvent, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::ComponentRetryConfig;
use concepts::ComponentType;
use concepts::ConfigId;
use concepts::ExecutionMetadata;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionReturnValue};
use executor::worker::WorkerResult;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument;
use tracing::Level;
use tracing::Span;
use tracing::{debug, error, trace};
use utils::time::ClockFn;
use val_json::type_wrapper::TypeWrapper;
use val_json::wast_val::{WastVal, WastValWithType};

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum ProcessingStatus {
    Unprocessed,
    Processed,
}

#[expect(clippy::struct_field_names)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct EventHistory<C: ClockFn> {
    execution_id: ExecutionId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    execution_deadline: DateTime<Utc>,
    retry_config: RetryConfig,
    event_history: Vec<(HistoryEvent, ProcessingStatus)>,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    non_blocking_event_batch_size: usize,
    non_blocking_event_batch: Option<Vec<NonBlockingCache>>,
    clock_fn: C,
    timeout_error_container: Arc<std::sync::Mutex<WorkerResult>>,
    topmost_parent: ExecutionId,
    worker_span: Span,
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
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        execution_deadline: DateTime<Utc>,
        child_activity_retry_exp_backoff: Option<Duration>,
        child_activity_max_retries: Option<u32>,
        non_blocking_event_batching: u32,
        clock_fn: C,
        timeout_error_container: Arc<std::sync::Mutex<WorkerResult>>,
        worker_span: Span,
        topmost_parent: ExecutionId,
    ) -> Self {
        let non_blocking_event_batch_size = non_blocking_event_batching as usize;
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
            retry_config: RetryConfig {
                child_activity_max_retries,
                child_activity_retry_exp_backoff,
            },
            non_blocking_event_batch_size,
            non_blocking_event_batch: if non_blocking_event_batch_size == 0 {
                None
            } else {
                Some(Vec::with_capacity(non_blocking_event_batch_size))
            },
            clock_fn,
            timeout_error_container,
            worker_span,
            topmost_parent,
        }
    }

    /// Apply the event and wait if new, replay if already in the event history, or
    /// apply with an interrupt.
    pub(crate) async fn apply<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_connection: &DB,
        version: &mut Version,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<SupportedFunctionReturnValue, WorkflowFunctionError> {
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
                        fn_registry,
                        called_at,
                        lock_expires_at,
                        version,
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
                        fn_registry,
                        called_at,
                        lock_expires_at,
                        version,
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
            *self.timeout_error_container.lock().unwrap() = poll_variant.as_worker_result();
            let key = poll_variant.as_key();

            // Subscribe to the next response.
            loop {
                let next_responses = db_connection
                    .subscribe_to_next_responses(self.execution_id, self.responses.len())
                    .await?;
                debug!("Got next responses {next_responses:?}");
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
    ) -> Result<Option<SupportedFunctionReturnValue>, WorkflowFunctionError> {
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

    // TODO: Check params, scheduled_at etc to catch non-deterministic errors.
    #[expect(clippy::too_many_lines)]
    fn process_event_by_key(
        &mut self,
        key: EventHistoryKey,
    ) -> Result<Option<SupportedFunctionReturnValue>, WorkflowFunctionError> {
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
                Ok(Some(SupportedFunctionReturnValue::Infallible(
                    WastValWithType {
                        r#type: TypeWrapper::String,
                        value: WastVal::String(join_set_id.to_string()),
                    },
                )))
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
                Ok(Some(SupportedFunctionReturnValue::Infallible(
                    WastValWithType {
                        r#type: TypeWrapper::String,
                        value: WastVal::String(execution_id.to_string()),
                    },
                )))
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
                Ok(Some(SupportedFunctionReturnValue::Infallible(
                    WastValWithType {
                        r#type: TypeWrapper::String,
                        value: WastVal::String(delay_id.to_string()),
                    },
                )))
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
                                Err(WorkflowFunctionError::ChildExecutionError(err.clone()))
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
                        Ok(Some(SupportedFunctionReturnValue::None))
                    }
                    _ => Ok(None), // no progress, still at JoinNext
                }
            }

            (
                EventHistoryKey::Schedule { execution_id },
                HistoryEvent::Schedule {
                    execution_id: found_execution_id,
                    ..
                },
            ) if execution_id == *found_execution_id => {
                trace!(%execution_id, "Matched Schedule");
                // return execution id
                Ok(Some(SupportedFunctionReturnValue::Infallible(
                    WastValWithType {
                        r#type: TypeWrapper::String,
                        value: WastVal::String(execution_id.to_string()),
                    },
                )))
            }

            (key, found) => Err(WorkflowFunctionError::NonDeterminismDetected(
                StrVariant::Arc(Arc::from(format!(
                    "unexpected key {key:?} not matching {found:?} at index {found_idx}",
                ))),
            )),
        }
    }

    pub(crate) async fn flush<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
    ) -> Result<(), DbError> {
        self.flush_non_blocking_event_cache(db_connection, (self.clock_fn)())
            .await
    }

    async fn flush_non_blocking_event_cache_if_full<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        created_at: DateTime<Utc>,
    ) -> Result<(), DbError> {
        match &self.non_blocking_event_batch {
            Some(vec) if vec.len() >= self.non_blocking_event_batch_size => {
                self.flush_non_blocking_event_cache(db_connection, created_at)
                    .await
            }
            _ => Ok(()),
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, db_connection))]
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
                    .append_batch_create_new_execution(
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

    #[instrument(level = Level::DEBUG, skip_all, fields(%version))]
    async fn append_to_db<DB: DbConnection>(
        &mut self,
        event_call: EventCall,
        db_connection: &DB,
        fn_registry: &dyn FunctionRegistry,
        called_at: DateTime<Utc>,
        lock_expires_at: DateTime<Utc>,
        version: &mut Version,
    ) -> Result<Vec<HistoryEvent>, WorkflowFunctionError> {
        async fn component_active_get_exported_function(
            fn_registry: &dyn FunctionRegistry,
            ffqn: &FunctionFqn,
        ) -> Result<(FunctionMetadata, ConfigId, ComponentRetryConfig), WorkflowFunctionError>
        {
            fn_registry
                .get_by_exported_function(ffqn)
                .await
                .ok_or_else(|| WorkflowFunctionError::FunctionMetadataNotFound {
                    ffqn: ffqn.clone(),
                })
        }

        trace!(%version, "append_to_db");
        match event_call {
            EventCall::CreateJoinSet { join_set_id } => {
                let event = HistoryEvent::JoinSet { join_set_id };
                let history_events = vec![event.clone()];
                let join_set = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "CreateJoinSet: Creating new JoinSet");
                *version = db_connection
                    .append(self.execution_id, version.clone(), join_set)
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
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type,
                    },
                    config_id,
                    child_retry_config,
                ) = component_active_get_exported_function(fn_registry, &ffqn).await?;
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((self.execution_id, join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: self
                        .retry_config
                        .child_retry_exp_backoff(config_id.component_type, child_retry_config),
                    max_retries: self
                        .retry_config
                        .child_max_retries(config_id.component_type, child_retry_config),
                    config_id,
                    return_type: return_type.map(|rt| rt.type_wrapper),
                    topmost_parent: self.topmost_parent,
                };
                *version =
                    if let Some(non_blocking_event_batch) = &mut self.non_blocking_event_batch {
                        non_blocking_event_batch.push(NonBlockingCache::StartAsync {
                            batch: vec![child_exec_req],
                            version: version.clone(),
                            child_req,
                        });
                        self.flush_non_blocking_event_cache_if_full(db_connection, called_at)
                            .await?;
                        Version::new(version.0 + 1)
                    } else {
                        db_connection
                            .append_batch_create_new_execution(
                                called_at,
                                vec![child_exec_req],
                                self.execution_id,
                                version.clone(),
                                vec![child_req],
                            )
                            .await?
                    };
                Ok(history_events)
            }

            EventCall::ScheduleRequest {
                scheduled_at,
                execution_id: new_execution_id,
                ffqn,
                params,
            } => {
                let event = HistoryEvent::Schedule {
                    execution_id: new_execution_id,
                    scheduled_at,
                };
                let scheduled_at = scheduled_at.as_date_time(called_at);
                let history_events = vec![event.clone()];
                let child_exec_req = ExecutionEventInner::HistoryEvent { event };
                debug!(%new_execution_id, "ScheduleRequest: appending");
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type,
                    },
                    config_id,
                    child_retry_config,
                ) = component_active_get_exported_function(fn_registry, &ffqn).await?;
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: new_execution_id,
                    metadata: ExecutionMetadata::from_linked_span(&self.worker_span),
                    ffqn,
                    params,
                    parent: None, // Schedule breaks from the parent-child relationship to avoid a linked list
                    scheduled_at,
                    retry_exp_backoff: self
                        .retry_config
                        .child_retry_exp_backoff(config_id.component_type, child_retry_config),
                    max_retries: self
                        .retry_config
                        .child_max_retries(config_id.component_type, child_retry_config),
                    config_id,
                    return_type: return_type.map(|rt| rt.type_wrapper),
                    topmost_parent: self.topmost_parent,
                };
                *version =
                    if let Some(non_blocking_event_batch) = &mut self.non_blocking_event_batch {
                        non_blocking_event_batch.push(NonBlockingCache::StartAsync {
                            batch: vec![child_exec_req],
                            version: version.clone(),
                            child_req,
                        });
                        self.flush_non_blocking_event_cache_if_full(db_connection, called_at)
                            .await?;
                        Version::new(version.0 + 1)
                    } else {
                        db_connection
                            .append_batch_create_new_execution(
                                called_at,
                                vec![child_exec_req],
                                self.execution_id,
                                version.clone(),
                                vec![child_req],
                            )
                            .await?
                    };
                Ok(history_events)
            }

            EventCall::BlockingChildJoinNext { join_set_id } => {
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    lock_expires_at,
                };
                let history_events = vec![event.clone()];
                let join_next = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "BlockingChildJoinNext: appending JoinNext");
                *version = db_connection
                    .append(self.execution_id, version.clone(), join_next)
                    .await?;
                Ok(history_events)
            }

            EventCall::BlockingChildExecutionRequest {
                ffqn,
                join_set_id,
                child_execution_id,
                params,
            } => {
                self.flush_non_blocking_event_cache(db_connection, called_at)
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
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type,
                    },
                    config_id,
                    child_retry_config,
                ) = component_active_get_exported_function(fn_registry, &ffqn).await?;
                let child = CreateRequest {
                    created_at: called_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((self.execution_id, join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: self
                        .retry_config
                        .child_retry_exp_backoff(config_id.component_type, child_retry_config),
                    max_retries: self
                        .retry_config
                        .child_max_retries(config_id.component_type, child_retry_config),
                    config_id,
                    return_type: return_type.map(|rt| rt.type_wrapper),
                    topmost_parent: self.topmost_parent,
                };
                *version = db_connection
                    .append_batch_create_new_execution(
                        called_at,
                        vec![join_set, child_exec_req, join_next],
                        self.execution_id,
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
                self.flush_non_blocking_event_cache(db_connection, called_at)
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
                        called_at,
                        vec![join_set, delay_req, join_next],
                        self.execution_id,
                        version.clone(),
                    )
                    .await?;
                Ok(history_events)
            }
        }
    }
}

#[derive(Clone)]
struct RetryConfig {
    child_activity_max_retries: Option<u32>,
    child_activity_retry_exp_backoff: Option<Duration>,
}

impl RetryConfig {
    fn child_max_retries(
        &self,
        component_type: ComponentType,
        child_config: ComponentRetryConfig,
    ) -> u32 {
        match component_type {
            ComponentType::WasmActivity => self
                .child_activity_max_retries
                .unwrap_or(child_config.max_retries),
            ComponentType::Workflow => 0,
            ComponentType::WebhookComponent => unreachable!("webhook can only be invoked via HTTP"),
        }
    }

    fn child_retry_exp_backoff(
        &self,
        component_type: ComponentType,
        child_config: ComponentRetryConfig,
    ) -> Duration {
        match component_type {
            ComponentType::WasmActivity => self
                .child_activity_retry_exp_backoff
                .unwrap_or(child_config.retry_exp_backoff),
            ComponentType::Workflow => Duration::ZERO,
            ComponentType::WebhookComponent => unreachable!("webhook can only be invoked via HTTP"),
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

    fn as_function_error(&self) -> WorkflowFunctionError {
        match self {
            PollVariant::JoinNextChild(_) => WorkflowFunctionError::ChildExecutionRequest,
            PollVariant::JoinNextDelay(_) => WorkflowFunctionError::DelayRequest,
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
    ScheduleRequest {
        scheduled_at: HistoryEventScheduledAt,
        execution_id: ExecutionId,
        ffqn: FunctionFqn,
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
            EventCall::CreateJoinSet { .. }
            | EventCall::StartAsync { .. }
            | EventCall::ScheduleRequest { .. } => None, // continue the execution
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
    Schedule {
        execution_id: ExecutionId,
    },
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
            EventCall::ScheduleRequest { execution_id, .. } => {
                vec![EventHistoryKey::Schedule {
                    execution_id: *execution_id,
                }]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::event_history::{EventCall, EventHistory};
    use crate::tests::fn_registry_dummy;
    use crate::workflow_ctx::WorkflowFunctionError;
    use crate::workflow_worker::JoinNextBlockingStrategy;
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::JoinSetId;
    use concepts::storage::{CreateRequest, DbPool};
    use concepts::storage::{DbConnection, JoinSetResponse, JoinSetResponseEvent, Version};
    use concepts::{
        ConfigId, ExecutionId, FunctionFqn, FunctionRegistry, Params, SupportedFunctionReturnValue,
    };
    use db_tests::Database;
    use executor::worker::{WorkerError, WorkerResult};
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::{info, info_span};
    use utils::time::ClockFn;
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    pub const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    async fn load_event_history<C: ClockFn>(
        db_connection: &impl DbConnection,
        execution_id: ExecutionId,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        non_blocking_event_batching: u32,
    ) -> (EventHistory<C>, Version) {
        let exec_log = db_connection.get(execution_id).await.unwrap();
        let event_history = EventHistory::new(
            execution_id,
            exec_log.event_history().collect(),
            exec_log
                .responses
                .into_iter()
                .map(|event| event.event)
                .collect(),
            join_next_blocking_strategy,
            execution_deadline,
            None,
            None,
            non_blocking_event_batching,
            clock_fn,
            Arc::new(std::sync::Mutex::new(WorkerResult::Err(
                WorkerError::IntermittentTimeout,
            ))),
            info_span!("worker-test"),
            execution_id,
        );
        (event_history, exec_log.version)
    }

    #[rstest]
    #[tokio::test]
    async fn regular_join_next_child(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        second_run_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;

        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = fn_registry_dummy(&[MOCK_FFQN]);
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();

        let (event_history, version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
            JoinNextBlockingStrategy::Interrupt, // first run needs to interrupt
            batching,
        )
        .await;

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        let blocking_join_first =
            |mut event_history: EventHistory<_>,
             mut version: Version,
             fn_registry: Arc<dyn FunctionRegistry>| {
                let db_pool = db_pool.clone();
                async move {
                    event_history
                        .apply(
                            EventCall::CreateJoinSet { join_set_id },
                            &db_pool.connection(),
                            &mut version,
                            fn_registry.as_ref(),
                        )
                        .await
                        .unwrap();

                    event_history
                        .apply(
                            EventCall::StartAsync {
                                ffqn: MOCK_FFQN,
                                join_set_id,
                                child_execution_id,
                                params: Params::empty(),
                            },
                            &db_pool.connection(),
                            &mut version,
                            fn_registry.as_ref(),
                        )
                        .await
                        .unwrap();
                    event_history
                        .apply(
                            EventCall::BlockingChildJoinNext { join_set_id },
                            &db_pool.connection(),
                            &mut version,
                            fn_registry.as_ref(),
                        )
                        .await
                }
            };

        assert_matches!(
            blocking_join_first(event_history, version, fn_registry.clone())
                .await
                .unwrap_err(),
            WorkflowFunctionError::ChildExecutionRequest,
            "should have ended with an interrupt"
        );
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id,
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id,
                        result: Ok(SupportedFunctionReturnValue::None),
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
            second_run_strategy,
            batching,
        )
        .await;
        blocking_join_first(event_history, version, fn_registry.clone())
            .await
            .expect("should finish successfuly");

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn start_async_respond_then_join_next(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        const CHILD_RESP: SupportedFunctionReturnValue =
            SupportedFunctionReturnValue::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });

        async fn start_async<DB: DbConnection, C: ClockFn>(
            event_history: &mut EventHistory<C>,
            version: &mut Version,
            db_pool: &impl DbPool<DB>,
            join_set_id: JoinSetId,
            child_execution_id: ExecutionId,
            fn_registry: &dyn FunctionRegistry,
        ) {
            event_history
                .apply(
                    EventCall::CreateJoinSet { join_set_id },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .unwrap();
            event_history
                .apply(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id,
                        params: Params::empty(),
                    },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .unwrap();
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = fn_registry_dummy(&[MOCK_FFQN]);
        // Create an execution.
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
            join_next_blocking_strategy,
            batching,
        )
        .await;

        let join_set_id = JoinSetId::generate();
        let child_execution_id = ExecutionId::generate();

        start_async(
            &mut event_history,
            &mut version,
            &db_pool,
            join_set_id,
            child_execution_id,
            fn_registry.as_ref(),
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
            join_next_blocking_strategy,
            batching,
        )
        .await;

        start_async(
            &mut event_history,
            &mut version,
            &db_pool,
            join_set_id,
            child_execution_id,
            fn_registry.as_ref(),
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .apply(
                EventCall::BlockingChildJoinNext { join_set_id },
                &db_pool.connection(),
                &mut version,
                fn_registry.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(CHILD_RESP, res);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn create_two_non_blocking_childs_then_two_join_nexts(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await)]
        second_run_strategy: JoinNextBlockingStrategy,
        #[values(0, 10)] batching: u32,
    ) {
        const KID_A: SupportedFunctionReturnValue =
            SupportedFunctionReturnValue::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });
        const KID_B: SupportedFunctionReturnValue =
            SupportedFunctionReturnValue::Infallible(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(2),
            });

        async fn blocking_join_first<DB: DbConnection, C: ClockFn>(
            event_history: &mut EventHistory<C>,
            version: &mut Version,
            db_pool: &impl DbPool<DB>,
            fn_registry: &dyn FunctionRegistry,
            join_set_id: JoinSetId,
            child_execution_id_a: ExecutionId,
            child_execution_id_b: ExecutionId,
        ) -> Result<SupportedFunctionReturnValue, WorkflowFunctionError> {
            event_history
                .apply(
                    EventCall::CreateJoinSet { join_set_id },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .unwrap();
            event_history
                .apply(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id: child_execution_id_a,
                        params: Params::empty(),
                    },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .unwrap();
            event_history
                .apply(
                    EventCall::StartAsync {
                        ffqn: MOCK_FFQN,
                        join_set_id,
                        child_execution_id: child_execution_id_b,
                        params: Params::empty(),
                    },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .unwrap();
            event_history
                .apply(
                    EventCall::BlockingChildJoinNext { join_set_id },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
        }

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;

        // Create an execution.
        let created_at = sim_clock.now();
        let db_connection = db_pool.connection();
        let fn_registry = fn_registry_dummy(&[MOCK_FFQN]);
        let execution_id = ExecutionId::generate();
        db_connection
            .create(CreateRequest {
                created_at,
                execution_id,
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                config_id: ConfigId::dummy(),
                return_type: None,
                topmost_parent: execution_id,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id,
            sim_clock.now(),
            sim_clock.get_clock_fn(),
            JoinNextBlockingStrategy::Interrupt,
            batching,
        )
        .await;

        let join_set_id = JoinSetId::generate();
        let child_execution_id_a = ExecutionId::generate();
        let child_execution_id_b = ExecutionId::generate();

        assert_matches!(
            blocking_join_first(
                &mut event_history,
                &mut version,
                &db_pool,
                fn_registry.as_ref(),
                join_set_id,
                child_execution_id_a,
                child_execution_id_b
            )
            .await
            .unwrap_err(),
            WorkflowFunctionError::ChildExecutionRequest
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
            second_run_strategy,
            batching,
        )
        .await;

        let res = blocking_join_first(
            &mut event_history,
            &mut version,
            &db_pool,
            fn_registry.as_ref(),
            join_set_id,
            child_execution_id_a,
            child_execution_id_b,
        )
        .await
        .unwrap();
        assert_eq!(KID_A, res);

        // second child result should be found
        let res = event_history
            .apply(
                EventCall::BlockingChildJoinNext { join_set_id },
                &db_pool.connection(),
                &mut version,
                fn_registry.as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(KID_B, res);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }
}
