use super::event_history::ProcessingStatus::Processed;
use super::event_history::ProcessingStatus::Unprocessed;
use super::workflow_ctx::InterruptRequested;
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::host_exports::delay_id_into_wast_val;
use crate::host_exports::execution_id_into_wast_val;
use crate::host_exports::join_set_id_into_wast_val;
use chrono::{DateTime, Utc};
use concepts::prefixed_ulid::{DelayId, JoinSetId};
use concepts::storage::HistoryEventScheduledAt;
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse,
    JoinSetResponseEvent, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::ComponentId;
use concepts::ComponentRetryConfig;
use concepts::ExecutionMetadata;
use concepts::FinishedExecutionError;
use concepts::FunctionMetadata;
use concepts::FunctionRegistry;
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionReturnValue};
use indexmap::IndexMap;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::instrument;
use tracing::Level;
use tracing::Span;
use tracing::{debug, error, trace};
use utils::time::ClockFn;
use val_json::wast_val::WastVal;

#[derive(Debug)]
pub(crate) enum ChildReturnValue {
    None,
    WastVal(WastVal),
    HostActionResp(HostActionResp),
}

#[derive(Debug)]
pub(crate) enum HostActionResp {
    CreateJoinSetResp(JoinSetId), // response to `CreateJoinSet`
}

impl ChildReturnValue {
    fn from_wast_val_or_none(result: Option<WastVal>) -> Self {
        if let Some(result) = result {
            Self::WastVal(result)
        } else {
            Self::None
        }
    }

    pub(crate) fn into_wast_val(self) -> Option<WastVal> {
        match self {
            Self::None => None,
            Self::WastVal(wast_val) => Some(wast_val),
            Self::HostActionResp(HostActionResp::CreateJoinSetResp(join_set_id)) => {
                Some(join_set_id_into_wast_val(join_set_id))
            }
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum ProcessingStatus {
    Unprocessed,
    Processed,
}

#[derive(Debug, Clone)]
pub(crate) enum ApplyError {
    // fatal errors:
    NondeterminismDetected(StrVariant),
    ChildExecutionError(FinishedExecutionError), // only on direct call
    // retriable errors:
    InterruptRequested,
    DbError(DbError),
}

#[expect(clippy::struct_field_names)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct EventHistory<C: ClockFn> {
    execution_id: ExecutionId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    execution_deadline: DateTime<Utc>,
    event_history: Vec<(HistoryEvent, ProcessingStatus)>,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    non_blocking_event_batch_size: usize,
    non_blocking_event_batch: Option<Vec<NonBlockingCache>>,
    clock_fn: C,
    interrupt_on_timeout_container: Arc<std::sync::Mutex<Option<InterruptRequested>>>,
    worker_span: Span,
    // TODO: optimize using start_from_idx: usize,
}

#[cfg_attr(test, derive(Clone))]
enum NonBlockingCache {
    StartAsync {
        batch: Vec<AppendRequest>,
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
        non_blocking_event_batching: u32,
        clock_fn: C,
        interrupt_on_timeout_container: Arc<std::sync::Mutex<Option<InterruptRequested>>>,
        worker_span: Span,
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
            non_blocking_event_batch_size,
            non_blocking_event_batch: if non_blocking_event_batch_size == 0 {
                None
            } else {
                Some(Vec::with_capacity(non_blocking_event_batch_size))
            },
            clock_fn,
            interrupt_on_timeout_container,
            worker_span,
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
    ) -> Result<ChildReturnValue, ApplyError> {
        trace!("replay_or_interrupt: {event_call:?}");
        if let Some(accept_resp) = self.find_matching_atomic(&event_call)? {
            return Ok(accept_resp);
        }
        // not found in the history, persisting the request
        let called_at = self.clock_fn.now();
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
                    .await
                    .map_err(ApplyError::DbError)?;
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
                    .await
                    .map_err(ApplyError::DbError)?;
                self.event_history
                    .extend(history_events.into_iter().map(|event| (event, Unprocessed)));
                let keys_len = keys.len();
                for (idx, key) in keys.into_iter().enumerate() {
                    let res = self.process_event_by_key(&key)?;
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
            *self.interrupt_on_timeout_container.lock().unwrap() = Some(InterruptRequested);
            let key = poll_variant.as_key();

            // Subscribe to the next response.
            loop {
                let next_responses = db_connection
                    .subscribe_to_next_responses(&self.execution_id, self.responses.len())
                    .await
                    .map_err(ApplyError::DbError)?;
                debug!("Got next responses {next_responses:?}");
                self.responses.extend(
                    next_responses
                        .into_iter()
                        .map(|outer| (outer.event, Unprocessed)),
                );
                trace!("All responses: {:?}", self.responses);
                if let Some(accept_resp) = self.process_event_by_key(&key)? {
                    debug!(join_set_id = %poll_variant.join_set_id(), "Got result");
                    return Ok(accept_resp);
                }
            }
        } else {
            debug!(join_set_id = %poll_variant.join_set_id(),  "Interrupting on {poll_variant:?}");
            Err(ApplyError::InterruptRequested)
        }
    }

    /// Scan the execution log for join sets containing more `[JoinSetRequest::ChildExecutionRequest]`-s
    /// than responses.
    /// For each open join set emit `EventCall::BlockingChildAwaitNext` and wait for the response.
    /// MUST NOT add items to `NonBlockingCache`, as only `EventCall::BlockingChildAwaitNext` and their
    /// processed responses are appended.
    pub(crate) async fn close_opened_join_sets<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        version: &mut Version,
        fn_registry: &dyn FunctionRegistry,
    ) -> Result<(), ApplyError> {
        let mut join_set_to_child_exec_count = IndexMap::new();
        for (event, _processing_sattus) in &self.event_history {
            match event {
                HistoryEvent::JoinSet { join_set_id } => {
                    let old = join_set_to_child_exec_count.insert(*join_set_id, 0);
                    assert!(old.is_none());
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { .. },
                } => {
                    let val = join_set_to_child_exec_count
                        .get_mut(join_set_id)
                        .expect("join set must have been created");
                    *val += 1;
                }
                _ => {} // delay requests are ignored if there is no interest awaiting them.
            }
        }
        for processed_response in self.responses.iter().filter_map(|(resp, status)| {
            if *status == ProcessingStatus::Processed {
                Some(resp)
            } else {
                None
            }
        }) {
            if let JoinSetResponseEvent {
                join_set_id,
                event: JoinSetResponse::ChildExecutionFinished { .. },
            } = processed_response
            {
                let val = join_set_to_child_exec_count
                    .get_mut(join_set_id)
                    .expect("join set must have been created");
                *val -= 1;
            }
        }
        // Keep only the join sets that have unanswered requests.
        let unanswered = join_set_to_child_exec_count
            .into_iter()
            .filter(|(_, unanswered)| *unanswered > 0);
        for (join_set_id, _) in unanswered {
            self.apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: true,
                },
                db_connection,
                version,
                fn_registry,
            )
            .await?;
            // The actual child response is not needed, just the fact that there may be more left.
        }
        Ok(())
    }

    fn find_matching_atomic(
        &mut self,
        event_call: &EventCall,
    ) -> Result<Option<ChildReturnValue>, ApplyError> {
        // None means no progress
        let keys = event_call.as_keys();
        assert!(!keys.is_empty());
        let mut last = None;
        for (idx, key) in keys.into_iter().enumerate() {
            last = self.process_event_by_key(&key)?;
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
        key: &EventHistoryKey,
    ) -> Result<Option<ChildReturnValue>, ApplyError> {
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
            ) if *join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "Matched JoinSet");
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::CreateJoinSet`] , return join set id
                Ok(Some(ChildReturnValue::HostActionResp(
                    HostActionResp::CreateJoinSetResp(*join_set_id),
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
            ) if *join_set_id == *found_join_set_id && *execution_id == *child_execution_id => {
                trace!(%child_execution_id, %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::StartAsync`] , return execution id
                Ok(Some(ChildReturnValue::WastVal(execution_id_into_wast_val(
                    execution_id,
                ))))
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
            ) if *join_set_id == *found_join_set_id && *delay_id == *found_delay_id => {
                trace!(%delay_id, %join_set_id, "Matched JoinSetRequest::DelayRequest");
                self.event_history[found_idx].1 = Processed;
                // return delay id
                Ok(Some(ChildReturnValue::WastVal(delay_id_into_wast_val(
                    *delay_id,
                ))))
            }

            (
                EventHistoryKey::JoinNextChild { join_set_id, kind },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if *join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "Peeked at JoinNext - Child");
                // TODO: If the child execution succeeded, perform type check between `SupportedFunctionReturnValue`
                // and what is expected by the `FunctionRegistry`
                match self.mark_next_unprocessed_response(found_idx, *join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                result,
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & ChildExecutionFinished");
                        match kind {
                            JoinNextKind::DirectCall => match result {
                                Ok(result) => Ok(Some(ChildReturnValue::from_wast_val_or_none(
                                    result.clone().into_value(),
                                ))),
                                Err(err) => {
                                    error!(%child_execution_id,
                                            %join_set_id,
                                            "Child execution finished with an execution error, failing the parent");
                                    Err(ApplyError::ChildExecutionError(err.clone()))
                                }
                            },
                            JoinNextKind::AwaitNext => {
                                match result {
                                    Ok(SupportedFunctionReturnValue::None) => {
                                        // result<execution-id, tuple<execution-id, execution-error>>
                                        Ok(Some(ChildReturnValue::WastVal(WastVal::Result(Ok(
                                            Some(Box::new(execution_id_into_wast_val(
                                                child_execution_id,
                                            ))),
                                        )))))
                                    }
                                    Ok(
                                        SupportedFunctionReturnValue::InfallibleOrResultOk(v)
                                        | SupportedFunctionReturnValue::FallibleResultErr(v),
                                    ) => {
                                        // result<(execution-id, inner>, tuple<execution-id, execution-error>>
                                        Ok(Some(ChildReturnValue::WastVal(WastVal::Result(Ok(
                                            Some(Box::new(WastVal::Tuple(vec![
                                                execution_id_into_wast_val(child_execution_id),
                                                v.value.clone(),
                                            ]))),
                                        )))))
                                    }
                                    Err(err) => {
                                        match kind {
                                            JoinNextKind::DirectCall => {
                                                error!(%child_execution_id,
                                                    %join_set_id,
                                                    "Child execution finished with an execution error, failing the parent");
                                                Err(ApplyError::ChildExecutionError(err.clone()))
                                            }
                                            JoinNextKind::AwaitNext => {
                                                let variant = match err {
                                                    FinishedExecutionError::PermanentTimeout => {
                                                        WastVal::Variant("permanent-timeout".to_string(), None)
                                                    }
                                                    FinishedExecutionError::NondeterminismDetected(_) => {
                                                        WastVal::Variant("nondeterminism".to_string(), None)
                                                    }
                                                    FinishedExecutionError::PermanentFailure{reason, detail:_} => WastVal::Variant(
                                                        "permanent-failure".to_string(),
                                                        Some(Box::new(WastVal::String(reason.to_string()))),
                                                    ),
                                                };
                                                Ok(Some(ChildReturnValue::WastVal(
                                                    WastVal::Result(Err(Some(Box::new(
                                                        WastVal::Tuple(vec![
                                                            execution_id_into_wast_val(
                                                                child_execution_id,
                                                            ),
                                                            variant,
                                                        ]),
                                                    )))),
                                                )))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    None => Ok(None), // no progress, still at JoinNext
                    Some(
                        delay_event @ JoinSetResponseEvent {
                            event: JoinSetResponse::DelayFinished { .. },
                            ..
                        },
                    ) => unreachable!("{delay_event:?} not implemented"),
                }
            }

            (
                EventHistoryKey::JoinNextDelay { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    ..
                },
            ) if *join_set_id == *found_join_set_id => {
                trace!(
                    %join_set_id, "Peeked at JoinNext - Delay");
                match self.mark_next_unprocessed_response(found_idx, *join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
                            JoinSetResponse::DelayFinished {
                                delay_id: _, // Currently only a single blocking delay is supported, no need to match the id
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & DelayFinished");
                        Ok(Some(ChildReturnValue::None))
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
            ) if *execution_id == *found_execution_id => {
                trace!(%execution_id, "Matched Schedule");
                // return execution id
                Ok(Some(ChildReturnValue::WastVal(execution_id_into_wast_val(
                    execution_id,
                ))))
            }

            (key, found) => Err(ApplyError::NondeterminismDetected(StrVariant::Arc(
                Arc::from(format!(
                    "unexpected key {key:?} not matching {found:?} at index {found_idx}",
                )),
            ))),
        }
    }

    pub(crate) async fn flush<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
    ) -> Result<(), DbError> {
        self.flush_non_blocking_event_cache(db_connection, self.clock_fn.now())
            .await
    }

    async fn flush_non_blocking_event_cache_if_full<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        current_time: DateTime<Utc>,
    ) -> Result<(), DbError> {
        match &self.non_blocking_event_batch {
            Some(vec) if vec.len() >= self.non_blocking_event_batch_size => {
                self.flush_non_blocking_event_cache(db_connection, current_time)
                    .await
            }
            _ => Ok(()),
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip(self, db_connection))]
    async fn flush_non_blocking_event_cache<DB: DbConnection>(
        &mut self,
        db_connection: &DB,
        current_time: DateTime<Utc>,
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
                        current_time,
                        batches,
                        self.execution_id.clone(),
                        first_version.expect("checked that !non_blocking_event_batch.is_empty()"),
                        childs,
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn get_called_function_metadata(
        &self,
        fn_registry: &dyn FunctionRegistry,
        ffqn: &FunctionFqn,
    ) -> (FunctionMetadata, ComponentId, ComponentRetryConfig) {
        let (fn_metadata, component_id, child_component_retry_config) = fn_registry
            .get_by_exported_function(ffqn)
            .await
            .unwrap_or_else(|| {
                panic!("imported function must be found during verification: {ffqn}")
            });
        let resolved_retry_config = child_component_retry_config;
        (fn_metadata, component_id, resolved_retry_config)
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
    ) -> Result<Vec<HistoryEvent>, DbError> {
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
                    .append(self.execution_id.clone(), version.clone(), join_set)
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
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: child_execution_id.clone(),
                    },
                };
                let history_events = vec![event.clone()];
                let child_exec_req = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%child_execution_id, %join_set_id, "StartAsync: appending ChildExecutionRequest");
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type: _,
                        extension,
                        submittable: _,
                    },
                    component_id,
                    resolved_retry_config,
                ) = self.get_called_function_metadata(fn_registry, &ffqn).await;
                assert!(extension.is_none());
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((self.execution_id.clone(), join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: resolved_retry_config.retry_exp_backoff,
                    max_retries: resolved_retry_config.max_retries,
                    component_id,
                    scheduled_by: None,
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
                                self.execution_id.clone(),
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
                    execution_id: new_execution_id.clone(),
                    scheduled_at,
                };
                let scheduled_at = scheduled_at.as_date_time(called_at);
                let history_events = vec![event.clone()];
                let child_exec_req = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };
                debug!(%new_execution_id, "ScheduleRequest: appending");
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type: _,
                        extension,
                        submittable: _,
                    },
                    component_id,
                    resolved_retry_config,
                ) = self.get_called_function_metadata(fn_registry, &ffqn).await;
                assert!(extension.is_none());
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: new_execution_id,
                    metadata: ExecutionMetadata::from_linked_span(&self.worker_span),
                    ffqn,
                    params,
                    parent: None, // Schedule breaks from the parent-child relationship to avoid a linked list
                    scheduled_at,
                    retry_exp_backoff: resolved_retry_config.retry_exp_backoff,
                    max_retries: resolved_retry_config.max_retries,
                    component_id,
                    scheduled_by: Some(self.execution_id.clone()),
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
                                self.execution_id.clone(),
                                version.clone(),
                                vec![child_req],
                            )
                            .await?
                    };
                Ok(history_events)
            }

            EventCall::BlockingChildAwaitNext {
                join_set_id,
                closing,
            } => {
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    run_expires_at: lock_expires_at,
                    closing,
                };
                let history_events = vec![event.clone()];
                let join_next = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%join_set_id, "BlockingChildJoinNext: appending JoinNext");
                *version = db_connection
                    .append(self.execution_id.clone(), version.clone(), join_next)
                    .await?;
                Ok(history_events)
            }

            EventCall::BlockingChildDirectCall {
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
                let join_set = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: child_execution_id.clone(),
                    },
                };
                history_events.push(event.clone());
                let child_exec_req = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    run_expires_at: lock_expires_at,
                    closing: false,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };
                debug!(%child_execution_id, %join_set_id, "BlockingChildExecutionRequest: Appending JoinSet,ChildExecutionRequest,JoinNext");
                let (
                    FunctionMetadata {
                        ffqn,
                        parameter_types: _,
                        return_type: _,
                        extension,
                        submittable: _,
                    },
                    component_id,
                    resolved_retry_config,
                ) = self.get_called_function_metadata(fn_registry, &ffqn).await;
                assert!(extension.is_none());
                let child = CreateRequest {
                    created_at: called_at,
                    execution_id: child_execution_id,
                    ffqn,
                    params,
                    parent: Some((self.execution_id.clone(), join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: resolved_retry_config.retry_exp_backoff,
                    max_retries: resolved_retry_config.max_retries,
                    component_id,
                    scheduled_by: None,
                };
                *version = db_connection
                    .append_batch_create_new_execution(
                        called_at,
                        vec![join_set, child_exec_req, join_next],
                        self.execution_id.clone(),
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
                let join_set = AppendRequest {
                    created_at: called_at,
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
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinNext {
                    join_set_id,
                    run_expires_at: lock_expires_at,
                    closing: false,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                debug!(%delay_id, %join_set_id, "BlockingDelayRequest: appending JoinSet,DelayRequest,JoinNext");
                *version = db_connection
                    .append_batch(
                        called_at,
                        vec![join_set, delay_req, join_next],
                        self.execution_id.clone(),
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
    JoinNextChild {
        join_set_id: JoinSetId,
        kind: JoinNextKind,
    },
    JoinNextDelay(JoinSetId),
}
impl PollVariant {
    fn join_set_id(&self) -> JoinSetId {
        match self {
            PollVariant::JoinNextChild { join_set_id, .. }
            | PollVariant::JoinNextDelay(join_set_id) => *join_set_id,
        }
    }
    fn as_key(&self) -> EventHistoryKey {
        match self {
            PollVariant::JoinNextChild { join_set_id, kind } => EventHistoryKey::JoinNextChild {
                join_set_id: *join_set_id,
                kind: *kind,
            },
            PollVariant::JoinNextDelay(join_set_id) => EventHistoryKey::JoinNextDelay {
                join_set_id: *join_set_id,
            },
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
    BlockingChildAwaitNext {
        join_set_id: JoinSetId,
        closing: bool,
    },
    /// combines [`Self::CreateJoinSet`] [`Self::StartAsync`] [`Self::BlockingChildJoinNext`]
    /// Direct call
    BlockingChildDirectCall {
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
            EventCall::BlockingChildDirectCall { join_set_id, .. } => {
                Some(PollVariant::JoinNextChild {
                    join_set_id: *join_set_id,
                    kind: JoinNextKind::DirectCall,
                })
            }
            EventCall::BlockingChildAwaitNext {
                join_set_id,
                closing: _,
            } => Some(PollVariant::JoinNextChild {
                join_set_id: *join_set_id,
                kind: JoinNextKind::AwaitNext,
            }),
            EventCall::BlockingDelayRequest { join_set_id, .. } => {
                Some(PollVariant::JoinNextDelay(*join_set_id))
            }
            EventCall::CreateJoinSet { .. }
            | EventCall::StartAsync { .. }
            | EventCall::ScheduleRequest { .. } => None, // continue the execution
        }
    }
}

#[derive(Debug, Clone)]
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
        kind: JoinNextKind,
        // TODO: Add wakeup conditions: ffqn (used by function_await), execution id
    },
    JoinNextDelay {
        join_set_id: JoinSetId,
    },
    Schedule {
        execution_id: ExecutionId,
    },
}

#[derive(Debug, Clone, Copy)]
enum JoinNextKind {
    AwaitNext,
    DirectCall,
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
                child_execution_id: child_execution_id.clone(),
            }],
            EventCall::BlockingChildAwaitNext {
                join_set_id,
                closing: _,
            } => {
                vec![EventHistoryKey::JoinNextChild {
                    join_set_id: *join_set_id,
                    kind: JoinNextKind::AwaitNext,
                }]
            }
            EventCall::BlockingChildDirectCall {
                join_set_id,
                child_execution_id,
                ..
            } => vec![
                EventHistoryKey::CreateJoinSet {
                    join_set_id: *join_set_id,
                },
                EventHistoryKey::ChildExecutionRequest {
                    join_set_id: *join_set_id,
                    child_execution_id: child_execution_id.clone(),
                },
                EventHistoryKey::JoinNextChild {
                    join_set_id: *join_set_id,
                    kind: JoinNextKind::DirectCall,
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
                    execution_id: execution_id.clone(),
                }]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::event_history::{EventCall, EventHistory};
    use super::super::workflow_worker::JoinNextBlockingStrategy;
    use crate::host_exports::execution_id_into_wast_val;
    use crate::tests::fn_registry_dummy;
    use crate::workflow::event_history::ApplyError;
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::JoinSetId;
    use concepts::storage::{CreateRequest, DbPool};
    use concepts::storage::{DbConnection, JoinSetResponse, JoinSetResponseEvent, Version};
    use concepts::{
        ComponentId, ExecutionId, FunctionFqn, FunctionRegistry, Params,
        SupportedFunctionReturnValue,
    };
    use db_tests::Database;
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
        let exec_log = db_connection.get(&execution_id).await.unwrap();
        let event_history = EventHistory::new(
            execution_id.clone(),
            exec_log.event_history().collect(),
            exec_log
                .responses
                .into_iter()
                .map(|event| event.event)
                .collect(),
            join_next_blocking_strategy,
            execution_deadline,
            non_blocking_event_batching,
            clock_fn,
            Arc::new(std::sync::Mutex::new(None)),
            info_span!("worker-test"),
        );
        (event_history, exec_log.next_version)
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
                execution_id: execution_id.clone(),
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let (event_history, version) = load_event_history(
            &db_connection,
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
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
                let child_execution_id = child_execution_id.clone();
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
                                child_execution_id: child_execution_id.clone(),
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
                            EventCall::BlockingChildAwaitNext {
                                join_set_id,
                                closing: false,
                            },
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
            ApplyError::InterruptRequested,
            "should have ended with an interrupt"
        );
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id.clone(),
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
            sim_clock.clone(),
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
            SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {
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
                execution_id: execution_id.clone(),
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
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
            child_execution_id.clone(),
            fn_registry.as_ref(),
        )
        .await;

        // append child response before issuing join_next
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id.clone(),
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
            sim_clock.clone(),
            join_next_blocking_strategy,
            batching,
        )
        .await;

        start_async(
            &mut event_history,
            &mut version,
            &db_pool,
            join_set_id,
            child_execution_id.clone(),
            fn_registry.as_ref(),
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                },
                &db_pool.connection(),
                &mut version,
                fn_registry.as_ref(),
            )
            .await
            .unwrap();

        let child_resp_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&child_execution_id),
            WastVal::U8(1),
        ]))))));

        assert_eq!(child_resp_wrapped, res.into_wast_val());

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
            SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });
        const KID_B: SupportedFunctionReturnValue =
            SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {
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
        ) -> Result<Option<WastVal>, ApplyError> {
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
                    EventCall::BlockingChildAwaitNext {
                        join_set_id,
                        closing: false,
                    },
                    &db_pool.connection(),
                    version,
                    fn_registry,
                )
                .await
                .map(super::ChildReturnValue::into_wast_val)
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
                execution_id: execution_id.clone(),
                ffqn: MOCK_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: created_at,
                retry_exp_backoff: Duration::ZERO,
                max_retries: 0,
                component_id: ComponentId::dummy_activity(),
                scheduled_by: None,
            })
            .await
            .unwrap();

        let (mut event_history, mut version) = load_event_history(
            &db_connection,
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
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
                child_execution_id_a.clone(),
                child_execution_id_b.clone()
            )
            .await
            .unwrap_err(),
            ApplyError::InterruptRequested
        );
        // append two responses
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_a.clone(),
                        result: Ok(KID_A),
                    },
                },
            )
            .await
            .unwrap();
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id,
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_b.clone(),
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
            sim_clock.clone(),
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
            child_execution_id_a.clone(),
            child_execution_id_b.clone(),
        )
        .await
        .unwrap();
        let kid_a_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&child_execution_id_a),
            WastVal::U8(1),
        ]))))));
        assert_eq!(kid_a_wrapped, res);

        // second child result should be found
        let res = event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                },
                &db_pool.connection(),
                &mut version,
                fn_registry.as_ref(),
            )
            .await
            .unwrap();
        let kid_b_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&child_execution_id_b),
            WastVal::U8(2),
        ]))))));
        assert_eq!(kid_b_wrapped, res.into_wast_val());

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    // TODO: Check -await-next for fn without return type
    // TODO: Check execution errors translating to execution-error
}
