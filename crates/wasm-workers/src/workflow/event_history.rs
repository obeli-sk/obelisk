use super::event_history::ProcessingStatus::Processed;
use super::event_history::ProcessingStatus::Unprocessed;
use super::host_exports::delay_id_into_wast_val;
use super::host_exports::execution_id_derived_into_wast_val;
use super::host_exports::execution_id_into_wast_val;
use super::host_exports::join_set_id_into_wast_val;
use super::workflow_worker::JoinNextBlockingStrategy;
use chrono::{DateTime, Utc};
use concepts::ClosingStrategy;
use concepts::ComponentId;
use concepts::ComponentRetryConfig;
use concepts::ExecutionMetadata;
use concepts::FinishedExecutionError;
use concepts::JoinSetId;
use concepts::JoinSetKind;
use concepts::PermanentFailureKind;
use concepts::StubReturnValue;
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::storage;
use concepts::storage::BacktraceInfo;
use concepts::storage::HistoryEventScheduledAt;
use concepts::storage::JoinSetResponseEventOuter;
use concepts::storage::PersistKind;
use concepts::storage::SpecificError;
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse,
    JoinSetResponseEvent, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::time::ClockFn;
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params, SupportedFunctionReturnValue};
use indexmap::IndexMap;
use std::fmt::Debug;
use std::fmt::Display;
use strum::IntoStaticStr;
use tracing::Level;
use tracing::Span;
use tracing::info;
use tracing::instrument;
use tracing::{debug, error, trace};
use val_json::wast_val::WastVal;

#[derive(Debug)]
pub(crate) enum ChildReturnValue {
    None,
    WastVal(WastVal),
    HostResource(HostResource),
}

#[derive(Debug)]
pub(crate) enum HostResource {
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
            Self::HostResource(HostResource::CreateJoinSetResp(join_set_id)) => {
                Some(join_set_id_into_wast_val(&join_set_id))
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
    NondeterminismDetected(String),
    // Fatal unless when closing a join set
    UnhandledChildExecutionError {
        child_execution_id: ExecutionIdDerived,
        root_cause_id: ExecutionIdDerived,
    },
    // retriable errors:
    InterruptRequested,
    DbError(DbError),
}

#[expect(clippy::struct_field_names)]
pub(crate) struct EventHistory<C: ClockFn> {
    execution_id: ExecutionId,
    component_id: ComponentId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    execution_deadline: DateTime<Utc>,
    event_history: Vec<(HistoryEvent, ProcessingStatus)>,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    non_blocking_event_batch_size: usize,
    non_blocking_event_batch: Option<Vec<NonBlockingCache>>,
    clock_fn: C,
    worker_span: Span,
    forward_unhandled_child_errors_in_join_set_close: bool,
    // TODO: optimize using start_from_idx: usize,
}

#[expect(clippy::large_enum_variant)]
enum NonBlockingCache {
    StartAsync {
        batch: Vec<AppendRequest>,
        version: Version,
        child_req: CreateRequest,
    },
    WasmBacktrace {
        append_backtrace: BacktraceInfo,
    },
}

#[derive(Debug)]
enum FindMatchingResponse {
    Found(ChildReturnValue),
    NotFound,
    FoundRequestButNotResponse,
}

impl<C: ClockFn> EventHistory<C> {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        component_id: ComponentId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        worker_span: Span,
        forward_unhandled_child_errors_in_join_set_close: bool,
    ) -> Self {
        let non_blocking_event_batch_size = match join_next_blocking_strategy {
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
            } => non_blocking_event_batching as usize,
            JoinNextBlockingStrategy::Interrupt => 0,
        };
        EventHistory {
            execution_id,
            component_id,
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
            worker_span,
            forward_unhandled_child_errors_in_join_set_close,
        }
    }

    pub(crate) fn join_set_name_exists(&self, join_set_name: &str, kind: JoinSetKind) -> bool {
        // TODO: optimize
        self.event_history.iter().any(|(event, processing_status)|
            // Do not look into the future as it would break replay.
            *processing_status == ProcessingStatus::Processed &&
            matches!(event, HistoryEvent::JoinSetCreate { join_set_id: found, .. }
                if found.name.as_ref() == join_set_name && found.kind == kind))
    }

    pub(crate) fn join_set_count(&self, kind: JoinSetKind) -> usize {
        // TODO: optimize
        self.event_history
            .iter()
            .filter(|(event, processing_status)| {
                // Do not look into the future as it would break replay.
                *processing_status == ProcessingStatus::Processed
                    && matches!(
                        event,
                        HistoryEvent::JoinSetCreate {
                            join_set_id: JoinSetId {
                                kind: found_kind,
                                ..
                            },
                            ..
                        }
                        if *found_kind == kind
                    )
            })
            .count()
    }

    pub(crate) fn execution_count(&self, join_set_id: &JoinSetId) -> usize {
        // TODO: optimize
        self.event_history
            .iter()
            .filter(|(event, processing_status)| {
                // Do not look into the future as it would break replay.
                *processing_status == ProcessingStatus::Processed
                    && matches!(
                        event,
                        HistoryEvent::JoinSetRequest {
                            join_set_id: found,
                            request: JoinSetRequest::ChildExecutionRequest { .. }
                        }
                        if found == join_set_id
                    )
            })
            .count()
    }

    /// Apply the event and wait if new, replay if already in the event history, or
    /// apply with an interrupt.
    #[instrument(skip_all, fields(otel.name = format!("apply {event_call}"), ?event_call))]
    pub(crate) async fn apply(
        &mut self,
        event_call: EventCall,
        db_connection: &dyn DbConnection,
        version: &mut Version,
    ) -> Result<ChildReturnValue, ApplyError> {
        trace!("apply({event_call:?})");
        if let Some(resp) = self.find_matching_atomic(&event_call)? {
            trace!("found_atomic: {resp:?}");
            return Ok(resp);
        }

        let called_at = self.clock_fn.now();
        let lock_expires_at =
            if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                called_at
            } else {
                self.execution_deadline
            };
        let poll_variant = match event_call.poll_variant() {
            None => {
                // Events that cannot block, e.g. creating new join sets.
                // TODO: Add speculative batching (avoid writing non-blocking responses immediately) to improve performance
                let cloned_non_blocking = event_call.clone();
                let history_events = self
                    .append_to_db(
                        event_call,
                        db_connection,
                        called_at,
                        lock_expires_at,
                        version,
                    )
                    .await
                    .map_err(ApplyError::DbError)?;
                self.event_history
                    .extend(history_events.into_iter().map(|event| (event, Unprocessed)));
                trace!("find_matching_atomic must mark the non-blocking event as Processed");
                let non_blocking_resp = self
                    .find_matching_atomic(&cloned_non_blocking)?
                    .expect("just stored the event as Unprocessed, it must be found");
                return Ok(non_blocking_resp);
            }
            Some(poll_variant) => poll_variant,
        };

        let keys = event_call.as_keys();
        // Persist
        let history_events = self
            .append_to_db(
                event_call,
                db_connection,
                called_at,
                lock_expires_at,
                version,
            )
            .await
            .map_err(ApplyError::DbError)?;
        self.event_history
            .extend(history_events.into_iter().map(|event| (event, Unprocessed)));

        let last_key_idx = keys.len() - 1;
        for (idx, key) in keys.into_iter().enumerate() {
            let res = self.process_event_by_key(&key)?;
            if idx == last_key_idx {
                if let FindMatchingResponse::Found(res) = res {
                    // Last key was replayed.
                    return Ok(res);
                }
            }
        }
        // Now either wait or interrupt.
        // TODO: perf: if start_from_index was at top, it should move forward to n - 1

        if matches!(
            self.join_next_blocking_strategy,
            JoinNextBlockingStrategy::Await { .. }
        ) {
            // JoinNext was written, wait for next response.
            debug!(join_set_id = %poll_variant.join_set_id(), "Waiting for {poll_variant:?}");
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
                if let FindMatchingResponse::Found(accept_resp) = self.process_event_by_key(&key)? {
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
    /// than corresponding awaits.
    /// For each open join set deterministically emit `EventCall::BlockingChildAwaitNext` and wait for the response.
    /// MUST NOT add items to `NonBlockingCache`, as only `EventCall::BlockingChildAwaitNext` and their
    /// processed responses are appended.
    #[instrument(skip_all)]
    pub(crate) async fn close_opened_join_sets(
        &mut self,
        db_connection: &dyn DbConnection,
        version: &mut Version,
    ) -> Result<(), ApplyError> {
        // We want to end with the same actions when called again.
        // Count the iteractions not counting the delay requests and closing JoinNext-s.
        // Every counted JoinNext must have been awaited at this point.
        let mut join_set_to_child_created_and_awaited = IndexMap::new(); // Must be deterministic.
        let delay_join_sets: hashbrown::HashSet<_> = self // Does not have to be deterministic.
            .event_history
            .iter()
            .filter_map(|(event, _processing_sattus)| {
                if let HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::DelayRequest { .. },
                } = event
                {
                    Some(join_set_id.clone())
                } else {
                    None
                }
            })
            .collect();
        for (event, _processing_sattus) in &self.event_history {
            match event {
                HistoryEvent::JoinSetCreate { join_set_id, .. }
                    if !delay_join_sets.contains(join_set_id) =>
                {
                    let old =
                        join_set_to_child_created_and_awaited.insert(join_set_id.clone(), (0, 0));
                    assert!(old.is_none());
                }
                HistoryEvent::JoinSetRequest {
                    join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { .. },
                } if !delay_join_sets.contains(join_set_id) => {
                    let (req_count, _) = join_set_to_child_created_and_awaited
                        .get_mut(join_set_id)
                        .expect("join set must have been created");
                    *req_count += 1;
                }
                HistoryEvent::JoinNext {
                    join_set_id,
                    closing: false,
                    ..
                } if !delay_join_sets.contains(join_set_id) => {
                    let (_, await_count) = join_set_to_child_created_and_awaited
                        .get_mut(join_set_id)
                        .expect("join set must have been created");
                    *await_count += 1;
                }
                _ => {}
            }
        }
        let mut first_unhandled_child_execution_error = None;
        for (join_set_id, remaining) in join_set_to_child_created_and_awaited.iter().filter_map(
            |(join_set, (created, awaited))| {
                let remaining = *created - *awaited;
                if remaining > 0 {
                    Some((join_set, remaining))
                } else {
                    None
                }
            },
        ) {
            for _ in 0..remaining {
                debug!("Adding BlockingChildAwaitNext to join set {join_set_id}");
                match self
                    .apply(
                        EventCall::BlockingChildAwaitNext {
                            join_set_id: join_set_id.clone(),
                            closing: true,
                            wasm_backtrace: None, // TODO: Investigate capturing backtrace on execution end.
                        },
                        db_connection,
                        version,
                    )
                    .await
                {
                    Ok(_) => {
                        // continue
                    }
                    Err(ApplyError::UnhandledChildExecutionError {
                        child_execution_id,
                        root_cause_id,
                    }) => {
                        if first_unhandled_child_execution_error.is_none() {
                            first_unhandled_child_execution_error =
                                Some((child_execution_id, root_cause_id));
                        }
                    }
                    Err(
                        apply_err @ (ApplyError::NondeterminismDetected(_)
                        | ApplyError::DbError(_)
                        | ApplyError::InterruptRequested),
                    ) => return Err(apply_err),
                }
            }
        }
        match first_unhandled_child_execution_error {
            Some((child_execution_id, root_cause_id))
                if self.forward_unhandled_child_errors_in_join_set_close =>
            {
                Err(ApplyError::UnhandledChildExecutionError {
                    child_execution_id,
                    root_cause_id,
                })
            }
            _ => Ok(()),
        }
    }

    // Return ChildReturnValue if response is found
    fn find_matching_atomic(
        &mut self,
        event_call: &EventCall,
    ) -> Result<Option<ChildReturnValue>, ApplyError> {
        let keys = event_call.as_keys();
        assert!(!keys.is_empty());
        let last_key_idx = keys.len() - 1;
        for (idx, key) in keys.into_iter().enumerate() {
            let resp = self.process_event_by_key(&key)?;
            match resp {
                FindMatchingResponse::NotFound => {
                    assert_eq!(idx, 0, "NotFound must be returned on the first key");
                    return Ok(None);
                }
                FindMatchingResponse::FoundRequestButNotResponse => {
                    unreachable!(
                        "FoundRequestButNotResponse in find_matching_atomic - {event_call:?}"
                    );
                }
                FindMatchingResponse::Found(found) => {
                    if idx == last_key_idx {
                        return Ok(Some(found));
                    }
                }
            }
        }
        unreachable!()
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
        parent_event_idx: usize, // needs to be marked as Processed as well if found
        join_set_id: &JoinSetId,
    ) -> Option<&JoinSetResponseEvent> {
        trace!(
            "mark_next_unprocessed_response responses: {:?}",
            self.responses
        );
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
                ) if found == join_set_id => Some(idx),
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
    fn process_event_by_key(
        &mut self,
        key: &EventHistoryKey,
    ) -> Result<FindMatchingResponse, ApplyError> {
        let Some((found_idx, (found_request_event, _))) = self.next_unprocessed_request() else {
            return Ok(FindMatchingResponse::NotFound);
        };
        trace!("Finding match for {key:?}, [{found_idx}] {found_request_event:?}");
        match (key, found_request_event) {
            (
                EventHistoryKey::CreateJoinSet {
                    join_set_id,
                    closing_strategy,
                },
                HistoryEvent::JoinSetCreate {
                    join_set_id: found_join_set_id,
                    closing_strategy: found_closing_strategy,
                },
            ) if *join_set_id == *found_join_set_id
                && closing_strategy == found_closing_strategy =>
            {
                trace!(%join_set_id, "Matched JoinSet");
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::CreateJoinSet`] , return join set id
                Ok(FindMatchingResponse::Found(ChildReturnValue::HostResource(
                    HostResource::CreateJoinSetResp(join_set_id.clone()),
                )))
            }

            (
                EventHistoryKey::Persist { value, kind },
                HistoryEvent::Persist {
                    value: found_value,
                    kind: found_kind,
                },
            ) if *value == *found_value && *kind == *found_kind => {
                trace!("Matched Persist");
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::CreateJoinSet`] , return join set id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    match kind {
                        PersistKind::RandomString { .. } => {
                            WastVal::String(String::from_utf8(value.clone()).map_err(|err| {
                                ApplyError::DbError(DbError::Specific(
                                    SpecificError::ConsistencyError(StrVariant::from(format!(
                                        "string must be UTF-8 - {err:?}"
                                    ))),
                                ))
                            })?)
                        }
                        PersistKind::RandomU64 { .. } => {
                            if value.len() != 8 {
                                return Err(ApplyError::DbError(DbError::Specific(
                                    SpecificError::ConsistencyError(StrVariant::Static(
                                        "value cannot be deserialized to u64",
                                    )),
                                )));
                            }
                            let value: [u8; 8] = value[..8].try_into().expect("size checked above");
                            let value = storage::from_bytes_to_u64(value);
                            WastVal::U64(value)
                        }
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
            ) if *join_set_id == *found_join_set_id && *execution_id == *child_execution_id => {
                trace!(%child_execution_id, %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::StartAsync`] , return execution id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    execution_id_derived_into_wast_val(execution_id),
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
            ) if *join_set_id == *found_join_set_id && *delay_id == *found_delay_id => {
                trace!(%delay_id, %join_set_id, "Matched JoinSetRequest::DelayRequest");
                self.event_history[found_idx].1 = Processed;
                // return delay id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    delay_id_into_wast_val(*delay_id),
                )))
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
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
                            JoinSetResponse::ChildExecutionFinished {
                                child_execution_id,
                                finished_version: _, // Will be needed when lazy(optional) result is implemented
                                result,
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & ChildExecutionFinished");
                        match kind {
                            JoinNextKind::DirectCall => match result {
                                Ok(result) => Ok(FindMatchingResponse::Found(
                                    ChildReturnValue::from_wast_val_or_none(
                                        result.clone().into_value(),
                                    ),
                                )),
                                // Copy root cause of UnhandledChildExecutionError
                                Err(FinishedExecutionError::UnhandledChildExecutionError {
                                    child_execution_id: _,
                                    root_cause_id,
                                }) => {
                                    error!(%child_execution_id,
                                            "Child execution finished with an execution error");
                                    Err(ApplyError::UnhandledChildExecutionError {
                                        child_execution_id: child_execution_id.clone(),
                                        root_cause_id: root_cause_id.clone(), // Copy the original root cause
                                    })
                                }
                                // All other FinishedExecutionErrors are unhandled with current child being the root cause
                                Err(_) => {
                                    error!(%child_execution_id,
                                            "Child execution finished with an execution error");
                                    Err(ApplyError::UnhandledChildExecutionError {
                                        child_execution_id: child_execution_id.clone(),
                                        // The child is the root cause
                                        root_cause_id: child_execution_id.clone(),
                                    })
                                }
                            },
                            JoinNextKind::AwaitNext => {
                                match result {
                                    Ok(SupportedFunctionReturnValue::None) => {
                                        // result<execution-id, tuple<execution-id, execution-error>>
                                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                            WastVal::Result(Ok(Some(Box::new(
                                                execution_id_derived_into_wast_val(
                                                    child_execution_id,
                                                ),
                                            )))),
                                        )))
                                    }
                                    Ok(
                                        SupportedFunctionReturnValue::InfallibleOrResultOk(v)
                                        | SupportedFunctionReturnValue::FallibleResultErr(v),
                                    ) => {
                                        // result<(execution-id, inner>, tuple<execution-id, execution-error>>
                                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                            WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(
                                                vec![
                                                    execution_id_derived_into_wast_val(
                                                        child_execution_id,
                                                    ),
                                                    v.value.clone(),
                                                ],
                                            ))))),
                                        )))
                                    }
                                    // Transform timeout to WastVal
                                    Err(FinishedExecutionError::PermanentTimeout) => {
                                        let variant =
                                            WastVal::Variant("permanent-timeout".to_string(), None);
                                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                            WastVal::Result(Err(Some(Box::new(WastVal::Tuple(
                                                vec![
                                                    execution_id_derived_into_wast_val(
                                                        child_execution_id,
                                                    ),
                                                    variant,
                                                ],
                                            ))))),
                                        )))
                                    }
                                    // Transform activity trap to WastVal
                                    Err(FinishedExecutionError::PermanentFailure {
                                        reason_inner,
                                        kind: PermanentFailureKind::ActivityTrap,
                                        ..
                                    }) => {
                                        let variant = WastVal::Variant(
                                            "activity-trap".to_string(),
                                            Some(Box::new(WastVal::String(
                                                reason_inner.to_string(),
                                            ))),
                                        );
                                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                            WastVal::Result(Err(Some(Box::new(WastVal::Tuple(
                                                vec![
                                                    execution_id_derived_into_wast_val(
                                                        child_execution_id,
                                                    ),
                                                    variant,
                                                ],
                                            ))))),
                                        )))
                                    }
                                    // Copy root cause of UnhandledChildExecutionError
                                    Err(FinishedExecutionError::UnhandledChildExecutionError {
                                        child_execution_id: _,
                                        root_cause_id,
                                    }) => {
                                        error!(%child_execution_id,
                                                "Child execution finished with an execution error");
                                        Err(ApplyError::UnhandledChildExecutionError {
                                            child_execution_id: child_execution_id.clone(),
                                            root_cause_id: root_cause_id.clone(), // Copy the original root cause
                                        })
                                    }
                                    // All other FinishedExecutionErrors are unhandled with current child being the root cause
                                    Err(_) => {
                                        error!(%child_execution_id,
                                                "Child execution finished with an execution error");
                                        Err(ApplyError::UnhandledChildExecutionError {
                                            child_execution_id: child_execution_id.clone(),
                                            // The child is the root cause
                                            root_cause_id: child_execution_id.clone(),
                                        })
                                    }
                                }
                            }
                        }
                    }
                    None => Ok(FindMatchingResponse::FoundRequestButNotResponse), // no progress, still at JoinNext
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
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEvent {
                        event:
                            JoinSetResponse::DelayFinished {
                                delay_id: _, // Currently only a single blocking delay is supported, no need to match the id
                            },
                        ..
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & DelayFinished");
                        Ok(FindMatchingResponse::Found(ChildReturnValue::None))
                    }
                    _ => Ok(FindMatchingResponse::FoundRequestButNotResponse), // no progress, still at JoinNext
                }
            }

            (
                EventHistoryKey::Schedule {
                    target_execution_id,
                },
                HistoryEvent::Schedule {
                    execution_id: found_execution_id,
                    ..
                },
            ) if *target_execution_id == *found_execution_id => {
                trace!(%target_execution_id, "Matched Schedule");
                self.event_history[found_idx].1 = Processed;
                // return execution id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    execution_id_into_wast_val(target_execution_id),
                )))
            }

            (
                EventHistoryKey::Stub {
                    target_execution_id,
                    return_value,
                },
                HistoryEvent::Stub {
                    target_execution_id: found_execution_id,
                    return_value: found_return_value,
                    target_result,
                },
            ) if target_execution_id == found_execution_id
                && return_value == found_return_value =>
            {
                trace!(%target_execution_id, "Matched Stub");
                let ret = match target_result {
                    Ok(()) => Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                        WastVal::Result(Ok(None)),
                    ))),
                    Err(()) => Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                        WastVal::Result(Err(Some(Box::new(WastVal::Variant(
                            "conflict".to_string(),
                            None,
                        ))))),
                    ))),
                };
                self.event_history[found_idx].1 = Processed;
                ret
            }

            (key, found) => Err(ApplyError::NondeterminismDetected(format!(
                "key {key:?} not matching {found:?} stored at index {found_idx}",
            ))),
        }
    }

    pub(crate) async fn flush(&mut self, db_connection: &dyn DbConnection) -> Result<(), DbError> {
        self.flush_non_blocking_event_cache(db_connection, self.clock_fn.now())
            .await
    }

    async fn flush_non_blocking_event_cache_if_full(
        &mut self,
        db_connection: &dyn DbConnection,
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
    async fn flush_non_blocking_event_cache(
        &mut self,
        db_connection: &dyn DbConnection,
        current_time: DateTime<Utc>,
    ) -> Result<(), DbError> {
        match &mut self.non_blocking_event_batch {
            Some(non_blocking_event_batch) if !non_blocking_event_batch.is_empty() => {
                let mut batches = Vec::with_capacity(non_blocking_event_batch.len());
                let mut childs = Vec::with_capacity(non_blocking_event_batch.len());
                let mut first_version = None;
                let mut wasm_backtraces = Vec::with_capacity(non_blocking_event_batch.len());
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
                        NonBlockingCache::WasmBacktrace { append_backtrace } => {
                            wasm_backtraces.push(append_backtrace);
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
                if let Err(err) = db_connection.append_backtrace_batch(wasm_backtraces).await {
                    debug!("Ignoring error while appending backtrace: {err:?}");
                }
            }
            _ => {}
        }
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, fields(%version))]
    async fn append_to_db(
        &mut self,
        event_call: EventCall,
        db_connection: &dyn DbConnection,
        called_at: DateTime<Utc>,
        lock_expires_at: DateTime<Utc>,
        version: &mut Version,
    ) -> Result<Vec<HistoryEvent>, DbError> {
        // NB: Flush the cache before writing to the DB.
        trace!(%version, "append_to_db");
        match event_call {
            EventCall::CreateJoinSet {
                join_set_id,
                closing_strategy,
                wasm_backtrace,
            } => {
                // a non-cacheable event: Flush the cache, write the event and persist_backtrace_blocking
                debug!(%join_set_id, "CreateJoinSet: Creating new JoinSet");
                let event = HistoryEvent::JoinSetCreate {
                    join_set_id,
                    closing_strategy,
                };
                let history_events = vec![event.clone()];
                let join_set = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                *version = {
                    let next_version = db_connection
                        .append(self.execution_id.clone(), version.clone(), join_set)
                        .await?;

                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;
                    next_version
                };
                Ok(history_events)
            }

            EventCall::Persist {
                value,
                kind,
                wasm_backtrace,
            } => {
                // Non-cacheable event.
                let event = HistoryEvent::Persist { value, kind };
                let history_events = vec![event.clone()];
                let join_set = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                *version = {
                    let next_version = db_connection
                        .append(self.execution_id.clone(), version.clone(), join_set)
                        .await?;
                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;
                    next_version
                };
                Ok(history_events)
            }

            EventCall::StartAsync {
                ffqn,
                fn_component_id,
                fn_retry_config,
                join_set_id,
                child_execution_id,
                params,
                wasm_backtrace,
            } => {
                // Cacheable event.
                debug!(%child_execution_id, %join_set_id, "StartAsync: appending ChildExecutionRequest");
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
                    request: JoinSetRequest::ChildExecutionRequest {
                        child_execution_id: child_execution_id.clone(),
                    },
                };
                let history_events = vec![event.clone()];
                let child_exec_req = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: ExecutionId::Derived(child_execution_id),
                    ffqn,
                    params,
                    parent: Some((self.execution_id.clone(), join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: fn_retry_config.retry_exp_backoff,
                    max_retries: fn_retry_config.max_retries,
                    component_id: fn_component_id,
                    scheduled_by: None,
                };
                *version =
                    if let Some(non_blocking_event_batch) = &mut self.non_blocking_event_batch {
                        non_blocking_event_batch.push(NonBlockingCache::StartAsync {
                            batch: vec![child_exec_req],
                            version: version.clone(),
                            child_req,
                        });
                        let next_version = Version::new(version.0 + 1);
                        if let Some(wasm_backtrace) = wasm_backtrace {
                            non_blocking_event_batch.push(NonBlockingCache::WasmBacktrace {
                                append_backtrace: BacktraceInfo {
                                    execution_id: self.execution_id.clone(),
                                    component_id: self.component_id.clone(),
                                    wasm_backtrace,
                                    version_min_including: version.clone(),
                                    version_max_excluding: next_version.clone(),
                                },
                            });
                        }
                        self.flush_non_blocking_event_cache_if_full(db_connection, called_at)
                            .await?;
                        next_version
                    } else {
                        let next_version = db_connection
                            .append_batch_create_new_execution(
                                called_at,
                                vec![child_exec_req],
                                self.execution_id.clone(),
                                version.clone(),
                                vec![child_req],
                            )
                            .await?;
                        if let Some(wasm_backtrace) = wasm_backtrace {
                            if let Err(err) = db_connection
                                .append_backtrace(BacktraceInfo {
                                    execution_id: self.execution_id.clone(),
                                    component_id: self.component_id.clone(),
                                    version_min_including: version.clone(),
                                    version_max_excluding: next_version.clone(),
                                    wasm_backtrace,
                                })
                                .await
                            {
                                debug!("Ignoring error while appending backtrace: {err:?}");
                            }
                        }
                        next_version
                    };
                Ok(history_events)
            }

            EventCall::ScheduleRequest {
                scheduled_at,
                execution_id: new_execution_id,
                ffqn,
                fn_component_id,
                fn_retry_config,
                params,
                wasm_backtrace,
            } => {
                // Cacheable event.
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
                let child_req = CreateRequest {
                    created_at: called_at,
                    execution_id: new_execution_id,
                    metadata: ExecutionMetadata::from_linked_span(&self.worker_span),
                    ffqn,
                    params,
                    parent: None, // Schedule breaks from the parent-child relationship to avoid a linked list
                    scheduled_at,
                    retry_exp_backoff: fn_retry_config.retry_exp_backoff,
                    max_retries: fn_retry_config.max_retries,
                    component_id: fn_component_id,
                    scheduled_by: Some(self.execution_id.clone()),
                };
                *version =
                    if let Some(non_blocking_event_batch) = &mut self.non_blocking_event_batch {
                        non_blocking_event_batch.push(NonBlockingCache::StartAsync {
                            batch: vec![child_exec_req],
                            version: version.clone(),
                            child_req,
                        });
                        let next_version = Version::new(version.0 + 1);
                        if let Some(wasm_backtrace) = wasm_backtrace {
                            non_blocking_event_batch.push(NonBlockingCache::WasmBacktrace {
                                append_backtrace: BacktraceInfo {
                                    execution_id: self.execution_id.clone(),
                                    component_id: self.component_id.clone(),
                                    wasm_backtrace,
                                    version_min_including: version.clone(),
                                    version_max_excluding: next_version.clone(),
                                },
                            });
                        }
                        self.flush_non_blocking_event_cache_if_full(db_connection, called_at)
                            .await?;
                        next_version
                    } else {
                        let next_version = db_connection
                            .append_batch_create_new_execution(
                                called_at,
                                vec![child_exec_req],
                                self.execution_id.clone(),
                                version.clone(),
                                vec![child_req],
                            )
                            .await?;
                        if let Some(wasm_backtrace) = wasm_backtrace {
                            if let Err(err) = db_connection
                                .append_backtrace(BacktraceInfo {
                                    execution_id: self.execution_id.clone(),
                                    component_id: self.component_id.clone(),
                                    version_min_including: version.clone(),
                                    version_max_excluding: next_version.clone(),
                                    wasm_backtrace,
                                })
                                .await
                            {
                                debug!("Ignoring error while appending backtrace: {err:?}");
                            }
                        }
                        next_version
                    };
                Ok(history_events)
            }

            EventCall::BlockingChildAwaitNext {
                join_set_id,
                closing,
                wasm_backtrace,
            } => {
                // Non-cacheable event.
                debug!(%join_set_id, "BlockingChildJoinNext: Flushing and appending JoinNext");
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
                *version = {
                    let next_version = db_connection
                        .append(self.execution_id.clone(), version.clone(), join_next)
                        .await?;
                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;
                    next_version
                };
                Ok(history_events)
            }

            EventCall::BlockingChildDirectCall {
                ffqn,
                fn_component_id,
                fn_retry_config,
                join_set_id,
                child_execution_id,
                params,
                wasm_backtrace,
            } => {
                // Non-cacheable event.
                debug!(%child_execution_id, %join_set_id, "BlockingChildExecutionRequest: Flushing and appending JoinSet,ChildExecutionRequest,JoinNext");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
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
                    join_set_id: join_set_id.clone(),
                    run_expires_at: lock_expires_at,
                    closing: false,
                };
                history_events.push(event.clone());
                let join_next = AppendRequest {
                    event: ExecutionEventInner::HistoryEvent { event },
                    created_at: called_at,
                };

                let child = CreateRequest {
                    created_at: called_at,
                    execution_id: ExecutionId::Derived(child_execution_id),
                    ffqn,
                    params,
                    parent: Some((self.execution_id.clone(), join_set_id)),
                    metadata: ExecutionMetadata::from_parent_span(&self.worker_span),
                    scheduled_at: called_at,
                    retry_exp_backoff: fn_retry_config.retry_exp_backoff,
                    max_retries: fn_retry_config.max_retries,
                    component_id: fn_component_id,
                    scheduled_by: None,
                };
                *version = {
                    let next_version = db_connection
                        .append_batch_create_new_execution(
                            called_at,
                            vec![join_set, child_exec_req, join_next],
                            self.execution_id.clone(),
                            version.clone(),
                            vec![child],
                        )
                        .await?;
                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;
                    next_version
                };

                Ok(history_events)
            }

            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                expires_at_if_new,
                wasm_backtrace,
            } => {
                // Non-cacheable event.
                debug!(%delay_id, %join_set_id, "BlockingDelayRequest: Flushing and appending JoinSet,DelayRequest,JoinNext");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let mut history_events = Vec::with_capacity(3);
                let event = HistoryEvent::JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                };
                history_events.push(event.clone());
                let join_set = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                let event = HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
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

                *version = {
                    let next_version = db_connection
                        .append_batch(
                            called_at,
                            vec![join_set, delay_req, join_next],
                            self.execution_id.clone(),
                            version.clone(),
                        )
                        .await?;
                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;
                    next_version
                };
                Ok(history_events)
            }

            EventCall::Stub {
                target_ffqn,
                target_execution_id,
                parent_id,
                join_set_id,
                return_value,
                wasm_backtrace,
            } => {
                // Attempt to write to target_execution_id, will continue on conflict.
                // Non-cacheable event. (could be turned into one)
                debug!(%target_execution_id, "StubRequest: Flushing and appending");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                // Check that the execution exists and FFQN matches.
                if target_ffqn
                    != db_connection
                        .get_create_request(&ExecutionId::Derived(target_execution_id.clone()))
                        .await?
                        .ffqn
                {
                    return Err(DbError::Specific(SpecificError::ValidationFailed(
                        "ffqn mismatch".into(),
                    )));
                }
                let stub_finished_version = Version::new(1); // Stub activities have no execution log except Created event.
                // Attempt to write to target_execution_id and its parent, ignoring the possible conflict error on this tx
                let write_attempt = {
                    let finished_req = AppendRequest {
                        created_at: called_at,
                        event: ExecutionEventInner::Finished {
                            result: Ok(return_value.clone()),
                            http_client_traces: None,
                        },
                    };
                    db_connection
                        .append_batch_respond_to_parent(
                            target_execution_id.clone(),
                            called_at,
                            vec![finished_req],
                            stub_finished_version.clone(),
                            parent_id,
                            JoinSetResponseEventOuter {
                                created_at: called_at,
                                event: JoinSetResponseEvent {
                                    join_set_id,
                                    event: JoinSetResponse::ChildExecutionFinished {
                                        child_execution_id: target_execution_id.clone(),
                                        finished_version: stub_finished_version.clone(),
                                        result: Ok(return_value.clone()),
                                    },
                                },
                            },
                        )
                        .await
                };
                debug!(%target_ffqn, %target_execution_id, "Executed append_batch_respond_to_parent: {write_attempt:?}");
                // The server might crash at this point, and restart processing.
                let target_result = match write_attempt {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        // TODO: check error == conflict
                        info!(%target_ffqn, %target_execution_id,
                            "append_batch_respond_to_parent was not successful, checking execution result - {err:?}"
                        );
                        // In case of conflict, select row (target_execution_id, version:1)
                        let found = db_connection
                            .get_execution_event(
                                &ExecutionId::Derived(target_execution_id.clone()),
                                &stub_finished_version,
                            )
                            .await?; // Not found at this point should not happen, unless the previous write failed. Will be retried.
                        match found.event {
                            ExecutionEventInner::Finished {
                                result: Ok(result), ..
                            } if result == return_value => Ok(()),
                            ExecutionEventInner::Finished { .. } => {
                                info!(%target_ffqn, %target_execution_id, "Different value found in stubbed execution's finished event");
                                Err(())
                            }
                            other => {
                                info!(%target_ffqn, %target_execution_id,
                                    "Unexpected execution event at stubbed execution - {other:?}"
                                );
                                Err(())
                            }
                        }
                    }
                };

                // Second write tx: Append the HistoryEvent with target_result.
                let event = HistoryEvent::Stub {
                    target_execution_id: target_execution_id.clone(),
                    return_value,
                    target_result,
                };
                let history_events = vec![event.clone()];
                let history_event_req = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                *version = {
                    let next_version = db_connection
                        .append_batch(
                            called_at,
                            vec![history_event_req],
                            self.execution_id.clone(),
                            version.clone(),
                        )
                        .await?;
                    self.persist_backtrace_blocking(
                        db_connection,
                        version,
                        &next_version,
                        wasm_backtrace,
                    )
                    .await;

                    next_version
                };
                Ok(history_events)
            }
        }
    }

    async fn persist_backtrace_blocking(
        &mut self,
        db_connection: &dyn DbConnection,
        version: &Version,
        next_version: &Version,
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) {
        if let Some(wasm_backtrace) = wasm_backtrace {
            assert_eq!(
                self.non_blocking_event_batch
                    .as_ref()
                    .map(std::vec::Vec::len)
                    .unwrap_or_default(),
                0,
                "persist_backtrace_blocking must be called only after flushing `non_blocking_event_batch`"
            );

            if let Err(err) = db_connection
                .append_backtrace(BacktraceInfo {
                    execution_id: self.execution_id.clone(),
                    component_id: self.component_id.clone(),
                    version_min_including: version.clone(),
                    version_max_excluding: next_version.clone(),
                    wasm_backtrace,
                })
                .await
            {
                debug!("Ignoring error while appending backtrace: {err:?}");
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
    fn join_set_id(&self) -> &JoinSetId {
        match self {
            PollVariant::JoinNextChild { join_set_id, .. }
            | PollVariant::JoinNextDelay(join_set_id) => join_set_id,
        }
    }
    fn as_key(&self) -> EventHistoryKey {
        match self {
            PollVariant::JoinNextChild { join_set_id, kind } => EventHistoryKey::JoinNextChild {
                join_set_id: join_set_id.clone(),
                kind: *kind,
            },
            PollVariant::JoinNextDelay(join_set_id) => EventHistoryKey::JoinNextDelay {
                join_set_id: join_set_id.clone(),
            },
        }
    }
}

#[derive(derive_more::Debug, Clone, IntoStaticStr)]
pub(crate) enum EventCall {
    CreateJoinSet {
        join_set_id: JoinSetId,
        closing_strategy: ClosingStrategy,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    StartAsync {
        ffqn: FunctionFqn, // TODO: rename to target_ffqn
        fn_component_id: ComponentId,
        fn_retry_config: ComponentRetryConfig,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionIdDerived,
        #[debug(skip)]
        params: Params,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    ScheduleRequest {
        scheduled_at: HistoryEventScheduledAt,
        execution_id: ExecutionId,
        ffqn: FunctionFqn, // TODO: rename to target_ffqn
        fn_component_id: ComponentId,
        fn_retry_config: ComponentRetryConfig,
        #[debug(skip)]
        params: Params,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Stub {
        target_ffqn: FunctionFqn,
        target_execution_id: ExecutionIdDerived,
        parent_id: ExecutionId,
        join_set_id: JoinSetId,
        #[debug(skip)]
        return_value: SupportedFunctionReturnValue,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    BlockingChildAwaitNext {
        join_set_id: JoinSetId,
        closing: bool,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    /// combines [`Self::CreateJoinSet`] [`Self::StartAsync`] [`Self::BlockingChildJoinNext`]
    /// Direct call
    BlockingChildDirectCall {
        ffqn: FunctionFqn,
        fn_component_id: ComponentId,
        fn_retry_config: ComponentRetryConfig,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionIdDerived,
        #[debug(skip)]
        params: Params,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    BlockingDelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
        expires_at_if_new: DateTime<Utc>,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
    Persist {
        #[debug(skip)]
        value: Vec<u8>,
        kind: PersistKind,
        #[debug(skip)]
        wasm_backtrace: Option<storage::WasmBacktrace>,
    },
}

impl Display for EventCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &'static str = self.into();
        write!(f, "{s}")
    }
}

impl EventCall {
    fn poll_variant(&self) -> Option<PollVariant> {
        match &self {
            // Blocking calls can be polled for JoinSetResponse
            EventCall::BlockingChildDirectCall { join_set_id, .. } => {
                Some(PollVariant::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextKind::DirectCall,
                })
            }
            EventCall::BlockingChildAwaitNext { join_set_id, .. } => {
                Some(PollVariant::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextKind::AwaitNext,
                })
            }
            EventCall::BlockingDelayRequest { join_set_id, .. } => {
                Some(PollVariant::JoinNextDelay(join_set_id.clone()))
            }
            EventCall::CreateJoinSet { .. }
            | EventCall::StartAsync { .. }
            | EventCall::ScheduleRequest { .. }
            | EventCall::Persist { .. }
            | EventCall::Stub { .. } => None, // continue the execution event if Interrupt strategy is set
        }
    }
}

/// Stores important properties of events as they are executed or replayed.
/// Those properties are checked for equality with `HistoryEvent` before it is marked as `Processed`.
/// One `EventCall` can be represented as multiple `EventHistoryKey`-s.
/// One `EventHistoryKey` corresponds to one `HistoryEvent`.
#[derive(derive_more::Debug, Clone)]
enum EventHistoryKey {
    Persist {
        #[debug(skip)]
        value: Vec<u8>,
        kind: PersistKind,
    },
    CreateJoinSet {
        join_set_id: JoinSetId,
        closing_strategy: ClosingStrategy,
    },
    ChildExecutionRequest {
        join_set_id: JoinSetId,
        child_execution_id: ExecutionIdDerived,
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
        target_execution_id: ExecutionId,
    },
    Stub {
        target_execution_id: ExecutionIdDerived,
        return_value: StubReturnValue,
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
            EventCall::CreateJoinSet {
                join_set_id,
                closing_strategy,
                ..
            } => {
                vec![EventHistoryKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: *closing_strategy,
                }]
            }
            EventCall::Persist { value, kind, .. } => {
                vec![EventHistoryKey::Persist {
                    value: value.clone(),
                    kind: *kind,
                }]
            }
            EventCall::StartAsync {
                join_set_id,
                child_execution_id,
                ..
            } => vec![EventHistoryKey::ChildExecutionRequest {
                join_set_id: join_set_id.clone(),
                child_execution_id: child_execution_id.clone(),
            }],
            EventCall::BlockingChildAwaitNext { join_set_id, .. } => {
                vec![EventHistoryKey::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextKind::AwaitNext,
                }]
            }
            EventCall::BlockingChildDirectCall {
                join_set_id,
                child_execution_id,
                ..
            } => vec![
                EventHistoryKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::default(),
                },
                EventHistoryKey::ChildExecutionRequest {
                    join_set_id: join_set_id.clone(),
                    child_execution_id: child_execution_id.clone(),
                },
                EventHistoryKey::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextKind::DirectCall,
                },
            ],
            EventCall::BlockingDelayRequest {
                join_set_id,
                delay_id,
                ..
            } => vec![
                EventHistoryKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::default(),
                },
                EventHistoryKey::DelayRequest {
                    join_set_id: join_set_id.clone(),
                    delay_id: *delay_id,
                },
                EventHistoryKey::JoinNextDelay {
                    join_set_id: join_set_id.clone(),
                },
            ],
            EventCall::ScheduleRequest { execution_id, .. } => {
                vec![EventHistoryKey::Schedule {
                    target_execution_id: execution_id.clone(),
                }]
            }
            EventCall::Stub {
                target_execution_id,
                return_value,
                ..
            } => {
                vec![EventHistoryKey::Stub {
                    target_execution_id: target_execution_id.clone(),
                    return_value: return_value.clone(),
                }]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::event_history::{EventCall, EventHistory};
    use super::super::host_exports::execution_id_into_wast_val;
    use super::super::workflow_worker::JoinNextBlockingStrategy;
    use crate::workflow::event_history::{ApplyError, ChildReturnValue};
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::ExecutionIdDerived;
    use concepts::storage::{CreateRequest, HistoryEventScheduledAt};
    use concepts::storage::{DbConnection, JoinSetResponse, JoinSetResponseEvent, Version};
    use concepts::time::ClockFn;
    use concepts::{
        ClosingStrategy, ComponentId, ComponentRetryConfig, ExecutionId, FunctionFqn, Params,
        SupportedFunctionReturnValue,
    };
    use concepts::{JoinSetId, StrVariant};
    use db_tests::Database;
    use rstest::rstest;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::{info, info_span};
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    pub const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn");

    #[rstest]
    #[tokio::test]
    async fn regular_join_next_child(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 100})]
        second_run_strategy: JoinNextBlockingStrategy,
    ) {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();

        // Create an execution.
        let execution_id = create_execution(db_connection.as_ref(), &sim_clock).await;

        let (event_history, version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt, // first run needs to interrupt
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        assert_matches!(
            apply_create_js_start_async_await_next(
                db_connection.as_ref(),
                child_execution_id.clone(),
                event_history,
                version,
                join_set_id.clone()
            )
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
                    join_set_id: join_set_id.clone(),
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id.clone(),
                        finished_version: Version(0), // does not matter
                        result: Ok(SupportedFunctionReturnValue::None),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");
        let (event_history, version) = load_event_history(
            db_connection.as_ref(),
            execution_id,
            sim_clock.now(),
            sim_clock.clone(),
            second_run_strategy,
        )
        .await;
        apply_create_js_start_async_await_next(
            db_connection.as_ref(),
            child_execution_id,
            event_history,
            version,
            join_set_id,
        )
        .await
        .expect("should finish successfuly");

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn start_async_respond_then_join_next(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) {
        const CHILD_RESP: SupportedFunctionReturnValue =
            SupportedFunctionReturnValue::InfallibleOrResultOk(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            });
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        // Create an execution.
        let execution_id = create_execution(db_connection.as_ref(), &sim_clock).await;

        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
            join_next_blocking_strategy,
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        apply_create_js_start_async(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            child_execution_id.clone(),
        )
        .await;

        // append child response before issuing join_next
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id: join_set_id.clone(),
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id.clone(),
                        finished_version: Version(0), // does not matter
                        result: Ok(CHILD_RESP),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id,
            sim_clock.now(),
            sim_clock.clone(),
            join_next_blocking_strategy,
        )
        .await;

        apply_create_js_start_async(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            child_execution_id.clone(),
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                    wasm_backtrace: None,
                },
                db_pool.connection().as_ref(),
                &mut version,
            )
            .await
            .unwrap();

        let child_resp_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id)),
            WastVal::U8(1),
        ]))))));

        assert_eq!(child_resp_wrapped, res.into_wast_val());

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn create_two_non_blocking_childs_then_two_join_nexts(
        #[values(JoinNextBlockingStrategy::Interrupt, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 0}, JoinNextBlockingStrategy::Await { non_blocking_event_batching: 10})]
        second_run_strategy: JoinNextBlockingStrategy,
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

        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();

        // Create an execution.
        let execution_id = create_execution(db_connection.as_ref(), &sim_clock).await;

        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt, // First blocking strategy is always Interrupt
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();

        let child_execution_id_a = execution_id.next_level(&join_set_id);
        let child_execution_id_b = child_execution_id_a.get_incremented();

        assert_matches!(
            apply_create_js_two_start_asyncs_await_next(
                db_connection.as_ref(),
                &mut event_history,
                &mut version,
                join_set_id.clone(),
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
                    join_set_id: join_set_id.clone(),
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_a.clone(),
                        finished_version: Version(0), // does not matter
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
                    join_set_id: join_set_id.clone(),
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: child_execution_id_b.clone(),
                        finished_version: Version(0), // does not matter
                        result: Ok(KID_B),
                    },
                },
            )
            .await
            .unwrap();

        info!("Second run");

        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id,
            sim_clock.now(),
            sim_clock.clone(),
            second_run_strategy,
        )
        .await;

        let res = apply_create_js_two_start_asyncs_await_next(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            child_execution_id_a.clone(),
            child_execution_id_b.clone(),
        )
        .await
        .unwrap();
        let kid_a_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id_a)),
            WastVal::U8(1),
        ]))))));
        assert_eq!(kid_a_wrapped, res);

        // second child result should be found
        let res = event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                    wasm_backtrace: None,
                },
                db_pool.connection().as_ref(),
                &mut version,
            )
            .await
            .unwrap();
        let kid_b_wrapped = Some(WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id_b)),
            WastVal::U8(2),
        ]))))));
        assert_eq!(kid_b_wrapped, res.into_wast_val());

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn schedule_event_should_be_processed() {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        let db_connection = db_connection.as_ref();

        // Create an execution.
        let execution_id = create_execution(db_connection, &sim_clock).await;

        let (mut event_history, mut version) = load_event_history(
            db_connection,
            execution_id.clone(),
            sim_clock.now(),
            sim_clock.clone(),
            JoinNextBlockingStrategy::Interrupt, // does not matter, there are no blocking events
        )
        .await;

        event_history
            .apply(
                EventCall::ScheduleRequest {
                    scheduled_at: HistoryEventScheduledAt::Now,
                    execution_id: ExecutionId::generate(),
                    ffqn: MOCK_FFQN,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    params: Params::empty(),
                    wasm_backtrace: None,
                },
                db_connection,
                &mut version,
            )
            .await
            .unwrap();
        // Create a join set to force checking of "Processed" status of the schedule event.
        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        event_history
            .apply(
                EventCall::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                    wasm_backtrace: None,
                },
                db_connection,
                &mut version,
            )
            .await
            .unwrap();

        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn submit_stub_await() {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        let db_connection = db_connection.as_ref();

        let execution_id = create_execution(db_connection, &sim_clock).await;
        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        for run_id in 0..1 {
            info!("Run {run_id}");
            let (mut event_history, mut version) = load_event_history(
                db_connection,
                execution_id.clone(),
                sim_clock.now(),
                sim_clock.clone(),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 0,
                },
            )
            .await;

            apply_create_js_start_async(
                db_connection,
                &mut event_history,
                &mut version,
                join_set_id.clone(),
                child_execution_id.clone(),
            )
            .await;

            event_history
                .apply(
                    EventCall::Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        return_value: SupportedFunctionReturnValue::None,
                        wasm_backtrace: None,
                    },
                    db_connection,
                    &mut version,
                )
                .await
                .unwrap();

            let child_return_value = event_history
                .apply(
                    EventCall::BlockingChildAwaitNext {
                        join_set_id: join_set_id.clone(),
                        closing: false,
                        wasm_backtrace: None,
                    },
                    db_connection,
                    &mut version,
                )
                .await
                .unwrap();

            assert_matches!(
                child_return_value,
                ChildReturnValue::WastVal(_child_execution_id)
            );
        }

        db_pool.close().await.unwrap();
    }

    #[tokio::test]
    // Simulate idempotency of two executions setting the same value to the same execution.
    async fn submit_stub_stub_with_same_value_should_be_ok() {
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        let db_connection = db_connection.as_ref();

        let execution_id = create_execution(db_connection, &sim_clock).await;
        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        for run_id in 0..1 {
            info!("Run {run_id}");
            let (mut event_history, mut version) = load_event_history(
                db_connection,
                execution_id.clone(),
                sim_clock.now(),
                sim_clock.clone(),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 0,
                },
            )
            .await;
            apply_create_js_start_async(
                db_connection,
                &mut event_history,
                &mut version,
                join_set_id.clone(),
                child_execution_id.clone(),
            )
            .await;
            event_history
                .apply(
                    EventCall::Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        return_value: SupportedFunctionReturnValue::None,
                        wasm_backtrace: None,
                    },
                    db_connection,
                    &mut version,
                )
                .await
                .unwrap();

            event_history
                .apply(
                    EventCall::Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        return_value: SupportedFunctionReturnValue::None,
                        wasm_backtrace: None,
                    },
                    db_connection,
                    &mut version,
                )
                .await
                .unwrap();
        }

        db_pool.close().await.unwrap();
    }

    // TODO: Check -await-next for fn without return type
    // TODO: Check execution errors translating to execution-error

    // utils

    async fn create_execution(
        db_connection: &dyn DbConnection,
        sim_clock: &SimClock,
    ) -> ExecutionId {
        let created_at = sim_clock.now();
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
        execution_id
    }

    async fn load_event_history<C: ClockFn>(
        db_connection: &dyn DbConnection,
        execution_id: ExecutionId,
        execution_deadline: DateTime<Utc>,
        clock_fn: C,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
    ) -> (EventHistory<C>, Version) {
        let exec_log = db_connection.get(&execution_id).await.unwrap();
        let event_history = EventHistory::new(
            execution_id.clone(),
            ComponentId::dummy_activity(),
            exec_log.event_history().collect(),
            exec_log
                .responses
                .into_iter()
                .map(|event| event.event)
                .collect(),
            join_next_blocking_strategy,
            execution_deadline,
            clock_fn,
            info_span!("worker-test"),
            false,
        );
        (event_history, exec_log.next_version)
    }

    async fn apply_create_js_start_async_await_next(
        db_connection: &dyn DbConnection,
        child_execution_id: ExecutionIdDerived,
        mut event_history: EventHistory<SimClock>,
        mut version: Version,
        join_set_id: JoinSetId,
    ) -> Result<ChildReturnValue, ApplyError> {
        apply_create_js_start_async(
            db_connection,
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            child_execution_id,
        )
        .await;
        event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                    wasm_backtrace: None,
                },
                db_connection,
                &mut version,
            )
            .await
    }

    async fn apply_create_js_start_async(
        db_connection: &dyn DbConnection,
        event_history: &mut EventHistory<SimClock>,
        version: &mut Version,
        join_set_id: JoinSetId,
        child_execution_id: ExecutionIdDerived,
    ) {
        event_history
            .apply(
                EventCall::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                    wasm_backtrace: None,
                },
                db_connection,
                version,
            )
            .await
            .unwrap();
        event_history
            .apply(
                EventCall::StartAsync {
                    ffqn: MOCK_FFQN,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    join_set_id,
                    child_execution_id,
                    params: Params::empty(),
                    wasm_backtrace: None,
                },
                db_connection,
                version,
            )
            .await
            .unwrap();
    }

    async fn apply_create_js_two_start_asyncs_await_next(
        db_connection: &dyn DbConnection,
        event_history: &mut EventHistory<SimClock>,
        version: &mut Version,
        join_set_id: JoinSetId,
        child_execution_id_a: ExecutionIdDerived,
        child_execution_id_b: ExecutionIdDerived,
    ) -> Result<Option<WastVal>, ApplyError> {
        apply_create_js_start_async(
            db_connection,
            event_history,
            version,
            join_set_id.clone(),
            child_execution_id_a,
        )
        .await;
        event_history
            .apply(
                EventCall::StartAsync {
                    ffqn: MOCK_FFQN,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    join_set_id: join_set_id.clone(),
                    child_execution_id: child_execution_id_b,
                    params: Params::empty(),
                    wasm_backtrace: None,
                },
                db_connection,
                version,
            )
            .await
            .unwrap();
        event_history
            .apply(
                EventCall::BlockingChildAwaitNext {
                    join_set_id,
                    closing: false,
                    wasm_backtrace: None,
                },
                db_connection,
                version,
            )
            .await
            .map(super::ChildReturnValue::into_wast_val)
    }
}
