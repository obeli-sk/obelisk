use super::event_history::ProcessingStatus::Processed;
use super::event_history::ProcessingStatus::Unprocessed;
use super::host_exports::delay_id_into_wast_val;
use super::host_exports::execution_id_derived_into_wast_val;
use super::host_exports::execution_id_into_wast_val;
use super::host_exports::v3_0_0::obelisk::types::execution::GetExtensionError;
use super::host_exports::v3_0_0::obelisk::workflow::workflow_support;
use super::workflow_ctx::WorkflowFunctionError;
use super::workflow_worker::JoinNextBlockingStrategy;
use crate::workflow::host_exports::ffqn_into_wast_val;
use crate::workflow::host_exports::v3_0_0::obelisk::types::execution as types_execution;
use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::ClosingStrategy;
use concepts::ComponentId;
use concepts::ComponentRetryConfig;
use concepts::ExecutionMetadata;
use concepts::FunctionRegistry;
use concepts::InvalidNameError;
use concepts::JoinSetId;
use concepts::JoinSetKind;
use concepts::SupportedFunctionReturnValue;
use concepts::prefixed_ulid::DelayId;
use concepts::prefixed_ulid::ExecutionIdDerived;
use concepts::storage;
use concepts::storage::BacktraceInfo;
use concepts::storage::HistoryEventScheduleAt;
use concepts::storage::JoinSetResponseEventOuter;
use concepts::storage::PersistKind;
use concepts::storage::SpecificError;
use concepts::storage::SubscribeError;
use concepts::storage::{
    AppendRequest, CreateRequest, DbConnection, DbError, ExecutionEventInner, JoinSetResponse,
    JoinSetResponseEvent, Version,
};
use concepts::storage::{HistoryEvent, JoinSetRequest};
use concepts::time::SleepFactory;
use concepts::{ExecutionId, StrVariant};
use concepts::{FunctionFqn, Params};
use hashbrown::HashMap;
use hashbrown::HashSet;
use indexmap::IndexMap;
use indexmap::indexmap;
use itertools::Either;
use itertools::Itertools;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoStaticStr;
use tracing::Level;
use tracing::Span;
use tracing::info;
use tracing::instrument;
use tracing::{debug, error, trace};
use val_json::wast_val::WastVal;
use wasmtime::component::Val;

#[derive(Debug)]
pub(crate) enum ChildReturnValue {
    WastVal(WastVal),
    JoinSetCreate(JoinSetId),
    JoinNext(Result<types_execution::ResponseId, workflow_support::JoinNextError>),
    OneOffDelay { scheduled_at: DateTime<Utc> },
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum ProcessingStatus {
    Unprocessed,
    Processed,
}

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum ApplyError {
    // fatal errors:
    #[error("nondeterminism detected: `{0}`")]
    NondeterminismDetected(String),
    // retriable errors:
    #[error("interrupt, db updated")]
    InterruptDbUpdated,
    #[error(transparent)]
    DbError(DbError),
}

#[expect(clippy::struct_field_names)]
pub(crate) struct EventHistory {
    execution_id: ExecutionId,
    component_id: ComponentId,
    join_next_blocking_strategy: JoinNextBlockingStrategy,
    deadline: DateTime<Utc>,
    event_history: Vec<(HistoryEvent, ProcessingStatus)>,
    // Used for `-get`ting the processed response by Execution Id.
    index_child_exe_to_processed_response_idx: HashMap<ExecutionIdDerived, usize>,
    index_child_exe_to_ffqn: HashMap<ExecutionIdDerived, FunctionFqn>,
    // Used for closing join sets in reverse order. One-off join sets are ignored.
    index_join_set_to_created_child_requests: IndexMap<JoinSetId, usize>,
    index_delay_id_to_expires_at: IndexMap<DelayId, DateTime<Utc>>,
    // Used for closing join sets.
    close_requests: std::collections::HashMap<JoinSetId, usize>,
    closed_join_sets: HashSet<JoinSetId>,
    responses: Vec<(JoinSetResponseEvent, ProcessingStatus)>,
    non_blocking_event_batch_size: usize,
    non_blocking_event_batch: Option<Vec<NonBlockingCache>>,
    worker_span: Span,
    deadline_tracker: SleepFactory,
    fn_registry: Arc<dyn FunctionRegistry>,
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

impl EventHistory {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        execution_id: ExecutionId,
        component_id: ComponentId,
        event_history: Vec<HistoryEvent>,
        responses: Vec<JoinSetResponseEvent>,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        deadline: DateTime<Utc>,
        worker_span: Span,
        deadline_duration: Duration,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> EventHistory {
        let non_blocking_event_batch_size = match join_next_blocking_strategy {
            JoinNextBlockingStrategy::Await {
                non_blocking_event_batching,
            } => non_blocking_event_batching as usize,
            JoinNextBlockingStrategy::Interrupt => 0,
        };
        let close_requests = event_history
            .iter()
            .filter_map(|event| {
                if let HistoryEvent::JoinNext {
                    closing: true,
                    join_set_id,
                    ..
                } = event
                {
                    Some(join_set_id.clone())
                } else {
                    None
                }
            })
            .counts();
        EventHistory {
            execution_id,
            component_id,
            index_child_exe_to_processed_response_idx: HashMap::default(),
            index_child_exe_to_ffqn: HashMap::default(),
            index_join_set_to_created_child_requests: IndexMap::default(),
            index_delay_id_to_expires_at: IndexMap::default(),
            closed_join_sets: HashSet::default(),
            event_history: event_history
                .into_iter()
                .filter_map(|event| {
                    if matches!(event, HistoryEvent::JoinNext { closing: true, .. }) {
                        None // Ignore closing requests. Join sets must be closed even after nondeterminism is detected.
                    } else {
                        Some((event, Unprocessed))
                    }
                })
                .collect(),
            close_requests,
            responses: responses
                .into_iter()
                .map(|event| (event, Unprocessed))
                .collect(),
            join_next_blocking_strategy,
            deadline,
            non_blocking_event_batch_size,
            non_blocking_event_batch: if non_blocking_event_batch_size == 0 {
                None
            } else {
                Some(Vec::with_capacity(non_blocking_event_batch_size))
            },
            worker_span,
            deadline_tracker: SleepFactory::new(deadline_duration),
            fn_registry,
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

    async fn apply(
        &mut self,
        event_call: EventCall,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<ChildReturnValue, WorkflowFunctionError> {
        Ok(self
            .apply_inner(event_call, db_connection, version, called_at)
            .await?)
    }

    /// Apply the event and wait if new, replay if already in the event history, or
    /// apply with an interrupt.
    #[instrument(skip_all, fields(otel.name = format!("apply {event_call}"), ?event_call))]
    async fn apply_inner(
        &mut self,
        event_call: EventCall,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<ChildReturnValue, ApplyError> {
        debug!("applying {event_call:?}");
        if let Some(resp) = self.find_matching_atomic(&event_call)? {
            trace!("found_atomic: {resp:?}");
            return Ok(resp);
        }

        let lock_expires_at =
            if self.join_next_blocking_strategy == JoinNextBlockingStrategy::Interrupt {
                called_at
            } else {
                self.deadline
            };
        let Some(join_next_variant) = event_call.join_next_variant() else {
            // Events that cannot block, e.g. creating new join sets, persisting random value, getting processed responses.
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
        };

        let keys = event_call.as_keys();
        // Create and append HistoryEvents.
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
        assert!(
            !history_events.is_empty(),
            "each EventCall must produce at least one HistoryEvent"
        );
        // Extend event history.
        self.event_history
            .extend(history_events.into_iter().map(|event| (event, Unprocessed)));

        let last_key_idx = keys.len() - 1;
        for (idx, key) in keys.into_iter().enumerate() {
            let res = self.process_event_by_key(&key)?;
            if idx == last_key_idx
                && let FindMatchingResponse::Found(res) = res
            {
                // Last key was marked as processed.
                assert_eq!(
                    Processed,
                    self.event_history
                        .last()
                        .expect("checked that `history_events` is not empty")
                        .1
                );
                return Ok(res);
            }
        }
        // Now either wait or interrupt.
        // TODO: perf: if start_from_index was at top, it should move forward to n - 1

        if matches!(
            self.join_next_blocking_strategy,
            JoinNextBlockingStrategy::Await { .. }
        ) {
            // JoinNext was written, wait for next response.
            debug!(join_set_id = %join_next_variant.join_set_id(), "Waiting for {join_next_variant:?}");
            let key = join_next_variant.as_key();

            // Subscribe to the next response.
            while !self.deadline_tracker.deadline_reached() {
                let next_responses = match db_connection
                    .subscribe_to_next_responses(
                        &self.execution_id,
                        self.responses.len(),
                        Box::pin(self.deadline_tracker.new_sleep()),
                    )
                    .await
                {
                    Ok(ok) => ok,
                    Err(SubscribeError::DbError(err)) => return Err(ApplyError::DbError(err)),
                    Err(SubscribeError::Interrupted) => return Err(ApplyError::InterruptDbUpdated),
                };
                debug!("Got next responses {next_responses:?}");
                self.responses.extend(
                    next_responses
                        .into_iter()
                        .map(|outer| (outer.event, Unprocessed)),
                );
                trace!("All responses: {:?}", self.responses);
                if let FindMatchingResponse::Found(accept_resp) = self.process_event_by_key(&key)? {
                    debug!(join_set_id = %join_next_variant.join_set_id(), "Got result");
                    return Ok(accept_resp);
                }
            }
            Err(ApplyError::InterruptDbUpdated)
        } else {
            debug!(join_set_id = %join_next_variant.join_set_id(),  "Interrupting on {join_next_variant:?}");
            Err(ApplyError::InterruptDbUpdated)
        }
    }

    pub(crate) async fn join_set_close(
        &mut self,
        join_set_id: JoinSetId,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<(), WorkflowFunctionError> {
        self.join_set_close_inner(
            join_set_id,
            db_connection,
            version,
            called_at,
            wasm_backtrace,
        )
        .await
        .map_err(WorkflowFunctionError::from)
    }

    /// For each open join set deterministically emit `JoinNext` and wait for the response.
    async fn join_set_close_inner(
        &mut self,
        join_set_id: JoinSetId,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
    ) -> Result<(), ApplyError> {
        // Keep submitting JoinNext-s until the created child requests == processed(paired) join nexts + closing join nexts.
        // After closing a join set, it must not be possible for code to add more join nexts.

        let created_child_request_count = *self
            .index_join_set_to_created_child_requests
            .get(&join_set_id)
            .expect("must have been created in JoinSetCreate");

        // TODO: optimize search for child execution responses.
        let processed_child_response_count = self.responses.iter().filter(|(event, status)| *status == Processed &&
            matches!(event, JoinSetResponseEvent { join_set_id:found, event: JoinSetResponse::ChildExecutionFinished { .. } }
                if join_set_id == *found)).count();
        let mut close_count = self
            .close_requests
            .get(&join_set_id)
            .copied()
            .unwrap_or_default();
        debug!(%join_set_id, created_child_request_count, processed_child_response_count,  close_count, "join_set_close");
        while created_child_request_count > processed_child_response_count + close_count {
            debug!(%join_set_id, created_child_request_count, processed_child_response_count, "Flusing and adding JoinNext");
            self.apply_inner(
                EventCall::JoinNext(JoinNext {
                    join_set_id: join_set_id.clone(),
                    closing: true,
                    wasm_backtrace: wasm_backtrace.clone(),
                }),
                db_connection,
                version,
                called_at,
            )
            .await?;
            close_count += 1;
        }
        // Clean up for `last_join_set`, otherwise `last_join_set` would return the same join set.
        *self
            .index_join_set_to_created_child_requests
            .get_mut(&join_set_id)
            .expect("must have been created in JoinSetCreate") = 0;
        self.closed_join_sets.insert(join_set_id);
        Ok(())
    }

    fn last_join_set(&self) -> Option<JoinSetId> {
        self.index_join_set_to_created_child_requests
            .iter()
            .rev()
            .find_map(|(js, remaining)| {
                if *remaining > 0 {
                    Some(js.clone())
                } else {
                    None
                }
            })
    }

    #[instrument(skip_all)]
    pub(crate) async fn close_forgotten_join_sets(
        &mut self,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<(), ApplyError> {
        debug!("close_forgotten_join_sets");
        while let Some(join_set_id) = self.last_join_set() {
            self.join_set_close_inner(join_set_id, db_connection, version, called_at, None)
                .await?;
            // No action was needed, continue with next join set.
        }
        Ok(())
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

    fn first_unprocessed_request(&self) -> Option<(usize, &HistoryEvent)> {
        self.event_history
            .iter()
            .enumerate()
            .find_map(|(idx, (event, status))| {
                if *status == Unprocessed {
                    Some((idx, event))
                } else {
                    None
                }
            })
    }

    fn mark_next_unprocessed_response(
        &'_ mut self,
        parent_event_idx: usize, // needs to be marked as Processed as well if found
        join_set_id: &JoinSetId,
    ) -> Option<JoinSetResponseEnriched<'_>> {
        trace!(
            "mark_next_unprocessed_response responses: {:?}",
            self.responses
        );
        let found_resp_idx =
            self.responses
                .iter()
                .enumerate()
                .find_map(|(idx, (event, status))| {
                    if *status == Unprocessed
                        && let JoinSetResponseEvent {
                            join_set_id: found_join_set_id,
                            event: _,
                        } = event
                        && found_join_set_id == join_set_id
                    {
                        Some(idx)
                    } else {
                        None
                    }
                });
        if let Some(found_resp_idx) = found_resp_idx {
            self.event_history[parent_event_idx].1 = Processed;
            self.responses[found_resp_idx].1 = Processed;
            let enriched = {
                match &self.responses[found_resp_idx].0.event {
                    JoinSetResponse::ChildExecutionFinished {
                        child_execution_id,
                        finished_version: _,
                        result,
                    } => {
                        self.index_child_exe_to_processed_response_idx
                            .insert(child_execution_id.clone(), found_resp_idx);

                        let response_ffqn = self
                            .index_child_exe_to_ffqn
                            .get(child_execution_id)
                            .expect("if finished the index must have it");
                        JoinSetResponseEnriched::ChildExecutionFinished(ChildExecutionFinished {
                            child_execution_id,
                            result,
                            response_ffqn,
                        })
                    }
                    JoinSetResponse::DelayFinished { delay_id } => {
                        // Find matching DelayRequest to extract expires_at
                        let expires_at = *self
                            .index_delay_id_to_expires_at
                            .get(delay_id)
                            .expect("found delay-id must have been indexed");
                        JoinSetResponseEnriched::DelayFinished {
                            delay_id,
                            expires_at,
                        }
                    }
                }
            };
            Some(enriched)
        } else {
            None
        }
    }

    fn process_event_by_key(
        &mut self,
        key: &DeterministicKey,
    ) -> Result<FindMatchingResponse, ApplyError> {
        let Some((found_idx, found_request_event)) = self.first_unprocessed_request() else {
            return Ok(FindMatchingResponse::NotFound);
        };
        trace!("Finding match for {key:?}, [{found_idx}] {found_request_event:?}");
        match (key, found_request_event) {
            (
                DeterministicKey::CreateJoinSet {
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
                Ok(FindMatchingResponse::Found(
                    ChildReturnValue::JoinSetCreate(join_set_id.clone()),
                ))
            }

            (
                DeterministicKey::Persist { value, kind },
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
                DeterministicKey::ChildExecutionRequest {
                    join_set_id,
                    child_execution_id: execution_id,
                    target_ffqn,
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request: JoinSetRequest::ChildExecutionRequest { child_execution_id },
                },
            ) if *join_set_id == *found_join_set_id && *execution_id == *child_execution_id => {
                // TODO: Check params to catch non-deterministic errors.
                trace!(%child_execution_id, %join_set_id, "Matched JoinSetRequest::ChildExecutionRequest");
                self.index_child_exe_to_ffqn
                    .insert(child_execution_id.clone(), target_ffqn.clone());
                self.event_history[found_idx].1 = Processed;
                // if this is a [`EventCall::StartAsync`] , return execution id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    execution_id_derived_into_wast_val(execution_id),
                )))
            }

            (
                DeterministicKey::DelayRequest {
                    join_set_id,
                    delay_id,
                    schedule_at,
                },
                HistoryEvent::JoinSetRequest {
                    join_set_id: found_join_set_id,
                    request:
                        JoinSetRequest::DelayRequest {
                            delay_id: found_delay_id,
                            expires_at,
                            schedule_at: found_schedule_at,
                        },
                },
            ) if *join_set_id == *found_join_set_id
                && *delay_id == *found_delay_id
                && schedule_at == found_schedule_at =>
            {
                trace!(%delay_id, %join_set_id, "Matched JoinSetRequest::DelayRequest");
                self.index_delay_id_to_expires_at
                    .insert(delay_id.clone(), *expires_at);
                self.event_history[found_idx].1 = Processed;
                // return delay id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    delay_id_into_wast_val(delay_id),
                )))
            }

            (
                DeterministicKey::JoinNextChild {
                    join_set_id,
                    kind: JoinNextChildKind::AwaitNext,
                    requested_ffqn,
                },
                HistoryEvent::JoinNextTooMany {
                    join_set_id: found_join_set_id,
                    requested_ffqn: found_requested_ffqn,
                },
            ) if *join_set_id == *found_join_set_id
                && Some(requested_ffqn) == found_requested_ffqn.as_ref() =>
            {
                trace!(%join_set_id, "matched JoinNextChild with JoinNextTooMany");
                self.event_history[found_idx].1 = Processed;
                let all_processed = ExecutionErrorVariant::AllProcessed.as_wast_val();
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    WastVal::Result(Err(Some(Box::new(all_processed)))),
                )))
            }

            (
                DeterministicKey::JoinNextChild {
                    join_set_id,
                    kind,
                    requested_ffqn,
                },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    requested_ffqn: found_requested_ffqn,
                    run_expires_at: _,
                    closing: false, // JoinNextChild originates from user's code
                },
            ) if *join_set_id == *found_join_set_id
                && Some(requested_ffqn) == found_requested_ffqn.as_ref() =>
            {
                trace!(%join_set_id, "Peeked at JoinNext - Child");
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEnriched::ChildExecutionFinished(
                        ChildExecutionFinished {
                            child_execution_id,
                            result,
                            response_ffqn,
                        },
                    )) if requested_ffqn == response_ffqn => {
                        trace!(%join_set_id, "Matched JoinNext & ChildExecutionFinished");
                        match kind {
                            JoinNextChildKind::DirectCall => {
                                let response_ffqn = response_ffqn.clone();
                                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                    result.clone().into_wast_val( || self.fn_registry.get_ret_type(&response_ffqn)
                                        .expect("response_ffqn can only be exported and no-ext, thus must be returned by get_ret_type"))
                                )))
                            }
                            JoinNextChildKind::AwaitNext => {
                                // result<(execution-id, inner-res>, await-next-extension-error>
                                // both cases will end in inner-res
                                let response_ffqn = response_ffqn.clone();
                                let child_execution_id = child_execution_id.clone();
                                let inner_res = result.clone().into_wast_val( || self.fn_registry.get_ret_type(&response_ffqn)
                                    .expect("response_ffqn can only be exported and no-ext, thus must be returned by get_ret_type"));
                                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                                    WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
                                        execution_id_derived_into_wast_val(&child_execution_id),
                                        inner_res,
                                    ]))))),
                                )))
                            }
                        }
                    }
                    Some(JoinSetResponseEnriched::ChildExecutionFinished(
                        ChildExecutionFinished {
                            child_execution_id,
                            result: _,
                            response_ffqn,
                        },
                    )) => {
                        // mismatch between ffqns
                        let function_mismatch = ExecutionErrorVariant::FunctionMismatch {
                            specified_function: requested_ffqn,
                            actual_function: Some(response_ffqn),
                            actual_id: Either::Left(child_execution_id),
                        }
                        .as_wast_val();
                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                            WastVal::Result(Err(Some(Box::new(function_mismatch)))),
                        )))
                    }
                    Some(JoinSetResponseEnriched::DelayFinished {
                        delay_id,
                        expires_at: _,
                    }) => {
                        let function_mismatch = ExecutionErrorVariant::FunctionMismatch {
                            specified_function: requested_ffqn,
                            actual_function: None,
                            actual_id: Either::Right(delay_id),
                        }
                        .as_wast_val();
                        Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                            WastVal::Result(Err(Some(Box::new(function_mismatch)))),
                        )))
                    }
                    None => Ok(FindMatchingResponse::FoundRequestButNotResponse), // no progress, still at JoinNext
                }
            }

            (
                DeterministicKey::JoinNextDelay { join_set_id },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    requested_ffqn: None,
                    closing: false, // JoinNextDelay originates from user's code
                    run_expires_at: _,
                },
            ) if *join_set_id == *found_join_set_id => {
                trace!(
                    %join_set_id, "Peeked at JoinNext - Delay");
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEnriched::DelayFinished {
                        delay_id: _, // one-shot
                        expires_at: scheduled_at,
                    }) => {
                        trace!(%join_set_id, "Matched JoinNext & DelayFinished");
                        Ok(FindMatchingResponse::Found(ChildReturnValue::OneOffDelay {
                            scheduled_at,
                        }))
                    }
                    None => Ok(FindMatchingResponse::FoundRequestButNotResponse), // no progress, still at JoinNext
                    Some(JoinSetResponseEnriched::ChildExecutionFinished { .. }) => unreachable!(
                        "DeterministicKey::JoinNextDelay is emitted only on one-shot join sets"
                    ),
                }
            }

            (
                DeterministicKey::JoinNext {
                    join_set_id,
                    closing,
                },
                HistoryEvent::JoinNext {
                    join_set_id: found_join_set_id,
                    requested_ffqn: None, // closing and `join-next` are agnostic of ffqn.
                    run_expires_at: _,
                    closing: found_closing,
                },
            ) if *join_set_id == *found_join_set_id && closing == found_closing => {
                trace!(%join_set_id, "DeterministicKey::JoinNext(closing:{closing}): Peeked at JoinNext");
                match self.mark_next_unprocessed_response(found_idx, join_set_id) {
                    Some(JoinSetResponseEnriched::ChildExecutionFinished(
                        ChildExecutionFinished {
                            child_execution_id,
                            result: _,
                            response_ffqn: _,
                        },
                    )) => {
                        trace!(%join_set_id, %child_execution_id, "DeterministicKey::JoinNext: Matched ChildExecutionFinished");
                        Ok(FindMatchingResponse::Found(ChildReturnValue::JoinNext(Ok(
                            types_execution::ResponseId::ExecutionId(child_execution_id.into()),
                        ))))
                    }
                    Some(JoinSetResponseEnriched::DelayFinished {
                        delay_id,
                        expires_at: _,
                    }) => {
                        trace!(%join_set_id, %delay_id, "DeterministicKey::JoinNext: Matched DelayFinished");
                        Ok(FindMatchingResponse::Found(ChildReturnValue::JoinNext(Ok(
                            types_execution::ResponseId::DelayId(delay_id.into()),
                        ))))
                    }
                    None => Ok(FindMatchingResponse::FoundRequestButNotResponse), // no progress, still at JoinNext
                }
            }

            (
                DeterministicKey::JoinNext {
                    join_set_id,
                    closing: false, // Runtime should never issue a bogus `JoinNext`.
                },
                HistoryEvent::JoinNextTooMany {
                    join_set_id: found_join_set_id,
                    requested_ffqn: None, // `join-next` is agnostic of ffqn.
                },
            ) if *join_set_id == *found_join_set_id => {
                trace!(%join_set_id, "matched JoinNext with JoinNextTooMany");
                self.event_history[found_idx].1 = Processed;
                Ok(FindMatchingResponse::Found(ChildReturnValue::JoinNext(
                    Err(workflow_support::JoinNextError::AllProcessed),
                )))
            }

            (
                DeterministicKey::Schedule {
                    target_execution_id,
                    schedule_at,
                },
                HistoryEvent::Schedule {
                    execution_id: found_execution_id,
                    schedule_at: found_schedule_at,
                    ..
                },
            ) if *target_execution_id == *found_execution_id
                && schedule_at == found_schedule_at =>
            {
                trace!(%target_execution_id, "Matched Schedule");
                self.event_history[found_idx].1 = Processed;
                // return execution id
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    execution_id_into_wast_val(target_execution_id),
                )))
            }

            (
                DeterministicKey::Stub {
                    target_execution_id,
                    return_value,
                },
                HistoryEvent::Stub {
                    target_execution_id: found_execution_id,
                    result: found_result,
                    persist_result: target_result,
                },
            ) if target_execution_id == found_execution_id && return_value == found_result => {
                trace!(%target_execution_id, "Matched Stub");
                let wat_val = match target_result {
                    Ok(()) => WastVal::Result(Ok(None)),
                    Err(()) => WastVal::Result(Err(Some(Box::new(WastVal::Variant(
                        "conflict".to_string(), // TODO: return generated StubError::Conflict
                        None,
                    ))))),
                };
                self.event_history[found_idx].1 = Processed;
                Ok(FindMatchingResponse::Found(ChildReturnValue::WastVal(
                    wat_val,
                )))
            }

            (key, found) => Err(ApplyError::NondeterminismDetected(format!(
                "key {key:?} not matching {found:?} stored at index {found_idx}",
            ))),
        }
    }

    pub(crate) async fn flush(
        &mut self,
        db_connection: &dyn DbConnection,
        called_at: DateTime<Utc>,
    ) -> Result<(), DbError> {
        self.flush_non_blocking_event_cache(db_connection, called_at)
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
            EventCall::JoinSetCreate(JoinSetCreate {
                join_set_id,
                closing_strategy,
                wasm_backtrace,
            }) => {
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

            EventCall::Persist(Persist {
                value,
                kind,
                wasm_backtrace,
            }) => {
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

            EventCall::SubmitChildExecution(SubmitChildExecution {
                target_ffqn,
                fn_component_id,
                fn_retry_config,
                join_set_id,
                child_execution_id,
                params,
                wasm_backtrace,
            }) => {
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
                    ffqn: target_ffqn,
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
                        if let Some(wasm_backtrace) = wasm_backtrace
                            && let Err(err) = db_connection
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
                        next_version
                    };
                Ok(history_events)
            }

            EventCall::SubmitDelay(SubmitDelay {
                join_set_id,
                delay_id,
                schedule_at,
                expires_at_if_new,
                wasm_backtrace,
            }) => {
                // Non-cacheable event.
                debug!(%delay_id, %join_set_id, "SubmitDelay");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;

                let event = HistoryEvent::JoinSetRequest {
                    join_set_id: join_set_id.clone(),
                    request: JoinSetRequest::DelayRequest {
                        delay_id,
                        expires_at: expires_at_if_new,
                        schedule_at,
                    },
                };
                let delay_req = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent {
                        event: event.clone(),
                    },
                };
                *version = {
                    let next_version = db_connection
                        .append_batch(
                            called_at,
                            vec![delay_req],
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
                Ok(vec![event])
            }

            EventCall::Schedule(Schedule {
                schedule_at,
                scheduled_at_if_new,
                execution_id: new_execution_id,
                ffqn,
                fn_component_id,
                fn_retry_config,
                params,
                wasm_backtrace,
            }) => {
                // Cacheable event.
                let event = HistoryEvent::Schedule {
                    execution_id: new_execution_id.clone(),
                    schedule_at,
                };

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
                    scheduled_at: scheduled_at_if_new,
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
                        if let Some(wasm_backtrace) = wasm_backtrace
                            && let Err(err) = db_connection
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
                        next_version
                    };
                Ok(history_events)
            }

            EventCall::JoinNext(JoinNext {
                join_set_id,
                closing,
                wasm_backtrace,
            }) => {
                // Non-cacheable event.
                debug!(%join_set_id, "JoinNext(closing:{closing}): Flushing and appending JoinNext");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let event =
                    if self.count_submissions(&join_set_id) > self.count_join_nexts(&join_set_id) {
                        HistoryEvent::JoinNext {
                            join_set_id,
                            run_expires_at: lock_expires_at,
                            requested_ffqn: None,
                            closing,
                        }
                    } else {
                        HistoryEvent::JoinNextTooMany {
                            join_set_id,
                            requested_ffqn: None,
                        }
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

            EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                join_set_id,
                requested_ffqn,
                wasm_backtrace,
            }) => {
                // Non-cacheable event.
                debug!(%join_set_id, "BlockingChildAwaitNext: Flushing and appending JoinNext");
                self.flush_non_blocking_event_cache(db_connection, called_at)
                    .await?;
                let event =
                    if self.count_submissions(&join_set_id) > self.count_join_nexts(&join_set_id) {
                        HistoryEvent::JoinNext {
                            join_set_id,
                            run_expires_at: lock_expires_at,
                            requested_ffqn: Some(requested_ffqn),
                            closing: false,
                        }
                    } else {
                        HistoryEvent::JoinNextTooMany {
                            join_set_id,
                            requested_ffqn: Some(requested_ffqn),
                        }
                    };
                let history_events = vec![event.clone()];
                let append_request = AppendRequest {
                    created_at: called_at,
                    event: ExecutionEventInner::HistoryEvent { event },
                };
                *version = {
                    let next_version = db_connection
                        .append(self.execution_id.clone(), version.clone(), append_request)
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

            EventCall::OneOffChildExecutionRequest(OneOffChildExecutionRequest {
                ffqn,
                fn_component_id,
                fn_retry_config,
                join_set_id,
                child_execution_id,
                params,
                wasm_backtrace,
            }) => {
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
                    requested_ffqn: Some(ffqn.clone()),
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

            EventCall::OneOffDelayRequest(OneOffDelayRequest {
                join_set_id,
                delay_id,
                schedule_at,
                expires_at_if_new,
                wasm_backtrace,
            }) => {
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
                        schedule_at,
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
                    requested_ffqn: None,
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

            EventCall::Stub(Stub {
                target_ffqn,
                target_execution_id,
                parent_id,
                join_set_id,
                result,
                wasm_backtrace,
            }) => {
                // Attempt to write to target_execution_id, will continue on conflict.
                // Non-cacheable event. (could be turned into one)
                // The idempotent write is needed to avoid race with stub requests originating from gRPC.
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
                            result: result.clone(),
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
                                        result: result.clone(),
                                    },
                                },
                            },
                        )
                        .await
                };
                debug!(%target_ffqn, %target_execution_id, "Executed append_batch_respond_to_parent: {write_attempt:?}");
                // The server might crash at this point, and restart processing.
                let persist_result = match write_attempt {
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
                                result: found_result,
                                ..
                            } if result == found_result => Ok(()),
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
                    result,
                    persist_result,
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

    #[expect(clippy::result_large_err)]
    pub(crate) fn get_processed_response(
        &self,
        child_execution_id: &ExecutionIdDerived,
        specified_ffqn: &FunctionFqn,
    ) -> Result<WastVal, GetExtensionError> {
        let found_ffqn = self
            .index_child_exe_to_ffqn
            .get(child_execution_id)
            .ok_or(GetExtensionError::NotFoundInProcessedResponses)?; // Not even found in unprocessed requests.
        let response_idx = self
            .index_child_exe_to_processed_response_idx
            .get(child_execution_id)
            .ok_or(GetExtensionError::NotFoundInProcessedResponses)?;

        if specified_ffqn != found_ffqn {
            return Err(GetExtensionError::FunctionMismatch(
                types_execution::FunctionMismatch {
                    specified_function: types_execution::Function::from(specified_ffqn),
                    actual_function: Some(types_execution::Function::from(found_ffqn)),
                    actual_id: types_execution::ResponseId::ExecutionId(
                        types_execution::ExecutionId::from(child_execution_id),
                    ),
                },
            ));
        }
        match &self
            .responses
            .get(*response_idx)
            .as_ref()
            .expect("`index_child_exe_to_processed_response_idx` must point to a response")
            .0
            .event
        {
            JoinSetResponse::ChildExecutionFinished {
                result,
                child_execution_id,
                finished_version: _,
            } => {
                let response_ffqn = self
                    .index_child_exe_to_ffqn
                    .get(child_execution_id)
                    .expect("got response so the request must have been processed");
                Ok(result
                    .clone()
                    .into_wast_val( || self.fn_registry.get_ret_type(response_ffqn)
                        .expect("response_ffqn can only be exported and no-ext, thus must be returned by get_ret_type"))

                )
            }
            JoinSetResponse::DelayFinished { .. } => unreachable!(
                "`index_child_exe_to_processed_response_idx` must point to a ChildExecutionFinished"
            ),
        }
    }

    pub(crate) fn next_join_set_name_generated(&self) -> String {
        self.next_join_set_name_index(JoinSetKind::Generated)
    }

    fn next_join_set_name_index(&self, kind: JoinSetKind) -> String {
        assert!(kind != JoinSetKind::Named);
        (self.join_set_count(kind) + 1).to_string()
    }

    fn next_join_set_one_off_named(
        &self,
        suffix: &str,
    ) -> Result<JoinSetId, InvalidNameError<JoinSetId>> {
        let index = self.next_join_set_name_index(JoinSetKind::OneOff);
        JoinSetId::new(
            JoinSetKind::OneOff,
            StrVariant::from(format!("{index}-{suffix}")),
        )
    }

    fn count_submissions(&self, join_set_id: &JoinSetId) -> usize {
        // Processing status does matter in order to make replays make the same decisions.
        self.event_history
            .iter()
            .filter(|(event, processing_status)| {
                *processing_status == Processed
                    && match event {
                        HistoryEvent::JoinSetRequest {
                            join_set_id: found_join_set_id,
                            ..
                        } => found_join_set_id == join_set_id,
                        HistoryEvent::Persist { .. }
                        | HistoryEvent::JoinSetCreate { .. }
                        | HistoryEvent::JoinNext { .. }
                        | HistoryEvent::JoinNextTooMany { .. }
                        | HistoryEvent::Schedule { .. }
                        | HistoryEvent::Stub { .. } => false,
                    }
            })
            .count()
    }

    fn count_join_nexts(&self, join_set_id: &JoinSetId) -> usize {
        // Processing status does matter in order to make replays make the same decisions.
        self.event_history
            .iter()
            .filter(|(event, processing_status)| {
                *processing_status == Processed
                    && match event {
                        HistoryEvent::JoinNext {
                            join_set_id: found_join_set_id,
                            ..
                        } => found_join_set_id == join_set_id,
                        HistoryEvent::Persist { .. }
                        | HistoryEvent::JoinSetCreate { .. }
                        | HistoryEvent::JoinSetRequest { .. }
                        | HistoryEvent::JoinNextTooMany { .. }
                        | HistoryEvent::Schedule { .. }
                        | HistoryEvent::Stub { .. } => false,
                    }
            })
            .count()
    }

    pub(crate) fn next_delay_id(&self, join_set_id: &JoinSetId) -> DelayId {
        // TODO: Optimize
        let idx = self
            .event_history
            .iter()
            .filter(|(event, processing_status)| {
                // Ignore unprocessed events, which happens only on replay where we need to produce "old" ids.
                // Applying the `EventCall::SubmitDelay` will immediately mark this as processed.
                *processing_status == Processed &&
                matches!(
                    event,
                    HistoryEvent::JoinSetRequest {join_set_id:found_join_set_id, request:JoinSetRequest::DelayRequest { .. } }
                    if join_set_id == found_join_set_id
                )
            })
            .count();
        DelayId::new_with_index(
            &self.execution_id,
            join_set_id,
            u64::try_from(idx).expect("too many delays in a join set"),
        )
    }
}

// TODO: Replace with generated AwaitNextExtensionError
#[derive(Clone)]
enum ExecutionErrorVariant<'a> {
    FunctionMismatch {
        specified_function: &'a FunctionFqn,
        actual_function: Option<&'a FunctionFqn>,
        actual_id: Either<&'a ExecutionIdDerived, &'a DelayId>,
    },
    AllProcessed,
}
impl ExecutionErrorVariant<'_> {
    fn as_wast_val(&self) -> WastVal {
        match self {
            ExecutionErrorVariant::FunctionMismatch {
                specified_function: specified,
                actual_function: actual,
                actual_id,
            } => WastVal::Variant(
                "function-mismatch".to_string(),
                Some(Box::new(WastVal::Record(indexmap! {
                    "specified-function".to_string() => ffqn_into_wast_val(specified),
                    "actual-function".to_string() =>  WastVal::Option(
                        actual.map(|actual| Box::from(ffqn_into_wast_val(actual)))),
                    "actual-id".to_string() =>
                        WastVal::Variant(
                            if matches!(actual_id, Either::Left(_)) {"execution-id"} else {"delay-id"}
                            .to_string(),
                        Some(Box::new(
                        WastVal::Record(indexmap!{
                            "id".to_string() => WastVal::String(match actual_id {
                                Either::Left(id) => id.to_string(),
                                Either::Right(id) => id.to_string()
                            })
                        }))
                    ))
                }))),
            ),
            ExecutionErrorVariant::AllProcessed => {
                WastVal::Variant("all-processed".to_string(), None)
            }
        }
    }
}

enum JoinSetResponseEnriched<'a> {
    DelayFinished {
        delay_id: &'a DelayId,
        expires_at: DateTime<Utc>,
    },
    ChildExecutionFinished(ChildExecutionFinished<'a>),
}

struct ChildExecutionFinished<'a> {
    child_execution_id: &'a ExecutionIdDerived,
    result: &'a SupportedFunctionReturnValue,
    response_ffqn: &'a FunctionFqn,
}

#[derive(Debug)]
enum JoinNextVariant {
    Child {
        join_set_id: JoinSetId,
        kind: JoinNextChildKind,
        requested_ffqn: FunctionFqn, // For storing index_child_exe_to_ffqn
    },
    Delay(JoinSetId),
    Opaque {
        join_set_id: JoinSetId,
        closing: bool,
    },
}
impl JoinNextVariant {
    fn join_set_id(&self) -> &JoinSetId {
        match self {
            JoinNextVariant::Child { join_set_id, .. }
            | JoinNextVariant::Delay(join_set_id)
            | JoinNextVariant::Opaque {
                join_set_id,
                closing: _,
            } => join_set_id,
        }
    }
    fn as_key(&self) -> DeterministicKey {
        match self {
            JoinNextVariant::Child {
                join_set_id,
                kind,
                requested_ffqn,
            } => DeterministicKey::JoinNextChild {
                join_set_id: join_set_id.clone(),
                kind: *kind,
                requested_ffqn: requested_ffqn.clone(),
            },
            JoinNextVariant::Delay(join_set_id) => DeterministicKey::JoinNextDelay {
                join_set_id: join_set_id.clone(),
            },
            JoinNextVariant::Opaque {
                join_set_id,
                closing,
            } => DeterministicKey::JoinNext {
                join_set_id: join_set_id.clone(),
                closing: *closing,
            },
        }
    }
}

#[derive(derive_more::Debug, Clone, IntoStaticStr)]
pub(crate) enum EventCall {
    JoinSetCreate(JoinSetCreate),
    SubmitChildExecution(SubmitChildExecution),
    SubmitDelay(SubmitDelay),
    Schedule(Schedule),
    Stub(Stub),
    JoinNextRequestingFfqn(JoinNextRequestingFfqn),
    JoinNext(JoinNext),
    OneOffChildExecutionRequest(OneOffChildExecutionRequest),
    OneOffDelayRequest(OneOffDelayRequest),
    Persist(Persist),
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct JoinSetCreate {
    pub(crate) join_set_id: JoinSetId,
    pub(crate) closing_strategy: ClosingStrategy,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl JoinSetCreate {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<JoinSetId, ApplyError> {
        assert!(self.join_set_id.kind != JoinSetKind::OneOff);
        let join_set_id = self.join_set_id.clone();
        let value = event_history
            .apply_inner(
                EventCall::JoinSetCreate(self),
                db_connection,
                version,
                called_at,
            )
            .await?;
        let value = assert_matches!(value,
            ChildReturnValue::JoinSetCreate(join_set_id) => join_set_id);
        assert_eq!(join_set_id, value);
        let prev_val = event_history
            .index_join_set_to_created_child_requests
            .insert(join_set_id, 0);
        assert!(prev_val.is_none());
        Ok(value)
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct SubmitChildExecution {
    pub(crate) target_ffqn: FunctionFqn,
    pub(crate) fn_component_id: ComponentId,
    pub(crate) fn_retry_config: ComponentRetryConfig,
    pub(crate) join_set_id: JoinSetId,
    pub(crate) child_execution_id: ExecutionIdDerived,
    #[debug(skip)]
    pub(crate) params: Params,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl SubmitChildExecution {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val /* ExecutionId */, WorkflowFunctionError> {
        assert!(
            self.join_set_id.kind != JoinSetKind::OneOff,
            "one-off join set cannot be constructed outside of OneOff*Request"
        );
        assert!(!event_history.closed_join_sets.contains(&self.join_set_id));
        let join_set_id = self.join_set_id.clone();
        let value = event_history
            .apply(
                EventCall::SubmitChildExecution(self),
                db_connection,
                version,
                called_at,
            )
            .await?;
        // TODO: Must be an ExecutionId
        let value = match value {
            ChildReturnValue::WastVal(wast_val) => Ok(wast_val.as_val()),
            ChildReturnValue::JoinSetCreate(_)
            | ChildReturnValue::JoinNext(_)
            | ChildReturnValue::OneOffDelay { .. } => {
                unreachable!("must be an ExecutionId")
            }
        };
        {
            // Increment `index_join_set_to_created_child_requests` counter
            let counter = event_history
                .index_join_set_to_created_child_requests
                .get_mut(&join_set_id)
                .expect("must have been created in JoinSetCreate");
            *counter += 1;
        }
        value
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct SubmitDelay {
    pub(crate) join_set_id: JoinSetId,
    pub(crate) delay_id: DelayId,
    pub(crate) schedule_at: HistoryEventScheduleAt, // Intention that must be compared when checking determinism
    pub(crate) expires_at_if_new: DateTime<Utc>, // Actual time based on first execution. Should be disregarded on replay.
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl SubmitDelay {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<types_execution::DelayId, WorkflowFunctionError> {
        assert!(
            self.join_set_id.kind != JoinSetKind::OneOff,
            "one-off join set cannot be constructed outside of OneOff*Request"
        );
        assert!(!event_history.closed_join_sets.contains(&self.join_set_id));
        let value = event_history
            .apply(
                EventCall::SubmitDelay(self),
                db_connection,
                version,
                called_at,
            )
            .await?;
        let id = assert_matches!(value, ChildReturnValue::WastVal(WastVal::Record(mut map)) => map.shift_remove("id").expect(
            "DelayId must be serialized as WastVal::Record"));
        let id = assert_matches!(id, WastVal::String(id) => id, "DelayId record must have `id` serialized as WastVal::String");
        Ok(types_execution::DelayId { id })
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct Schedule {
    #[expect(clippy::struct_field_names)]
    pub(crate) schedule_at: HistoryEventScheduleAt, // Intention that must be compared when checking determinism
    pub(crate) scheduled_at_if_new: DateTime<Utc>, // Actual time based on first execution. Should be disregarded on replay.
    pub(crate) execution_id: ExecutionId,
    pub(crate) ffqn: FunctionFqn,
    pub(crate) fn_component_id: ComponentId,
    pub(crate) fn_retry_config: ComponentRetryConfig,
    #[debug(skip)]
    pub(crate) params: Params,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl Schedule {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val /* ExecutionId */, WorkflowFunctionError> {
        let value = event_history
            .apply(EventCall::Schedule(self), db_connection, version, called_at)
            .await?;
        // TODO: Must be an ExecutionId
        match value {
            ChildReturnValue::WastVal(wast_val) => Ok(wast_val.as_val()),
            ChildReturnValue::JoinSetCreate(_)
            | ChildReturnValue::JoinNext(_)
            | ChildReturnValue::OneOffDelay { .. } => {
                unreachable!("must be an ExecutionId")
            }
        }
    }
}

// -stub: func(execution-id: execution-id, return-value: result<?>) -> result<_, stub-error>
#[derive(derive_more::Debug, Clone)]
pub(crate) struct Stub {
    pub(crate) target_ffqn: FunctionFqn,
    pub(crate) target_execution_id: ExecutionIdDerived,
    pub(crate) parent_id: ExecutionId,
    pub(crate) join_set_id: JoinSetId,
    #[debug(skip)]
    pub(crate) result: SupportedFunctionReturnValue, // stubbed return value
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl Stub {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let value = event_history
            .apply(EventCall::Stub(self), db_connection, version, called_at)
            .await?;

        match value {
            ChildReturnValue::WastVal(wast_val) => Ok(wast_val.as_val()),
            ChildReturnValue::JoinSetCreate(_)
            | ChildReturnValue::JoinNext(_)
            | ChildReturnValue::OneOffDelay { .. } => {
                unreachable!("must be WastVal")
            }
        }
    }
}

// -await-next: func(join-set-id: borrow<join-set-id>) -> result<(execution_id,?), await-next-extension-error>
#[derive(derive_more::Debug, Clone)]
pub(crate) struct JoinNextRequestingFfqn {
    pub(crate) join_set_id: JoinSetId,
    pub(crate) requested_ffqn: FunctionFqn,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl JoinNextRequestingFfqn {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<
        wasmtime::component::Val, /* result<?, await-next-extension-error> */
        WorkflowFunctionError,
    > {
        assert!(
            self.join_set_id.kind != JoinSetKind::OneOff,
            "one-off join set cannot be constructed outside of OneOff*Request"
        );
        assert!(!event_history.closed_join_sets.contains(&self.join_set_id));
        let value = event_history
            .apply(
                EventCall::JoinNextRequestingFfqn(self),
                db_connection,
                version,
                called_at,
            )
            .await?;

        let value = match value {
            ChildReturnValue::WastVal(wast_val) => wast_val,
            ChildReturnValue::JoinSetCreate(_)
            | ChildReturnValue::JoinNext(_)
            | ChildReturnValue::OneOffDelay { .. } => {
                unreachable!("must be WastVal containing result<?, await-next-extension-error>")
            }
        };
        Ok(value.as_val())
    }
}

// join-next: func(join-set-id: borrow<join-set-id>) -> result<response-id, join-next-error>
#[derive(derive_more::Debug, Clone)]
pub(crate) struct JoinNext {
    pub(crate) join_set_id: JoinSetId,
    pub(crate) closing: bool,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl JoinNext {
    pub(crate) async fn apply(
        self,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<Result<types_execution::ResponseId, workflow_support::JoinNextError>, ApplyError>
    {
        assert!(
            self.join_set_id.kind != JoinSetKind::OneOff,
            "one-off join set cannot be constructed outside of OneOff*Request"
        );
        assert!(!event_history.closed_join_sets.contains(&self.join_set_id));
        let value = event_history
            .apply_inner(EventCall::JoinNext(self), db_connection, version, called_at)
            .await?;
        let value = assert_matches!(value,ChildReturnValue::JoinNext(value) => value);
        Ok(value)
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct OneOffChildExecutionRequest {
    ffqn: FunctionFqn,
    fn_component_id: ComponentId,
    fn_retry_config: ComponentRetryConfig,
    join_set_id: JoinSetId,
    child_execution_id: ExecutionIdDerived,
    #[debug(skip)]
    params: Params,
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl OneOffChildExecutionRequest {
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn apply(
        ffqn: FunctionFqn,
        fn_component_id: ComponentId,
        fn_retry_config: ComponentRetryConfig,
        params: Params,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<wasmtime::component::Val, WorkflowFunctionError> {
        let join_set_id = event_history
            .next_join_set_one_off_named(&ffqn.function_name)
            .expect("no illegal chars in fn name: only alphanumeric and dash");
        let child_execution_id = event_history.execution_id.next_level(&join_set_id);
        let event = EventCall::OneOffChildExecutionRequest(OneOffChildExecutionRequest {
            ffqn,
            fn_component_id,
            fn_retry_config,
            join_set_id,
            child_execution_id,
            params,
            wasm_backtrace,
        });

        let value = event_history
            .apply(event, db_connection, version, called_at)
            .await?;
        match value {
            ChildReturnValue::WastVal(wast_val) => Ok(wast_val.as_val()),
            ChildReturnValue::JoinSetCreate(_)
            | ChildReturnValue::JoinNext(_)
            | ChildReturnValue::OneOffDelay { .. } => {
                unreachable!("applying OneOffChildExecutionRequest must return WastVal")
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn apply_invoke(
        ffqn: FunctionFqn,
        fn_component_id: ComponentId,
        fn_retry_config: ComponentRetryConfig,
        label: &str,
        params: Params,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<Val, WorkflowFunctionError> {
        let join_set_id = match event_history.next_join_set_one_off_named(label) {
            Ok(ok) => ok,
            Err(err) => {
                // invoke-extension-error
                return Ok(Val::Result(Err(Some(Box::new(Val::Variant(
                    "invalid-name".to_string(),
                    Some(Box::new(Val::String(err.to_string()))),
                ))))));
            }
        };
        let child_execution_id = event_history.execution_id.next_level(&join_set_id);
        let event = EventCall::OneOffChildExecutionRequest(OneOffChildExecutionRequest {
            ffqn,
            fn_component_id,
            fn_retry_config,
            join_set_id,
            child_execution_id,
            params,
            wasm_backtrace,
        });

        match event_history
            .apply_inner(event, db_connection, version, called_at)
            .await
        {
            Ok(ChildReturnValue::WastVal(wast_val)) => {
                // wrap with an Ok in order to return result<T, invoke-extension-error>
                Ok(Val::Result(Ok(Some(Box::new(wast_val.as_val())))))
            }
            Ok(
                ChildReturnValue::JoinSetCreate(_)
                | ChildReturnValue::JoinNext(_)
                | ChildReturnValue::OneOffDelay { .. },
            ) => {
                unreachable!("applying OneOffChildExecutionRequest must return WastVal or None")
            }
            Err(apply_err) => Err(WorkflowFunctionError::from(apply_err)),
        }
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct OneOffDelayRequest {
    join_set_id: JoinSetId,
    delay_id: DelayId,
    schedule_at: HistoryEventScheduleAt, // Intention that must be compared when checking determinism
    expires_at_if_new: DateTime<Utc>, // Actual time based on first execution. Should be disregarded on replay.
    #[debug(skip)]
    wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl OneOffDelayRequest {
    pub(crate) async fn apply(
        schedule_at: HistoryEventScheduleAt,
        expires_at_if_new: DateTime<Utc>,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, WorkflowFunctionError> {
        let join_set_id = event_history
            .next_join_set_one_off_named("sleep")
            .expect("no illegal chars in sleep");
        let delay_id = DelayId::new(&event_history.execution_id, &join_set_id);

        let ChildReturnValue::OneOffDelay { scheduled_at } = event_history
            .apply(
                EventCall::OneOffDelayRequest(OneOffDelayRequest {
                    join_set_id,
                    delay_id,
                    schedule_at,
                    expires_at_if_new,
                    wasm_backtrace,
                }),
                db_connection,
                version,
                called_at,
            )
            .await?
        else {
            unreachable!()
        };
        Ok(scheduled_at)
    }
}

#[derive(derive_more::Debug, Clone)]
pub(crate) struct Persist {
    #[debug(skip)]
    pub(crate) value: Vec<u8>,
    pub(crate) kind: PersistKind,
    #[debug(skip)]
    pub(crate) wasm_backtrace: Option<storage::WasmBacktrace>,
}
impl Persist {
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn apply_string(
        value: String,
        min_length: u64,
        max_length_exclusive: u64,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<String, WorkflowFunctionError> {
        let value = Vec::from_iter(value.bytes());
        let value = event_history
            .apply(
                EventCall::Persist(Persist {
                    value,
                    kind: PersistKind::RandomString {
                        min_length,
                        max_length_exclusive,
                    },
                    wasm_backtrace,
                }),
                db_connection,
                version,
                called_at,
            )
            .await?;
        let value =
            assert_matches!(value, ChildReturnValue::WastVal(WastVal::String(value)) => value);
        Ok(value)
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn apply_u64(
        value: u64,
        min: u64,
        max_inclusive: u64,
        wasm_backtrace: Option<storage::WasmBacktrace>,
        event_history: &mut EventHistory,
        db_connection: &dyn DbConnection,
        version: &mut Version,
        called_at: DateTime<Utc>,
    ) -> Result<u64, WorkflowFunctionError> {
        let value = Vec::from(storage::from_u64_to_bytes(value));
        let value = event_history
            .apply(
                EventCall::Persist(Persist {
                    value,
                    kind: PersistKind::RandomU64 { min, max_inclusive },
                    wasm_backtrace,
                }),
                db_connection,
                version,
                called_at,
            )
            .await?;
        let value = assert_matches!(value, ChildReturnValue::WastVal(WastVal::U64(value)) => value);
        Ok(value)
    }
}

impl Display for EventCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &'static str = self.into();
        write!(f, "{s}")
    }
}

impl EventCall {
    fn join_next_variant(&self) -> Option<JoinNextVariant> {
        match &self {
            // Blocking calls can be polled for JoinSetResponse
            EventCall::OneOffChildExecutionRequest(OneOffChildExecutionRequest {
                join_set_id,
                ffqn,
                fn_component_id: _,
                fn_retry_config: _,
                child_execution_id: _,
                params: _,
                wasm_backtrace: _,
            }) => Some(JoinNextVariant::Child {
                join_set_id: join_set_id.clone(),
                kind: JoinNextChildKind::DirectCall,
                requested_ffqn: ffqn.clone(),
            }),
            EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                join_set_id,
                requested_ffqn,
                wasm_backtrace: _,
            }) => Some(JoinNextVariant::Child {
                join_set_id: join_set_id.clone(),
                kind: JoinNextChildKind::AwaitNext,
                requested_ffqn: requested_ffqn.clone(),
            }),
            EventCall::OneOffDelayRequest(OneOffDelayRequest {
                join_set_id,
                delay_id: _,
                schedule_at: _,
                expires_at_if_new: _,
                wasm_backtrace: _,
            }) => Some(JoinNextVariant::Delay(join_set_id.clone())),
            EventCall::JoinNext(JoinNext {
                join_set_id,
                closing,
                wasm_backtrace: _,
            }) => Some(JoinNextVariant::Opaque {
                join_set_id: join_set_id.clone(),
                closing: *closing,
            }),

            EventCall::JoinSetCreate { .. }
            | EventCall::SubmitChildExecution { .. }
            | EventCall::SubmitDelay { .. }
            | EventCall::Schedule { .. }
            | EventCall::Persist { .. }
            | EventCall::Stub { .. } => None, // No response polling is needed.
        }
    }
}

/// Stores important properties of events as they are executed or replayed.
/// Those properties are checked for equality with `HistoryEvent` before it is marked as `Processed`.
/// One `EventCall` can be represented as multiple `DeterministicKey`-s.
/// One `DeterministicKey` corresponds to one `HistoryEvent`.
#[derive(derive_more::Debug, Clone)]
enum DeterministicKey {
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
        target_ffqn: FunctionFqn,
    },
    DelayRequest {
        join_set_id: JoinSetId,
        delay_id: DelayId,
        schedule_at: HistoryEventScheduleAt,
    },
    JoinNextChild {
        join_set_id: JoinSetId,
        kind: JoinNextChildKind,
        requested_ffqn: FunctionFqn,
    },
    // one-off delay requests only, represents host function sleep
    JoinNextDelay {
        join_set_id: JoinSetId,
    },
    JoinNext {
        join_set_id: JoinSetId,
        closing: bool,
    },
    Schedule {
        target_execution_id: ExecutionId,
        schedule_at: HistoryEventScheduleAt,
    },
    Stub {
        target_execution_id: ExecutionIdDerived,
        return_value: SupportedFunctionReturnValue,
    },
}

#[derive(Debug, Clone, Copy)]
enum JoinNextChildKind {
    AwaitNext,
    DirectCall,
}

impl EventCall {
    fn as_keys(&self) -> Vec<DeterministicKey> {
        match self {
            EventCall::JoinSetCreate(JoinSetCreate {
                join_set_id,
                closing_strategy,
                ..
            }) => {
                vec![DeterministicKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: *closing_strategy,
                }]
            }
            EventCall::Persist(Persist { value, kind, .. }) => {
                vec![DeterministicKey::Persist {
                    value: value.clone(),
                    kind: *kind,
                }]
            }
            EventCall::SubmitChildExecution(SubmitChildExecution {
                join_set_id,
                child_execution_id,
                target_ffqn,
                ..
            }) => vec![DeterministicKey::ChildExecutionRequest {
                join_set_id: join_set_id.clone(),
                child_execution_id: child_execution_id.clone(),
                target_ffqn: target_ffqn.clone(),
            }],
            EventCall::SubmitDelay(SubmitDelay {
                delay_id,
                join_set_id,
                schedule_at: timeout,
                ..
            }) => vec![DeterministicKey::DelayRequest {
                join_set_id: join_set_id.clone(),
                delay_id: delay_id.clone(),
                schedule_at: *timeout,
            }],
            EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                join_set_id,
                requested_ffqn,
                ..
            }) => {
                vec![DeterministicKey::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextChildKind::AwaitNext,
                    requested_ffqn: requested_ffqn.clone(),
                }]
            }
            EventCall::OneOffChildExecutionRequest(OneOffChildExecutionRequest {
                join_set_id,
                child_execution_id,
                ffqn,
                ..
            }) => vec![
                DeterministicKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::default(),
                },
                DeterministicKey::ChildExecutionRequest {
                    join_set_id: join_set_id.clone(),
                    child_execution_id: child_execution_id.clone(),
                    target_ffqn: ffqn.clone(),
                },
                DeterministicKey::JoinNextChild {
                    join_set_id: join_set_id.clone(),
                    kind: JoinNextChildKind::DirectCall,
                    requested_ffqn: ffqn.clone(),
                },
            ],
            EventCall::OneOffDelayRequest(OneOffDelayRequest {
                join_set_id,
                delay_id,
                schedule_at,
                ..
            }) => vec![
                DeterministicKey::CreateJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::default(),
                },
                DeterministicKey::DelayRequest {
                    join_set_id: join_set_id.clone(),
                    delay_id: delay_id.clone(),
                    schedule_at: *schedule_at,
                },
                DeterministicKey::JoinNextDelay {
                    join_set_id: join_set_id.clone(),
                },
            ],
            EventCall::JoinNext(JoinNext {
                join_set_id,
                closing,
                wasm_backtrace: _,
            }) => vec![DeterministicKey::JoinNext {
                join_set_id: join_set_id.clone(),
                closing: *closing,
            }],
            EventCall::Schedule(Schedule {
                execution_id,
                schedule_at,
                ..
            }) => {
                vec![DeterministicKey::Schedule {
                    target_execution_id: execution_id.clone(),
                    schedule_at: *schedule_at,
                }]
            }
            EventCall::Stub(Stub {
                target_execution_id,
                result: return_value,
                ..
            }) => {
                vec![DeterministicKey::Stub {
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
    use super::SubmitChildExecution;
    use crate::testing_fn_registry::TestingFnRegistry;
    use crate::workflow::event_history::{
        ApplyError, ChildReturnValue, JoinNextRequestingFfqn, JoinSetCreate, Schedule, Stub,
    };
    use crate::workflow::host_exports::ffqn_into_wast_val;
    use assert_matches::assert_matches;
    use chrono::{DateTime, Utc};
    use concepts::prefixed_ulid::ExecutionIdDerived;
    use concepts::storage::{CreateRequest, HistoryEventScheduleAt};
    use concepts::storage::{DbConnection, JoinSetResponse, JoinSetResponseEvent, Version};
    use concepts::time::ClockFn;
    use concepts::{
        ClosingStrategy, ComponentId, ComponentRetryConfig, ExecutionId, FunctionFqn,
        FunctionRegistry, Params, SUPPORTED_RETURN_VALUE_OK_EMPTY, SupportedFunctionReturnValue,
    };
    use concepts::{JoinSetId, StrVariant};
    use db_tests::Database;
    use indexmap::indexmap;
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use test_utils::sim_clock::SimClock;
    use tracing::{info, info_span};
    use val_json::type_wrapper::TypeWrapper;
    use val_json::wast_val::{WastVal, WastValWithType};

    pub const MOCK_FFQN: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn1");
    pub const MOCK_FFQN_2: FunctionFqn = FunctionFqn::new_static("namespace:pkg/ifc", "fn2");

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

        let fn_registry = TestingFnRegistry::new_from_components(vec![]);

        let (event_history, version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            Duration::from_secs(1),              // execution deadline
            JoinNextBlockingStrategy::Interrupt, // first run needs to interrupt
            fn_registry.clone(),
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        assert_matches!(
            apply_create_join_set_start_async_await_next(
                db_connection.as_ref(),
                MOCK_FFQN,
                child_execution_id.clone(),
                event_history,
                version,
                join_set_id.clone(),
                sim_clock.now()
            )
            .await
            .unwrap_err(),
            ApplyError::InterruptDbUpdated,
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
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
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
            Duration::from_secs(1), // execution deadline
            second_run_strategy,
            fn_registry,
        )
        .await;
        apply_create_join_set_start_async_await_next(
            db_connection.as_ref(),
            MOCK_FFQN,
            child_execution_id,
            event_history,
            version,
            join_set_id,
            sim_clock.now(),
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
        const CHILD_RESP: SupportedFunctionReturnValue = SupportedFunctionReturnValue::Ok {
            ok: Some(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            }),
        };
        test_utils::set_up();
        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();
        // Create an execution.
        let execution_id = create_execution(db_connection.as_ref(), &sim_clock).await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![]);
        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            Duration::from_secs(1), // execution deadline
            join_next_blocking_strategy,
            fn_registry.clone(),
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let child_execution_id = execution_id.next_level(&join_set_id);

        apply_create_join_set_start_async(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            MOCK_FFQN,
            child_execution_id.clone(),
            sim_clock.now(),
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
                        result: CHILD_RESP,
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
            Duration::from_secs(1), // execution deadline
            join_next_blocking_strategy,
            fn_registry,
        )
        .await;

        apply_create_join_set_start_async(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            MOCK_FFQN,
            child_execution_id.clone(),
            sim_clock.now(),
        )
        .await;
        // issue BlockingChildJoinNext
        let res = event_history
            .apply(
                EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                    join_set_id,
                    wasm_backtrace: None,
                    requested_ffqn: MOCK_FFQN,
                }),
                db_pool.connection().as_ref(),
                &mut version,
                sim_clock.now(),
            )
            .await
            .unwrap();

        let child_resp_wrapped = WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
            execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id)),
            WastVal::Result(Ok(Some(Box::new(WastVal::U8(1))))),
        ])))));
        let res = assert_matches!(res, ChildReturnValue::WastVal(res) => res);
        assert_eq!(child_resp_wrapped, res);

        drop(db_connection);
        db_pool.close().await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn create_two_non_blocking_childs_then_two_join_nexts(
        #[values(true, false)] submits_and_awaits_in_correct_order: bool,
    ) {
        const KID_A_RET: SupportedFunctionReturnValue = SupportedFunctionReturnValue::Ok {
            ok: Some(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(1),
            }),
        };
        const KID_B_RET: SupportedFunctionReturnValue = SupportedFunctionReturnValue::Ok {
            ok: Some(WastValWithType {
                r#type: TypeWrapper::U8,
                value: WastVal::U8(2),
            }),
        };

        test_utils::set_up();

        let submit_ffqn_1 = if submits_and_awaits_in_correct_order {
            MOCK_FFQN
        } else {
            MOCK_FFQN_2
        };
        let submit_ffqn_2 = if submits_and_awaits_in_correct_order {
            MOCK_FFQN_2
        } else {
            MOCK_FFQN
        };

        let sim_clock = SimClock::new(DateTime::default());
        let (_guard, db_pool) = Database::Memory.set_up().await;
        let db_connection = db_pool.connection();

        // Create an execution.
        let execution_id = create_execution(db_connection.as_ref(), &sim_clock).await;
        let fn_registry = TestingFnRegistry::new_from_components(vec![]);
        let (mut event_history, mut version) = load_event_history(
            db_connection.as_ref(),
            execution_id.clone(),
            sim_clock.now(),
            Duration::from_secs(1),              // execution deadline
            JoinNextBlockingStrategy::Interrupt, // First blocking strategy is always Interrupt
            fn_registry.clone(),
        )
        .await;

        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::Generated, StrVariant::empty()).unwrap();

        let child_execution_id_a = execution_id.next_level(&join_set_id);
        let child_execution_id_b = child_execution_id_a.get_incremented();

        // persist create_join_set, 2x start_async, 1x await_next
        assert_matches!(
            apply_create_join_set_two_start_asyncs_await_next_a(
                db_connection.as_ref(),
                &mut event_history,
                &mut version,
                join_set_id.clone(),
                submit_ffqn_1.clone(),
                child_execution_id_a.clone(),
                submit_ffqn_2.clone(),
                child_execution_id_b.clone(),
                sim_clock.now()
            )
            .await
            .unwrap_err(),
            ApplyError::InterruptDbUpdated
        );
        // append two responses
        db_connection
            .append_response(
                sim_clock.now(),
                execution_id.clone(),
                JoinSetResponseEvent {
                    join_set_id: join_set_id.clone(),
                    event: JoinSetResponse::ChildExecutionFinished {
                        child_execution_id: if submits_and_awaits_in_correct_order {
                            child_execution_id_a.clone()
                        } else {
                            child_execution_id_b.clone()
                        },
                        finished_version: Version(0), // does not matter
                        result: KID_A_RET,            // won't matter on mismatch
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
                        child_execution_id: if submits_and_awaits_in_correct_order {
                            child_execution_id_b.clone()
                        } else {
                            child_execution_id_a.clone()
                        },
                        finished_version: Version(0), // does not matter
                        result: KID_B_RET,            // won't matter on mismatch
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
            Duration::ZERO, // deadline
            JoinNextBlockingStrategy::Interrupt,
            fn_registry,
        )
        .await;

        let res = apply_create_join_set_two_start_asyncs_await_next_a(
            db_connection.as_ref(),
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            submit_ffqn_1.clone(),
            child_execution_id_a.clone(),
            submit_ffqn_2.clone(),
            child_execution_id_b.clone(),
            sim_clock.now(),
        )
        .await
        .unwrap();
        if !submits_and_awaits_in_correct_order {
            // actually execution B was processed.
            let mismatch = WastVal::Result(Err(Some(Box::new(WastVal::Variant(
                "function-mismatch".to_string(),
                Some(Box::new(WastVal::Record(indexmap! {
                    "specified-function".to_string() => ffqn_into_wast_val(&submit_ffqn_1),
                    "actual-function".to_string() => WastVal::Option(Some(Box::new(
                        ffqn_into_wast_val(&submit_ffqn_2)
                    ))),
                    "actual-id".to_string() => WastVal::Variant("execution-id".to_string(),
                        Some(Box::from(execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id_b))))),
                }))),
            )))));
            assert_eq!(mismatch, res);
        } else {
            let kid_a = WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
                execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id_a)),
                WastVal::Result(Ok(Some(Box::new(WastVal::U8(1))))),
            ])))));
            assert_eq!(kid_a, res);
            // second child result should be found
            let res = event_history
                .apply(
                    EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                        join_set_id,
                        wasm_backtrace: None,
                        requested_ffqn: submit_ffqn_2.clone(),
                    }),
                    db_pool.connection().as_ref(),
                    &mut version,
                    sim_clock.now(),
                )
                .await
                .unwrap();
            let kid_b = WastVal::Result(Ok(Some(Box::new(WastVal::Tuple(vec![
                execution_id_into_wast_val(&ExecutionId::Derived(child_execution_id_b)),
                WastVal::Result(Ok(Some(Box::new(WastVal::U8(2))))),
            ])))));
            let res = assert_matches!(res, ChildReturnValue::WastVal(res) => res);
            assert_eq!(kid_b, res);
        }
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
        let fn_registry = TestingFnRegistry::new_from_components(vec![]);
        let (mut event_history, mut version) = load_event_history(
            db_connection,
            execution_id.clone(),
            sim_clock.now(),
            Duration::from_secs(1),              // execution deadline
            JoinNextBlockingStrategy::Interrupt, // does not matter, there are no blocking events
            fn_registry,
        )
        .await;

        event_history
            .apply(
                EventCall::Schedule(Schedule {
                    schedule_at: HistoryEventScheduleAt::Now,
                    scheduled_at_if_new: sim_clock.now(),
                    execution_id: ExecutionId::generate(),
                    ffqn: MOCK_FFQN,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    params: Params::empty(),
                    wasm_backtrace: None,
                }),
                db_connection,
                &mut version,
                sim_clock.now(),
            )
            .await
            .unwrap();
        // Create a join set to force checking of "Processed" status of the schedule event.
        let join_set_id =
            JoinSetId::new(concepts::JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        event_history
            .apply(
                EventCall::JoinSetCreate(JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                    wasm_backtrace: None,
                }),
                db_connection,
                &mut version,
                sim_clock.now(),
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
        let fn_registry = TestingFnRegistry::new_from_components(vec![]);
        for run_id in 0..1 {
            info!("Run {run_id}");
            let (mut event_history, mut version) = load_event_history(
                db_connection,
                execution_id.clone(),
                sim_clock.now(),
                Duration::from_secs(1), // execution deadline
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 0,
                },
                fn_registry.clone(),
            )
            .await;

            apply_create_join_set_start_async(
                db_connection,
                &mut event_history,
                &mut version,
                join_set_id.clone(),
                MOCK_FFQN,
                child_execution_id.clone(),
                sim_clock.now(),
            )
            .await;

            event_history
                .apply(
                    EventCall::Stub(Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        wasm_backtrace: None,
                    }),
                    db_connection,
                    &mut version,
                    sim_clock.now(),
                )
                .await
                .unwrap();

            let child_return_value = event_history
                .apply(
                    EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                        join_set_id: join_set_id.clone(),
                        wasm_backtrace: None,
                        requested_ffqn: MOCK_FFQN,
                    }),
                    db_connection,
                    &mut version,
                    sim_clock.now(),
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
        let fn_registry = TestingFnRegistry::new_from_components(vec![]);
        for run_id in 0..1 {
            info!("Run {run_id}");
            let (mut event_history, mut version) = load_event_history(
                db_connection,
                execution_id.clone(),
                sim_clock.now(),
                Duration::from_secs(1),
                JoinNextBlockingStrategy::Await {
                    non_blocking_event_batching: 0,
                },
                fn_registry.clone(),
            )
            .await;
            apply_create_join_set_start_async(
                db_connection,
                &mut event_history,
                &mut version,
                join_set_id.clone(),
                MOCK_FFQN,
                child_execution_id.clone(),
                sim_clock.now(),
            )
            .await;
            event_history
                .apply(
                    EventCall::Stub(Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        wasm_backtrace: None,
                    }),
                    db_connection,
                    &mut version,
                    sim_clock.now(),
                )
                .await
                .unwrap();

            event_history
                .apply(
                    EventCall::Stub(Stub {
                        target_ffqn: MOCK_FFQN,
                        target_execution_id: child_execution_id.clone(),
                        parent_id: execution_id.clone(),
                        join_set_id: join_set_id.clone(),
                        result: SUPPORTED_RETURN_VALUE_OK_EMPTY,
                        wasm_backtrace: None,
                    }),
                    db_connection,
                    &mut version,
                    sim_clock.now(),
                )
                .await
                .unwrap();
        }

        db_pool.close().await.unwrap();
    }

    // TODO: Check -await-next for fn without return type
    // TODO: Check execution errors translating to await-next-extension-error

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

    async fn load_event_history(
        db_connection: &dyn DbConnection,
        execution_id: ExecutionId,
        now: DateTime<Utc>,
        deadline_duration: Duration,
        join_next_blocking_strategy: JoinNextBlockingStrategy,
        fn_registry: Arc<dyn FunctionRegistry>,
    ) -> (EventHistory, Version) {
        let exec_log = db_connection.get(&execution_id).await.unwrap();
        let execution_deadline = now + deadline_duration;
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
            info_span!("worker-test"),
            deadline_duration,
            fn_registry,
        );
        (event_history, exec_log.next_version)
    }

    async fn apply_create_join_set_start_async_await_next(
        db_connection: &dyn DbConnection,
        ffqn: FunctionFqn,
        child_execution_id: ExecutionIdDerived,
        mut event_history: EventHistory,
        mut version: Version,
        join_set_id: JoinSetId,
        called_at: DateTime<Utc>,
    ) -> Result<ChildReturnValue, ApplyError> {
        apply_create_join_set_start_async(
            db_connection,
            &mut event_history,
            &mut version,
            join_set_id.clone(),
            ffqn.clone(),
            child_execution_id,
            called_at,
        )
        .await;
        event_history
            .apply_inner(
                EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                    join_set_id,
                    wasm_backtrace: None,
                    requested_ffqn: ffqn,
                }),
                db_connection,
                &mut version,
                called_at,
            )
            .await
    }

    async fn apply_create_join_set_start_async(
        db_connection: &dyn DbConnection,
        event_history: &mut EventHistory,
        version: &mut Version,
        join_set_id: JoinSetId,
        ffqn: FunctionFqn,
        child_execution_id: ExecutionIdDerived,
        called_at: DateTime<Utc>,
    ) {
        event_history
            .apply(
                EventCall::JoinSetCreate(JoinSetCreate {
                    join_set_id: join_set_id.clone(),
                    closing_strategy: ClosingStrategy::Complete,
                    wasm_backtrace: None,
                }),
                db_connection,
                version,
                called_at,
            )
            .await
            .unwrap();
        event_history
            .apply(
                EventCall::SubmitChildExecution(SubmitChildExecution {
                    target_ffqn: ffqn,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    join_set_id,
                    child_execution_id,
                    params: Params::empty(),
                    wasm_backtrace: None,
                }),
                db_connection,
                version,
                called_at,
            )
            .await
            .unwrap();
    }

    #[expect(clippy::too_many_arguments)]
    async fn apply_create_join_set_two_start_asyncs_await_next_a(
        db_connection: &dyn DbConnection,
        event_history: &mut EventHistory,
        version: &mut Version,
        join_set_id: JoinSetId,
        ffqn_a: FunctionFqn,
        child_execution_id_a: ExecutionIdDerived,
        ffqn_b: FunctionFqn,
        child_execution_id_b: ExecutionIdDerived,
        called_at: DateTime<Utc>,
    ) -> Result<WastVal, ApplyError> {
        apply_create_join_set_start_async(
            db_connection,
            event_history,
            version,
            join_set_id.clone(),
            ffqn_a.clone(),
            child_execution_id_a,
            called_at,
        )
        .await;
        event_history
            .apply(
                EventCall::SubmitChildExecution(SubmitChildExecution {
                    target_ffqn: ffqn_b,
                    fn_component_id: ComponentId::dummy_activity(),
                    fn_retry_config: ComponentRetryConfig::ZERO,
                    join_set_id: join_set_id.clone(),
                    child_execution_id: child_execution_id_b,
                    params: Params::empty(),
                    wasm_backtrace: None,
                }),
                db_connection,
                version,
                called_at,
            )
            .await
            .unwrap();
        event_history
            .apply_inner(
                EventCall::JoinNextRequestingFfqn(JoinNextRequestingFfqn {
                    join_set_id,
                    wasm_backtrace: None,
                    requested_ffqn: ffqn_a,
                }),
                db_connection,
                version,
                called_at,
            )
            .await
            .map(|res| match res {
                ChildReturnValue::WastVal(res) => res,
                other => {
                    unreachable!("BlockingChildAwaitNext returns WastVal, got {other:?}")
                }
            })
    }
}
