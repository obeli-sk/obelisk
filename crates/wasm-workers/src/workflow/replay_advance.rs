use crate::workflow::replay_db_proxy::InternalCapturedWrite;
use chrono::{DateTime, Utc};
use concepts::{
    SupportedFunctionReturnValue,
    prefixed_ulid::ExecutionIdDerived,
    storage::{
        AppendEventsToExecution, AppendRequest, AppendResponseToExecution, CapturedDbWrite,
        CreateRequest, DbErrorWrite, ExecutionRequest, HistoryEvent, HistoryEventScheduleAt,
        JoinSetRequest, Version,
    },
};
use db_common::JoinSetResponseId;
use executor::worker::FatalError;

/// Replay outcome for a workflow execution.
#[derive(Debug, Clone)]
#[cfg_attr(any(test, feature = "test"), derive(serde::Serialize))]
pub enum ReplayResponse {
    /// Execution can be advanced by one or more captured writes.
    Advanceable(ReplayAdvanceable),
    /// Execution is already finished.
    Finished {
        result: SupportedFunctionReturnValue,
    },
    /// Replay did not capture any writes and the execution is not finished.
    Blocked,
}

/// Preview writes captured by replay that can be supplied to `advance`.
#[derive(Debug, Clone)]
#[cfg_attr(any(test, feature = "test"), derive(serde::Serialize))]
pub struct ReplayAdvanceable {
    /// Write operations that the workflow would produce next,
    /// including the Finished event if the workflow completes.
    pub captured_writes: Vec<CapturedDbWrite>,
}

impl ReplayAdvanceable {
    /// Extract the starting version from the first captured write that targets
    /// the current execution. `AppendStubResponse` is skipped because it
    /// targets the child execution, not the parent.
    pub(crate) fn starting_version(&self) -> Option<&Version> {
        self.captured_writes.iter().find_map(|w| match w {
            CapturedDbWrite::Append { version, .. }
            | CapturedDbWrite::AppendBatch { version, .. }
            | CapturedDbWrite::AppendBatchCreateNewExecution { version, .. }
            | CapturedDbWrite::AppendFinished { version, .. } => Some(version),
            CapturedDbWrite::AppendStubResponse { .. } => None,
        })
    }

    pub(crate) fn is_prefix_of(&self, fresh_replay: &[CapturedDbWrite]) -> bool {
        self.captured_writes.len() <= fresh_replay.len()
            && self
                .captured_writes
                .iter()
                .zip(fresh_replay)
                .all(|(requested, fresh)| requested_write_matches_fresh_replay(requested, fresh))
    }

    pub(crate) fn get_return_value(&self) -> Option<&SupportedFunctionReturnValue> {
        if let Some(CapturedDbWrite::AppendFinished { retval, .. }) = self.captured_writes.last() {
            return Some(retval);
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn history_events(&self) -> Vec<&concepts::storage::HistoryEvent> {
        self.captured_writes
            .iter()
            .flat_map(|w| {
                let requests: &[concepts::storage::AppendRequest] = match w {
                    CapturedDbWrite::Append { req, .. } => std::slice::from_ref(req),
                    CapturedDbWrite::AppendBatch { batch, .. }
                    | CapturedDbWrite::AppendBatchCreateNewExecution { batch, .. } => batch,
                    CapturedDbWrite::AppendStubResponse { events, .. } => &events.batch,
                    CapturedDbWrite::AppendFinished { .. } => &[],
                };
                requests.iter().filter_map(|req| {
                    if let concepts::storage::ExecutionRequest::HistoryEvent { event } = &req.event
                    {
                        Some(event)
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn truncate_to(&self, len: usize) -> Self {
        let mut captured_writes = self.captured_writes.clone();
        captured_writes.truncate(len);
        Self { captured_writes }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.captured_writes.is_empty()
    }
}

/// Result of advancing a paused workflow execution.
#[derive(Debug, Clone)]
pub struct AdvanceResponse {
    pub finished: Option<SupportedFunctionReturnValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum AdvanceError {
    #[error("no writes supplied")]
    NoWrites,
    #[error("version mismatch: expected {expected}")]
    VersionMismatch { expected: Version },
    #[error("replay mismatch")]
    ReplayMismatch,
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
    // Errors from ReplayInternalError
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    #[error("executor closing")]
    ExecutorClosing,
}
impl From<ReplayInternalError> for AdvanceError {
    fn from(value: ReplayInternalError) -> Self {
        match value {
            ReplayInternalError::DbError(err) => Self::DbError(err),
            ReplayInternalError::LimitReached { reason, version } => {
                Self::LimitReached { reason, version }
            }
            ReplayInternalError::LockExpired(_) => {
                unreachable!(
                    "advance() asserts DeadlineTrackerFactoryForReplay, which never expires the lock"
                )
            }
            ReplayInternalError::ExecutorClosing(_) => Self::ExecutorClosing,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdvanceFromLogError {
    #[error("replay mismatch")]
    ReplayMismatch,
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
}
impl From<AdvanceFromLogError> for AdvanceError {
    fn from(value: AdvanceFromLogError) -> Self {
        match value {
            AdvanceFromLogError::DbError(db_err) => AdvanceError::DbError(db_err),
            AdvanceFromLogError::ReplayMismatch => AdvanceError::ReplayMismatch,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
    // Transient error
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    // Transient error
    #[error("executor closing")]
    ExecutorClosing,
    /// Replay failed.
    /// `captured_writes` is non-empty iff execution has not finished yet, and therefore can be advanced to an execution error.
    #[error("fatal error: {err}")]
    ReplayFailed {
        err: FatalError,
        captured_writes: Vec<CapturedDbWrite>,
    },
}

// Does not contain `FatalError`
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplayInternalError {
    #[error(transparent)]
    DbError(#[from] DbErrorWrite),
    #[error("limit reached: {reason}")]
    LimitReached { reason: String, version: Version },
    #[error("lock expired")]
    LockExpired(Version),
    #[error("executor closing")]
    ExecutorClosing(Version),
}
impl From<ReplayInternalError> for ReplayError {
    fn from(value: ReplayInternalError) -> Self {
        match value {
            ReplayInternalError::DbError(db_error_write) => Self::DbError(db_error_write),
            ReplayInternalError::LimitReached { reason, version } => {
                Self::LimitReached { reason, version }
            }
            ReplayInternalError::LockExpired(_) => {
                unreachable!(
                    "replay() asserts DeadlineTrackerFactoryForReplay, which never expires the lock"
                )
            }
            ReplayInternalError::ExecutorClosing(_) => Self::ExecutorClosing,
        }
    }
}

pub(crate) fn is_closing_join_next(req: &AppendRequest) -> bool {
    matches!(
        &req.event,
        ExecutionRequest::HistoryEvent {
            event: HistoryEvent::JoinNext { closing: true, .. },
        }
    )
}

#[derive(Debug, Clone)]
pub(crate) struct JoinSetCloseCancellations {
    /// Order based on creation. Must be cancelled in the reverse order.
    activity_and_delay_ids: Vec<JoinSetResponseId>,
    /// Signalled (`CancellationRequested`), not finished-cancelled; the driver closes them.
    cancellable_child_ids: Vec<ExecutionIdDerived>,
    pub(crate) cancelled_at: DateTime<Utc>,
}
impl JoinSetCloseCancellations {
    pub(crate) fn new(
        activity_and_delay_ids: Vec<JoinSetResponseId>,
        cancellable_child_ids: Vec<ExecutionIdDerived>,
        cancelled_at: DateTime<Utc>,
    ) -> JoinSetCloseCancellations {
        JoinSetCloseCancellations {
            activity_and_delay_ids,
            cancellable_child_ids,
            cancelled_at,
        }
    }

    pub(crate) fn iterate_in_cancellation_order(&self) -> impl Iterator<Item = &JoinSetResponseId> {
        self.activity_and_delay_ids.iter().rev()
    }

    pub(crate) fn cancellable_child_ids(&self) -> &[ExecutionIdDerived] {
        &self.cancellable_child_ids
    }
}

pub(crate) fn requested_write_matches_fresh_replay(
    requested: &CapturedDbWrite,
    fresh: &CapturedDbWrite,
) -> bool {
    normalize_captured_write_for_matching(requested.clone())
        == normalize_captured_write_for_matching(fresh.clone())
}

fn normalize_captured_write_for_matching(write: CapturedDbWrite) -> CapturedDbWrite {
    match write {
        CapturedDbWrite::Append {
            execution_id,
            version,
            req,
            backtraces: _,
        } => CapturedDbWrite::Append {
            execution_id,
            version,
            req: normalize_append_request_for_matching(req),
            backtraces: vec![],
        },
        CapturedDbWrite::AppendBatch {
            current_time: _,
            batch,
            execution_id,
            version,
            backtraces: _,
        } => CapturedDbWrite::AppendBatch {
            current_time: DateTime::UNIX_EPOCH,
            batch: batch
                .into_iter()
                .map(normalize_append_request_for_matching)
                .collect(),
            execution_id,
            version,
            backtraces: vec![],
        },
        CapturedDbWrite::AppendBatchCreateNewExecution {
            current_time: _,
            batch,
            execution_id,
            version,
            child_req,
            backtraces: _,
        } => CapturedDbWrite::AppendBatchCreateNewExecution {
            current_time: DateTime::UNIX_EPOCH,
            batch: batch
                .into_iter()
                .map(normalize_append_request_for_matching)
                .collect(),
            execution_id,
            version,
            child_req: child_req
                .into_iter()
                .map(normalize_create_request_for_matching)
                .collect(),
            backtraces: vec![],
        },
        CapturedDbWrite::AppendStubResponse {
            events,
            response,
            current_time: _,
            backtraces: _,
        } => CapturedDbWrite::AppendStubResponse {
            events: AppendEventsToExecution {
                execution_id: events.execution_id,
                version: events.version,
                batch: events
                    .batch
                    .into_iter()
                    .map(normalize_append_request_for_matching)
                    .collect(),
            },
            response: AppendResponseToExecution {
                parent_execution_id: response.parent_execution_id,
                created_at: DateTime::UNIX_EPOCH,
                join_set_id: response.join_set_id,
                child_execution_id: response.child_execution_id,
                finished_version: response.finished_version,
                result: response.result,
            },
            current_time: DateTime::UNIX_EPOCH,
            backtraces: vec![],
        },
        CapturedDbWrite::AppendFinished {
            execution_id,
            version,
            current_time: _,
            retval,
            parent,
        } => CapturedDbWrite::AppendFinished {
            execution_id,
            version,
            current_time: DateTime::UNIX_EPOCH,
            retval,
            parent,
        },
    }
}

fn normalize_append_request_for_matching(req: AppendRequest) -> AppendRequest {
    AppendRequest {
        created_at: DateTime::UNIX_EPOCH,
        event: normalize_execution_request_for_matching(req.event),
    }
}

fn normalize_create_request_for_matching(req: CreateRequest) -> CreateRequest {
    let CreateRequest {
        created_at: _,
        execution_id,
        ffqn,
        params,
        parent,
        scheduled_at: _,
        component_id,
        deployment_id,
        metadata,
        scheduled_by,
        paused: _, // Ignore for comparison, user's flag will make it to the database in `merge_requested_overrides_into_fresh_write`
    } = req;
    CreateRequest {
        created_at: DateTime::UNIX_EPOCH,
        execution_id,
        ffqn,
        params,
        parent,
        scheduled_at: DateTime::UNIX_EPOCH,
        component_id,
        deployment_id,
        metadata,
        scheduled_by,
        paused: false,
    }
}

fn normalize_execution_request_for_matching(req: ExecutionRequest) -> ExecutionRequest {
    match req {
        ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            scheduled_at: _,
            component_id,
            deployment_id,
            metadata,
            scheduled_by,
        } => ExecutionRequest::Created {
            ffqn,
            params,
            parent,
            scheduled_at: DateTime::UNIX_EPOCH,
            component_id,
            deployment_id,
            metadata,
            scheduled_by,
        },
        ExecutionRequest::Locked(mut locked) => {
            locked.lock_expires_at = DateTime::UNIX_EPOCH;
            ExecutionRequest::Locked(locked)
        }
        ExecutionRequest::Unlocked(mut unlocked) => {
            unlocked.unlocked_at = DateTime::UNIX_EPOCH;
            ExecutionRequest::Unlocked(unlocked)
        }
        ExecutionRequest::ComponentUpgradeFinished {
            component_digest,
            deployment_id,
            outcome,
        } => ExecutionRequest::ComponentUpgradeFinished {
            component_digest,
            deployment_id,
            outcome,
        },
        ExecutionRequest::TemporarilyFailed {
            backoff_expires_at: _,
            reason,
            detail,
            http_client_traces,
        } => ExecutionRequest::TemporarilyFailed {
            backoff_expires_at: DateTime::UNIX_EPOCH,
            reason,
            detail,
            http_client_traces,
        },
        ExecutionRequest::TemporarilyTimedOut {
            backoff_expires_at: _,
            http_client_traces,
        } => ExecutionRequest::TemporarilyTimedOut {
            backoff_expires_at: DateTime::UNIX_EPOCH,
            http_client_traces,
        },
        ExecutionRequest::Finished {
            retval,
            http_client_traces,
        } => ExecutionRequest::Finished {
            retval,
            http_client_traces,
        },
        ExecutionRequest::HistoryEvent { event } => ExecutionRequest::HistoryEvent {
            event: normalize_history_event_for_matching(event),
        },
        ExecutionRequest::Paused => ExecutionRequest::Paused,
        ExecutionRequest::Unpaused => ExecutionRequest::Unpaused,
        ExecutionRequest::CancellationRequested => ExecutionRequest::CancellationRequested,
    }
}

fn normalize_history_event_for_matching(event: HistoryEvent) -> HistoryEvent {
    match event {
        HistoryEvent::Persist { value, kind } => HistoryEvent::Persist { value, kind },
        HistoryEvent::JoinSetCreate { join_set_id } => HistoryEvent::JoinSetCreate { join_set_id },
        HistoryEvent::JoinSetRequest {
            join_set_id,
            request,
        } => HistoryEvent::JoinSetRequest {
            join_set_id,
            request: normalize_join_set_request_for_matching(request),
        },
        HistoryEvent::JoinNext {
            join_set_id,
            run_expires_at: _,
            requested_ffqn,
            closing,
        } => HistoryEvent::JoinNext {
            join_set_id,
            run_expires_at: DateTime::UNIX_EPOCH,
            requested_ffqn,
            closing,
        },
        HistoryEvent::JoinNextTry {
            join_set_id,
            outcome,
        } => HistoryEvent::JoinNextTry {
            join_set_id,
            outcome,
        },
        HistoryEvent::JoinNextTooMany {
            join_set_id,
            requested_ffqn,
        } => HistoryEvent::JoinNextTooMany {
            join_set_id,
            requested_ffqn,
        },
        HistoryEvent::Schedule {
            execution_id,
            schedule_at,
            result,
        } => HistoryEvent::Schedule {
            execution_id,
            schedule_at: normalize_schedule_at_for_matching(schedule_at),
            result,
        },
        HistoryEvent::Stub {
            target_execution_id,
            retval_hash,
            result,
        } => HistoryEvent::Stub {
            target_execution_id,
            retval_hash,
            result,
        },
    }
}

fn normalize_join_set_request_for_matching(request: JoinSetRequest) -> JoinSetRequest {
    match request {
        JoinSetRequest::DelayRequest {
            delay_id,
            expires_at: _,
            schedule_at,
            paused: _, // Ignore for comparison, user's flag will make it to the database in `merge_requested_overrides_into_fresh_write`
        } => JoinSetRequest::DelayRequest {
            delay_id,
            expires_at: DateTime::UNIX_EPOCH,
            schedule_at: normalize_schedule_at_for_matching(schedule_at),
            paused: false,
        },
        JoinSetRequest::ChildExecutionRequest {
            child_execution_id,
            target_ffqn,
            params,
            result,
        } => JoinSetRequest::ChildExecutionRequest {
            child_execution_id,
            target_ffqn,
            params,
            result,
        },
    }
}

fn normalize_schedule_at_for_matching(
    schedule_at: HistoryEventScheduleAt,
) -> HistoryEventScheduleAt {
    match schedule_at {
        HistoryEventScheduleAt::Now => HistoryEventScheduleAt::Now,
        HistoryEventScheduleAt::At(_) => HistoryEventScheduleAt::At(DateTime::UNIX_EPOCH),
        HistoryEventScheduleAt::In(duration) => HistoryEventScheduleAt::In(duration),
    }
}

pub(crate) fn merge_requested_overrides_into_fresh_prefix(
    requested: &[CapturedDbWrite],
    fresh_replay: &[InternalCapturedWrite],
) -> Vec<InternalCapturedWrite> {
    requested
        .iter()
        .zip(fresh_replay.iter())
        .map(|(requested, fresh)| merge_requested_overrides_into_fresh_write(requested, fresh))
        .collect()
}

pub(crate) fn merge_requested_overrides_into_fresh_write(
    requested: &CapturedDbWrite,
    fresh: &InternalCapturedWrite,
) -> InternalCapturedWrite {
    match (requested, &fresh.write) {
        (
            CapturedDbWrite::AppendBatchCreateNewExecution {
                child_req: requested_child_req,
                batch: requested_batch,
                ..
            },
            CapturedDbWrite::AppendBatchCreateNewExecution { child_req: _, .. },
        ) => {
            let mut merged = fresh.clone();
            let CapturedDbWrite::AppendBatchCreateNewExecution {
                child_req: merged_child_req,
                batch: merged_batch,
                ..
            } = &mut merged.write
            else {
                unreachable!("matched variant must stay matched")
            };
            for (requested, fresh) in requested_child_req.iter().zip(merged_child_req.iter_mut()) {
                fresh.paused = requested.paused; // Allow users to specify the paused behavior.
            }
            merge_delay_paused_flags(requested_batch, merged_batch);
            merged
        }
        (
            CapturedDbWrite::Append {
                req: requested_req, ..
            },
            CapturedDbWrite::Append { .. },
        ) => {
            let mut merged = fresh.clone();
            let CapturedDbWrite::Append {
                req: merged_req, ..
            } = &mut merged.write
            else {
                unreachable!("matched variant must stay matched")
            };
            merge_delay_paused_flag(requested_req, merged_req);
            merged
        }
        (
            CapturedDbWrite::AppendBatch {
                batch: requested_batch,
                ..
            },
            CapturedDbWrite::AppendBatch { .. },
        ) => {
            let mut merged = fresh.clone();
            let CapturedDbWrite::AppendBatch {
                batch: merged_batch,
                ..
            } = &mut merged.write
            else {
                unreachable!("matched variant must stay matched")
            };
            merge_delay_paused_flags(requested_batch, merged_batch);
            merged
        }
        _ => fresh.clone(),
    }
}

fn merge_delay_paused_flags(requested: &[AppendRequest], merged: &mut [AppendRequest]) {
    for (requested, merged) in requested.iter().zip(merged.iter_mut()) {
        merge_delay_paused_flag(requested, merged);
    }
}

fn merge_delay_paused_flag(requested: &AppendRequest, merged: &mut AppendRequest) {
    if let (
        ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinSetRequest {
                    request:
                        JoinSetRequest::DelayRequest {
                            paused: requested_paused,
                            ..
                        },
                    ..
                },
        },
        ExecutionRequest::HistoryEvent {
            event:
                HistoryEvent::JoinSetRequest {
                    request:
                        JoinSetRequest::DelayRequest {
                            paused: merged_paused,
                            ..
                        },
                    ..
                },
        },
    ) = (&requested.event, &mut merged.event)
    {
        *merged_paused = *requested_paused;
    }
}
