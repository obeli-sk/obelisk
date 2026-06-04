use crate::workflow::{
    host_exports::response_id::ResponseId, replay_db_proxy::InternalCapturedWrite,
};
use chrono::{DateTime, Utc};
use concepts::storage::{
    AppendEventsToExecution, AppendRequest, AppendResponseToExecution, CapturedDbWrite,
    CreateRequest, ExecutionRequest, HistoryEvent, HistoryEventScheduleAt, JoinSetRequest,
};

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
    activity_and_delay_ids: Vec<ResponseId>,
    pub(crate) cancelled_at: DateTime<Utc>,
}
impl JoinSetCloseCancellations {
    pub(crate) fn new(
        activity_and_delay_ids: Vec<ResponseId>,
        cancelled_at: DateTime<Utc>,
    ) -> JoinSetCloseCancellations {
        JoinSetCloseCancellations {
            activity_and_delay_ids,
            cancelled_at,
        }
    }

    pub(crate) fn iterate_in_cancellation_order(&self) -> impl Iterator<Item = &ResponseId> {
        self.activity_and_delay_ids.iter().rev()
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
        ExecutionRequest::Unlocked {
            backoff_expires_at: _,
            reason,
        } => ExecutionRequest::Unlocked {
            backoff_expires_at: DateTime::UNIX_EPOCH,
            reason,
        },
        ExecutionRequest::ComponentUpgraded {
            component_digest,
            deployment_id,
            reason,
        } => ExecutionRequest::ComponentUpgraded {
            component_digest,
            deployment_id,
            reason,
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
