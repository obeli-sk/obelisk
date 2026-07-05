//! Combined state types representing execution state from the database.

use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionId, FunctionFqn, JoinSetId,
    component_id::ComponentDigest,
    prefixed_ulid::{DeploymentId, ExecutorId, RunId},
    storage::{
        CancelOutcome, DbErrorGeneric, DbErrorWrite, DbErrorWriteNonRetriable, ExecutionWithState,
        Lifecycle, LockedBy, PendingState, PendingStateBlockedByJoinSet, PendingStateCancelling,
        PendingStateFinished, PendingStateFinishedResultKind, PendingStateLocked,
        PendingStatePaused, PendingStatePendingAt, STATE_BLOCKED_BY_JOIN_SET, STATE_FINISHED,
        STATE_LOCKED, STATE_PENDING_AT, Version,
    },
};
use std::panic::Location;
use tracing::debug;
use tracing_error::SpanTrace;

/// DTO for extracting execution state from `t_state` table.
#[derive(Debug)]
pub struct CombinedStateDTO {
    pub execution_id: ExecutionId,
    pub state: String,
    pub ffqn: FunctionFqn,
    pub component_digest: ComponentDigest,
    pub component_type: ComponentType,
    pub deployment_id: DeploymentId,
    pub created_at: DateTime<Utc>,
    pub first_scheduled_at: DateTime<Utc>,
    pub pending_expires_finished: DateTime<Utc>,
    pub lifecycle: Lifecycle,
    // Locked:
    pub last_lock_version: Option<Version>,
    pub executor_id: Option<ExecutorId>,
    pub run_id: Option<RunId>,
    // Blocked by join set:
    pub join_set_id: Option<JoinSetId>,
    pub join_set_closing: Option<bool>,
    // Finished:
    pub result_kind: Option<PendingStateFinishedResultKind>,
}

/// Combined execution state with version information.
#[derive(Debug)]
pub struct CombinedState {
    pub execution_with_state: ExecutionWithState,
    pub corresponding_version: Version,
}

impl CombinedState {
    #[must_use]
    pub fn get_next_version_assert_not_finished(&self) -> Version {
        assert!(!self.execution_with_state.pending_state.is_finished());
        self.corresponding_version.increment()
    }

    pub fn get_next_version_fail_if_finished(&self) -> Result<Version, DbErrorWrite> {
        if self.execution_with_state.pending_state.is_finished() {
            debug!("Execution is already finished");
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::AlreadyFinished,
            ));
        }
        Ok(self.corresponding_version.increment())
    }

    /// Outcome to return without appending `CancellationRequested`: cancellation is
    /// already underway or the execution already finished. `None` means the append
    /// should proceed; no state needs releasing first, since paused executions are
    /// never running and a locked workflow's run is fenced by the version bump.
    #[must_use]
    pub fn cancel_short_circuit(&self) -> Option<CancelOutcome> {
        match &self.execution_with_state.pending_state {
            PendingState::Finished(_) => Some(CancelOutcome::AlreadyFinished),
            PendingState::Cancelling(_) => Some(CancelOutcome::AlreadyCancelling),
            _ => None,
        }
    }

    /// `pause_execution` guard: disallow transitioning to `Paused`
    /// if an activity can still be in-flight.
    pub fn reject_locked_activities(&self) -> Result<bool, DbErrorWrite> {
        let locked = matches!(
            self.execution_with_state.pending_state,
            PendingState::Locked(_)
        );
        if locked && self.execution_with_state.component_type.is_activity() {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "cannot pause a running activity; cancel it instead".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            ));
        }
        Ok(locked)
    }

    /// `cancel_workflow` guard rejecting a non-cancellable target. Control-plane
    /// entrypoint only; the worker/driver paths skip it, having already classified
    /// the target as cancellable.
    pub fn assert_cancellable_workflow_ffqn(&self) -> Result<(), DbErrorWrite> {
        if self.execution_with_state.ffqn.is_cancellable() {
            Ok(())
        } else {
            Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "cannot cancel, execution is not a cancellable workflow".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            ))
        }
    }

    #[must_use]
    pub fn get_next_version_or_finished(&self) -> Version {
        if self.execution_with_state.pending_state.is_finished() {
            self.corresponding_version.clone()
        } else {
            self.corresponding_version.increment()
        }
    }

    /// Create a `CombinedState` from a DTO and version.
    ///
    /// Returns an error if the DTO fields don't match a valid state pattern.
    pub fn new(
        dto: CombinedStateDTO,
        corresponding_version: Version,
    ) -> Result<Self, DbErrorGeneric> {
        let execution_with_state = match dto {
            // Pending - just created
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Active,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::PendingAt(PendingStatePendingAt {
                    scheduled_at,
                    last_lock: None,
                }),
            },
            // Pending, previously locked
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Active,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::PendingAt(PendingStatePendingAt {
                    scheduled_at,
                    last_lock: Some(LockedBy {
                        executor_id,
                        run_id,
                    }),
                }),
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: lock_expires_at,
                last_lock_version: Some(_),
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Active,
            } if state == STATE_LOCKED => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Locked(PendingStateLocked {
                    locked_by: LockedBy {
                        executor_id,
                        run_id,
                    },
                    lock_expires_at,
                }),
            },
            // Cancelling - Locked (running activity teardown pending)
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: lock_expires_at,
                last_lock_version: Some(_),
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Cancelling,
            } if state == STATE_LOCKED => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Cancelling(PendingStateCancelling::Locked(
                    PendingStateLocked {
                        locked_by: LockedBy {
                            executor_id,
                            run_id,
                        },
                        lock_expires_at,
                    },
                )),
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: lock_expires_at,
                last_lock_version: None,
                executor_id: _,
                run_id: _,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
                lifecycle: Lifecycle::Active,
            } if state == STATE_BLOCKED_BY_JOIN_SET => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::BlockedByJoinSet(PendingStateBlockedByJoinSet {
                    join_set_id: join_set_id.clone(),
                    closing: join_set_closing,
                    lock_expires_at,
                }),
            },
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: finished_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: Some(result_kind),
                lifecycle: Lifecycle::Active,
            } if state == STATE_FINISHED => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Finished(PendingStateFinished {
                    finished_at,
                    version: corresponding_version.0,
                    result_kind,
                }),
            },
            // Paused - PendingAt (just created)
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Paused,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Paused(PendingStatePaused::PendingAt(
                    PendingStatePendingAt {
                        scheduled_at,
                        last_lock: None,
                    },
                )),
            },
            // Paused - PendingAt (previously locked)
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Paused,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Paused(PendingStatePaused::PendingAt(
                    PendingStatePendingAt {
                        scheduled_at,
                        last_lock: Some(LockedBy {
                            executor_id,
                            run_id,
                        }),
                    },
                )),
            },
            // Paused - BlockedByJoinSet
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: lock_expires_at,
                last_lock_version: None,
                executor_id: _,
                run_id: _,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
                lifecycle: Lifecycle::Paused,
            } if state == STATE_BLOCKED_BY_JOIN_SET => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Paused(PendingStatePaused::BlockedByJoinSet(
                    PendingStateBlockedByJoinSet {
                        join_set_id: join_set_id.clone(),
                        closing: join_set_closing,
                        lock_expires_at,
                    },
                )),
            },
            // Cancelling - PendingAt (just created)
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: None,
                run_id: None,
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Cancelling,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Cancelling(PendingStateCancelling::PendingAt(
                    PendingStatePendingAt {
                        scheduled_at,
                        last_lock: None,
                    },
                )),
            },
            // Cancelling - PendingAt (previously locked)
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: scheduled_at,
                last_lock_version: None,
                executor_id: Some(executor_id),
                run_id: Some(run_id),
                join_set_id: None,
                join_set_closing: None,
                result_kind: None,
                lifecycle: Lifecycle::Cancelling,
            } if state == STATE_PENDING_AT => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Cancelling(PendingStateCancelling::PendingAt(
                    PendingStatePendingAt {
                        scheduled_at,
                        last_lock: Some(LockedBy {
                            executor_id,
                            run_id,
                        }),
                    },
                )),
            },
            // Cancelling - BlockedByJoinSet
            CombinedStateDTO {
                execution_id,
                created_at,
                first_scheduled_at,
                state,
                ffqn,
                component_digest,
                component_type,
                deployment_id,
                pending_expires_finished: lock_expires_at,
                last_lock_version: None,
                executor_id: _,
                run_id: _,
                join_set_id: Some(join_set_id),
                join_set_closing: Some(join_set_closing),
                result_kind: None,
                lifecycle: Lifecycle::Cancelling,
            } if state == STATE_BLOCKED_BY_JOIN_SET => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Cancelling(PendingStateCancelling::BlockedByJoinSet(
                    PendingStateBlockedByJoinSet {
                        join_set_id: join_set_id.clone(),
                        closing: join_set_closing,
                        lock_expires_at,
                    },
                )),
            },
            dto => {
                tracing::error!("Cannot deserialize pending state from {dto:?}");
                return Err(DbErrorGeneric::Uncategorized {
                    reason: "invalid `t_state`".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                });
            }
        };
        Ok(Self {
            execution_with_state,
            corresponding_version,
        })
    }
}
