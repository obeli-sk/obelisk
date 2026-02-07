//! Combined state types representing execution state from the database.

use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionId, FunctionFqn, JoinSetId,
    component_id::InputContentDigest,
    prefixed_ulid::{DeploymentId, ExecutorId, RunId},
    storage::{
        DbErrorGeneric, DbErrorWrite, DbErrorWriteNonRetriable, ExecutionWithState, LockedBy,
        PendingState, PendingStateBlockedByJoinSet, PendingStateFinished,
        PendingStateFinishedResultKind, PendingStateLocked, PendingStatePaused,
        PendingStatePendingAt, STATE_BLOCKED_BY_JOIN_SET, STATE_FINISHED, STATE_LOCKED,
        STATE_PENDING_AT, Version,
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
    pub component_digest: InputContentDigest,
    pub component_type: ComponentType,
    pub deployment_id: DeploymentId,
    pub created_at: DateTime<Utc>,
    pub first_scheduled_at: DateTime<Utc>,
    pub pending_expires_finished: DateTime<Utc>,
    pub is_paused: bool,
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

    #[track_caller]
    pub fn get_next_version_fail_if_finished(&self) -> Result<Version, DbErrorWrite> {
        if self.execution_with_state.pending_state.is_finished() {
            debug!("Execution is already finished");
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState {
                    reason: "already finished".into(),
                    context: SpanTrace::capture(),
                    source: None,
                    loc: Location::caller(),
                },
            ));
        }
        Ok(self.corresponding_version.increment())
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
                is_paused: false,
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
                is_paused: false,
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
                is_paused: false,
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
                is_paused: false,
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
                is_paused: false,
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
                is_paused: true,
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
                is_paused: true,
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
            // Paused - Locked
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
                is_paused: true,
            } if state == STATE_LOCKED => ExecutionWithState {
                component_digest,
                component_type,
                deployment_id,
                execution_id,
                ffqn,
                created_at,
                first_scheduled_at,
                pending_state: PendingState::Paused(PendingStatePaused::Locked(
                    PendingStateLocked {
                        locked_by: LockedBy {
                            executor_id,
                            run_id,
                        },
                        lock_expires_at,
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
                is_paused: true,
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
