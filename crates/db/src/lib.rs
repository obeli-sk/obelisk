use std::time::Duration;

use assert_matches::assert_matches;
use concepts::{
    prefixed_ulid::JoinSetId, ExecutionId, Params, StrVariant, SupportedFunctionResult,
};
use storage::{journal::PendingState, ExecutionEvent, Version};

use crate::storage::ExecutionEventInner;

pub mod storage;

#[derive(Debug)]
pub struct ExecutionHistory {
    pub execution_id: ExecutionId,
    pub events: Vec<ExecutionEvent>,
    pub version: Version,
    pub pending_state: PendingState,
}

impl ExecutionHistory {
    fn already_retried_count(&self) -> u32 {
        self.events
            .iter()
            .filter(|event| event.event.is_retry())
            .count() as u32
    }

    pub fn can_be_retried_after(&self) -> Option<Duration> {
        let already_retried_count = self.already_retried_count();
        if already_retried_count < self.max_retries() {
            let duration = self.retry_exp_backoff() * 2_u32.saturating_pow(already_retried_count);
            Some(duration)
        } else {
            None
        }
    }

    pub fn retry_exp_backoff(&self) -> Duration {
        assert_matches!(self.events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { retry_exp_backoff, .. },
            ..
        }) => *retry_exp_backoff)
    }

    pub fn max_retries(&self) -> u32 {
        assert_matches!(self.events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { max_retries, .. },
            ..
        }) => *max_retries)
    }

    pub fn params(&self) -> Params {
        assert_matches!(self.events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { params, .. },
            ..
        }) => params.clone())
    }

    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        assert_matches!(self.events.get(0), Some(ExecutionEvent {
            event: ExecutionEventInner::Created { parent, .. },
            ..
        }) => parent.clone())
    }

    pub fn last_event(&self) -> &ExecutionEvent {
        self.events.last().expect("must contain at least one event")
    }
}

pub type FinishedExecutionResult = Result<SupportedFunctionResult, FinishedExecutionError>;

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub enum FinishedExecutionError {
    #[error("permanent timeout")]
    PermanentTimeout,
    #[error("non-determinism detected, reason: `{0}`")]
    NonDeterminismDetected(StrVariant),
    #[error("uncategorized error: `{0}`")]
    PermanentFailure(StrVariant), // intermittent failure that is not retried (anymore)
    #[error("cancelled, reason: `{0}`")]
    Cancelled(StrVariant),
    #[error("continuing as {execution_id}")]
    ContinueAsNew { execution_id: ExecutionId },
}
