use concepts::cas::CasError;
use concepts::storage::{
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorStubResponse, DbErrorWrite,
    DbErrorWriteNonRetriable, ResponseSubscriptionEnd, SubscribeToResponsesError, TimeoutOutcome,
    Version,
};
use serde::{Deserialize, Serialize};
use std::panic::Location;
use tracing_error::SpanTrace;

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub enum WireError {
    Closed,
    Generic(String),
    NotFound,
    ValidationFailed(String),
    Conflict,
    AlreadyFinished,
    IllegalState(String),
    UnlockedCannotBeAppended(String),
    VersionConflict {
        expected: Version,
        requested: Version,
    },
    StubConflict,
    SubscriptionEnded(ResponseSubscriptionEnd),
    Timeout(TimeoutOutcome),
    Cas(String),
}

pub(crate) fn generic(reason: impl Into<String>) -> DbErrorGeneric {
    DbErrorGeneric::Uncategorized {
        reason: reason.into().into(),
        context: SpanTrace::capture(),
        source: None,
        loc: Location::caller(),
    }
}

impl From<DbErrorGeneric> for WireError {
    fn from(err: DbErrorGeneric) -> Self {
        match err {
            DbErrorGeneric::Close => Self::Closed,
            DbErrorGeneric::Uncategorized { reason, .. } => Self::Generic(reason.to_string()),
        }
    }
}
impl From<WireError> for DbErrorGeneric {
    fn from(err: WireError) -> Self {
        match err {
            WireError::Closed => Self::Close,
            WireError::Generic(reason) => generic(reason),
            other => generic(format!("unexpected storage error: {other:?}")),
        }
    }
}
impl From<DbErrorWrite> for WireError {
    fn from(err: DbErrorWrite) -> Self {
        match err {
            DbErrorWrite::NotFound => Self::NotFound,
            DbErrorWrite::Generic(err) => err.into(),
            DbErrorWrite::NonRetriable(err) => match err {
                DbErrorWriteNonRetriable::ValidationFailed(reason) => {
                    Self::ValidationFailed(reason.to_string())
                }
                DbErrorWriteNonRetriable::Conflict => Self::Conflict,
                DbErrorWriteNonRetriable::AlreadyFinished => Self::AlreadyFinished,
                DbErrorWriteNonRetriable::IllegalState { reason, .. } => {
                    Self::IllegalState(reason.to_string())
                }
                DbErrorWriteNonRetriable::UnlockedCannotBeAppended(state) => {
                    Self::UnlockedCannotBeAppended(state.to_owned())
                }
                DbErrorWriteNonRetriable::VersionConflict {
                    expected,
                    requested,
                } => Self::VersionConflict {
                    expected,
                    requested,
                },
            },
        }
    }
}
impl From<WireError> for DbErrorWrite {
    fn from(err: WireError) -> Self {
        let non_retriable = match err {
            WireError::NotFound => return Self::NotFound,
            WireError::ValidationFailed(reason) => {
                DbErrorWriteNonRetriable::ValidationFailed(reason.into())
            }
            WireError::Conflict => DbErrorWriteNonRetriable::Conflict,
            WireError::AlreadyFinished => DbErrorWriteNonRetriable::AlreadyFinished,
            WireError::IllegalState(reason) => DbErrorWriteNonRetriable::IllegalState {
                reason: reason.into(),
                context: SpanTrace::capture(),
                source: None,
                loc: Location::caller(),
            },
            WireError::UnlockedCannotBeAppended(state) => match state.as_str() {
                "pending" => DbErrorWriteNonRetriable::UnlockedCannotBeAppended("pending"),
                "paused" => DbErrorWriteNonRetriable::UnlockedCannotBeAppended("paused"),
                "cancelling" => DbErrorWriteNonRetriable::UnlockedCannotBeAppended("cancelling"),
                _ => return Self::Generic(generic(format!("unknown execution state: {state}"))),
            },
            WireError::VersionConflict {
                expected,
                requested,
            } => DbErrorWriteNonRetriable::VersionConflict {
                expected,
                requested,
            },
            other => return Self::Generic(other.into()),
        };
        Self::NonRetriable(non_retriable)
    }
}
impl From<DbErrorRead> for WireError {
    fn from(err: DbErrorRead) -> Self {
        match err {
            DbErrorRead::NotFound => Self::NotFound,
            DbErrorRead::Generic(err) => err.into(),
        }
    }
}
impl From<WireError> for DbErrorRead {
    fn from(err: WireError) -> Self {
        match err {
            WireError::NotFound => Self::NotFound,
            other => Self::Generic(other.into()),
        }
    }
}
impl From<DbErrorStubResponse> for WireError {
    fn from(err: DbErrorStubResponse) -> Self {
        match err {
            DbErrorStubResponse::StubConflict => Self::StubConflict,
            DbErrorStubResponse::Write(err) => err.into(),
        }
    }
}
impl From<WireError> for DbErrorStubResponse {
    fn from(err: WireError) -> Self {
        match err {
            WireError::StubConflict => Self::StubConflict,
            other => Self::Write(other.into()),
        }
    }
}
impl From<SubscribeToResponsesError> for WireError {
    fn from(err: SubscribeToResponsesError) -> Self {
        match err {
            SubscribeToResponsesError::SubscriptionEnded(reason) => Self::SubscriptionEnded(reason),
            SubscribeToResponsesError::DbErrorRead(err) => err.into(),
        }
    }
}
impl From<WireError> for SubscribeToResponsesError {
    fn from(err: WireError) -> Self {
        match err {
            WireError::SubscriptionEnded(reason) => Self::SubscriptionEnded(reason),
            other => Self::DbErrorRead(other.into()),
        }
    }
}
impl From<DbErrorReadWithTimeout> for WireError {
    fn from(err: DbErrorReadWithTimeout) -> Self {
        match err {
            DbErrorReadWithTimeout::Timeout(reason) => Self::Timeout(reason),
            DbErrorReadWithTimeout::DbErrorRead(err) => err.into(),
        }
    }
}
impl From<WireError> for DbErrorReadWithTimeout {
    fn from(err: WireError) -> Self {
        match err {
            WireError::Timeout(reason) => Self::Timeout(reason),
            other => Self::DbErrorRead(other.into()),
        }
    }
}
impl From<CasError> for WireError {
    fn from(err: CasError) -> Self {
        match err {
            CasError::Uncategorized(reason) => Self::Cas(reason),
        }
    }
}
impl From<WireError> for CasError {
    fn from(err: WireError) -> Self {
        match err {
            WireError::Cas(reason) => Self::Uncategorized(reason),
            other => Self::Uncategorized(format!("storage error: {other:?}")),
        }
    }
}
