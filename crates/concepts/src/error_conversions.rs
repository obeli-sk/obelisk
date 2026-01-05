use crate::{
    JoinSetIdParseError,
    prefixed_ulid::{DerivedIdParseError, ExecutionIdParseError, PrefixedUlidParseError},
    storage::{
        DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, VersionParseError,
    },
};
use std::{panic::Location, sync::Arc};
use tracing_error::SpanTrace;

impl From<DbErrorRead> for DbErrorWrite {
    fn from(value: DbErrorRead) -> DbErrorWrite {
        match value {
            DbErrorRead::NotFound => DbErrorWrite::NotFound,
            DbErrorRead::Generic(err) => DbErrorWrite::Generic(err),
        }
    }
}

impl From<PrefixedUlidParseError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: PrefixedUlidParseError) -> Self {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<JoinSetIdParseError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: JoinSetIdParseError) -> Self {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<ExecutionIdParseError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: ExecutionIdParseError) -> Self {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<VersionParseError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: VersionParseError) -> Self {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<VersionParseError> for DbErrorRead {
    fn from(err: VersionParseError) -> Self {
        DbErrorGeneric::from(err).into()
    }
}

impl From<DerivedIdParseError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: DerivedIdParseError) -> Self {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<DbErrorGeneric> for DbErrorReadWithTimeout {
    fn from(value: DbErrorGeneric) -> DbErrorReadWithTimeout {
        Self::from(DbErrorRead::from(value))
    }
}
