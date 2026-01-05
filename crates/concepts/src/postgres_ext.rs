use crate::storage::{
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
};
use std::{panic::Location, sync::Arc};
use tokio_postgres::error::SqlState;
use tracing_error::SpanTrace;

impl From<tokio_postgres::Error> for DbErrorGeneric {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> DbErrorGeneric {
        DbErrorGeneric::Uncategorized {
            reason: err.to_string().into(),
            context: SpanTrace::capture(),
            source: Some(Arc::new(err)),
            loc: Location::caller(),
        }
    }
}

impl From<tokio_postgres::Error> for DbErrorRead {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> Self {
        let err_str = err.to_string();
        if err_str == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorRead::NotFound
        } else {
            DbErrorRead::from(DbErrorGeneric::from(err))
        }
    }
}

impl From<tokio_postgres::Error> for DbErrorReadWithTimeout {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> Self {
        Self::from(DbErrorRead::from(err))
    }
}

impl From<tokio_postgres::Error> for DbErrorWrite {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> Self {
        // Check for specific SQL State codes (e.g., Unique Violation)
        if let Some(code) = err.code()
            && *code == SqlState::UNIQUE_VIOLATION
        {
            return DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict);
        }
        let err_str = err.to_string();
        if err_str == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorWrite::NotFound
        } else {
            DbErrorWrite::from(DbErrorGeneric::from(err))
        }
    }
}

impl From<deadpool_postgres::PoolError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: deadpool_postgres::PoolError) -> DbErrorGeneric {
        match err {
            deadpool_postgres::PoolError::Closed => DbErrorGeneric::Close,
            err => DbErrorGeneric::from(err),
        }
    }
}
