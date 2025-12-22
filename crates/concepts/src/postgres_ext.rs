use tokio_postgres::error::SqlState;
use tracing::warn;

use crate::storage::{
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
};

impl From<tokio_postgres::Error> for DbErrorGeneric {
    fn from(err: tokio_postgres::Error) -> DbErrorGeneric {
        warn!(backtrace = %std::backtrace::Backtrace::capture(), "Postgres error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<tokio_postgres::Error> for DbErrorRead {
    fn from(err: tokio_postgres::Error) -> Self {
        warn!(backtrace = %std::backtrace::Backtrace::capture(), "Postgres error {err:?}");
        let err = err.to_string();
        if err == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorRead::NotFound
        } else {
            DbErrorRead::from(DbErrorGeneric::Uncategorized(err.into()))
        }
    }
}

impl From<tokio_postgres::Error> for DbErrorReadWithTimeout {
    fn from(err: tokio_postgres::Error) -> Self {
        Self::from(DbErrorRead::from(err))
    }
}

impl From<tokio_postgres::Error> for DbErrorWrite {
    fn from(err: tokio_postgres::Error) -> Self {
        // Check for specific SQL State codes (e.g., Unique Violation)
        if let Some(code) = err.code()
            && *code == SqlState::UNIQUE_VIOLATION
        {
            return DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict);
        }
        warn!(backtrace = %std::backtrace::Backtrace::capture(), "Postgres error {err:?}");
        let err = err.to_string();
        if err == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorWrite::NotFound
        } else {
            DbErrorWrite::from(DbErrorGeneric::Uncategorized(err.into()))
        }
    }
}
