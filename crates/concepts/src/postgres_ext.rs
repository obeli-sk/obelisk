use crate::storage::{
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
};
use tokio_postgres::error::SqlState;
use tracing::{error, warn};

impl From<tokio_postgres::Error> for DbErrorGeneric {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> DbErrorGeneric {
        let loc = std::panic::Location::caller();
        let (loc_file, loc_line) = (loc.file(), loc.line());
        error!(loc_file, loc_line, "Postgres error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<tokio_postgres::Error> for DbErrorRead {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> Self {
        let err = err.to_string();
        if err == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorRead::NotFound
        } else {
            let loc = std::panic::Location::caller();
            let (loc_file, loc_line) = (loc.file(), loc.line());
            warn!(loc_file, loc_line, "Postgres error {err:?}");
            DbErrorRead::from(DbErrorGeneric::Uncategorized(err.into()))
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
        let loc = std::panic::Location::caller();
        let (loc_file, loc_line) = (loc.file(), loc.line());
        warn!(loc_file, loc_line, "Postgres error {err:?}");
        let err = err.to_string();
        if err == "query returned an unexpected number of rows" {
            // Refactor after https://github.com/rust-postgres/rust-postgres/pull/1185 Make error::Kind public
            DbErrorWrite::NotFound
        } else {
            DbErrorWrite::from(DbErrorGeneric::Uncategorized(err.into()))
        }
    }
}

impl From<deadpool_postgres::PoolError> for DbErrorGeneric {
    #[track_caller]
    fn from(err: deadpool_postgres::PoolError) -> DbErrorGeneric {
        match err {
            deadpool_postgres::PoolError::Backend(err) => DbErrorGeneric::from(err),
            deadpool_postgres::PoolError::Closed => DbErrorGeneric::Close,
            other => {
                let loc = std::panic::Location::caller();
                let (loc_file, loc_line) = (loc.file(), loc.line());
                warn!(
                    loc_file,
                    loc_line, "Cannot get connection from pool: {other:?}"
                );
                DbErrorGeneric::Uncategorized(other.to_string().into())
            }
        }
    }
}
