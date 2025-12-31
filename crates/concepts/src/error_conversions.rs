use tracing::error;

use crate::{
    JoinSetIdParseError,
    prefixed_ulid::{DerivedIdParseError, ExecutionIdParseError, PrefixedUlidParseError},
    storage::{
        DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, VersionParseError,
    },
};

impl From<DbErrorRead> for DbErrorWrite {
    fn from(value: DbErrorRead) -> DbErrorWrite {
        match value {
            DbErrorRead::NotFound => DbErrorWrite::NotFound,
            DbErrorRead::Generic(err) => DbErrorWrite::Generic(err),
        }
    }
}

impl From<PrefixedUlidParseError> for DbErrorGeneric {
    fn from(err: PrefixedUlidParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<JoinSetIdParseError> for DbErrorGeneric {
    fn from(err: JoinSetIdParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<ExecutionIdParseError> for DbErrorGeneric {
    fn from(err: ExecutionIdParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<VersionParseError> for DbErrorGeneric {
    fn from(err: VersionParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<VersionParseError> for DbErrorRead {
    fn from(err: VersionParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into()).into()
    }
}

impl From<DerivedIdParseError> for DbErrorGeneric {
    fn from(err: DerivedIdParseError) -> Self {
        error!("Cannot convert: {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}

impl From<DbErrorGeneric> for DbErrorReadWithTimeout {
    fn from(value: DbErrorGeneric) -> DbErrorReadWithTimeout {
        Self::from(DbErrorRead::from(value))
    }
}
