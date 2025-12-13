use crate::{
    ContentDigest, ExecutionId, HashType, InputContentDigest, JoinSetId,
    prefixed_ulid::{DelayId, ExecutionIdDerived, ExecutorId, RunId},
    storage::{DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite},
};
use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use tracing::error;

impl ToSql for ExecutionId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ExecutionId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<ExecutionId>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to ExecutionId value:`{str}` - {err:?}"
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for ExecutionIdDerived {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ExecutionIdDerived {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<ExecutionIdDerived>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to ExecutionIdDerived value:`{str}` - {err:?}"
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for JoinSetId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for JoinSetId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<JoinSetId>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to JoinSetId value:`{str}` - {err:?}"
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for DelayId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for DelayId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<Self>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to {} value:`{str}` - {err:?}", std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for RunId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for RunId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<Self>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to {} value:`{str}` - {err:?}", std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for ExecutorId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ExecutorId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<Self>().map_err(|err| {
            error!(
                backtrace = %std::backtrace::Backtrace::capture(),
                "Cannot convert to {} value:`{str}` - {err:?}", std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for InputContentDigest {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0.digest.as_slice()))
    }
}
impl FromSql for InputContentDigest {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) if b.len() == 32 => {
                let mut digest = [0u8; 32];
                digest.copy_from_slice(b);
                Ok(InputContentDigest(ContentDigest::new(
                    HashType::Sha256,
                    digest,
                )))
            }
            ValueRef::Blob(_) => Err(FromSqlError::Other(Box::from("invalid blob length"))),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl From<rusqlite::Error> for DbErrorGeneric {
    fn from(err: rusqlite::Error) -> DbErrorGeneric {
        error!(backtrace = %std::backtrace::Backtrace::capture(), "Sqlite error {err:?}");
        DbErrorGeneric::Uncategorized(err.to_string().into())
    }
}
impl From<rusqlite::Error> for DbErrorRead {
    fn from(err: rusqlite::Error) -> Self {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Self::NotFound
        } else {
            Self::from(DbErrorGeneric::from(err))
        }
    }
}
impl From<rusqlite::Error> for DbErrorReadWithTimeout {
    fn from(err: rusqlite::Error) -> Self {
        Self::from(DbErrorRead::from(err))
    }
}
impl From<rusqlite::Error> for DbErrorWrite {
    fn from(err: rusqlite::Error) -> Self {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Self::NotFound
        } else {
            Self::from(DbErrorGeneric::from(err))
        }
    }
}
