use crate::{
    ExecutionId, JoinSetId,
    prefixed_ulid::{DelayId, ExecutionIdDerived, ExecutorId, RunId},
};
use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlError, ToSqlOutput},
};
use tracing::error;

impl ToSql for ExecutionId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ExecutionId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
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
