use crate::{ExecutionId, JoinSetId};
use rusqlite::{
    types::{FromSql, FromSqlError, ToSqlOutput},
    ToSql,
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
