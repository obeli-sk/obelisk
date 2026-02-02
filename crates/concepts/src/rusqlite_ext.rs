use crate::{
    ComponentType, ContentDigest, ExecutionId, FunctionFqn, JoinSetId,
    component_id::{Digest, InputContentDigest},
    prefixed_ulid::{DelayId, DeploymentId, ExecutionIdDerived, ExecutorId, RunId},
    storage::{
        DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorWrite, DbErrorWriteNonRetriable,
    },
};
use rusqlite::{
    ErrorCode, ToSql,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use std::{panic::Location, str::FromStr, sync::Arc};
use tracing::error;
use tracing_error::SpanTrace;

impl ToSql for ExecutionId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ExecutionId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<ExecutionId>().map_err(|err| {
            error!("Cannot convert to ExecutionId value:`{str}` - {err:?}");
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
            error!("Cannot convert to ExecutionIdDerived value:`{str}` - {err:?}");
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
            error!("Cannot convert to JoinSetId value:`{str}` - {err:?}");
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
                "Cannot convert to {} value:`{str}` - {err:?}",
                std::any::type_name::<Self>()
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
                "Cannot convert to {} value:`{str}` - {err:?}",
                std::any::type_name::<Self>()
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
                "Cannot convert to {} value:`{str}` - {err:?}",
                std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for InputContentDigest {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.0.0.0.as_slice()))
    }
}
impl FromSql for InputContentDigest {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                let digest = Digest::try_from(b).map_err(|err| {
                    error!(
                        "Cannot convert to {} - {err:?}",
                        std::any::type_name::<Self>()
                    );
                    FromSqlError::Other(Box::from(err.to_string()))
                })?;
                Ok(InputContentDigest(ContentDigest(digest)))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl From<rusqlite::Error> for DbErrorRead {
    #[track_caller]
    fn from(err: rusqlite::Error) -> Self {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Self::NotFound
        } else {
            DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            }
            .into()
        }
    }
}
impl From<rusqlite::Error> for DbErrorReadWithTimeout {
    #[track_caller]
    fn from(err: rusqlite::Error) -> Self {
        Self::from(DbErrorRead::from(err))
    }
}
impl From<rusqlite::Error> for DbErrorWrite {
    #[track_caller]
    fn from(err: rusqlite::Error) -> Self {
        if matches!(err, rusqlite::Error::QueryReturnedNoRows) {
            Self::NotFound
        } else if err.sqlite_error().map(|err| err.code) == Some(ErrorCode::ConstraintViolation) {
            DbErrorWrite::NonRetriable(DbErrorWriteNonRetriable::Conflict)
        } else {
            DbErrorGeneric::Uncategorized {
                reason: err.to_string().into(),
                context: SpanTrace::capture(),
                source: Some(Arc::new(err)),
                loc: Location::caller(),
            }
            .into()
        }
    }
}

impl FromSql for FunctionFqn {
    #[track_caller]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(ffqn) => {
                let ffqn = String::from_utf8_lossy(ffqn);
                let ffqn = FunctionFqn::from_str(&ffqn).map_err(|err| {
                    let err = DbErrorGeneric::Uncategorized {
                        reason: "Cannot deserialize ffqn".into(),
                        context: SpanTrace::capture(),
                        source: Some(Arc::new(err)),
                        loc: Location::caller(),
                    };
                    FromSqlError::Other(Box::from(err))
                })?;
                Ok(ffqn)
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for DeploymentId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for DeploymentId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<Self>().map_err(|err| {
            error!(
                "Cannot convert to {} value:`{str}` - {err:?}",
                std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}

impl ToSql for ComponentType {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.to_string()))
    }
}
impl FromSql for ComponentType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let str = value.as_str()?;
        str.parse::<Self>().map_err(|err| {
            error!(
                "Cannot convert to {} value:`{str}` - {err:?}",
                std::any::type_name::<Self>()
            );
            FromSqlError::InvalidType
        })
    }
}
