use crate::{
    ComponentType, FunctionFqn,
    component_id::{ComponentDigest, Digest},
};
use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef},
};
use std::str::FromStr;
use tracing::error;

impl ToSql for ComponentDigest {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}
impl FromSql for ComponentDigest {
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
                Ok(ComponentDigest(digest))
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl FromSql for FunctionFqn {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(ffqn) => {
                let ffqn = String::from_utf8_lossy(ffqn);
                let ffqn = FunctionFqn::from_str(&ffqn).map_err(|err| {
                    error!("Cannot deserialize ffqn `{ffqn}` - {err:?}");
                    FromSqlError::Other(Box::from(err))
                })?;
                Ok(ffqn)
            }
            _ => Err(FromSqlError::InvalidType),
        }
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
