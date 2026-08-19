use crate::storage::{
    DbErrorGeneric, DbErrorRead, DbErrorReadWithTimeout, DbErrorStubResponse, DbErrorWrite,
    DbErrorWriteNonRetriable, SubscribeToResponsesError,
};
use crate::{
    ComponentType, ContentDigest, FunctionFqn,
    component_id::{ComponentDigest, Digest},
};
use std::{panic::Location, sync::Arc};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{FromSql, ToSql};
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

impl From<tokio_postgres::Error> for SubscribeToResponsesError {
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

impl From<tokio_postgres::Error> for DbErrorStubResponse {
    #[track_caller]
    fn from(err: tokio_postgres::Error) -> Self {
        DbErrorStubResponse::Write(DbErrorWrite::from(err))
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

impl ToSql for ComponentDigest {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.as_slice().to_sql(ty, out)
    }

    tokio_postgres::types::accepts!(BYTEA);

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.as_slice().to_sql_checked(ty, out)
    }
}

impl<'a> FromSql<'a> for ComponentDigest {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let bytes = <&[u8] as FromSql>::from_sql(ty, raw)?;
        Ok(ComponentDigest(Digest::try_from(bytes)?))
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <&[u8] as FromSql>::accepts(ty)
    }
}

impl ToSql for ComponentType {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.to_string().to_sql(ty, out)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <String as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.to_string().to_sql_checked(ty, out)
    }
}

impl<'a> FromSql<'a> for ComponentType {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let str = <&str as FromSql>::from_sql(ty, raw)?;
        Ok(str.parse::<ComponentType>()?)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

impl<'a> FromSql<'a> for FunctionFqn {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let str = <&str as FromSql>::from_sql(ty, raw)?;
        Ok(str.parse::<FunctionFqn>()?)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

impl<'a> FromSql<'a> for ContentDigest {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let str = <&str as FromSql>::from_sql(ty, raw)?;
        Ok(str.parse::<ContentDigest>()?)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}
