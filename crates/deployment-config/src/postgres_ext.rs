use crate::{
    ComponentType, ContentDigest, FunctionFqn,
    component_id::{ComponentDigest, Digest},
};
use tokio_postgres::types::{FromSql, ToSql};

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
