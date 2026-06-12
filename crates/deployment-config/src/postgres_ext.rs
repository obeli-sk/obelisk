use crate::component_id::ComponentDigest;
use tokio_postgres::types::ToSql;

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
