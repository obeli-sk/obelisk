//! SQL facade and the interface for external transports using the logical-transaction worker.
use rusqlite::{
    ToSql,
    types::{FromSql, ToSqlOutput, Value},
};

type Result<T> = rusqlite::Result<T>;

#[derive(Debug)]
pub enum SqlParams {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ConfirmedAbort(pub Box<dyn std::error::Error + Send + Sync>);

/// External transports own session cleanup, abort classification and transaction boundaries.
pub trait SqlTransport: Send {
    /// Returns SQL ready for execute/query, including any backend-specific materialization.
    fn prepare_sql(&self, sql: &str) -> Result<String>;
    fn execute(&self, sql: &str, params: SqlParams) -> Result<usize>;
    fn query(&self, sql: &str, params: SqlParams) -> Result<QueryResult>;
    fn execute_batch(&self, sql: &str) -> Result<()>;
    fn begin(&self, sql: &str) -> Result<()>;
    fn commit(&self) -> Result<()>;
    fn rollback_for_retry(&self) -> Result<()>;
    fn rollback(&self);
    fn concurrent(&self) -> bool;
    fn transport_failed(&self) -> bool;
    fn aborted(&self) -> bool;
}

pub(crate) trait Params: rusqlite::Params {
    fn bindings(&self) -> Result<SqlParams>;
}
fn value(value: &dyn ToSql) -> Result<Value> {
    let value = match value.to_sql()? {
        ToSqlOutput::Borrowed(value) => match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(v) => Value::Integer(v),
            rusqlite::types::ValueRef::Real(v) => Value::Real(v),
            rusqlite::types::ValueRef::Text(v) => {
                Value::Text(String::from_utf8_lossy(v).into_owned())
            }
            rusqlite::types::ValueRef::Blob(v) => Value::Blob(v.to_vec()),
        },
        ToSqlOutput::Owned(value) => value,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    Ok(value)
}
impl Params for () {
    fn bindings(&self) -> Result<SqlParams> {
        Ok(SqlParams::None)
    }
}
impl Params for [&(dyn ToSql + Send + Sync); 0] {
    fn bindings(&self) -> Result<SqlParams> {
        Ok(SqlParams::None)
    }
}
impl Params for &[&dyn ToSql] {
    fn bindings(&self) -> Result<SqlParams> {
        Ok(SqlParams::Positional(
            self.iter().map(|v| value(*v)).collect::<Result<_>>()?,
        ))
    }
}
impl<T: ToSql> Params for &[(&str, T)] {
    fn bindings(&self) -> Result<SqlParams> {
        Ok(SqlParams::Named(
            self.iter()
                .map(|(k, v)| Ok(((*k).to_owned(), value(v)?)))
                .collect::<Result<_>>()?,
        ))
    }
}
macro_rules! array_params {
    ($($n:literal),*) => { $(impl<T: ToSql> Params for [T; $n] {
        fn bindings(&self) -> Result<SqlParams> {
            Ok(SqlParams::Positional(self.iter().map(|v| value(v)).collect::<Result<_>>()?))
        }
    })* };
}
array_params!(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

pub(crate) enum Connection {
    Local(rusqlite::Connection),
    External(Box<dyn SqlTransport>),
}
impl Connection {
    pub(crate) fn retryable_begin_error(remote: bool, error: &rusqlite::Error) -> bool {
        !remote || Self::retryable_abort_error(error)
    }
    pub(crate) fn retryable_abort_error(error: &rusqlite::Error) -> bool {
        matches!(error, rusqlite::Error::ToSqlConversionFailure(source) if source.is::<ConfirmedAbort>())
    }
    pub(crate) fn concurrent(&self) -> bool {
        matches!(self, Self::External(transport) if transport.concurrent())
    }
    pub(crate) fn transport_failed(&self) -> bool {
        matches!(self, Self::External(transport) if transport.transport_failed())
    }
    pub(crate) fn aborted(&self) -> bool {
        matches!(self, Self::External(transport) if transport.aborted())
    }
    pub(crate) fn is_remote(&self) -> bool {
        matches!(self, Self::External(_))
    }
    pub(crate) fn prepare(&self, sql: &str) -> Result<CachedStatement<'_>> {
        match self {
            Self::Local(conn) => Ok(CachedStatement::Local(conn.prepare_cached(sql)?)),
            Self::External(transport) => Ok(CachedStatement::External {
                transport: transport.as_ref(),
                sql: transport.prepare_sql(sql)?,
            }),
        }
    }
    pub(crate) fn prepare_cached(&self, sql: &str) -> Result<CachedStatement<'_>> {
        self.prepare(sql)
    }
    pub(crate) fn query_row<P: Params, T>(
        &self,
        sql: &str,
        params: P,
        f: impl FnOnce(&Row<'_>) -> Result<T>,
    ) -> Result<T> {
        self.prepare(sql)?.query_row(params, f)
    }
    pub(crate) fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare(sql)?.execute(params)
    }
    pub(crate) fn worker_transaction(&mut self) -> Result<Transaction<'_>> {
        let sql = if self.concurrent() {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN IMMEDIATE"
        };
        match self {
            Self::Local(conn) => conn.execute_batch(sql)?,
            Self::External(transport) => transport.begin(sql)?,
        }
        Ok(Transaction {
            conn: self,
            finished: false,
        })
    }
}

pub(crate) struct Transaction<'a> {
    pub(crate) conn: &'a Connection,
    finished: bool,
}
impl std::ops::Deref for Transaction<'_> {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.conn
    }
}
impl Transaction<'_> {
    pub(crate) fn rollback_for_retry(mut self) -> Result<()> {
        self.finished = true;
        match self.conn {
            Connection::Local(conn) => conn.execute_batch("ROLLBACK"),
            Connection::External(transport) => transport.rollback_for_retry(),
        }
    }
    pub(crate) fn commit(mut self) -> Result<()> {
        let result = match self.conn {
            Connection::Local(conn) => conn.execute_batch("COMMIT"),
            Connection::External(transport) => {
                self.finished = true;
                transport.commit()
            }
        };
        if result.is_ok() {
            self.finished = true;
        }
        result
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            match self.conn {
                Connection::Local(conn) => {
                    if let Err(error) = conn.execute_batch("ROLLBACK") {
                        tracing::error!(%error, "SQL transaction rollback failed");
                    }
                }
                Connection::External(transport) => transport.rollback(),
            }
        }
    }
}

pub(crate) enum CachedStatement<'a> {
    Local(rusqlite::CachedStatement<'a>),
    External {
        transport: &'a dyn SqlTransport,
        sql: String,
    },
}
impl CachedStatement<'_> {
    pub(crate) fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        match self {
            Self::Local(stmt) => stmt.execute(params),
            Self::External { transport, sql } => transport.execute(sql, params.bindings()?),
        }
    }
    fn external_rows(&self, params: SqlParams) -> Result<QueryResult> {
        let Self::External { transport, sql } = self else {
            unreachable!()
        };
        transport.query(sql, params)
    }

    pub(crate) fn query_row<T, P: Params, F: FnOnce(&Row<'_>) -> Result<T>>(
        &mut self,
        params: P,
        f: F,
    ) -> Result<T> {
        match self {
            Self::Local(stmt) => stmt.query_row(params, |row| f(&Row::Local(row))),
            Self::External { .. } => {
                let QueryResult { columns, rows } = self.external_rows(params.bindings()?)?;
                let values = rows.first().ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                f(&Row::External {
                    columns: &columns,
                    values,
                })
            }
        }
    }
    pub(crate) fn query_map<'a, T: 'a, P: Params, F>(
        &'a mut self,
        params: P,
        mut f: F,
    ) -> Result<Box<dyn Iterator<Item = Result<T>> + 'a>>
    where
        F: FnMut(&Row<'_>) -> Result<T> + 'a,
    {
        match self {
            Self::Local(stmt) => Ok(Box::new(
                stmt.query_map(params, move |row| f(&Row::Local(row)))?,
            )),
            Self::External { .. } => {
                let QueryResult { columns, rows } = self.external_rows(params.bindings()?)?;
                Ok(Box::new(rows.into_iter().map(move |values| {
                    f(&Row::External {
                        columns: &columns,
                        values: &values,
                    })
                })))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum Row<'a> {
    Local(&'a rusqlite::Row<'a>),
    External {
        columns: &'a [String],
        values: &'a [Value],
    },
}
pub(crate) trait RowIndex: Copy {
    fn index(&self, columns: &[String]) -> Result<usize>;
    fn local<T: FromSql>(&self, row: &rusqlite::Row<'_>) -> Result<T>;
}
impl RowIndex for usize {
    fn index(&self, _columns: &[String]) -> Result<usize> {
        Ok(*self)
    }
    fn local<T: FromSql>(&self, row: &rusqlite::Row<'_>) -> Result<T> {
        row.get(*self)
    }
}
impl RowIndex for &str {
    fn index(&self, columns: &[String]) -> Result<usize> {
        columns
            .iter()
            .position(|v| v.eq_ignore_ascii_case(self))
            .ok_or_else(|| rusqlite::Error::InvalidColumnName((*self).to_owned()))
    }
    fn local<T: FromSql>(&self, row: &rusqlite::Row<'_>) -> Result<T> {
        row.get(*self)
    }
}
impl Row<'_> {
    pub(crate) fn get<I: RowIndex, T: FromSql>(&self, index: I) -> Result<T> {
        match self {
            Self::Local(row) => index.local(row),
            Self::External { columns, values } => {
                let index = index.index(columns)?;
                let value = values
                    .get(index)
                    .ok_or(rusqlite::Error::InvalidColumnIndex(index))?;
                T::column_result(value.into()).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(e))
                })
            }
        }
    }
}
