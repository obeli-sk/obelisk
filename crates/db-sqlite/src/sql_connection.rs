//! SQL facade over a local `SQLite` connection or a stateless remote batch API.
use crate::replay::Replay;
use rusqlite::{
    ToSql,
    types::{FromSql, ToSqlOutput, Value},
};
use std::sync::Arc;

type Result<T> = rusqlite::Result<T>;

#[derive(Debug, Clone, PartialEq)]
pub enum SqlParams {
    None,
    Positional(Vec<Value>),
    Named(Vec<(String, Value)>),
}

#[derive(Debug, Default)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub affected: usize,
}

/// Decides on the server whether a batch step runs.
#[derive(Debug, Clone)]
pub enum Condition {
    Ok(usize),
    Not(Box<Condition>),
    And(Vec<Condition>),
    IsAutocommit,
}

#[derive(Debug, Clone)]
pub struct BatchStep {
    pub sql: String,
    pub params: SqlParams,
    pub want_rows: bool,
    pub condition: Option<Condition>,
}

#[derive(Debug)]
pub enum StepOutcome {
    Done(QueryResult),
    Error {
        code: String,
        message: String,
    },
    /// The step's condition evaluated to false.
    Skipped,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("{0}")]
    Failed(String),
    /// The server refused to return that many rows in one batch.
    #[error("batch response is too large")]
    ResponseTooLarge,
}

/// One-shot batches: the server keeps no state between two calls.
pub trait SqlPipeline: Send {
    /// A failure leaves the batch outcome unknown, including its COMMIT step.
    fn batch(&self, steps: &[BatchStep]) -> std::result::Result<Vec<StepOutcome>, PipelineError>;
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
    External(Replay),
}
impl Connection {
    pub(crate) fn is_remote(&self) -> bool {
        matches!(self, Self::External(_))
    }
    pub(crate) fn prepare(&self, sql: &str) -> Result<CachedStatement<'_>> {
        match self {
            Self::Local(conn) => Ok(CachedStatement::Local(conn.prepare_cached(sql)?)),
            Self::External(replay) => Ok(CachedStatement::External {
                replay,
                sql: sql.to_owned(),
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
    #[cfg(test)]
    pub(crate) fn execute_batch(&self, sql: &str) -> Result<()> {
        match self {
            Self::Local(conn) => conn.execute_batch(sql),
            Self::External(_) => Err(rusqlite::Error::InvalidQuery),
        }
    }
    /// Opens a `SQLite` writer transaction, panics on [`Connection::External`].
    pub(crate) fn local_transaction(&mut self) -> Result<Transaction<'_>> {
        let Self::Local(conn) = self else {
            unreachable!("replayed connections have no server-side transaction")
        };
        conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(Transaction {
            conn: self,
            finished: false,
        })
    }
    /// A handle for logical transactions whose statements are recorded by [`Replay`].
    pub(crate) fn replay_transaction(&self) -> Transaction<'_> {
        Transaction {
            conn: self,
            finished: true,
        }
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
    pub(crate) fn commit(mut self) -> Result<()> {
        let Connection::Local(conn) = self.conn else {
            unreachable!("replayed connections commit in Replay")
        };
        conn.execute_batch("COMMIT")?;
        self.finished = true;
        Ok(())
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.finished
            && let Connection::Local(conn) = self.conn
            && let Err(error) = conn.execute_batch("ROLLBACK")
        {
            tracing::error!(%error, "SQL transaction rollback failed");
        }
    }
}

pub(crate) enum CachedStatement<'a> {
    Local(rusqlite::CachedStatement<'a>),
    External { replay: &'a Replay, sql: String },
}
impl CachedStatement<'_> {
    pub(crate) fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        match self {
            Self::Local(stmt) => stmt.execute(params),
            Self::External { replay, sql } => replay.execute(sql, params.bindings()?),
        }
    }
    fn external_rows(&self, params: SqlParams) -> Result<Arc<QueryResult>> {
        let Self::External { replay, sql } = self else {
            unreachable!()
        };
        replay.query(sql, params)
    }

    pub(crate) fn query_row<T, P: Params, F: FnOnce(&Row<'_>) -> Result<T>>(
        &mut self,
        params: P,
        f: F,
    ) -> Result<T> {
        match self {
            Self::Local(stmt) => stmt.query_row(params, |row| f(&Row::Local(row))),
            Self::External { .. } => {
                let result = self.external_rows(params.bindings()?)?;
                let values = result
                    .rows
                    .first()
                    .ok_or(rusqlite::Error::QueryReturnedNoRows)?;
                f(&Row::External {
                    columns: &result.columns,
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
                let result = self.external_rows(params.bindings()?)?;
                Ok(Box::new((0..result.rows.len()).map(move |index| {
                    f(&Row::External {
                        columns: &result.columns,
                        values: &result.rows[index],
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
