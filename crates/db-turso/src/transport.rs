use crate::TransactionMode;
use db_sqlite::sql_connection::{ConfirmedAbort, QueryResult, SqlParams, SqlTransport};
use rusqlite::types::Value;
use std::{borrow::Cow, cell::Cell, time::Duration};
use turso_serverless::{Value as RemoteValue, params::Params as RemoteParams};

type Result<T> = rusqlite::Result<T>;

pub(crate) struct Remote {
    runtime: tokio::runtime::Runtime,
    conn: turso_serverless::Connection,
    timeout: Duration,
    poisoned: Cell<bool>,
    namespace: String,
    transaction_mode: TransactionMode,
    aborted: Cell<bool>,
}
fn retryable_abort(error: &turso_serverless::Error) -> bool {
    match error {
        turso_serverless::Error::Busy(_) | turso_serverless::Error::BusySnapshot(_) => true,
        turso_serverless::Error::Error(message) => matches!(
            message.as_str(),
            "Write-write conflict"
                | "Tursodb error: Write-write conflict"
                | "interactive transaction was rolled back because the stream was idle for too long; retry the transaction"
                | "SQLite error: interactive transaction was rolled back because the stream was idle for too long; retry the transaction"
        ),
        _ => false,
    }
}

fn remote_error(error: turso_serverless::Error) -> rusqlite::Error {
    if retryable_abort(&error) {
        rusqlite::Error::ToSqlConversionFailure(Box::new(ConfirmedAbort(Box::new(error))))
    } else if let turso_serverless::Error::Constraint(message) = error {
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT),
            Some(message),
        )
    } else {
        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
    }
}
impl Remote {
    fn run<T>(
        &self,
        future: impl std::future::Future<Output = turso_serverless::Result<T>>,
    ) -> Result<T> {
        let result = self
            .runtime
            .block_on(async { tokio::time::timeout(self.timeout, future).await });
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                if retryable_abort(&error) {
                    self.aborted.set(true);
                }
                if matches!(error, turso_serverless::Error::Http(_)) {
                    tracing::warn!(%error, "remote SQL session failed");
                    self.poisoned.set(true);
                }
                Err(remote_error(error))
            }
            Err(_) => {
                self.poisoned.set(true);
                Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "remote SQL operation timed out; transaction outcome may be unknown",
                    ),
                )))
            }
        }
    }
    fn check_statement(&self) -> Result<()> {
        // An abort ends the server transaction, so later writes must not autocommit.
        if self.poisoned.get() || self.aborted.get() {
            Err(rusqlite::Error::InvalidQuery)
        } else {
            Ok(())
        }
    }
    fn sql(&self, sql: &str) -> String {
        if self.namespace.is_empty() {
            sql.to_owned()
        } else {
            static IDENTIFIERS: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(
                || {
                    regex::Regex::new(r"\b(t_[a-zA-Z0-9_]+|idx_[a-zA-Z0-9_]+|refinery_schema_history|_v[0-9]+_[a-zA-Z0-9_]+)\b").expect("identifier regex")
                },
            );
            IDENTIFIERS
                .replace_all(sql, |c: &regex::Captures<'_>| {
                    format!("{}{}", self.namespace, &c[0])
                })
                .into_owned()
        }
    }
    fn reset(&self) -> Result<()> {
        self.run(self.conn.close())?;
        self.poisoned.set(false);
        self.aborted.set(false);
        Ok(())
    }
}
impl Drop for Remote {
    fn drop(&mut self) {
        let _ = self.reset();
    }
}
impl Remote {
    pub(crate) fn new(
        url: &str,
        token: &str,
        timeout: Duration,
        namespace: String,
        transaction_mode: TransactionMode,
    ) -> Result<Self> {
        // Poll HTTP connection tasks even while the SQL worker waits for commands.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let db = runtime
            .block_on(
                turso_serverless::Builder::new_remote(url)
                    .with_auth_token(token)
                    .build(),
            )
            .map_err(remote_error)?;
        let conn = db.connect().map_err(remote_error)?;
        Ok(Self {
            runtime,
            conn,
            timeout,
            poisoned: Cell::new(false),
            namespace,
            transaction_mode,
            aborted: Cell::new(false),
        })
    }
}

fn parameters(params: SqlParams) -> RemoteParams {
    let value = |value| match value {
        Value::Null => RemoteValue::Null,
        Value::Integer(v) => RemoteValue::Integer(v),
        Value::Real(v) => RemoteValue::Real(v),
        Value::Text(v) => RemoteValue::Text(v),
        Value::Blob(v) => RemoteValue::Blob(v),
    };
    match params {
        SqlParams::None => RemoteParams::None,
        SqlParams::Positional(values) => {
            RemoteParams::Positional(values.into_iter().map(value).collect())
        }
        SqlParams::Named(values) => RemoteParams::Named(
            values
                .into_iter()
                .map(|(name, v)| (Cow::Owned(name), value(v)))
                .collect(),
        ),
    }
}

impl SqlTransport for Remote {
    fn prepare_sql(&self, sql: &str) -> Result<String> {
        self.check_statement()?;
        let mut sql = sql.to_owned();
        // The engine needs the history discriminator materialized on insertion.
        if sql.trim_start().starts_with("INSERT INTO t_execution_log") {
            let columns_end = sql.find(')').ok_or(rusqlite::Error::InvalidQuery)?;
            sql.insert_str(columns_end, ", history_event_type");
            let values_end = sql.rfind(')').ok_or(rusqlite::Error::InvalidQuery)?;
            sql.insert_str(
                values_end,
                ", json_extract(:json_value, '$.history_event.event.type')",
            );
        }
        Ok(self.sql(&sql))
    }
    fn execute(&self, sql: &str, params: SqlParams) -> Result<usize> {
        self.check_statement()?;
        usize::try_from(self.run(self.conn.execute(sql, parameters(params)))?)
            .map_err(|_| rusqlite::Error::InvalidQuery)
    }
    fn query(&self, sql: &str, params: SqlParams) -> Result<QueryResult> {
        self.check_statement()?;
        self.run(async {
            let mut rows = self.conn.query(sql, parameters(params)).await?;
            let columns = rows.column_names();
            let mut result = Vec::new();
            while let Some(row) = rows.next().await? {
                let mut values = Vec::new();
                for i in 0..row.column_count() {
                    values.push(match row.get_value(i)? {
                        RemoteValue::Null => Value::Null,
                        RemoteValue::Integer(v) => Value::Integer(v),
                        RemoteValue::Real(v) => Value::Real(v),
                        RemoteValue::Text(v) => Value::Text(v),
                        RemoteValue::Blob(v) => Value::Blob(v),
                    });
                }
                result.push(values);
            }
            Ok(QueryResult {
                columns,
                rows: result,
            })
        })
    }
    fn execute_batch(&self, sql: &str) -> Result<()> {
        self.run(self.conn.execute_batch(self.sql(sql)))
    }
    fn begin(&self, sql: &str) -> Result<()> {
        self.reset()?;
        // Allocate a known stream before BEGIN so a lost reply can still be closed.
        self.run(self.conn.execute("SELECT 1", ()))?;
        let result = self.execute_batch(sql);
        if result.is_err() {
            let _ = self.reset();
        }
        result
    }
    fn commit(&self) -> Result<()> {
        let result = self.execute_batch("COMMIT");
        if result.as_ref().is_err_and(|error| matches!(error, rusqlite::Error::ToSqlConversionFailure(source) if source.is::<ConfirmedAbort>())) {
            self.rollback_for_retry().map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        } else {
            self.reset()?;
        }
        result
    }
    fn rollback_for_retry(&self) -> Result<()> {
        if self.poisoned.get() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        if !self.conn.is_autocommit().map_err(remote_error)? {
            self.run(self.conn.execute("ROLLBACK", ()))?;
        }
        self.reset()
    }
    fn rollback(&self) {
        if !self.poisoned.get()
            && self
                .conn
                .is_autocommit()
                .is_ok_and(|autocommit| !autocommit)
            && let Err(error) = self.execute_batch("ROLLBACK")
        {
            tracing::error!(%error, "SQL transaction rollback failed");
        }
        let _ = self.reset();
    }
    fn concurrent(&self) -> bool {
        self.transaction_mode == TransactionMode::Concurrent
    }
    fn transport_failed(&self) -> bool {
        self.poisoned.get()
    }
    fn aborted(&self) -> bool {
        self.aborted.get()
    }
}
