//! Runs the `SQLite` migrations, libSQL being a server around the same engine.
use crate::hrana::HranaClient;
use db_sqlite::{
    replay::EPOCH_TABLE_SQL,
    sqlite_dao::{REMOTE_FILE_CHUNK_TABLE_SQL, migrations_runner},
};
use refinery::Migration;
use refinery_core::traits::sync::{Migrate, Query, Transaction};
use rusqlite::types::{FromSql, Value};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const MAX_MIGRATION_ATTEMPTS: u32 = 5;

/// The files stay byte-identical since existing `SQLite` databases validate their checksums, so the
/// two constructs rejected by `sqld` are rewritten here: double-quoted strings and temp tables.
fn adapt_to_libsql(sql: &str) -> String {
    sql.replace("=\"join_next\"", "='join_next'")
        .replace("CREATE TEMPORARY TABLE", "CREATE TABLE")
}

fn failure(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

struct MigrationConnection<'a>(&'a HranaClient);

impl Transaction for MigrationConnection<'_> {
    type Error = rusqlite::Error;

    fn execute<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let queries = queries.collect::<Vec<_>>();
        if !queries.is_empty() {
            // Atomic with the history rows, concurrent nodes serialize on the write lock.
            self.0
                .sequence(&format!(
                    "BEGIN IMMEDIATE;\n{};\nCOMMIT;",
                    adapt_to_libsql(&queries.join(";\n"))
                ))
                .map_err(failure)?;
        }
        Ok(queries.len())
    }
}

impl Query<Vec<Migration>> for MigrationConnection<'_> {
    fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let result = self.0.query(query).map_err(failure)?;
        result
            .rows
            .iter()
            .map(|row| {
                fn get<T: FromSql>(row: &[Value], index: usize) -> rusqlite::Result<T> {
                    let value = row
                        .get(index)
                        .ok_or(rusqlite::Error::InvalidColumnIndex(index))?;
                    T::column_result(value.into()).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            index,
                            value.data_type(),
                            Box::new(error),
                        )
                    })
                }
                let applied_on: String = get(row, 2)?;
                let checksum: String = get(row, 3)?;
                Ok(Migration::applied(
                    get(row, 0)?,
                    get(row, 1)?,
                    OffsetDateTime::parse(&applied_on, &Rfc3339).map_err(failure)?,
                    checksum.parse().map_err(failure)?,
                ))
            })
            .collect()
    }
}

impl Migrate for MigrationConnection<'_> {}

pub(crate) fn run(client: &HranaClient) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A node losing the race for a migration succeeds once it sees the winner's history row.
    let mut attempt = 0;
    while let Err(error) = migrations_runner()
        .set_grouped(true)
        .run(&mut MigrationConnection(client))
    {
        attempt += 1;
        if attempt == MAX_MIGRATION_ATTEMPTS {
            return Err(error.into());
        }
        tracing::debug!(%error, "retrying libSQL migrations");
        std::thread::sleep(Duration::from_millis(200));
    }
    client.sequence(&format!(
        "BEGIN IMMEDIATE;\n{EPOCH_TABLE_SQL}\n{REMOTE_FILE_CHUNK_TABLE_SQL}\nCOMMIT;"
    ))?;
    Ok(())
}
