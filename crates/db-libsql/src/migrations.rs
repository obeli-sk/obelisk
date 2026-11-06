//! Runs the `SQLite` migrations, libSQL being a server around the same engine.
use crate::{hrana::HranaClient, split::split_statements};
use db_sqlite::{
    sql_connection::{BatchStep, Condition, SqlParams, SqlPipeline, StepOutcome},
    sqlite_dao::migrations_runner,
};
use refinery::{Migration, Runner};
use refinery_core::traits::sync::{Migrate, Query, Transaction};
use rusqlite::types::{FromSql, Value};
use std::time::Duration;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Each failure means another node applied the migration first, so this is about its total time.
const MAX_MIGRATION_ATTEMPTS: u32 = 50;

/// The files stay byte-identical since existing `SQLite` databases validate their checksums, so the
/// two constructs rejected by `sqld` are rewritten here: double-quoted strings and temp tables.
fn adapt_to_libsql(sql: &str) -> String {
    sql.replace("=\"join_next\"", "='join_next'")
        .replace("CREATE TEMPORARY TABLE", "CREATE TABLE")
}

fn failure(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn message(error: String) -> rusqlite::Error {
    failure(std::io::Error::other(error))
}

/// Sends every request as one stateless batch, so no stream or lock outlives a request.
struct MigrationConnection<'a>(&'a HranaClient);

impl Transaction for MigrationConnection<'_> {
    type Error = rusqlite::Error;

    // refinery passes a migration together with its history insert, so both commit or neither.
    fn execute<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let queries = queries.map(adapt_to_libsql).collect::<Vec<_>>();
        let mut statements = Vec::new();
        for query in &queries {
            statements.extend(split_statements(query).map_err(message)?);
        }
        if !statements.is_empty() {
            let step = |sql: &str, condition| BatchStep {
                sql: sql.to_owned(),
                params: SqlParams::None,
                want_rows: false,
                condition,
            };
            // Foreign keys stay off as in `SQLite`'s migration procedure.
            let mut steps = vec![step("PRAGMA foreign_keys = OFF", None)];
            for sql in ["BEGIN IMMEDIATE"].into_iter().chain(statements) {
                steps.push(step(sql, Some(Condition::Ok(steps.len() - 1))));
            }
            let commit = steps.len();
            steps.push(step("COMMIT", Some(Condition::Ok(commit - 1))));
            let not_committed = Condition::Not(Box::new(Condition::Ok(commit)));
            steps.push(step("ROLLBACK", Some(not_committed)));
            let outcomes = self.0.batch(&steps).map_err(failure)?;
            if !matches!(outcomes.get(commit), Some(StepOutcome::Done(_))) {
                let error = outcomes.iter().find_map(|outcome| match outcome {
                    StepOutcome::Error { message, .. } => Some(message.clone()),
                    StepOutcome::Done(_) | StepOutcome::Skipped => None,
                });
                return Err(message(
                    error.unwrap_or_else(|| "migration did not commit".to_string()),
                ));
            }
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

/// Applies each pending migration with its history row in one stateless transaction.
pub fn apply(client: &HranaClient, runner: Runner) -> Result<(), refinery::Error> {
    runner
        .set_grouped(false)
        .run(&mut MigrationConnection(client))?;
    Ok(())
}

pub(crate) fn run(client: &HranaClient) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // A concurrent run fails on the history primary key and sees the winner's row on retry.
    let mut attempt = 0;
    while let Err(error) = apply(client, migrations_runner()) {
        attempt += 1;
        if attempt == MAX_MIGRATION_ATTEMPTS {
            return Err(error.into());
        }
        tracing::debug!(%error, "retrying libSQL migrations");
        std::thread::sleep(Duration::from_millis(200));
    }
    Ok(())
}
