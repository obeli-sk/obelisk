use crate::transport::Remote;
use db_sqlite::sql_connection::{SqlParams, SqlTransport};
use refinery::{Migration, Runner};
use refinery_core::traits::sync::{Migrate, Query, Transaction};
use rusqlite::types::{FromSql, Value};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

mod embedded {
    refinery::embed_migrations!("migrations");
}

struct MigrationConnection<'a>(&'a Remote);

impl Transaction for MigrationConnection<'_> {
    type Error = rusqlite::Error;

    fn execute<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let queries = queries.collect::<Vec<_>>();
        if !queries.is_empty() {
            self.0.execute_batch(&queries.join(";\n"))?;
        }
        Ok(queries.len())
    }
}

impl Query<Vec<Migration>> for MigrationConnection<'_> {
    fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let result = self.0.query(&self.0.prepare_sql(query)?, SqlParams::None)?;
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
                    OffsetDateTime::parse(&applied_on, &Rfc3339).map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?,
                    checksum.parse().map_err(|error| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                    })?,
                ))
            })
            .collect()
    }
}

impl Migrate for MigrationConnection<'_> {}

pub(crate) fn run(remote: &Remote) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with(remote, embedded::migrations::runner())
}

fn run_with(
    remote: &Remote,
    runner: Runner,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    remote.begin("BEGIN IMMEDIATE")?;
    let result = runner
        .set_grouped(true)
        .run(&mut MigrationConnection(remote));
    match result {
        Ok(_) => remote.commit().map_err(Into::into),
        Err(error) => {
            remote.rollback();
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransactionMode;
    use std::time::Duration;

    fn remote(url: &str, mode: TransactionMode) -> Remote {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Remote::new(url, "", Duration::from_secs(5), String::new(), mode).unwrap()
    }

    fn integer(remote: &Remote, sql: &str) -> i64 {
        let result = remote.query(sql, SqlParams::None).unwrap();
        let Value::Integer(value) = result.rows[0][0] else {
            panic!("expected an integer")
        };
        value
    }

    async fn in_both_modes(check: fn(&Remote)) {
        let server = db_test_server::TestServer::start().await.unwrap();
        for (name, mode) in [
            ("immediate", TransactionMode::Immediate),
            ("mvcc_concurrent", TransactionMode::Concurrent),
        ] {
            let url = format!("{}/{name}", server.url);
            tokio::task::spawn_blocking(move || check(&remote(&url, mode)))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn turso_migrations_initialize_response_counter_and_preserve_it_on_reopen() {
        in_both_modes(|remote| {
            run(remote).unwrap();
            remote.execute_batch("INSERT INTO t_state(execution_id,is_top_level,corresponding_version,ffqn,created_at,component_id_input_digest,component_type,first_scheduled_at,deployment_id,pending_expires_finished,state,updated_at,intermittent_event_count) VALUES ('parent',1,0,'test','2026-10-06',X'01','workflow','2026-10-06','deployment','2026-10-06','pending_at','2026-10-06',0);
                INSERT INTO t_join_set_response(created_at,execution_id,join_set_id,child_execution_id,finished_version,seq) VALUES ('2026-10-06','parent','join','child1',1,1),('2026-10-06','parent','join','child2',1,2);").unwrap();
            assert_eq!(integer(remote, "SELECT response_sequence FROM t_state WHERE execution_id='parent'"), 0);
            remote.execute_batch("UPDATE t_state SET response_sequence=2 WHERE execution_id='parent'").unwrap();
            run(remote).unwrap();
            assert_eq!(integer(remote, "SELECT response_sequence FROM t_state WHERE execution_id='parent'"), 2);
            assert_eq!(integer(remote, "SELECT COUNT(*) FROM t_join_set_response WHERE execution_id='parent'"), 2);
            assert_eq!(integer(remote, "SELECT COUNT(*) FROM refinery_schema_history"), 1);
        }).await;
    }

    #[tokio::test]
    async fn turso_migrations_reject_divergent_history_without_rewriting_it() {
        in_both_modes(|remote| {
            run(remote).unwrap();
            remote
                .execute_batch("UPDATE refinery_schema_history SET checksum='1' WHERE version=1")
                .unwrap();
            let error = run(remote).unwrap_err();
            assert!(matches!(
                error.downcast_ref::<refinery::Error>().unwrap().kind(),
                refinery::error::Kind::DivergentVersion(..)
            ));
            assert_eq!(
                integer(
                    remote,
                    "SELECT COUNT(*) FROM refinery_schema_history WHERE version=1 AND checksum='1'"
                ),
                1
            );
        })
        .await;
    }

    #[tokio::test]
    async fn turso_migrations_roll_back_the_entire_failed_batch() {
        in_both_modes(|remote| {
            let mut migrations = embedded::migrations::runner().get_migrations().clone();
            migrations.push(Migration::unapplied("V2__invalid.sql", "CREATE TABLE rollback_marker(id INTEGER); INSERT INTO missing_table VALUES (1);").unwrap());
            assert!(run_with(remote, Runner::new(&migrations)).is_err());
            assert_eq!(integer(remote, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('rollback_marker','t_state','refinery_schema_history')"), 0);
            run(remote).unwrap();
            assert_eq!(integer(remote, "SELECT COUNT(*) FROM refinery_schema_history"), 1);
        }).await;
    }

    #[tokio::test]
    #[ignore]
    async fn update_turso_schema() {
        let server = db_test_server::TestServer::start().await.unwrap();
        let url = format!("{}/schema_dump", server.url);
        let schema = tokio::task::spawn_blocking(move || {
            let remote = remote(&url, TransactionMode::Immediate);
            run(&remote).unwrap();
            let mut schema = String::from("-- Generated by scripts/update-schemas.sh from the embedded migrations. Do not edit.\n\n");
            let result = remote.query("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid", SqlParams::None).unwrap();
            for row in result.rows {
                let Value::Text(sql) = &row[0] else { panic!("expected schema SQL") };
                schema.push_str(sql.trim_end_matches(';'));
                schema.push_str(";\n");
            }
            schema
        }).await.unwrap();
        std::fs::write(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../assets/schemas/sql/turso.sql"),
            schema,
        )
        .unwrap();
    }
}
