//! libSQL storage over stateless Hrana batches, sharing the `SQLite` DAO and its migrations.
mod config;
mod hrana;
mod migrations;
mod split;

pub use config::LibsqlConfig;
pub use db_sqlite::sqlite_dao::{InitializationError, SqlitePool as LibsqlPool};
pub use hrana::HranaClient;
pub use migrations::apply;

pub async fn connect(config: LibsqlConfig) -> Result<LibsqlPool, InitializationError> {
    connect_with(config, |client| Box::new(client)).await
}

/// Allows wrapping the client, e.g. to observe or fail the batches in tests.
pub async fn connect_with(
    config: LibsqlConfig,
    wrap: impl FnOnce(HranaClient) -> Box<dyn db_sqlite::sql_connection::SqlPipeline> + Send + 'static,
) -> Result<LibsqlPool, InitializationError> {
    LibsqlPool::with_pipeline(config.queue_capacity, config.metrics_threshold, move || {
        let initialize = || {
            let client = HranaClient::new(&config)?;
            migrations::run(&client)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(client)
        };
        initialize().map(wrap).map_err(|error| {
            tracing::error!(%error, "cannot initialize libSQL backend");
            InitializationError
        })
    })
    .await
}
