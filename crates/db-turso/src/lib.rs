//! Direct HTTP storage using the shared SQL logical-transaction worker.
mod config;
mod migrations;
mod transport;

pub use config::{TransactionMode, TursoConfig};
pub use db_sqlite::sqlite_dao::{InitializationError, SqlitePool as TursoPool};
use secrecy::ExposeSecret as _;

pub async fn connect(config: TursoConfig) -> Result<TursoPool, InitializationError> {
    if !config
        .namespace
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(InitializationError);
    }
    TursoPool::with_transport(config.queue_capacity, config.metrics_threshold, move || {
        let initialize = || {
            let remote = transport::Remote::new(
                &config.url,
                config.auth_token.expose_secret(),
                config.request_timeout,
                config.namespace,
                config.transaction_mode,
            )?;
            migrations::run(&remote)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(remote)
        };
        initialize()
            .map(|remote| Box::new(remote) as _)
            .map_err(|error| {
                tracing::error!(%error, "cannot initialize remote SQL backend");
                InitializationError
            })
    })
    .await
}
