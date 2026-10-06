use crate::args::StorageServeArgs;
use anyhow::Context;
use concepts::storage::DbPoolCloseable;
use db_http_server::StorageServer;
use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
use std::sync::Arc;

pub(crate) async fn serve(args: StorageServeArgs) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .context("cannot bind storage listener")?;
    let address = listener.local_addr()?;
    let pool = Arc::new(
        SqlitePool::new(&args.database, SqliteConfig::default())
            .await
            .context("cannot open storage database")?,
    );
    let server = StorageServer::start(listener, pool.clone(), &args.token)?;
    eprintln!("Storage service listening at http://{address}");
    let (sender, _) = tokio::sync::watch::channel(());
    crate::command::termination_notifier::termination_notifier(sender).await;
    let result = server.close().await;
    pool.close().await;
    result.context("cannot stop storage server")
}
