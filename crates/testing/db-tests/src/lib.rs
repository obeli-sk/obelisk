use async_trait::async_trait;
use concepts::FunctionFqn;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use db_libsql::{LibsqlConfig, LibsqlPool};
use db_postgres::postgres_dao::DbInitialzationOutcome;
use db_postgres::postgres_dao::PostgresConfig;
use db_postgres::postgres_dao::PostgresPool;
use db_postgres::postgres_dao::ProvisionPolicy;
use db_sqlite::sqlite_dao::SqlitePool;
use secrecy::SecretString;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir};
use tracing::debug;

pub const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("ns:pkg/ifc", "fn");
/// A workflow FFQN whose `-cancellable` suffix makes [`FunctionFqn::is_cancellable`] true.
pub const CANCELLABLE_FFQN: FunctionFqn = FunctionFqn::new_static("ns:pkg/ifc", "fn-cancellable");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Database {
    Sqlite,
    Postgres,
    Libsql,
}
impl Database {
    pub const ALL: [Database; 3] = [Database::Sqlite, Database::Postgres, Database::Libsql];
}

pub enum DbGuard {
    Libsql(SqldServer),
    Sqlite(Option<NamedTempFile>),
    Postgres,
}

/// A disposable libSQL server, the database lives as long as this value.
pub struct SqldServer {
    process: Child,
    port: u16,
    _dir: TempDir,
}

impl SqldServer {
    pub async fn start() -> Self {
        // Installs the crypto provider needed by `reqwest`.
        test_utils::set_up();
        // The port is free only momentarily, another test can steal it before `sqld` binds.
        for _ in 0..5 {
            let dir = tempfile::tempdir().expect("create libSQL data directory");
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))
                .and_then(|listener| listener.local_addr())
                .expect("find a free port")
                .port();
            let process = Command::new("sqld")
                .arg("--no-welcome")
                .arg("-d")
                .arg(dir.path().join("db"))
                .arg("--http-listen-addr")
                .arg(format!("127.0.0.1:{port}"))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("cannot start `sqld`, enter the development shell using `nix develop`");
            let mut server = Self {
                process,
                port,
                _dir: dir,
            };
            if server.wait_until_healthy().await {
                return server;
            }
        }
        panic!("`sqld` did not become healthy")
    }

    async fn wait_until_healthy(&mut self) -> bool {
        let client = reqwest::Client::new();
        let health = format!("{}/health", self.url());
        for _ in 0..200 {
            if !matches!(self.process.try_wait(), Ok(None)) {
                return false;
            }
            if client
                .get(&health)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    #[must_use]
    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    #[must_use]
    pub fn config(&self) -> LibsqlConfig {
        LibsqlConfig {
            url: self.url(),
            auth_token: SecretString::from(String::new()),
            queue_capacity: 100,
            request_timeout: Duration::from_secs(30),
            metrics_threshold: None,
        }
    }
}

impl Drop for SqldServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl Database {
    pub async fn set_up(self) -> (DbGuard, Arc<dyn DbPool>, DbPoolCloseableWrapper) {
        match self {
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (sqlite, guard) = sqlite_pool().await;
                let closeable = DbPoolCloseableWrapper::Sqlite(sqlite.clone());
                (DbGuard::Sqlite(guard), Arc::new(sqlite.clone()), closeable)
            }
            Database::Libsql => {
                let server = SqldServer::start().await;
                let pool = db_libsql::connect(server.config())
                    .await
                    .expect("initialize libSQL backend");
                let closeable = DbPoolCloseableWrapper::Libsql(pool.clone());
                (DbGuard::Libsql(server), Arc::new(pool), closeable)
            }

            Database::Postgres => {
                let pool = initialize_fresh_postgres_db().await;
                let pool = Arc::new(pool);
                let closeable = DbPoolCloseableWrapper::Postgres(pool.clone());
                (DbGuard::Postgres, pool, closeable)
            }
        }
    }
}

pub async fn initialize_fresh_postgres_db() -> PostgresPool {
    use rand::SeedableRng;
    let config = PostgresConfig {
        host: get_env_val("TEST_POSTGRES_HOST"),
        user: get_env_val("TEST_POSTGRES_USER"),
        password: SecretString::from(get_env_val("TEST_POSTGRES_PASSWORD")),
        db_name: get_env_val("TEST_POSTGRES_DATABASE_PREFIX"),
    };
    for _ in 0..10 {
        let mut config = config.clone();
        let mut rng = rand::rngs::SmallRng::from_os_rng();
        let suffix = (0..5)
            .map(|_| rand::Rng::random_range(&mut rng, b'a'..=b'z') as char)
            .collect::<String>();
        config.db_name = format!("{}_{}", config.db_name, suffix);
        debug!("Using database {}", config.db_name);
        if let Ok((pool, outcome)) =
            PostgresPool::new_with_outcome(config, ProvisionPolicy::MustCreate).await
        {
            assert_eq!(DbInitialzationOutcome::Created, outcome);
            return pool;
        }
    }
    panic!("cannot create an empty database")
}

fn get_env_val(name: &'static str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("cannot get value of environment variable `{name}`"))
}

pub enum DbPoolCloseableWrapper {
    Libsql(LibsqlPool),
    Sqlite(SqlitePool),
    Postgres(Arc<PostgresPool>),
}

#[async_trait]
impl DbPoolCloseable for DbPoolCloseableWrapper {
    async fn close(&self) {
        match self {
            DbPoolCloseableWrapper::Sqlite(db) | DbPoolCloseableWrapper::Libsql(db) => {
                db.close().await;
            }
            DbPoolCloseableWrapper::Postgres(db) => {
                db.close().await; // Close the target db.
                std::cfg_select! {
                    feature = "test" => {
                        db.drop_database().await; // Drop it using the admin database.
                    }
                    _ => {
                        unreachable!("test feature must be enabled");
                    }
                }
            }
        }
    }
}
