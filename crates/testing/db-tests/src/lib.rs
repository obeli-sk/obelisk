mod cloud;

use async_trait::async_trait;
use concepts::FunctionFqn;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use db_postgres::postgres_dao::DbInitialzationOutcome;
use db_postgres::postgres_dao::PostgresConfig;
use db_postgres::postgres_dao::PostgresPool;
use db_postgres::postgres_dao::ProvisionPolicy;
use db_sqlite::sqlite_dao::SqlitePool;
use secrecy::SecretString;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tracing::debug;

pub const SOME_FFQN: FunctionFqn = FunctionFqn::new_static("ns:pkg/ifc", "fn");
/// A workflow FFQN whose `-cancellable` suffix makes [`FunctionFqn::is_cancellable`] true.
pub const CANCELLABLE_FFQN: FunctionFqn = FunctionFqn::new_static("ns:pkg/ifc", "fn-cancellable");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Database {
    Sqlite,
    Postgres,
    Turso,
    TursoConcurrent,
}
impl Database {
    pub const ALL: [Database; 4] = [
        Database::Sqlite,
        Database::Postgres,
        Database::Turso,
        Database::TursoConcurrent,
    ];

    #[must_use]
    pub fn wall_clock_timeout(self, local: std::time::Duration) -> std::time::Duration {
        if matches!(self, Self::Turso | Self::TursoConcurrent)
            && (std::env::var_os("TEST_TURSO_URL").is_some()
                || std::env::var_os("TEST_TURSO_PLATFORM_TOKEN_FILE").is_some())
        {
            local * 10
        } else {
            local
        }
    }
}

pub enum DbGuard {
    Turso,
    Sqlite(Option<NamedTempFile>),
    Postgres,
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
            Database::Turso | Database::TursoConcurrent => {
                for attempt in 0..3 {
                    let mode = if self == Self::TursoConcurrent {
                        db_turso::TransactionMode::Concurrent
                    } else {
                        db_turso::TransactionMode::Immediate
                    };
                    let (config, cleanup) = turso_test_config_mode(mode).await;
                    match db_turso::connect(config).await {
                        Ok(pool) => {
                            let closeable = DbPoolCloseableWrapper::Turso(pool.clone(), cleanup);
                            return (DbGuard::Turso, Arc::new(pool), closeable);
                        }
                        Err(error) => {
                            if let Some(TursoCleanup::Database(database)) = &cleanup {
                                database.delete().await;
                                if attempt < 2 {
                                    tracing::warn!(
                                        attempt,
                                        "isolated Cloud database initialization failed; creating a fresh database"
                                    );
                                } else {
                                    panic!("initialize remote SQL backend: {error:?}");
                                }
                            } else {
                                panic!("initialize remote SQL backend: {error:?}");
                            }
                        }
                    }
                }
                unreachable!("initialization returns a pool or fails after three fresh databases")
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
    Turso(SqlitePool, Option<TursoCleanup>),
    Sqlite(SqlitePool),
    Postgres(Arc<PostgresPool>),
}

#[async_trait]
impl DbPoolCloseable for DbPoolCloseableWrapper {
    async fn close(&self) {
        match self {
            DbPoolCloseableWrapper::Sqlite(db) => db.close().await,
            DbPoolCloseableWrapper::Turso(db, cleanup) => {
                db.close().await;
                if let Some(TursoCleanup::Database(database)) = cleanup {
                    database.delete().await;
                } else if let Some(TursoCleanup::Tables(config)) = cleanup {
                    use secrecy::ExposeSecret as _;
                    let database = turso_serverless::Builder::new_remote(&config.url)
                        .with_auth_token(config.auth_token.expose_secret())
                        .build()
                        .await
                        .unwrap();
                    let conn = database.connect().unwrap();
                    let mut rows=conn.query("SELECT name FROM sqlite_master WHERE type='table' AND substr(name,1,?1)=?2",turso_serverless::params![i64::try_from(config.namespace.len()).expect("namespace length fits i64"),config.namespace.clone()]).await.unwrap();
                    let mut drops = String::new();
                    while let Some(row) = rows.next().await.unwrap() {
                        let name: String = row.get(0).unwrap();
                        assert!(
                            name.starts_with(&config.namespace)
                                && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                        );
                        use std::fmt::Write as _;
                        writeln!(&mut drops, "DROP TABLE {name};").unwrap();
                    }
                    if !drops.is_empty() {
                        conn.execute_batch(drops).await.unwrap();
                    }
                    conn.close().await.unwrap();
                }
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

pub enum TursoCleanup {
    Database(cloud::CloudDatabase),
    Tables(db_turso::TursoConfig),
}

pub async fn turso_test_config() -> (db_turso::TursoConfig, Option<TursoCleanup>) {
    turso_test_config_mode(db_turso::TransactionMode::Immediate).await
}

pub async fn turso_test_config_mode(
    transaction_mode: db_turso::TransactionMode,
) -> (db_turso::TursoConfig, Option<TursoCleanup>) {
    use std::{sync::OnceLock, time::Duration};
    test_utils::set_up();
    let suffix = format!(
        "test_{}_",
        concepts::prefixed_ulid::DeploymentId::generate()
    )
    .to_lowercase();
    if let Ok(path) = std::env::var("TEST_TURSO_PLATFORM_TOKEN_FILE") {
        let (database, url, auth_token) = cloud::CloudDatabase::create(&path).await;
        return (
            db_turso::TursoConfig {
                url,
                auth_token,
                queue_capacity: 100,
                transaction_mode,
                request_timeout: Duration::from_secs(30),
                metrics_threshold: None,
                namespace: String::new(),
            },
            Some(TursoCleanup::Database(database)),
        );
    }
    let cloud = std::env::var("TEST_TURSO_URL").ok();
    let (url, namespace, token) = if let Some(url) = cloud {
        let token = if let Ok(path) = std::env::var("TEST_TURSO_TOKEN_FILE") {
            std::fs::read_to_string(path)
                .expect("read SQL token")
                .trim()
                .to_owned()
        } else {
            std::env::var("TEST_TURSO_TOKEN").unwrap_or_default()
        };
        (url, suffix, token)
    } else {
        static LOCAL: OnceLock<String> = OnceLock::new();
        let url = LOCAL.get_or_init(|| {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let server = db_test_server::TestServer::start().await.unwrap();
                    sender.send(server.url.clone()).unwrap();
                    std::future::pending::<()>().await;
                    drop(server);
                });
            });
            receiver.recv().unwrap()
        });
        let database_name = if transaction_mode == db_turso::TransactionMode::Concurrent {
            format!("mvcc_{suffix}")
        } else {
            suffix
        };
        (
            format!("{url}/{database_name}"),
            String::new(),
            String::new(),
        )
    };
    let config = db_turso::TursoConfig {
        url,
        auth_token: SecretString::from(token),
        queue_capacity: 100,
        transaction_mode,
        request_timeout: Duration::from_secs(30),
        metrics_threshold: None,
        namespace,
    };
    let cleanup = (!config.namespace.is_empty()).then(|| TursoCleanup::Tables(config.clone()));
    (config, cleanup)
}
