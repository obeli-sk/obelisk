use async_trait::async_trait;
use concepts::FunctionFqn;
use concepts::storage::DbPool;
use concepts::storage::DbPoolCloseable;
use db_mem::inmemory_dao::InMemoryPool;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Database {
    Memory,
    Sqlite,
    Postgres,
}
impl Database {
    pub const ALL: [Database; 3] = [Database::Sqlite, Database::Memory, Database::Postgres];
}

pub enum DbGuard {
    Memory,
    Sqlite(Option<NamedTempFile>),
    Postgres,
}

impl Database {
    pub async fn set_up(self) -> (DbGuard, Arc<dyn DbPool>, DbPoolCloseableWrapper) {
        match self {
            Database::Memory => {
                let mem_db = InMemoryPool::new();
                let closeable = DbPoolCloseableWrapper::Memory(mem_db.clone());
                (DbGuard::Memory, Arc::new(mem_db), closeable)
            }
            Database::Sqlite => {
                use db_sqlite::sqlite_dao::tempfile::sqlite_pool;
                let (sqlite, guard) = sqlite_pool().await;
                let closeable = DbPoolCloseableWrapper::Sqlite(sqlite.clone());
                (DbGuard::Sqlite(guard), Arc::new(sqlite.clone()), closeable)
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
    Memory(InMemoryPool),
    Sqlite(SqlitePool),
    Postgres(Arc<PostgresPool>),
}

#[async_trait]
impl DbPoolCloseable for DbPoolCloseableWrapper {
    async fn close(&self) {
        match self {
            DbPoolCloseableWrapper::Memory(db) => db.close().await,
            DbPoolCloseableWrapper::Sqlite(db) => db.close().await,
            DbPoolCloseableWrapper::Postgres(db) => {
                db.close().await; // Close the target db.
                db.drop_database().await; // Drop it using the admin database.
            }
        }
    }
}
