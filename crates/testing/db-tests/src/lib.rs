use db_sqlite::sqlite_dao::SqlitePool;
use tempfile::NamedTempFile;

pub async fn sqlite_pool() -> (SqlitePool, Option<NamedTempFile>) {
    if let Ok(path) = std::env::var("SQLITE_FILE") {
        (SqlitePool::new(path).await.unwrap(), None)
    } else {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();
        (SqlitePool::new(path).await.unwrap(), Some(file))
    }
}
