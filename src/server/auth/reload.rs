use super::ApiAuth;
use crate::config::server::ServerConfigToml;
use concepts::{
    component_id::Digest,
    storage::{DbPool, SystemEventCode},
};
use sha2::{Digest as _, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tracing::warn;

pub(crate) struct TokenHashReload {
    auth: Arc<ApiAuth>,
    path: Option<PathBuf>,
    hashes: Vec<Digest>,
    last_contents: Option<[u8; 32]>,
    unreadable: bool,
    pending: Option<SystemEventCode>,
}

fn normalize(mut hashes: Vec<Digest>) -> Vec<Digest> {
    hashes.sort_unstable_by_key(|digest| digest.0);
    hashes.dedup();
    hashes
}

pub(crate) fn fingerprint(hashes: &[Digest]) -> Digest {
    let mut fingerprint = Sha256::new();
    for digest in normalize(hashes.to_vec()) {
        fingerprint.update(digest.0);
    }
    Digest(fingerprint.finalize().into())
}

impl TokenHashReload {
    pub(crate) fn new(auth: Arc<ApiAuth>, path: Option<PathBuf>, hashes: Vec<Digest>) -> Self {
        Self {
            auth,
            path,
            hashes: normalize(hashes),
            last_contents: None,
            unreadable: false,
            pending: Some(SystemEventCode::ApiTokenHashesLoaded),
        }
    }

    async fn refresh(&mut self) {
        if let Some(path) = &self.path {
            match tokio::fs::read(path).await {
                Ok(contents) => {
                    self.unreadable = false;
                    let digest = Sha256::digest(&contents).into();
                    if self.last_contents != Some(digest) {
                        self.last_contents = Some(digest);
                        if let Ok(hashes) = parse_hashes(&contents) {
                            let hashes = normalize(hashes);
                            if hashes != self.hashes {
                                self.auth.replace_configured(&hashes);
                                self.hashes = hashes;
                                self.pending = Some(SystemEventCode::ApiTokenHashesChanged);
                            }
                        } else {
                            warn!(
                                ?path,
                                "Invalid server configuration during API token hash reload; retaining last valid set"
                            );
                        }
                    }
                }
                Err(err) => {
                    if !self.unreadable {
                        warn!(?path, kind = ?err.kind(), "Cannot read server configuration during API token hash reload; retaining last valid set");
                    }
                    self.unreadable = true;
                    self.last_contents = None;
                }
            }
        }
    }

    fn details(&self) -> serde_json::Value {
        serde_json::json!({ "fingerprint": fingerprint(&self.hashes), "count": self.hashes.len() })
    }

    pub(crate) async fn run(mut self, db_pool: &dyn DbPool) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if let Some(code) = self.pending
                && crate::server::system_event_writer::record_retryable(
                    db_pool,
                    code,
                    None,
                    None,
                    self.details(),
                )
                .await
                .is_some()
            {
                self.pending = None;
            }
            interval.tick().await;
            self.refresh().await;
        }
    }
}

fn parse_hashes(contents: &[u8]) -> Result<Vec<Digest>, ()> {
    let contents = std::str::from_utf8(contents).map_err(|_| ())?;
    let config: ServerConfigToml = toml::from_str(contents).map_err(|_| ())?;
    Ok(config.api.token_hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::server::ApiConfig;
    use concepts::storage::DbPoolCloseable as _;
    use db_sqlite::sqlite_dao::{SqliteConfig, SqlitePool};
    use rusqlite::OptionalExtension as _;
    use secrecy::ExposeSecret as _;

    #[test]
    fn api_auth_fingerprint_matches_canonical_raw_digest_vector() {
        let hashes = [Digest([255; 32]), Digest([0; 32]), Digest([255; 32])];
        assert_eq!(
            fingerprint(&hashes).to_string(),
            "sha256:bba91ca85dc914b2ec3efb9e16e7267bf9193b14350d20fba8a8b406730ae30a"
        );
    }

    #[tokio::test]
    async fn api_auth_reload_retries_current_event_and_preserves_process_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("server.toml");
        std::fs::write(&path, "api.token_hashes = []").unwrap();
        let explicit = crate::api::generate_token();
        let auth =
            Arc::new(ApiAuth::new(&ApiConfig::default(), Some(explicit.clone().into())).unwrap());
        let startup = auth.startup_token().expose_secret().to_owned();
        let reload = TokenHashReload::new(auth.clone(), Some(path.clone()), Vec::new());
        let database = dir.path().join("events.sqlite");
        let pool = Arc::new(
            SqlitePool::new(&database, SqliteConfig::default())
                .await
                .unwrap(),
        );
        let connection = rusqlite::Connection::open(&database).unwrap();
        let block_events = || {
            connection.execute_batch("CREATE TRIGGER block_events BEFORE INSERT ON t_system_event BEGIN SELECT RAISE(FAIL, 'event storage unavailable'); END;").unwrap();
        };
        block_events();
        let task_pool = pool.clone();
        let task = tokio::spawn(async move { reload.run(task_pool.as_ref()).await });
        let _abort = executor::AbortOnDropHandle::new(task.abort_handle());
        let event_count = || {
            connection
                .query_row("SELECT count(*) FROM t_system_event", [], |row| {
                    row.get::<_, u32>(0)
                })
                .unwrap()
        };
        let wait_for_event = async |code: &str, count: usize| {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let latest: Option<(String, String)> = connection.query_row(
                        "SELECT code, details FROM t_system_event ORDER BY event_id DESC LIMIT 1",
                        [], |row| Ok((row.get(0)?, row.get(1)?)),
                    ).optional().unwrap();
                    if let Some((persisted_code, details)) = latest {
                        let details: serde_json::Value = serde_json::from_str(&details).unwrap();
                        if persisted_code == code && details["count"] == count {
                            return details;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap()
        };
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(event_count(), 0);
        connection
            .execute_batch("DROP TRIGGER block_events")
            .unwrap();
        let details = wait_for_event("api.token_hashes_loaded", 0).await;
        assert_eq!(
            details["fingerprint"],
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        block_events();
        let token = crate::api::generate_token();
        let hash = crate::api::token_hash(&token);
        let check = async |token: &str| {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
            auth.check(&headers).await
        };
        let wait_for_auth = async |accepted: bool| {
            tokio::time::timeout(Duration::from_secs(5), async {
                while check(&token).await.is_ok() != accepted {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        };
        std::fs::write(&path, format!("api.token_hashes = ['{hash}']")).unwrap();
        wait_for_auth(true).await;
        assert_eq!(event_count(), 1);
        std::fs::write(&path, "api.token_hashes = []").unwrap();
        wait_for_auth(false).await;
        assert_eq!(event_count(), 1);
        assert!(check(&startup).await.is_ok());
        assert!(check(&explicit).await.is_ok());
        connection
            .execute_batch("DROP TRIGGER block_events")
            .unwrap();
        let details = wait_for_event("api.token_hashes_changed", 0).await;
        assert_eq!(details["fingerprint"], fingerprint(&[]).to_string());
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(event_count(), 2);
        task.abort();
        let _ = task.await;
        pool.close().await;
    }
}
