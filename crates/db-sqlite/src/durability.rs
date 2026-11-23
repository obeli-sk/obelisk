//! Group fsync for `synchronous = NORMAL`, optionally followed by a Litestream sync.
//!
//! With `NORMAL` a commit writes the WAL without fsyncing it: a process crash loses nothing, but
//! an OS crash or power loss can roll back the newest commits. Instead of fsyncing every commit,
//! a single syncer fsyncs the WAL when a commit is about to become visible outside the process:
//! before an activity runs, before an API write responds, before an external read returns an
//! execution and before a webhook responds. Every commit made before the fsync started is
//! covered by it, so concurrent waiters share one fsync.
//!
//! The writer thread numbers each physical commit that changed something and records, for each
//! execution whose state changed, the latest such commit. External reads of an execution wait
//! until its latest commit is synced; reads of executions not in the map never wait. External
//! reads of state not tracked per execution (deployments, system events, blobs) wait until
//! everything committed before them is synced. Everything is in memory: the WAL is fsynced once
//! on startup, so the map starts empty.
//!
//! With Litestream, every sync also asks Litestream to upload the WAL to the replica and waits for
//! it, so a commit is revealed only once it is replicated. When only deployments are replicated
//! synchronously, syncs upload only while a replication waiter is pending; other commits reach the
//! replica on Litestream's own schedule. The startup sync replicates everything
//! a previous process left behind. The WAL is fsynced first, so the local database is not behind
//! the replica after a power loss; Litestream's own monitor can still upload commits made after
//! that fsync, which the launcher must handle by comparing the database with its local position.
use concepts::ExecutionId;
use hashbrown::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};
use tracing::{debug, error, warn};

/// Writes nobody waited for are synced after this long, so the map drains when the server goes
/// quiet and later reads of those executions do not wait.
const IDLE_SYNC_DELAY: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);
/// A Litestream sync that takes longer is abandoned and retried.
const LITESTREAM_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct State {
    committed: u64,
    wanted: u64,
    /// Highest commit a replication waiter needs uploaded.
    replicate_wanted: u64,
    /// Executions written by the physical transaction being applied.
    touched: Vec<ExecutionId>,
    /// Latest commit of each execution whose state changed after the last completed sync.
    unsynced: HashMap<ExecutionId, u64>,
}

pub(crate) struct Durability {
    database: PathBuf,
    wal: PathBuf,
    /// Litestream control socket.
    litestream: Option<PathBuf>,
    /// Every sync uploads, rather than only those a replication waiter needs.
    replicate_all: bool,
    state: Mutex<State>,
    synced: watch::Sender<u64>,
    replicated: watch::Sender<u64>,
    wanted: Notify,
}

impl Durability {
    pub(crate) fn new(database: &Path, litestream: Option<PathBuf>, replicate_all: bool) -> Self {
        let mut wal = database.as_os_str().to_owned();
        wal.push("-wal");
        Self {
            database: database.to_owned(),
            wal: PathBuf::from(wal),
            litestream,
            replicate_all,
            state: Mutex::default(),
            synced: watch::Sender::new(0),
            replicated: watch::Sender::new(0),
            wanted: Notify::new(),
        }
    }

    /// Writer thread: records executions changed by the current physical transaction.
    pub(crate) fn touch(&self, execution_ids: impl IntoIterator<Item = ExecutionId>) {
        self.state.lock().unwrap().touched.extend(execution_ids);
    }

    /// Writer thread, after a physical transaction committed. Returns its commit number, or the
    /// latest one if it changed nothing. Executions touched by a rolled back attempt are kept,
    /// which only makes their readers wait for one more sync.
    pub(crate) fn committed(&self, changed: bool) -> u64 {
        let mut state = self.state.lock().unwrap();
        if !changed && state.touched.is_empty() {
            return state.committed;
        }
        state.committed += 1;
        let seq = state.committed;
        let was_empty = state.unsynced.is_empty();
        let touched = std::mem::take(&mut state.touched);
        for execution_id in touched {
            state.unsynced.insert(execution_id, seq);
        }
        if was_empty && !state.unsynced.is_empty() {
            // Start the syncer's idle timer.
            self.wanted.notify_one();
        }
        seq
    }

    /// Waits until commit `seq` is synced.
    pub(crate) async fn wait_seq(&self, seq: u64) {
        if *self.synced.borrow() >= seq {
            return;
        }
        {
            let mut state = self.state.lock().unwrap();
            if state.wanted < seq {
                state.wanted = seq;
            }
        }
        self.wanted.notify_one();
        let mut synced = self.synced.subscribe();
        // The sender lives as long as `self`.
        let _ = synced.wait_for(|synced| *synced >= seq).await;
    }

    /// Waits until commit `seq` is synced and, with Litestream, replicated.
    pub(crate) async fn wait_replicated(&self, seq: u64) {
        if self.litestream.is_none() || self.replicate_all {
            return self.wait_seq(seq).await;
        }
        if *self.replicated.borrow() >= seq {
            return;
        }
        {
            let mut state = self.state.lock().unwrap();
            if state.replicate_wanted < seq {
                state.replicate_wanted = seq;
            }
        }
        self.wanted.notify_one();
        let mut replicated = self.replicated.subscribe();
        // The sender lives as long as `self`.
        let _ = replicated.wait_for(|replicated| *replicated >= seq).await;
    }

    /// Waits until the latest writes of the given executions are synced.
    pub(crate) async fn wait_executions<'a>(
        &self,
        execution_ids: impl IntoIterator<Item = &'a ExecutionId>,
    ) {
        let seq = {
            let state = self.state.lock().unwrap();
            if state.unsynced.is_empty() {
                return;
            }
            execution_ids
                .into_iter()
                .filter_map(|execution_id| state.unsynced.get(execution_id).copied())
                .max()
        };
        if let Some(seq) = seq {
            self.wait_seq(seq).await;
        }
    }

    /// Waits until everything committed so far is synced.
    pub(crate) async fn barrier(&self) {
        let seq = self.state.lock().unwrap().committed;
        self.wait_seq(seq).await;
    }

    /// Waits until a commit newer than `synced` (or `replicated`) is wanted, or until unsynced
    /// writes have sat for `IDLE_SYNC_DELAY`. Returns the commit number the next sync will cover
    /// and whether it must upload; the sync must start after this returns.
    async fn next_target(&self) -> (u64, bool) {
        loop {
            let notified = self.wanted.notified();
            let pending = {
                let state = self.state.lock().unwrap();
                let replication_wanted = state.replicate_wanted > *self.replicated.borrow();
                if replication_wanted || state.wanted > *self.synced.borrow() {
                    return (state.committed, self.replicate_all || replication_wanted);
                }
                !state.unsynced.is_empty()
            };
            if pending {
                if tokio::time::timeout(IDLE_SYNC_DELAY, notified)
                    .await
                    .is_err()
                {
                    return (self.state.lock().unwrap().committed, self.replicate_all);
                }
            } else {
                notified.await;
            }
        }
    }

    /// A sync that started after `next_target` returned `target` has completed.
    fn synced(&self, target: u64, replicated: bool) {
        self.state
            .lock()
            .unwrap()
            .unsynced
            .retain(|_, seq| *seq > target);
        self.synced
            .send_modify(|synced| *synced = (*synced).max(target));
        if replicated {
            self.replicated
                .send_modify(|replicated| *replicated = (*replicated).max(target));
        }
    }

    /// Flushes everything written to the WAL so far. Any descriptor of the file flushes all of its
    /// dirty pages. SQLite holds no locks on the `-wal` file itself (they are on the `-shm` file),
    /// so opening and closing another descriptor does not release them.
    pub(crate) async fn fsync_wal(&self) -> std::io::Result<()> {
        let wal = self.wal.clone();
        match tokio::task::spawn_blocking(move || {
            // Write access: `FlushFileBuffers` on Windows requires it.
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal)
            {
                Ok(file) => file.sync_data(),
                // Not created yet, or removed after a checkpoint, which synced the database file.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        })
        .await
        {
            Ok(result) => result,
            Err(join_err) => Err(std::io::Error::other(join_err)),
        }
    }

    /// Fsyncs the WAL, then optionally waits for Litestream to replicate it.
    async fn sync(&self, replicate: bool) -> Result<(), SyncError> {
        self.fsync_wal().await.map_err(SyncError::Fsync)?;
        if let Some(socket) = self.litestream.as_ref().filter(|_| replicate) {
            match tokio::time::timeout(
                LITESTREAM_SYNC_TIMEOUT,
                litestream::sync(socket, &self.database),
            )
            .await
            {
                Ok(result) => result.map_err(SyncError::Litestream)?,
                Err(_elapsed) => {
                    return Err(SyncError::Litestream(format!(
                        "timed out after {LITESTREAM_SYNC_TIMEOUT:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Makes everything a previous process left behind durable, since nothing tracks it. A
    /// failed fsync fails the startup; Litestream is retried until it succeeds.
    pub(crate) async fn startup_sync(&self) -> std::io::Result<()> {
        let mut delay = RETRY_DELAY;
        loop {
            match self.sync(true).await {
                Ok(()) => return Ok(()),
                Err(SyncError::Fsync(err)) => return Err(err),
                Err(SyncError::Litestream(err)) => {
                    warn!("Waiting for the startup Litestream sync - {err}");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    }

    /// Runs syncs until the task is aborted. One sync runs at a time; waiters that arrive during
    /// a sync are released together by the next one.
    pub(crate) async fn run_syncer(self: std::sync::Arc<Self>) {
        let mut previous = 0;
        let mut delay = RETRY_DELAY;
        loop {
            let (target, replicate) = self.next_target().await;
            let started = Instant::now();
            match self.sync(replicate).await {
                Ok(()) => {
                    debug!(
                        commits = target - previous,
                        replicate,
                        elapsed_ms = started.elapsed().as_millis(),
                        "WAL synced"
                    );
                    previous = target;
                    delay = RETRY_DELAY;
                    self.synced(target, replicate);
                }
                Err(err) => {
                    // Waiters keep waiting: nothing they wait for may be revealed before it is durable.
                    // A Litestream failure follows a successful fsync, so only replication waiters
                    // wait, unless every sync must replicate.
                    if matches!(err, SyncError::Litestream(_)) && !self.replicate_all {
                        self.synced(target, false);
                    }
                    error!("Cannot sync the SQLite WAL - {err}");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum SyncError {
    #[error("fsync failed: {0}")]
    Fsync(std::io::Error),
    #[error("Litestream sync failed: {0}")]
    Litestream(String),
}

#[cfg(unix)]
mod litestream {
    use http_body_util::{BodyExt as _, Full};
    use hyper::body::Bytes;
    use std::path::Path;

    /// `POST /sync` on the Litestream control socket, waiting until the WAL is uploaded.
    pub(super) async fn sync(socket: &Path, database: &Path) -> Result<(), String> {
        let body = serde_json::json!({ "path": database, "wait": true }).to_string();
        let stream = tokio::net::UnixStream::connect(socket)
            .await
            .map_err(|err| format!("cannot connect to {socket:?}: {err}"))?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|err| err.to_string())?;
        let connection = tokio::spawn(connection);
        let request = hyper::Request::post("/sync")
            .header(hyper::header::HOST, "localhost")
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(body)))
            .map_err(|err| err.to_string())?;
        let result = async {
            let response = sender
                .send_request(request)
                .await
                .map_err(|err| err.to_string())?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|err| err.to_string())?
                .to_bytes();
            if status.is_success() {
                Ok(())
            } else {
                Err(format!(
                    "{status}: {}",
                    String::from_utf8_lossy(&body).trim()
                ))
            }
        }
        .await;
        connection.abort();
        result
    }
}

#[cfg(not(unix))]
mod litestream {
    use std::path::Path;

    pub(super) async fn sync(_socket: &Path, _database: &Path) -> Result<(), String> {
        Err("Litestream sync requires a Unix socket".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn durability() -> (tempfile::TempDir, Arc<Durability>) {
        let dir = tempfile::tempdir().unwrap();
        let durability = Arc::new(Durability::new(&dir.path().join("db.sqlite"), None, true));
        (dir, durability)
    }

    #[test]
    fn commit_without_changes_keeps_the_number() {
        let (_dir, durability) = durability();
        assert_eq!(0, durability.committed(false));
        assert_eq!(1, durability.committed(true));
        assert_eq!(1, durability.committed(false));
        durability.touch([ExecutionId::generate()]);
        assert_eq!(2, durability.committed(false));
    }

    #[tokio::test]
    async fn waiters_are_released_by_the_syncer() {
        let (_dir, durability) = durability();
        let execution_id = ExecutionId::generate();
        let other_id = ExecutionId::generate();
        durability.touch([execution_id.clone()]);
        let seq = durability.committed(true);
        // Not in the map: does not wait.
        durability.wait_executions([&other_id]).await;
        let syncer = tokio::spawn(durability.clone().run_syncer());
        tokio::time::timeout(Duration::from_secs(5), async {
            durability.wait_executions([&execution_id]).await;
            durability.barrier().await;
        })
        .await
        .unwrap();
        assert!(*durability.synced.borrow() >= seq);
        assert!(durability.state.lock().unwrap().unsynced.is_empty());
        syncer.abort();
    }

    #[tokio::test]
    async fn unwaited_writes_are_synced_when_idle() {
        let (_dir, durability) = durability();
        durability.touch([ExecutionId::generate()]);
        durability.committed(true);
        let syncer = tokio::spawn(durability.clone().run_syncer());
        let mut synced_rx = durability.synced.subscribe();
        tokio::time::timeout(
            IDLE_SYNC_DELAY * 5,
            synced_rx.wait_for(|synced| *synced == 1),
        )
        .await
        .unwrap()
        .unwrap();
        syncer.abort();
    }
    /// Fake Litestream answering each `POST /sync` with the next status, recording the bodies.
    #[cfg(unix)]
    fn fake_litestream(
        socket: &std::path::Path,
        statuses: Vec<u16>,
    ) -> (tokio::task::JoinHandle<()>, Arc<Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::UnixListener::bind(socket).unwrap();
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let bodies = bodies.clone();
            async move {
                for status in statuses {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buf = [0; 1024];
                    // Read headers and the body, sent together by hyper for a small `Full` body.
                    while !request.ends_with(b"}") {
                        let n = stream.read(&mut buf).await.unwrap();
                        assert_ne!(0, n);
                        request.extend_from_slice(&buf[..n]);
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert!(request.starts_with("POST /sync HTTP/1.1\r\n"), "{request}");
                    let body = request.split("\r\n\r\n").nth(1).unwrap().to_string();
                    bodies.lock().unwrap().push(body);
                    stream
                        .write_all(
                            format!("HTTP/1.1 {status} X\r\ncontent-length: 2\r\n\r\nok")
                                .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            }
        });
        (task, bodies)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn only_replication_waiters_upload() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("litestream.sock");
        let durability = Arc::new(Durability::new(
            &dir.path().join("db.sqlite"),
            Some(socket.clone()),
            false,
        ));
        let (server, bodies) = fake_litestream(&socket, vec![200]);
        let syncer = tokio::spawn(durability.clone().run_syncer());
        tokio::time::timeout(Duration::from_secs(5), async {
            let seq = durability.committed(true);
            durability.wait_seq(seq).await;
            assert!(bodies.lock().unwrap().is_empty());
            let seq = durability.committed(true);
            durability.wait_replicated(seq).await;
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(1, bodies.lock().unwrap().len());
        syncer.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_replication_releases_local_waiters() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("litestream.sock");
        let durability = Arc::new(Durability::new(
            &dir.path().join("db.sqlite"),
            Some(socket.clone()),
            false,
        ));
        let (server, bodies) = fake_litestream(&socket, vec![500; 100]);
        let syncer = tokio::spawn(durability.clone().run_syncer());
        let replicated_seq = durability.committed(true);
        let replication = tokio::spawn({
            let durability = durability.clone();
            async move { durability.wait_replicated(replicated_seq).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            // Committed while the replica refuses uploads.
            while bodies.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let seq = durability.committed(true);
            durability.wait_seq(seq).await;
        })
        .await
        .unwrap();
        assert!(!replication.is_finished());
        syncer.abort();
        server.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn litestream_sync_is_retried_until_it_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("litestream.sock");
        let database = dir.path().join("db.sqlite");
        let durability = Durability::new(&database, Some(socket.clone()), true);
        let (server, bodies) = fake_litestream(&socket, vec![500, 200]);
        tokio::time::timeout(Duration::from_secs(5), durability.startup_sync())
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        let expected = serde_json::json!({ "path": database, "wait": true });
        let bodies = bodies.lock().unwrap();
        assert_eq!(2, bodies.len());
        for body in bodies.iter() {
            assert_eq!(
                expected,
                serde_json::from_str::<serde_json::Value>(body).unwrap()
            );
        }
    }
}
