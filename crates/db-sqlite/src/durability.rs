//! Group fsync for `synchronous = NORMAL`.
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
use concepts::ExecutionId;
use hashbrown::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};
use tracing::{debug, error};

/// Writes nobody waited for are synced after this long, so the map drains when the server goes
/// quiet and later reads of those executions do not wait.
const IDLE_SYNC_DELAY: Duration = Duration::from_secs(1);
const RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Default)]
struct State {
    committed: u64,
    wanted: u64,
    /// Executions written by the physical transaction being applied.
    touched: Vec<ExecutionId>,
    /// Latest commit of each execution whose state changed after the last completed sync.
    unsynced: HashMap<ExecutionId, u64>,
}

pub(crate) struct Durability {
    wal: PathBuf,
    state: Mutex<State>,
    synced: watch::Sender<u64>,
    wanted: Notify,
}

impl Durability {
    pub(crate) fn new(database: &std::path::Path) -> Self {
        let mut wal = database.as_os_str().to_owned();
        wal.push("-wal");
        Self {
            wal: PathBuf::from(wal),
            state: Mutex::default(),
            synced: watch::Sender::new(0),
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

    /// Waits until a commit newer than `synced` is wanted, or until unsynced writes have sat for
    /// `IDLE_SYNC_DELAY`. Returns the commit number the next sync will cover; the sync must start
    /// after this returns.
    async fn next_target(&self) -> u64 {
        loop {
            let notified = self.wanted.notified();
            let pending = {
                let state = self.state.lock().unwrap();
                if state.wanted > *self.synced.borrow() {
                    return state.committed;
                }
                !state.unsynced.is_empty()
            };
            if pending {
                if tokio::time::timeout(IDLE_SYNC_DELAY, notified)
                    .await
                    .is_err()
                {
                    return self.state.lock().unwrap().committed;
                }
            } else {
                notified.await;
            }
        }
    }

    /// A sync that started after `next_target` returned `target` has completed.
    fn synced(&self, target: u64) {
        self.state
            .lock()
            .unwrap()
            .unsynced
            .retain(|_, seq| *seq > target);
        self.synced
            .send_modify(|synced| *synced = (*synced).max(target));
    }

    /// Flushes everything written to the WAL so far. Any descriptor of the file flushes all of its
    /// dirty pages. SQLite holds no locks on the `-wal` file itself (they are on the `-shm` file),
    /// so opening and closing another descriptor does not release them.
    pub(crate) fn fsync_wal(&self) -> std::io::Result<()> {
        // Write access: `FlushFileBuffers` on Windows requires it.
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.wal)
        {
            Ok(file) => file.sync_data(),
            // Not created yet, or removed after a checkpoint, which synced the database file.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Runs syncs until the task is aborted. One sync runs at a time; waiters that arrive during
    /// a sync are released together by the next one.
    pub(crate) async fn run_syncer(self: std::sync::Arc<Self>) {
        let mut previous = 0;
        loop {
            let target = self.next_target().await;
            let started = Instant::now();
            let durability = self.clone();
            let result = match tokio::task::spawn_blocking(move || durability.fsync_wal()).await {
                Ok(result) => result,
                Err(join_err) => Err(std::io::Error::other(join_err)),
            };
            match result {
                Ok(()) => {
                    debug!(
                        commits = target - previous,
                        elapsed_ms = started.elapsed().as_millis(),
                        "WAL synced"
                    );
                    previous = target;
                    self.synced(target);
                }
                Err(err) => {
                    // Waiters keep waiting: nothing they wait for may be revealed before it is durable.
                    error!("Cannot fsync the SQLite WAL - {err:?}");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn durability() -> (tempfile::TempDir, Arc<Durability>) {
        let dir = tempfile::tempdir().unwrap();
        let durability = Arc::new(Durability::new(&dir.path().join("db.sqlite")));
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
}
