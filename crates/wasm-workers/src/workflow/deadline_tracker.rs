use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{storage::TimeoutOutcome, time::ClockFn};
use std::{cmp::min, pin::Pin, time::Duration};
use tokio::sync::watch;
use tracing::{trace, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptKind {
    /// Send [`WorkerError::ExecutorClosing`]
    ExecutorClosing,
    /// Send [`WorkerResultOk::DbUpdatedByWorkerOrWatcher`], the pause/cancel RPC must have appended the event.
    PauseOrCancel,
}

#[async_trait]
pub trait DeadlineTracker: Send + Sync {
    /// Host functions must check whether the execution was interrupted.
    fn check_preempt(&self) -> Result<(), PreemptRequested>;

    /// Called after the workflow made progress and is now blocked waiting for a response.
    /// Returns a future that resolves once the caller should stop waiting (deadline, or
    /// an interrupt via the generic `TimeoutOutcome::Cancel`); the caller then re-calls
    /// `track`, whose `Err` reports the concrete reason. If `max_duration` is specified,
    /// the future resolves at deadline or after this duration, whichever comes first.
    /// `Err` means the caller must not block: the lock deadline already passed, or the
    /// run is being interrupted (`TrackPrecheck::Interrupt` carries the kind).
    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Result<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>, TrackPrecheck>;

    fn close_to_expired(&self) -> bool;

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError>;

    /// Called after `close_to_expired` returned `true`, Return new lock expiry date (now + duration). Internally track that time minus leeway.
    fn extend_by(&mut self, lock_extension: Duration) -> DateTime<Utc>;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PreemptRequested {
    #[error("execution interrupt: {0:?}")]
    Interrupt(InterruptKind),
}

/// Reason `track` declined to return a waiting future.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TrackPrecheck {
    #[error("lock deadline reached")]
    LockDeadlineReached,
    #[error("execution interrupt: {0:?}")]
    Interrupt(InterruptKind),
}

pub trait DeadlineTrackerFactory: Send + Sync {
    /// `execution_interrupt_watcher` is the executor-wide shutdown signal
    /// (`InterruptKind::ExecutorClosing`); `local_interrupt_watcher` is the
    /// per-execution pause/cancel signal (`InterruptKind::PauseOrCancel`). Either
    /// firing interrupts the run; the kind decides the disposition.
    fn create(
        &self,
        lock_expires_at: DateTime<Utc>,
        execution_interrupt_watcher: watch::Receiver<bool>,
        local_interrupt_watcher: watch::Receiver<bool>,
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired>;

    /// True iff this factory produces trackers that never expire the lock.
    /// Required precondition for `replay()` / `advance()` entry points.
    fn is_for_replay(&self) -> bool {
        false
    }
}

#[derive(Debug, thiserror::Error)]
#[error("lock already expired before {started_at}")]
pub struct LockAlreadyExpired {
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum EpochCallbackError {
    #[error("lock expired")]
    LockExpired,
    #[error("execution interrupt: {0:?}")]
    Interrupt(InterruptKind),
}

pub(crate) struct DeadlineTrackerTokio {
    pub(crate) deadline: tokio::time::Instant,
    pub(crate) deadline_minus_leeway: tokio::time::Instant, // Tracked as instant because calling track happens later after creation.
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) leeway: Duration, // Fire this much sooner than requested.
    execution_interrupt_watcher: watch::Receiver<bool>,
    local_interrupt_watcher: watch::Receiver<bool>,
}

fn interrupt_kind_from_watchers(
    execution_interrupt_watcher: &watch::Receiver<bool>,
    local_interrupt_watcher: &watch::Receiver<bool>,
) -> Option<InterruptKind> {
    if *execution_interrupt_watcher.borrow() {
        Some(InterruptKind::ExecutorClosing)
    } else if *local_interrupt_watcher.borrow() {
        Some(InterruptKind::PauseOrCancel)
    } else {
        None
    }
}

impl DeadlineTrackerTokio {
    fn interrupt_kind(&self) -> Option<InterruptKind> {
        interrupt_kind_from_watchers(
            &self.execution_interrupt_watcher,
            &self.local_interrupt_watcher,
        )
    }
}

#[async_trait]
impl DeadlineTracker for DeadlineTrackerTokio {
    fn check_preempt(&self) -> Result<(), PreemptRequested> {
        if let Some(kind) = self.interrupt_kind() {
            Err(PreemptRequested::Interrupt(kind))
        } else {
            Ok(())
        }
    }

    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Result<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>, TrackPrecheck> {
        if self.deadline <= tokio::time::Instant::now() {
            Err(TrackPrecheck::LockDeadlineReached)
        } else if let Some(kind) = self.interrupt_kind() {
            Err(TrackPrecheck::Interrupt(kind))
        } else {
            let expiry = if let Some(max_duration) = max_duration {
                let max_instant = tokio::time::Instant::now() + max_duration;
                min(max_instant, self.deadline_minus_leeway)
            } else {
                self.deadline_minus_leeway
            };
            let mut execution_interrupt_watcher = self.execution_interrupt_watcher.clone();
            let mut local_interrupt_watcher = self.local_interrupt_watcher.clone();
            Ok(Box::pin(async move {
                tokio::select! {
                    () = tokio::time::sleep_until(expiry) => TimeoutOutcome::Timeout,
                    _ = execution_interrupt_watcher.wait_for(|&v| v) => TimeoutOutcome::Cancel,
                    _ = local_interrupt_watcher.wait_for(|&v| v) => TimeoutOutcome::Cancel,
                }
            }))
        }
    }

    fn close_to_expired(&self) -> bool {
        self.deadline_minus_leeway <= tokio::time::Instant::now()
    }

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError> {
        if let Some(kind) = self.interrupt_kind() {
            Err(EpochCallbackError::Interrupt(kind))
        } else if self.deadline <= tokio::time::Instant::now() {
            Err(EpochCallbackError::LockExpired)
        } else {
            Ok(())
        }
    }

    fn extend_by(&mut self, lock_extension: Duration) -> DateTime<Utc> {
        let now_instant = tokio::time::Instant::now();
        self.deadline = now_instant + lock_extension;
        let lock_duration = if lock_extension > self.leeway {
            lock_extension.checked_sub(self.leeway).unwrap()
        } else {
            warn!(
                "Not setting the leeway as deadline duration {lock_extension:?} is shorter than leeway {:?}",
                self.leeway
            );
            lock_extension
        };
        self.deadline_minus_leeway = now_instant + lock_duration;

        self.clock_fn.now() + lock_extension
    }
}

pub struct DeadlineTrackerFactoryTokio {
    pub leeway: Duration, // Fire this much sooner than requested.
    pub clock_fn: Box<dyn ClockFn>,
}
impl DeadlineTrackerFactoryTokio {
    #[must_use]
    pub fn new(leeway: Duration, clock_fn: Box<dyn ClockFn>) -> Self {
        Self { leeway, clock_fn }
    }
}
impl Clone for DeadlineTrackerFactoryTokio {
    fn clone(&self) -> Self {
        Self {
            leeway: self.leeway,
            clock_fn: self.clock_fn.clone_box(),
        }
    }
}

impl DeadlineTrackerFactory for DeadlineTrackerFactoryTokio {
    fn create(
        &self,
        lock_expires_at: DateTime<Utc>,
        execution_interrupt_watcher: watch::Receiver<bool>,
        local_interrupt_watcher: watch::Receiver<bool>,
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired> {
        let started_at = self.clock_fn.now();
        let Ok(deadline_duration) = (lock_expires_at - started_at).to_std() else {
            return Err(LockAlreadyExpired { started_at });
        };
        let now = tokio::time::Instant::now();
        let deadline = now + deadline_duration;
        let deadline_duration_minus_leeway = if deadline_duration > self.leeway {
            deadline_duration.checked_sub(self.leeway).unwrap()
        } else {
            warn!("Not setting the leeway as deadline duration is too short");
            deadline_duration
        };
        trace!("Setting deadline to now + {deadline_duration_minus_leeway:?}");

        let deadline_minus_leeway = now + deadline_duration_minus_leeway;
        let tracker = DeadlineTrackerTokio {
            deadline,
            deadline_minus_leeway,
            clock_fn: self.clock_fn.clone_box(),
            leeway: self.leeway,
            execution_interrupt_watcher,
            local_interrupt_watcher,
        };
        Ok(Box::new(tracker))
    }
}

/// Deterministic-test deadline tracker driven by `SimClock` instead of real
/// tokio time. Deadlines are compared against simulated time and the `track`
/// future only resolves when the test advances the sim clock past the deadline,
/// so a blocked `Await` join-next waits for the response notification rather than
/// racing real wall-clock latency.
#[cfg(test)]
pub(crate) struct DeadlineTrackerSim {
    deadline: DateTime<Utc>,
    deadline_minus_leeway: DateTime<Utc>,
    leeway: Duration,
    clock: test_utils::sim_clock::SimClock,
    execution_interrupt_watcher: watch::Receiver<bool>,
    local_interrupt_watcher: watch::Receiver<bool>,
}

#[cfg(test)]
fn add_duration(time: DateTime<Utc>, duration: Duration) -> DateTime<Utc> {
    time + chrono::TimeDelta::from_std(duration).expect("test durations never overflow TimeDelta")
}

#[cfg(test)]
impl DeadlineTrackerSim {
    fn interrupt_kind(&self) -> Option<InterruptKind> {
        interrupt_kind_from_watchers(
            &self.execution_interrupt_watcher,
            &self.local_interrupt_watcher,
        )
    }
}

#[cfg(test)]
#[async_trait]
impl DeadlineTracker for DeadlineTrackerSim {
    fn check_preempt(&self) -> Result<(), PreemptRequested> {
        if let Some(kind) = self.interrupt_kind() {
            Err(PreemptRequested::Interrupt(kind))
        } else {
            Ok(())
        }
    }

    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Result<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>, TrackPrecheck> {
        let now = self.clock.now();
        if self.deadline <= now {
            return Err(TrackPrecheck::LockDeadlineReached);
        } else if let Some(kind) = self.interrupt_kind() {
            return Err(TrackPrecheck::Interrupt(kind));
        }
        let expiry = if let Some(max_duration) = max_duration {
            min(add_duration(now, max_duration), self.deadline_minus_leeway)
        } else {
            self.deadline_minus_leeway
        };
        let clock = self.clock.clone();
        // Subscribe now, before the future is awaited, so time advances between
        // this call and the first poll are not missed.
        let mut time_watcher = self.clock.subscribe();
        let mut execution_interrupt_watcher = self.execution_interrupt_watcher.clone();
        let mut local_interrupt_watcher = self.local_interrupt_watcher.clone();
        Ok(Box::pin(async move {
            loop {
                if clock.now() >= expiry {
                    return TimeoutOutcome::Timeout;
                }
                tokio::select! {
                    // `Err` means the `SimClock` was dropped: time will never advance again.
                    res = time_watcher.changed() => if res.is_err() {
                        return TimeoutOutcome::Cancel;
                    },
                    _ = execution_interrupt_watcher.wait_for(|&v| v) => return TimeoutOutcome::Timeout,
                    _ = local_interrupt_watcher.wait_for(|&v| v) => return TimeoutOutcome::Timeout,
                }
            }
        }))
    }

    fn close_to_expired(&self) -> bool {
        self.deadline_minus_leeway <= self.clock.now()
    }

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError> {
        if let Some(kind) = self.interrupt_kind() {
            Err(EpochCallbackError::Interrupt(kind))
        } else if self.deadline <= self.clock.now() {
            Err(EpochCallbackError::LockExpired)
        } else {
            Ok(())
        }
    }

    fn extend_by(&mut self, lock_extension: Duration) -> DateTime<Utc> {
        let now = self.clock.now();
        self.deadline = add_duration(now, lock_extension);
        let lock_duration = if lock_extension > self.leeway {
            lock_extension.checked_sub(self.leeway).unwrap()
        } else {
            warn!(
                "Not setting the leeway as deadline duration {lock_extension:?} is shorter than leeway {:?}",
                self.leeway
            );
            lock_extension
        };
        self.deadline_minus_leeway = add_duration(now, lock_duration);
        self.deadline
    }
}

#[cfg(test)]
pub(crate) struct DeadlineTrackerFactorySim {
    leeway: Duration,
    clock: test_utils::sim_clock::SimClock,
}

#[cfg(test)]
impl DeadlineTrackerFactory for DeadlineTrackerFactorySim {
    fn create(
        &self,
        lock_expires_at: DateTime<Utc>,
        execution_interrupt_watcher: watch::Receiver<bool>,
        local_interrupt_watcher: watch::Receiver<bool>,
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired> {
        let started_at = self.clock.now();
        let Ok(deadline_duration) = (lock_expires_at - started_at).to_std() else {
            return Err(LockAlreadyExpired { started_at });
        };
        let deadline_minus_leeway = if deadline_duration > self.leeway {
            lock_expires_at
                - chrono::TimeDelta::from_std(self.leeway).expect("leeway fits in TimeDelta")
        } else {
            warn!("Not setting the leeway as deadline duration is too short");
            lock_expires_at
        };
        Ok(Box::new(DeadlineTrackerSim {
            deadline: lock_expires_at,
            deadline_minus_leeway,
            leeway: self.leeway,
            clock: self.clock.clone(),
            execution_interrupt_watcher,
            local_interrupt_watcher,
        }))
    }
}

#[cfg(test)]
#[must_use]
pub fn deadline_tracker_factory_test(
    sim_clock: &test_utils::sim_clock::SimClock,
) -> std::sync::Arc<impl DeadlineTrackerFactory + use<>> {
    std::sync::Arc::new(DeadlineTrackerFactorySim {
        leeway: Duration::ZERO,
        clock: sim_clock.clone(),
    })
}

pub struct DeadlineTrackerFactoryForReplay {}

impl DeadlineTrackerFactory for DeadlineTrackerFactoryForReplay {
    fn create(
        &self,
        _lock_expires_at: DateTime<Utc>,
        _execution_interrupt_watcher: watch::Receiver<bool>,
        _local_interrupt_watcher: watch::Receiver<bool>,
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired> {
        Ok(Box::new(DeadlineTrackerFactoryForReplay {}))
    }

    fn is_for_replay(&self) -> bool {
        true
    }
}
impl DeadlineTracker for DeadlineTrackerFactoryForReplay {
    fn check_preempt(&self) -> Result<(), PreemptRequested> {
        Ok(())
    }

    fn track(
        &self,
        _max_duration: Option<Duration>,
    ) -> Result<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>, TrackPrecheck> {
        unreachable!("`track` is not called for the interrupt strategy")
    }

    fn close_to_expired(&self) -> bool {
        false
    }

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError> {
        Ok(())
    }

    fn extend_by(&mut self, _lock_extension: Duration) -> DateTime<Utc> {
        unreachable!("`close_to_expired` returns always false")
    }
}
