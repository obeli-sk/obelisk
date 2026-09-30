use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{storage::ResponseSubscriptionEnd, time::ClockFn};
use std::{pin::Pin, time::Duration};
use tokio::sync::watch;

/// Future that reports why a blocked wait should stop.
pub type ResponseSubscriptionFuture = Pin<Box<dyn Future<Output = ResponseSubscriptionEnd> + Send>>;
/// Either a future that will report the wait's end, or an immediate end reason.
pub type TrackResult = Result<ResponseSubscriptionFuture, ResponseSubscriptionEnd>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptKind {
    /// Send an executor-closing execution yield.
    ExecutorClosing,
    /// The real-run durable event budget was exhausted.
    WorkflowEventLimitReached,
    /// Send [`WorkerResultOk::DbUpdatedByWorkerOrWatcher`], the pause/cancel RPC must have appended the event.
    PauseOrCancel,
}

#[async_trait]
pub trait DeadlineTracker: Send + Sync {
    /// Host functions must check whether the execution was interrupted because epoch callback is triggered only in WASM execution.
    fn check_preempt(&self) -> Result<(), PreemptRequested>;

    /// Called after the workflow made progress and is now blocked waiting for a response.
    /// Returns a future that reports why the caller should stop waiting. If
    /// `subscription_interruption` is specified, it can also end for periodic polling.
    /// Future must return on lock expiry with `ResponseSubscriptionEnd::LockDeadlineReached`.
    /// Leeway for lock expiry is not observed, so a successful return might trigger lock extension that might race with executor locking.
    fn track(&self, subscription_interruption: Option<Duration>) -> TrackResult;

    /// Returns `true` if `now` >= `lock_expires_at` - `leeway`.
    fn close_to_expired(&self) -> bool;

    /// Called by epoch callback
    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError>;

    /// Called after `close_to_expired` returned `true`, Return new lock expiry date (now + duration). Internally track that time minus leeway.
    fn extend_by(&mut self, lock_extension: Duration) -> ExtendBy;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum PreemptRequested {
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
pub enum EpochCallbackError {
    #[error("lock expired")]
    LockExpired,
    #[error("execution interrupt: {0:?}")]
    Interrupt(InterruptKind),
}

pub(crate) struct DeadlineTrackerClockFn {
    pub(crate) lock_expires_at: DateTime<Utc>, // updated on every extension
    pub(crate) close_to_expired: DateTime<Utc>, // lock_expires_at - leeway, updated on every extension
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) leeway: Duration, // for close_to_expired
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

impl DeadlineTrackerClockFn {
    fn interrupt_kind(&self) -> Option<InterruptKind> {
        interrupt_kind_from_watchers(
            &self.execution_interrupt_watcher,
            &self.local_interrupt_watcher,
        )
    }
}

#[async_trait]
impl DeadlineTracker for DeadlineTrackerClockFn {
    fn check_preempt(&self) -> Result<(), PreemptRequested> {
        if let Some(kind) = self.interrupt_kind() {
            Err(PreemptRequested::Interrupt(kind))
        } else {
            Ok(())
        }
    }

    fn track(&self, subscription_interruption: Option<Duration>) -> TrackResult {
        let now = self.clock_fn.now();

        let Ok(duration_to_expiry) = (self.lock_expires_at - now).to_std() else {
            return Err(ResponseSubscriptionEnd::LockDeadlineReached);
        };

        if let Some(kind) = self.interrupt_kind() {
            return Err(match kind {
                InterruptKind::ExecutorClosing => ResponseSubscriptionEnd::ExecutorClosing,
                InterruptKind::PauseOrCancel => ResponseSubscriptionEnd::ExecutionUpdated,
                InterruptKind::WorkflowEventLimitReached => unreachable!("not watcher-driven"), // FIXME: Exract narrower enum
            });
        }

        let (expiry, expiry_reason) = match subscription_interruption {
            Some(max_duration) if max_duration < duration_to_expiry => {
                (max_duration, ResponseSubscriptionEnd::PollIntervalElapsed)
            }
            _ => (
                duration_to_expiry,
                ResponseSubscriptionEnd::LockDeadlineReached,
            ),
        };
        let mut execution_interrupt_watcher = self.execution_interrupt_watcher.clone();
        let mut local_interrupt_watcher = self.local_interrupt_watcher.clone();
        Ok(Box::pin(async move {
            tokio::select! {
                () = tokio::time::sleep(expiry) => expiry_reason,
                Ok(_) = execution_interrupt_watcher.wait_for(|&v| v) => ResponseSubscriptionEnd::ExecutorClosing,
                Ok(_) = local_interrupt_watcher.wait_for(|&v| v) => ResponseSubscriptionEnd::ExecutionUpdated,
            }
        }))
    }

    fn extend_by(&mut self, lock_extension: Duration) -> ExtendBy {
        let now = self.clock_fn.now();
        self.lock_expires_at = now + lock_extension;
        self.close_to_expired = self.lock_expires_at - self.leeway;
        ExtendBy {
            lock_expires_at: self.lock_expires_at,
            now,
        }
    }

    fn close_to_expired(&self) -> bool {
        self.close_to_expired <= self.clock_fn.now()
    }

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError> {
        if let Some(kind) = self.interrupt_kind() {
            Err(EpochCallbackError::Interrupt(kind))
        } else if self.lock_expires_at <= self.clock_fn.now() {
            Err(EpochCallbackError::LockExpired)
        } else {
            Ok(())
        }
    }
}

pub struct ExtendBy {
    pub lock_expires_at: DateTime<Utc>,
    pub now: DateTime<Utc>,
}

// TODO: Rename to DeadlineTrackerFactoryClockFn
pub struct DeadlineTrackerFactoryTokio {
    pub leeway: Duration, // Used by `close_to_expiry` for lock extension.
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
        if (lock_expires_at - started_at).to_std().is_err() {
            return Err(LockAlreadyExpired { started_at });
        }
        let close_to_expired = lock_expires_at - self.leeway;
        let tracker = DeadlineTrackerClockFn {
            lock_expires_at,
            close_to_expired,
            clock_fn: self.clock_fn.clone_box(),
            leeway: self.leeway,
            execution_interrupt_watcher,
            local_interrupt_watcher,
        };
        Ok(Box::new(tracker))
    }
}

#[cfg(test)]
#[must_use]
pub fn deadline_tracker_factory_test(
    sim_clock: &test_utils::sim_clock::SimClock,
) -> std::sync::Arc<impl DeadlineTrackerFactory + use<>> {
    std::sync::Arc::new(DeadlineTrackerFactoryTokio {
        leeway: Duration::ZERO,
        clock_fn: sim_clock.clone_box(),
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

    fn track(&self, _max_duration: Option<Duration>) -> TrackResult {
        unreachable!("`track` is not called for the interrupt strategy")
    }

    fn close_to_expired(&self) -> bool {
        false
    }

    fn check_epoch_callback(&self) -> Result<(), EpochCallbackError> {
        Ok(())
    }

    fn extend_by(&mut self, _lock_extension: Duration) -> ExtendBy {
        unreachable!("`close_to_expired` returns always false")
    }
}
