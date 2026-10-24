use chrono::{DateTime, Utc};
use concepts::{storage::ResponseSubscriptionEnd, time::ClockFn};
use std::{pin::Pin, time::Duration};
use tokio::sync::watch;

/// Future that reports why a blocked wait should stop.
pub(crate) type ResponseSubscriptionFuture =
    Pin<Box<dyn Future<Output = ResponseSubscriptionEnd> + Send>>;
/// Either a future that will report the wait's end, or an immediate end reason.
pub(crate) type TrackResult = Result<ResponseSubscriptionFuture, TrackInterruptKind>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptKind {
    /// Send an executor-closing execution yield.
    ExecutorClosing,
    /// The real-run durable event budget was exhausted.
    WorkflowEventLimitReached,
    /// Send [`WorkerResultOk::DbUpdatedByWorkerOrWatcher`], the pause/cancel RPC must have appended the event.
    PauseOrCancel,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WatcherInterruptKind {
    ExecutorClosing,
    PauseOrCancel,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum TrackInterruptKind {
    LockDeadlineReached,
    ExecutorClosing,
    PauseOrCancel,
}

impl From<WatcherInterruptKind> for InterruptKind {
    fn from(kind: WatcherInterruptKind) -> Self {
        match kind {
            WatcherInterruptKind::ExecutorClosing => Self::ExecutorClosing,
            WatcherInterruptKind::PauseOrCancel => Self::PauseOrCancel,
        }
    }
}

pub enum DeadlineTracker {
    ClockFn(DeadlineTrackerClockFn),
    Replay,
}

impl DeadlineTracker {
    pub(crate) fn check_preempt(&self) -> Result<(), WatcherInterruptKind> {
        match self {
            Self::ClockFn(tracker) => tracker.check_preempt(),
            Self::Replay => Ok(()),
        }
    }

    pub(crate) fn track(&self, subscription_interruption: Option<Duration>) -> TrackResult {
        match self {
            Self::ClockFn(tracker) => tracker.track(subscription_interruption),
            Self::Replay => unreachable!("`track` is not called for the interrupt strategy"),
        }
    }

    /// True once half of `lock_duration` remains until the lock expires.
    #[must_use]
    pub fn close_to_expired(&self, lock_duration: Duration) -> bool {
        match self {
            Self::ClockFn(tracker) => tracker.close_to_expired(lock_duration),
            Self::Replay => false,
        }
    }

    pub fn epoch_callback_check(&self) -> Result<(), EpochCallbackError> {
        match self {
            Self::ClockFn(tracker) => tracker.epoch_callback_check(),
            Self::Replay => Ok(()),
        }
    }

    pub fn extend_by(&mut self, lock_extension: Duration) -> ExtendBy {
        match self {
            Self::ClockFn(tracker) => tracker.extend_by(lock_extension),
            Self::Replay => unreachable!("`close_to_expired` is always false for replay"),
        }
    }
}

pub enum DeadlineTrackerFactory {
    ClockFn { clock_fn: Box<dyn ClockFn> },
    Replay,
}

impl DeadlineTrackerFactory {
    #[must_use]
    pub fn new(clock_fn: Box<dyn ClockFn>) -> Self {
        Self::ClockFn { clock_fn }
    }

    #[must_use]
    pub fn for_replay() -> Self {
        Self::Replay
    }

    #[must_use]
    pub fn is_for_replay(&self) -> bool {
        matches!(self, Self::Replay)
    }

    pub fn create(
        &self,
        lock_expires_at: DateTime<Utc>,
        execution_interrupt_watcher: watch::Receiver<bool>,
        local_interrupt_watcher: watch::Receiver<bool>,
    ) -> Result<DeadlineTracker, LockAlreadyExpired> {
        let Self::ClockFn { clock_fn } = self else {
            return Ok(DeadlineTracker::Replay);
        };
        let started_at = clock_fn.now();
        if (lock_expires_at - started_at).to_std().is_err() {
            return Err(LockAlreadyExpired { started_at });
        }
        Ok(DeadlineTracker::ClockFn(DeadlineTrackerClockFn {
            lock_expires_at,
            clock_fn: clock_fn.clone_box(),
            execution_interrupt_watcher,
            local_interrupt_watcher,
        }))
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

pub struct DeadlineTrackerClockFn {
    pub(crate) lock_expires_at: DateTime<Utc>, // updated on every extension
    pub(crate) clock_fn: Box<dyn ClockFn>,
    execution_interrupt_watcher: watch::Receiver<bool>,
    local_interrupt_watcher: watch::Receiver<bool>,
}

fn interrupt_kind_from_watchers(
    execution_interrupt_watcher: &watch::Receiver<bool>,
    local_interrupt_watcher: &watch::Receiver<bool>,
) -> Option<WatcherInterruptKind> {
    if *execution_interrupt_watcher.borrow() {
        Some(WatcherInterruptKind::ExecutorClosing)
    } else if *local_interrupt_watcher.borrow() {
        Some(WatcherInterruptKind::PauseOrCancel)
    } else {
        None
    }
}

impl DeadlineTrackerClockFn {
    fn interrupt_kind(&self) -> Option<WatcherInterruptKind> {
        interrupt_kind_from_watchers(
            &self.execution_interrupt_watcher,
            &self.local_interrupt_watcher,
        )
    }
}

impl DeadlineTrackerClockFn {
    fn check_preempt(&self) -> Result<(), WatcherInterruptKind> {
        if let Some(kind) = self.interrupt_kind() {
            Err(kind)
        } else {
            Ok(())
        }
    }

    fn track(&self, subscription_interruption: Option<Duration>) -> TrackResult {
        let now = self.clock_fn.now();

        let Ok(duration_to_expiry) = (self.lock_expires_at - now).to_std() else {
            return Err(TrackInterruptKind::LockDeadlineReached);
        };

        if let Some(kind) = self.interrupt_kind() {
            return Err(match kind {
                WatcherInterruptKind::ExecutorClosing => TrackInterruptKind::ExecutorClosing,
                WatcherInterruptKind::PauseOrCancel => TrackInterruptKind::PauseOrCancel,
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
                Ok(_) = local_interrupt_watcher.wait_for(|&v| v) => ResponseSubscriptionEnd::PauseOrCancel,
            }
        }))
    }

    fn extend_by(&mut self, lock_extension: Duration) -> ExtendBy {
        let now = self.clock_fn.now();
        self.lock_expires_at = now + lock_extension;
        ExtendBy {
            lock_expires_at: self.lock_expires_at,
            now,
        }
    }

    fn close_to_expired(&self, lock_duration: Duration) -> bool {
        self.lock_expires_at - lock_duration / 2 <= self.clock_fn.now()
    }

    fn epoch_callback_check(&self) -> Result<(), EpochCallbackError> {
        if let Some(kind) = self.interrupt_kind() {
            Err(EpochCallbackError::Interrupt(kind.into()))
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

impl Clone for DeadlineTrackerFactory {
    fn clone(&self) -> Self {
        match self {
            Self::ClockFn { clock_fn } => Self::new(clock_fn.clone_box()),
            Self::Replay => Self::Replay,
        }
    }
}

#[cfg(test)]
#[must_use]
pub fn deadline_tracker_factory_test(
    sim_clock: &test_utils::sim_clock::SimClock,
) -> std::sync::Arc<DeadlineTrackerFactory> {
    std::sync::Arc::new(DeadlineTrackerFactory::new(sim_clock.clone_box()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::sim_clock::SimClock;

    #[test]
    fn short_lock_renews_after_half_elapsed() {
        const LOCK: Duration = Duration::from_secs(2);
        let sim_clock = SimClock::epoch();
        let mut tracker = DeadlineTrackerFactory::new(sim_clock.clone_box())
            .create(
                sim_clock.now() + LOCK,
                watch::channel(false).1,
                watch::channel(false).1,
            )
            .unwrap();
        for _ in 0..3 {
            sim_clock.move_time_forward(Duration::from_millis(999));
            assert!(!tracker.close_to_expired(LOCK));
            sim_clock.move_time_forward(Duration::from_millis(1));
            assert!(tracker.close_to_expired(LOCK));
            let extend_by = tracker.extend_by(LOCK);
            assert_eq!(sim_clock.now() + LOCK, extend_by.lock_expires_at);
        }
    }
}
