use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{storage::TimeoutOutcome, time::ClockFn};
use std::{cmp::min, pin::Pin, time::Duration};
use tracing::{trace, warn};

#[async_trait]
pub trait DeadlineTracker: Send + Sync {
    /// Return a future that resolves on deadline. If `max_duration` is specified, the future resolves
    /// at deadline or after this duration, whatever expires first.
    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>;

    fn close_to_expired(&self) -> bool;

    /// Return new lock expiry date (now + duration). Internally track that time minus leeway.
    fn extend_by(&mut self, lock_extension: Duration) -> DateTime<Utc>;
}

pub trait DeadlineTrackerFactory: Send + Sync {
    fn create(
        &self,
        lock_expires_at: DateTime<Utc>,
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired>;
}

#[derive(Debug, thiserror::Error)]
#[error("lock already expired before {started_at}")]
pub struct LockAlreadyExpired {
    pub started_at: DateTime<Utc>,
}

pub(crate) struct DeadlineTrackerTokio {
    pub(crate) deadline: tokio::time::Instant, // Tracked as instant because calling track happens later after creation.
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) leeway: Duration, // Fire this much sooner than requested.
}

#[async_trait]
impl DeadlineTracker for DeadlineTrackerTokio {
    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>> {
        if self.close_to_expired() {
            None
        } else {
            let expiry = if let Some(max_duration) = max_duration {
                let max_instant = tokio::time::Instant::now() + max_duration;
                min(max_instant, self.deadline)
            } else {
                self.deadline
            };

            Some(Box::pin(async move {
                tokio::time::sleep_until(expiry).await;
                TimeoutOutcome::Timeout
            }))
        }
    }

    fn close_to_expired(&self) -> bool {
        self.deadline <= tokio::time::Instant::now()
    }

    fn extend_by(&mut self, lock_extension: Duration) -> DateTime<Utc> {
        let now = self.clock_fn.now();
        let lock_duration = if lock_extension > self.leeway {
            lock_extension.checked_sub(self.leeway).unwrap()
        } else {
            warn!("Not setting the leeway as deadline duration is too short");
            lock_extension
        };
        self.deadline = tokio::time::Instant::now() + lock_duration;

        now + lock_extension
    }
}

pub struct DeadlineTrackerFactoryTokio {
    pub leeway: Duration, // Fire this much sooner than requested.
    pub clock_fn: Box<dyn ClockFn>,
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
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired> {
        let started_at = self.clock_fn.now();
        let Ok(deadline_duration) = (lock_expires_at - started_at).to_std() else {
            return Err(LockAlreadyExpired { started_at });
        };
        let deadline_duration = if deadline_duration > self.leeway {
            deadline_duration.checked_sub(self.leeway).unwrap()
        } else {
            warn!("Not setting the leeway as deadline duration is too short");
            deadline_duration
        };
        trace!("Setting deadline to now + {deadline_duration:?}");
        let deadline = tokio::time::Instant::now() + deadline_duration;
        let tracker = DeadlineTrackerTokio {
            deadline,
            clock_fn: self.clock_fn.clone_box(),
            leeway: self.leeway,
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
    ) -> Result<Box<dyn DeadlineTracker>, LockAlreadyExpired> {
        Ok(Box::new(DeadlineTrackerFactoryForReplay {}))
    }
}
impl DeadlineTracker for DeadlineTrackerFactoryForReplay {
    fn track(
        &self,
        _max_duration: Option<Duration>,
    ) -> Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>> {
        unreachable!("`track` is not called for the interrupt strategy")
    }

    fn close_to_expired(&self) -> bool {
        false
    }

    fn extend_by(&mut self, _lock_extension: Duration) -> DateTime<Utc> {
        unreachable!("`close_to_expired` returns always false")
    }
}
