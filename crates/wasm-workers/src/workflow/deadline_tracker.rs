use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::time::ClockFn;
use std::{cmp::min, pin::Pin, time::Duration};
use tracing::{trace, warn};

#[async_trait]
pub trait DeadlineTracker: Send + Sync {
    fn track(&self, max_duration: Duration) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>>;

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

pub(crate) struct DeadlineTrackerTokio<C: ClockFn> {
    pub(crate) deadline: tokio::time::Instant, // Tracked as instant because calling track happens later after creation.
    pub(crate) clock_fn: C,
    pub(crate) leeway: Duration, // Fire this much sooner than requested.
}

#[async_trait]
impl<C: ClockFn> DeadlineTracker for DeadlineTrackerTokio<C> {
    fn track(&self, max_duration: Duration) -> Option<Pin<Box<dyn Future<Output = ()> + Send>>> {
        if self.close_to_expired() {
            None
        } else {
            let max_instant = tokio::time::Instant::now() + max_duration;
            let shorter = min(max_instant, self.deadline);
            Some(Box::pin(tokio::time::sleep_until(shorter)))
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

#[derive(Clone)]
pub struct DeadlineTrackerFactoryTokio<C: ClockFn> {
    pub leeway: Duration, // Fire this much sooner than requested.
    pub clock_fn: C,
}

impl<C: ClockFn> DeadlineTrackerFactory for DeadlineTrackerFactoryTokio<C> {
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
            clock_fn: self.clock_fn.clone(),
            leeway: self.leeway,
        };
        Ok(Box::new(tracker))
    }
}

#[cfg(test)]
#[must_use]
pub fn deadline_tracker_factory_test(
    sim_clock: test_utils::sim_clock::SimClock,
) -> std::sync::Arc<impl DeadlineTrackerFactory> {
    std::sync::Arc::new(DeadlineTrackerFactoryTokio {
        leeway: Duration::ZERO,
        clock_fn: sim_clock,
    })
}
