use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{storage::TimeoutOutcome, time::ClockFn};
use std::{cmp::min, pin::Pin, sync::Arc, time::Duration};
use tracing::{trace, warn};

#[async_trait]
pub trait DeadlineTracker: Send + Sync {

    /// Called after the workflow made progress and is now blocked waiting for a response.
    /// Return a future that resolves on deadline. If `max_duration` is specified, the future resolves
    /// at deadline or after this duration, whatever expires first.
    /// Return `None` if expired.
    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>>;

    fn close_to_expired(&self) -> bool;

    /// Called after `close_to_expired` returned `true`, Return new lock expiry date (now + duration). Internally track that time minus leeway.
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
    pub(crate) deadline: tokio::time::Instant,
    pub(crate) deadline_minus_leeway: tokio::time::Instant, // Tracked as instant because calling track happens later after creation.
    pub(crate) clock_fn: Box<dyn ClockFn>,
    pub(crate) leeway: Duration, // Fire this much sooner than requested.
}

#[async_trait]
impl DeadlineTracker for DeadlineTrackerTokio {
    fn track(
        &self,
        max_duration: Option<Duration>,
    ) -> Option<Pin<Box<dyn Future<Output = TimeoutOutcome> + Send>>> {
        if self.deadline <= tokio::time::Instant::now() {
            None
        } else {
            let expiry = if let Some(max_duration) = max_duration {
                let max_instant = tokio::time::Instant::now() + max_duration;
                min(max_instant, self.deadline_minus_leeway)
            } else {
                self.deadline_minus_leeway
            };

            let mut interrupt_rx = self.hot_redeploy_signal.subscribe();
            Some(Box::pin(async move {
                tokio::select! {
                    () = tokio::time::sleep_until(expiry) => TimeoutOutcome::Timeout,
                    _ = interrupt_rx.wait_for(|&v| v) => TimeoutOutcome::Timeout,
                }
            }))
        }
    }

    fn close_to_expired(&self) -> bool {
        self.deadline_minus_leeway <= tokio::time::Instant::now()
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
