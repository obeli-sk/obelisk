use chrono::{DateTime, Utc};
use concepts::time::ClockFn;
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use tracing::info;

#[derive(Clone)]
pub struct SimClock {
    // The watched value is the single source of truth for the current simulated
    // time. Advancing time notifies async waiters (e.g. the deterministic
    // deadline tracker) so they can re-check their deadline against sim time.
    current_time: Arc<watch::Sender<DateTime<Utc>>>,
}

impl Default for SimClock {
    fn default() -> Self {
        Self::epoch()
    }
}

impl SimClock {
    #[must_use]
    pub fn epoch() -> SimClock {
        Self::new(DateTime::from_timestamp_nanos(0))
    }
}

impl SimClock {
    #[must_use]
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            current_time: Arc::new(watch::Sender::new(now)),
        }
    }

    pub fn move_time_forward(&self, duration: Duration) {
        let mut old = DateTime::default();
        self.current_time.send_modify(|current| {
            old = *current;
            *current += duration;
        });
        info!("Set clock from `{old}` to `{new}`", new = self.now());
    }

    pub fn move_time_to(&self, new: DateTime<Utc>) {
        let mut old = DateTime::default();
        self.current_time.send_modify(|current| {
            old = *current;
            assert!(old <= new);
            *current = new;
        });
        info!("Set clock from `{old}` to `{new}`");
    }

    /// Subscribe to simulated time advances. The returned receiver is woken
    /// (via `changed()`) every time `move_time_*` is called.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<DateTime<Utc>> {
        self.current_time.subscribe()
    }
}
impl ClockFn for SimClock {
    fn now(&self) -> DateTime<Utc> {
        *self.current_time.borrow()
    }

    fn clone_box(&self) -> Box<dyn ClockFn> {
        Box::new(self.clone())
    }
}
