use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tracing::info;

#[derive(Clone)]
pub struct SimClock {
    current_time: Arc<std::sync::Mutex<DateTime<Utc>>>,
}

impl SimClock {
    #[must_use]
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            current_time: Arc::new(std::sync::Mutex::new(now)),
        }
    }

    /// Called by a test to move the time forward.
    pub fn sleep(&self, duration: Duration) {
        // FIXME: async, yield/sleep? with a probability in madsim, so that
        // other tasks can proceed
        let mut guard = self.current_time.lock().unwrap();
        let old = *guard;
        let new = old + duration;
        *guard = new;
        info!("Set clock from `{old}` to `{new}`");
    }

    pub fn sleep_until(&self, new: DateTime<Utc>) {
        // TODO async, yield
        let mut guard = self.current_time.lock().unwrap();
        let old = *guard;
        assert!(old <= new);
        *guard = new;
        info!("Set clock from `{old}` to `{new}`");
    }

    pub fn clock_fn(&self) -> impl Fn() -> DateTime<Utc> + Clone {
        let current_time = self.current_time.clone();
        move || *current_time.lock().unwrap()
    }

    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        *self.current_time.lock().unwrap()
    }
}
