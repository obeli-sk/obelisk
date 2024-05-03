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

    pub fn move_time_forward(&self, duration: Duration) {
        let mut guard = self.current_time.lock().unwrap();
        let old = *guard;
        let new = old + duration;
        *guard = new;
        info!("Set clock from `{old}` to `{new}`");
    }

    pub fn move_time_to(&self, new: DateTime<Utc>) {
        let mut guard = self.current_time.lock().unwrap();
        let old = *guard;
        assert!(old <= new);
        *guard = new;
        info!("Set clock from `{old}` to `{new}`");
    }

    pub fn get_clock_fn(&self) -> impl Fn() -> DateTime<Utc> + Clone {
        let current_time = self.current_time.clone();
        move || *current_time.lock().unwrap()
    }

    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        *self.current_time.lock().unwrap()
    }
}
