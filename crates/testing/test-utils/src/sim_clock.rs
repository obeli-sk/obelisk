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

    pub fn sleep(&self, duration: Duration) {
        // FIXME: yield with a probability in madsim
        let old = *self.current_time.lock().unwrap();
        let new = old + duration;
        *self.current_time.lock().unwrap() = new;
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
