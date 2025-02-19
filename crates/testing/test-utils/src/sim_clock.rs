use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tracing::info;
use utils::time::{ClockFn, Now};

#[derive(Clone)]
pub struct SimClock {
    current_time: Arc<std::sync::Mutex<DateTime<Utc>>>,
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new(Now.now())
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
            current_time: Arc::new(std::sync::Mutex::new(now)),
        }
    }

    pub async fn move_time_forward(&self, duration: Duration) {
        let (old, new) = {
            let mut guard = self.current_time.lock().unwrap();
            let old = *guard;
            let new = old + duration;
            *guard = new;
            (old, new)
        };
        tokio::time::sleep(Duration::ZERO).await; // Hack that makes sure the other task have a chance to make progress.
        info!("Set clock from `{old}` to `{new}`");
    }

    pub async fn move_time_to(&self, new: DateTime<Utc>) {
        let (old, new) = {
            let mut guard = self.current_time.lock().unwrap();
            let old = *guard;
            assert!(old <= new);
            *guard = new;
            (old, new)
        };
        tokio::time::sleep(Duration::ZERO).await; // Hack that makes sure the other task have a chance to make progress.
        info!("Set clock from `{old}` to `{new}`");
    }
}
impl ClockFn for SimClock {
    fn now(&self) -> DateTime<Utc> {
        *self.current_time.lock().unwrap()
    }
}

#[derive(Clone, Copy)]
pub struct ConstClock(pub DateTime<Utc>);
impl ClockFn for ConstClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}
