use std::{sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use tracing::info;
use utils::time::now;

#[derive(Clone)]
pub struct SimClock {
    current_time: Arc<std::sync::Mutex<DateTime<Utc>>>,
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new(now())
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

    pub fn get_clock_fn(&self) -> impl Fn() -> DateTime<Utc> + Clone {
        let current_time = self.current_time.clone();
        move || *current_time.lock().unwrap()
    }

    #[must_use]
    pub fn now(&self) -> DateTime<Utc> {
        *self.current_time.lock().unwrap()
    }
}
