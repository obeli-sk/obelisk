use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;

pub trait ClockFn: Send + Sync + Clone + 'static {
    fn now(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait Sleep: Send + Sync + Clone {
    async fn sleep(&self, duration: Duration);
}

#[derive(Clone)]
pub struct TokioSleep;

#[async_trait]
impl Sleep for TokioSleep {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[must_use]
pub fn now_tokio_instant() -> tokio::time::Instant {
    tokio::time::Instant::now()
}

#[derive(Clone)]
pub struct Now;

impl ClockFn for Now {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct SleepFactory {
    deadline: tokio::time::Instant,
}
impl SleepFactory {
    #[must_use]
    pub fn new(deadline_duration: Duration) -> Self {
        Self {
            deadline: tokio::time::Instant::now() + deadline_duration,
        }
    }
    pub fn new_sleep(&self) -> tokio::time::Sleep {
        tokio::time::sleep_until(self.deadline)
    }

    #[must_use]
    pub fn deadline_reached(&self) -> bool {
        self.deadline <= tokio::time::Instant::now()
    }
}
