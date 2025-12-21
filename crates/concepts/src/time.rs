use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use std::time::Duration;

pub trait ClockFn: Send + Sync + Clone + 'static {
    fn now(&self) -> DateTime<Utc>;
}

#[async_trait]
pub trait Sleep: Send + Sync + Clone + 'static {
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
    #[cfg(not(feature = "test"))]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
    #[cfg(feature = "test")]
    fn now(&self) -> DateTime<Utc> {
        let micros = Utc::now().timestamp_micros();
        chrono::TimeZone::timestamp_micros(&Utc, micros).unwrap()
    }
}
