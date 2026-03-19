use chrono::DateTime;
use chrono::Utc;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub trait ClockFn: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
    fn clone_box(&self) -> Box<dyn ClockFn>;
}

pub trait Sleep: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[derive(Clone)]
pub struct TokioSleep;

impl Sleep for TokioSleep {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
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

    fn clone_box(&self) -> Box<dyn ClockFn> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Copy)]
pub struct ConstClock(pub DateTime<Utc>);
impl ClockFn for ConstClock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }

    fn clone_box(&self) -> Box<dyn ClockFn> {
        Box::new(*self)
    }
}
