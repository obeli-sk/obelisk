use jiff::Timestamp;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub trait ClockFn: Send + Sync + 'static {
    fn now(&self) -> Timestamp;
    fn clone_box(&self) -> Box<dyn ClockFn>;
}

pub trait Sleep: Send + Sync + 'static {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct TokioSleep;

#[cfg(feature = "runtime")]
impl Sleep for TokioSleep {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[cfg(feature = "runtime")]
#[must_use]
pub fn now_tokio_instant() -> tokio::time::Instant {
    tokio::time::Instant::now()
}

#[derive(Clone)]
pub struct Now;

impl ClockFn for Now {
    #[cfg(not(feature = "test"))]
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
    #[cfg(feature = "test")]
    fn now(&self) -> Timestamp {
        let micros = Timestamp::now().as_microsecond();
        Timestamp::from_microsecond(micros).unwrap()
    }

    fn clone_box(&self) -> Box<dyn ClockFn> {
        Box::new(self.clone())
    }
}

#[derive(Clone, Copy)]
pub struct ConstClock(pub Timestamp);
impl ClockFn for ConstClock {
    fn now(&self) -> Timestamp {
        self.0
    }

    fn clone_box(&self) -> Box<dyn ClockFn> {
        Box::new(*self)
    }
}
