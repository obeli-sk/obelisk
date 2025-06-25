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

cfg_if::cfg_if! {
    if #[cfg(all(test, madsim))] {
        #[must_use]
        pub fn now_tokio_instant() -> tokio::time::Instant {
            if madsim::rand::random() {
                madsim::time::advance(std::time::Duration::from_millis(madsim::rand::random()));
            }
            madsim::time::Instant::now()
        }
    } else {
        #[must_use]
        pub fn now_tokio_instant() -> tokio::time::Instant {
            tokio::time::Instant::now()
        }
    }
}

#[derive(Clone)]
pub struct Now;

impl ClockFn for Now {
    cfg_if::cfg_if! {
        if #[cfg(all(test, madsim))] {
            fn now(&self) -> DateTime<Utc> {
                if madsim::rand::random() {
                    madsim::time::advance(std::time::Duration::from_millis(madsim::rand::random()));
                }
                DateTime::from(madsim::time::TimeHandle::current().now_time())
            }

        } else {
            fn now(&self) -> DateTime<Utc> {
                Utc::now()
            }

        }
    }
}
