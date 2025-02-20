pub mod sha256sum;
pub mod wasm_tools;
mod wit;

pub mod time {
    use std::time::Duration;
    use std::time::Instant;

    use async_trait::async_trait;
    use chrono::DateTime;
    use chrono::Utc;

    pub trait ClockFn: Send + Sync + Clone {
        fn now(&self) -> DateTime<Utc>;
    }

    #[async_trait]
    pub trait Sleep: Send + Sync + Clone {
        async fn sleep(&self, duration: Duration);
        async fn sleep_until(&self, deadline: Instant);
    }

    #[derive(Clone)]
    pub struct TokioSleep;

    #[async_trait]
    impl Sleep for TokioSleep {
        async fn sleep(&self, duration: Duration) {
            tokio::time::sleep(duration).await
        }
        async fn sleep_until(&self, deadline: Instant) {
            tokio::time::sleep_until(deadline.into()).await
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
                #[must_use]
                fn now(&self) -> DateTime<Utc> {
                    if madsim::rand::random() {
                        madsim::time::advance(std::time::Duration::from_millis(madsim::rand::random()));
                    }
                    DateTime::from(madsim::time::TimeHandle::current().now_time())
                }

            } else {
                #[must_use]
                fn now(&self) -> DateTime<Utc> {
                    Utc::now()
                }

            }
        }
    }
}

pub fn tracing_panic_hook(panic_info: &std::panic::PanicHookInfo) {
    let payload = panic_info.payload();
    #[expect(clippy::manual_map)]
    let payload = if let Some(s) = payload.downcast_ref::<&str>() {
        Some(&**s)
    } else if let Some(s) = payload.downcast_ref::<String>() {
        Some(s.as_str())
    } else {
        None
    };
    let location = panic_info.location().map(ToString::to_string);
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred: {backtrace}"
        );
        if let Some(payload) = payload {
            eprintln!("A panic occurred: {payload}\n{backtrace}");
        } else {
            eprintln!("A panic occurred\n{backtrace}");
        }
    } else {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred",
        );
        if let Some(payload) = payload {
            eprintln!("A panic occurred: {payload}");
        } else {
            eprintln!("A panic occurred");
        }
    }
}
