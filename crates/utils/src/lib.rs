pub mod wasi_http;
pub mod wasm_tools;

pub mod time {
    use chrono::DateTime;
    use chrono::Utc;

    cfg_if::cfg_if! {
        if #[cfg(all(test, madsim))] {
            pub fn now() -> DateTime<Utc> {
                if madsim::rand::random() {
                    madsim::time::advance(std::time::Duration::from_millis(madsim::rand::random()));
                }
                DateTime::from(madsim::time::TimeHandle::current().now_time())
            }
            pub fn now_tokio_instant() -> tokio::time::Instant {
                if madsim::rand::random() {
                    madsim::time::advance(std::time::Duration::from_millis(madsim::rand::random()));
                }
                madsim::time::Instant::now()
            }
        } else {
            pub fn now() -> DateTime<Utc> {
                Utc::now()
            }
            pub fn now_tokio_instant() -> tokio::time::Instant {
                tokio::time::Instant::now()
            }
        }
    }
}

// FIXME: replace tracing-unwrap with this hook
pub fn tracing_panic_hook(panic_info: &std::panic::PanicInfo) {
    let payload = panic_info.payload();

    #[allow(clippy::manual_map)]
    let payload = if let Some(s) = payload.downcast_ref::<&str>() {
        Some(&**s)
    } else if let Some(s) = payload.downcast_ref::<String>() {
        Some(s.as_str())
    } else {
        None
    };

    let location = panic_info.location().map(|l| l.to_string());

    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred: {backtrace}"
        );
    } else {
        tracing::error!(
            panic.payload = payload,
            panic.location = location,
            "A panic occurred",
        );
    }
}
