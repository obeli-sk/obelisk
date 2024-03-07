pub mod wasi_http;
pub mod wasm_tools;

pub mod time {
    use chrono::DateTime;
    use chrono::Utc;

    cfg_if::cfg_if! {
        if #[cfg(all(test, madsim))] {
            pub fn now() -> DateTime<Utc> {
                if madsim::rand::random() {
                    madsim::time::advance(std::time::Duration::from_millis(1));
                }
                DateTime::from(madsim::time::TimeHandle::current().now_time())
            }
            pub fn now_tokio_instant() -> tokio::time::Instant {
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
