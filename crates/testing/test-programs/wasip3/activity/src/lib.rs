use generated::export;
use generated::exports::testing::wasip3::sleeper::Guest;
use generated::obelisk::log::log;
use generated::wasi::clocks::monotonic_clock;

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    async fn sleep_and_double(n: u64, sleep_millis: u64) -> Result<u64, String> {
        log::info(&format!("wasip3 activity sleeping for {sleep_millis}ms"));
        monotonic_clock::wait_for(sleep_millis.saturating_mul(1_000_000)).await;
        log::info("wasip3 activity woke up");
        Ok(n.saturating_mul(2))
    }
}
