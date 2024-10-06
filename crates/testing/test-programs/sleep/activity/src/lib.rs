use std::time::Duration;

use bindings::exports::testing::sleep::sleep::Duration as DurationEnum;

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep::sleep::Guest for Component {
    fn sleep(duration: DurationEnum) {
        std::thread::sleep(Duration::from(duration));
    }

    fn sleep_loop(duration: DurationEnum, iterations: u32) {
        for _ in 0..iterations {
            Self::sleep(duration);
        }
    }
}

impl From<DurationEnum> for Duration {
    fn from(value: DurationEnum) -> Self {
        match value {
            DurationEnum::Millis(millis) => Duration::from_millis(millis),
            DurationEnum::Secs(secs) => Duration::from_secs(secs),
            DurationEnum::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
            DurationEnum::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
            DurationEnum::Days(days) => Duration::from_secs(u64::from(days * 24 * 60 * 60)),
        }
    }
}
