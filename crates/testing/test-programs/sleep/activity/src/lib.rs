use exports::testing::sleep::sleep::Guest;
use obelisk::types::time::Duration as DurationEnum;
use std::time::Duration;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn sleep(duration: DurationEnum) -> Result<(), ()> {
        std::thread::sleep(Duration::from(duration));
        Ok(())
    }

    fn sleep_loop(duration: DurationEnum, iterations: u32) -> Result<(), ()> {
        for _ in 0..iterations {
            Self::sleep(duration).unwrap();
        }
        Ok(())
    }

    fn noop() -> Result<(), ()> {
        Ok(())
    }
}

impl From<DurationEnum> for Duration {
    fn from(value: DurationEnum) -> Self {
        match value {
            DurationEnum::Milliseconds(millis) => Duration::from_millis(millis),
            DurationEnum::Seconds(secs) => Duration::from_secs(secs),
            DurationEnum::Minutes(mins) => Duration::from_secs(u64::from(mins * 60)),
            DurationEnum::Hours(hours) => Duration::from_secs(u64::from(hours * 60 * 60)),
            DurationEnum::Days(days) => Duration::from_secs(u64::from(days * 24 * 60 * 60)),
        }
    }
}
