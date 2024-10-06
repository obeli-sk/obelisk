mod bindings;
use std::time::Duration;

use crate::bindings::testing::sleep::sleep as sleep_activity;
use bindings::exports::testing::sleep_workflow::workflow::Duration as DurationEnum;
use bindings::obelisk::workflow::host_activities::{self};
use bindings::testing::sleep_workflow_obelisk_ext::workflow as workflow_ext;
bindings::export!(Component with_types_in bindings);
use crate::bindings::obelisk::types::time::ScheduleAt;

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep_host_activity(duration: DurationEnum) {
        host_activities::sleep(duration);
    }

    fn sleep_activity(duration: DurationEnum) {
        sleep_activity::sleep(duration);
    }

    fn reschedule(duration: DurationEnum, iterations: u8) {
        if iterations > 0 {
            workflow_ext::reschedule_schedule(ScheduleAt::In(duration), duration, iterations - 1);
        }
    }
}
