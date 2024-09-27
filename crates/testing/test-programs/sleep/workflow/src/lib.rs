mod bindings;
use std::time::Duration;

use crate::bindings::testing::sleep::sleep as sleep_activity;
use bindings::obelisk::workflow::host_activities::{self};
use bindings::testing::sleep_workflow_obelisk_ext::workflow as workflow_ext;
bindings::export!(Component with_types_in bindings);
use crate::bindings::obelisk::types::time::ScheduleAt;

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep_host_activity(millis: u64) {
        host_activities::sleep(u64::try_from(Duration::from_millis(millis).as_nanos()).unwrap());
    }

    fn sleep_activity(millis: u64) {
        sleep_activity::sleep(millis);
    }

    fn reschedule(nanos: host_activities::Duration, iterations: u8) {
        if iterations > 0 {
            workflow_ext::reschedule_schedule(ScheduleAt::In(nanos), nanos, iterations - 1);
        }
    }
}
