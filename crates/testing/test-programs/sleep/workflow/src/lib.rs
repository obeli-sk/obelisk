mod bindings;
use std::time::Duration;

use crate::bindings::testing::sleep::sleep as sleep_activity;
use bindings::obelisk::workflow::host_activities;
use bindings::testing::sleep_workflow_obelisk_ext::workflow as workflow_ext;
bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep_host_activity(millis: u32) {
        host_activities::sleep(millis);
    }

    fn sleep_activity(millis: u32) {
        sleep_activity::sleep(millis);
    }

    fn reschedule(nanos: host_activities::Duration, iterations: u8) {
        if iterations > 0 {
            workflow_ext::reschedule_schedule(
                workflow_ext::ScheduledAt::In(nanos),
                nanos,
                iterations - 1,
            );
        }
    }
}
