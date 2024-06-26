mod bindings;
use std::time::Duration;

use crate::bindings::testing::sleep::sleep as sleep_activity;
use bindings::obelisk::workflow::host_activities;
bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::sleep_workflow::workflow::Guest for Component {
    fn sleep_host_activity(millis: u32) {
        host_activities::sleep(millis);
    }

    fn sleep_activity(millis: u32) {
        sleep_activity::sleep(millis);
    }

    fn reschedule(schedule_millis: u32) {
        // reschedule itself with the same parameters.
        host_activities::schedule(
            "testing:sleep-workflow/workflow.reschedule",
            &format!("[{schedule_millis}]"),
            host_activities::ScheduledAt::In(
                Duration::from_millis(schedule_millis.into())
                    .as_nanos()
                    .try_into()
                    .unwrap(),
            ),
        );
    }
}
