use exports::testing::sleep_workflow::workflow::Guest;
use obelisk::types::execution::ExecutionId;
use obelisk::types::time::Duration as DurationEnum;
use obelisk::types::time::ScheduleAt;
use obelisk::workflow1_1_0::workflow_support as workflow_support1;
use obelisk::workflow1_1_0::workflow_support::ClosingStrategy as ClosingStrategy1;
use obelisk::workflow1_1_0::workflow_support::new_join_set_generated as new_join_set_generated1;
use obelisk::workflow2_0_0::workflow_support as workflow_support2;
use testing::sleep::sleep as sleep_activity;
use testing::sleep_obelisk_ext::sleep as sleep_activity_ext;
use testing::sleep_workflow_obelisk_ext::workflow as workflow_ext;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn sleep_host_activity(duration: DurationEnum) {
        workflow_support1::sleep(duration);
    }

    fn sleep_schedule_at(schedule_at: ScheduleAt) {
        workflow_support2::sleep(schedule_at);
    }

    fn sleep_activity(duration: DurationEnum) {
        sleep_activity::sleep(duration);
    }

    fn sleep_activity_submit(duration: DurationEnum) -> ExecutionId {
        let join_set_id = new_join_set_generated1(ClosingStrategy1::Complete);
        sleep_activity_ext::sleep_submit(&join_set_id, duration)
    }

    fn reschedule(duration: DurationEnum, iterations: u8) {
        if iterations > 0 {
            workflow_ext::reschedule_schedule(ScheduleAt::In(duration), duration, iterations - 1);
        }
    }

    fn sleep_random(min_millis: u64, max_millis_inclusive: u64) {
        let random_millis =
            workflow_support2::random_u64_inclusive(min_millis, max_millis_inclusive);
        let random_duration = DurationEnum::Milliseconds(random_millis);
        workflow_support1::sleep(random_duration);
    }
}
