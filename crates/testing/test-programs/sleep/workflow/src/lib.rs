use exports::testing::sleep_workflow::workflow::Guest;
use obelisk::types::execution::ExecutionId;
use obelisk::types::time::Duration as DurationEnum;
use obelisk::types::time::ScheduleAt;
use obelisk::workflow::workflow_support;
use obelisk::workflow::workflow_support::ClosingStrategy;
use obelisk::workflow::workflow_support::new_join_set_generated as new_join_set_generated1;
use testing::sleep::sleep as sleep_activity;
use testing::sleep_obelisk_ext::sleep as sleep_activity_ext;
use testing::sleep_workflow_obelisk_schedule::workflow as workflow_schedule;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn sleep_host_activity(duration: DurationEnum) {
        workflow_support::sleep(ScheduleAt::In(duration));
    }

    fn sleep_schedule_at(schedule_at: ScheduleAt) {
        workflow_support::sleep(schedule_at);
    }

    fn sleep_activity(duration: DurationEnum) {
        sleep_activity::sleep(duration);
    }

    fn sleep_activity_submit(duration: DurationEnum) -> ExecutionId {
        let join_set_id = new_join_set_generated1(ClosingStrategy::Complete);
        sleep_activity_ext::sleep_submit(&join_set_id, duration)
    }

    fn reschedule(duration: DurationEnum, iterations: u8) {
        if iterations > 0 {
            workflow_schedule::reschedule_schedule(
                ScheduleAt::In(duration),
                duration,
                iterations - 1,
            );
        }
    }

    fn sleep_random(min_millis: u64, max_millis_inclusive: u64) {
        let random_millis =
            workflow_support::random_u64_inclusive(min_millis, max_millis_inclusive);
        let random_duration = DurationEnum::Milliseconds(random_millis);
        workflow_support::sleep(ScheduleAt::In(random_duration));
    }

    fn two_delays_in_same_join_set() {
        let join_set_id = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
        let _long =
            workflow_support::submit_delay(&join_set_id, ScheduleAt::In(DurationEnum::Seconds(10)));
        // let _short = workflow_support::submit_delay(
        //     &join_set_id,
        //     ScheduleAt::In(DurationEnum::Milliseconds(10)),
        // );
        // let obelisk::types::execution::ResponseId::DelayId(first) =
        //     workflow_support::join_next(&join_set_id).unwrap()
        // else {
        //     unreachable!("only delays have been submitted");
        // };
        // assert_eq!(short.id, first.id);
    }
}
