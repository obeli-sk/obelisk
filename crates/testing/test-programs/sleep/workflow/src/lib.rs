use generated::export;
use generated::exports::testing::sleep_workflow::workflow::Guest;
use generated::obelisk::types::execution::ExecutionId;
use generated::obelisk::types::execution::ResponseId;
use generated::obelisk::types::join_set::JoinNextError;
use generated::obelisk::types::time::Duration as DurationEnum;
use generated::obelisk::types::time::ScheduleAt;
use generated::obelisk::workflow::workflow_support;
use generated::testing::sleep::sleep as sleep_activity;
use generated::testing::sleep_obelisk_ext::sleep as sleep_activity_ext;
use generated::testing::sleep_obelisk_schedule::sleep as sleep_activity_schedule;

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn sleep_host_activity(duration: DurationEnum) -> Result<(), ()> {
        workflow_support::sleep(ScheduleAt::In(duration)).expect("not cancelled");
        Ok(())
    }

    fn sleep_schedule_at(schedule_at: ScheduleAt) -> Result<(), ()> {
        workflow_support::sleep(schedule_at).expect("not cancelled");
        Ok(())
    }

    fn sleep_activity(duration: DurationEnum) -> Result<(), ()> {
        sleep_activity::sleep(duration).unwrap();
        Ok(())
    }

    fn sleep_activity_submit(duration: DurationEnum) -> Result<ExecutionId, ()> {
        let join_set = workflow_support::join_set_create();
        Ok(sleep_activity_ext::sleep_submit(&join_set, duration))
    }

    fn sleep_random(min_millis: u64, max_millis_inclusive: u64) -> Result<(), ()> {
        let random_millis =
            workflow_support::random_u64_inclusive(min_millis, max_millis_inclusive);
        let random_duration = DurationEnum::Milliseconds(random_millis);
        workflow_support::sleep(ScheduleAt::In(random_duration)).expect("not cancelled");
        Ok(())
    }

    fn schedule_noop(duration: DurationEnum) -> Result<(), ()> {
        sleep_activity_schedule::noop_schedule(ScheduleAt::In(duration));
        Ok(())
    }

    fn two_delays_in_same_join_set() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create();
        let _long = join_set.submit_delay(ScheduleAt::In(DurationEnum::Seconds(10)));
        let short = join_set.submit_delay(ScheduleAt::In(DurationEnum::Milliseconds(10)));
        // result<tuple<response-id, result>, join-next-error>
        let (ResponseId::DelayId(first), res) = join_set
            .join_next()
            .expect("submitted two delays, joining first")
        else {
            unreachable!("only delays have been submitted");
        };
        assert!(res.is_ok());
        assert_eq!(short.id, first.id);
        // long delay will be cancelled when the join set is closed.
        Ok(())
    }

    fn join_next_produces_all_processed_error() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create();
        join_set.submit_delay(ScheduleAt::In(DurationEnum::Milliseconds(10)));
        let (_delay_id, res) = join_set.join_next().expect("join set contains 1 delay");
        res.expect("not cancelled");
        let JoinNextError::AllProcessed = join_set.join_next().unwrap_err();
        Ok(())
    }
}
