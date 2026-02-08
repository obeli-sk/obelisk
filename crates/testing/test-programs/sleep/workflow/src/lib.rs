use crate::generated::obelisk::log::log;
use generated::export;
use generated::exports::testing::sleep_workflow::workflow::Guest;
use generated::obelisk::types::execution::ResponseId;
use generated::obelisk::types::time::Duration as DurationEnum;
use generated::obelisk::types::time::ScheduleAt;
use generated::obelisk::workflow::workflow_support::{self, JoinNextError};
use generated::testing::sleep::sleep as sleep_activity;
use generated::testing::sleep_obelisk_ext::sleep as sleep_activity_ext;
use generated::testing::sleep_obelisk_schedule::sleep as sleep_activity_schedule;

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn sleep_host_activity(duration: DurationEnum) -> Result<(), ()> {
        log::info("changed");
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

    fn sleep_activity_submit() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create();
        sleep_activity_ext::sleep_submit(&join_set, DurationEnum::Days(1));
        // Should be cancelled in join set close
        Ok(())
    }

    fn sleep_activity_submit_then_trap() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create();
        sleep_activity_ext::sleep_submit(&join_set, DurationEnum::Days(1));
        // Should be cancelled in join set close
        panic!()
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
        let _long =
            workflow_support::submit_delay(&join_set, ScheduleAt::In(DurationEnum::Seconds(10)));
        let short = workflow_support::submit_delay(
            &join_set,
            ScheduleAt::In(DurationEnum::Milliseconds(10)),
        );
        // result<tuple<response-id, result>, join-next-error>
        let (ResponseId::DelayId(first), res) =
            workflow_support::join_next(&join_set).expect("submitted two delays, joining first")
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
        workflow_support::submit_delay(&join_set, ScheduleAt::In(DurationEnum::Milliseconds(10)));
        let (_delay_id, res) =
            workflow_support::join_next(&join_set).expect("join set contains 1 delay");
        res.expect("not cancelled");
        let JoinNextError::AllProcessed = workflow_support::join_next(&join_set).unwrap_err();
        Ok(())
    }
}
