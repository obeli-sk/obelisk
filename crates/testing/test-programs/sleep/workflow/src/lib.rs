use assert_matches::assert_matches;
use generated::export;
use generated::exports::testing::sleep_workflow::workflow::Guest;
use generated::obelisk::log::log;
use generated::obelisk::types::execution::ResponseId;
use generated::obelisk::types::time::Duration as DurationEnum;
use generated::obelisk::types::time::ScheduleAt;
use generated::obelisk::workflow::workflow_support::{self, JoinNextError, JoinNextTryError};
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
        // Required by `advance_forwards_captured_application_logs`.
        log::info("changed");
        workflow_support::sleep(ScheduleAt::In(duration), None, None).expect("not cancelled");
        Ok(())
    }

    fn sleep_host_activity_schedule_many(
        n: u64,
        schedule_at: ScheduleAt,
        duration_secs: u64,
    ) -> Result<(), ()> {
        let params = format!(r#"[{{"seconds":{}}}]"#, duration_secs);
        for _ in 0..n {
            let exe = workflow_support::execution_id_generate(None);
            workflow_support::schedule_json(
                &exe,
                schedule_at,
                &generated::obelisk::types::execution::Function {
                    interface_name: "testing:sleep-workflow/workflow".to_string(),
                    function_name: "sleep-host-activity".to_string(),
                },
                &params,
                None,
                None,
            )
            .unwrap();
        }
        Ok(())
    }

    fn sleep_schedule_at(schedule_at: ScheduleAt) -> Result<(), ()> {
        workflow_support::sleep(schedule_at, None, None).expect("not cancelled");
        Ok(())
    }

    fn sleep_activity(duration: DurationEnum) -> Result<(), ()> {
        sleep_activity::sleep(duration).unwrap();
        Ok(())
    }

    fn sleep_activity_submit() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        sleep_activity_ext::sleep_submit(&join_set, DurationEnum::Days(1));
        // Should be cancelled in join set close
        Ok(())
    }

    fn sleep_activity_submit_then_trap() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        sleep_activity_ext::sleep_submit(&join_set, DurationEnum::Days(1));
        // Should be cancelled in join set close
        panic!()
    }

    fn sleep_random(min_millis: u64, max_millis_inclusive: u64) -> Result<(), ()> {
        let random_millis =
            workflow_support::random_u64_inclusive(min_millis, max_millis_inclusive, None);
        let random_duration = DurationEnum::Milliseconds(random_millis);
        workflow_support::sleep(ScheduleAt::In(random_duration), None, None).expect("not cancelled");
        Ok(())
    }

    fn schedule_noop(duration: DurationEnum) -> Result<(), ()> {
        sleep_activity_schedule::noop_schedule(ScheduleAt::In(duration));
        Ok(())
    }

    fn two_delays_in_same_join_set() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        let _long =
            workflow_support::submit_delay(&join_set, ScheduleAt::In(DurationEnum::Seconds(10)), None);
        let short = workflow_support::submit_delay(
            &join_set,
            ScheduleAt::In(DurationEnum::Milliseconds(10)),
            None,
        );
        // join-next returns the value; the delay id comes from `last-id`.
        let res = workflow_support::join_next(&join_set, None)
            .expect("submitted two delays, joining first");
        let Some(ResponseId::DelayId(first)) = join_set.last_id() else {
            unreachable!("only delays have been submitted");
        };
        assert!(res.is_ok());
        assert_eq!(short.id, first.id);
        // long delay will be cancelled when the join set is closed.
        Ok(())
    }

    fn join_next_produces_all_processed_error() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        workflow_support::submit_delay(&join_set, ScheduleAt::In(DurationEnum::Milliseconds(10)), None);
        let res = workflow_support::join_next(&join_set, None).expect("join set contains 1 delay");
        res.expect("not cancelled");
        let JoinNextError::AllProcessed = workflow_support::join_next(&join_set, None).unwrap_err();
        Ok(())
    }

    fn join_next_try_pending() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        // Submit a long delay
        workflow_support::submit_delay(&join_set, ScheduleAt::In(DurationEnum::Seconds(10)), None);
        // Try to join immediately - should return Pending since delay hasn't expired yet
        assert_matches!(
            workflow_support::join_next_try(&join_set, None),
            Err(JoinNextTryError::Pending)
        );
        Ok(())
    }

    fn join_next_try_all_processed() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        // No requests submitted - should return AllProcessed
        assert!(matches!(
            workflow_support::join_next_try(&join_set, None),
            Err(JoinNextTryError::AllProcessed)
        ));
        Ok(())
    }

    fn join_next_try_found() -> Result<(), ()> {
        let join_set = workflow_support::join_set_create(None);
        // Submit a very short delay (should complete almost immediately)
        let delay_id = workflow_support::submit_delay(
            &join_set,
            ScheduleAt::In(DurationEnum::Milliseconds(1)),
            None,
        );
        // Wait a bit for the delay to expire using `sleep`
        workflow_support::sleep(ScheduleAt::In(DurationEnum::Milliseconds(10)), None, None)
            .expect("should not been cancelled");
        // join-next-try should find the response; the delay id comes from `last-id`.
        let res = workflow_support::join_next_try(&join_set, None)
            .expect("should get the delay response");
        let Some(ResponseId::DelayId(first)) = join_set.last_id() else {
            unreachable!("only delays have been submitted");
        };
        res.expect("should not been cancelled");
        assert_eq!(delay_id.id, first.id);
        // Now join_next_try should return AllProcessed since we already processed it
        assert_matches!(
            workflow_support::join_next_try(&join_set, None),
            Err(JoinNextTryError::AllProcessed)
        );
        Ok(())
    }
}
