use generated::export;
use generated::exports::testing::stub_workflow::workflow::Guest;
use generated::obelisk::log::log;
use generated::obelisk::types::execution::{
    AwaitNextExtensionError, ExecutionId, GetExtensionError, ResponseId, StubError,
};
use generated::obelisk::types::time::{Duration, ScheduleAt};
use generated::obelisk::workflow::workflow_support::join_set_create_named;
use generated::obelisk::workflow::workflow_support::{self, join_set_create};
use generated::testing::stub_activity::activity;
use generated::testing::stub_activity_obelisk_ext::activity as activity_ext;
use generated::testing::stub_activity_obelisk_stub::activity as activity_stub;

use crate::generated::obelisk;

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn submit_stub_await(arg: String) -> Result<String, ()> {
        let join_set = join_set_create(None);
        let execution_id = activity_ext::foo_submit(&join_set, &arg);
        activity_stub::foo_stub(&execution_id, Ok(&format!("stubbing {arg}")))
            .expect("stubbed activity must accept returned value once");
        let ret_val =
            activity_ext::foo_await_next(&join_set).expect("stubbed execution result above");
        let Some(ResponseId::ExecutionId(actual_execution_id)) = join_set.last_id() else {
            unreachable!("await-next processed a child execution")
        };
        assert_eq!(execution_id.id, actual_execution_id.id);
        ret_val
    }

    fn submit_await(arg: String) -> Result<String, ()> {
        activity::foo(&arg)
    }

    fn noret_submit_await() -> Result<(), ()> {
        activity::noret()
    }

    fn submit_race_join_next_stub() -> Result<(), ()> {
        submit_race_join_next(RaceConfig::Stub);
        Ok(())
    }
    fn submit_race_join_next_stub_error() -> Result<(), ()> {
        submit_race_join_next(RaceConfig::StubError);
        Ok(())
    }
    fn submit_race_join_next_delay() -> Result<(), ()> {
        submit_race_join_next(RaceConfig::Delay);
        Ok(())
    }

    fn stub_subworkflow(execution_id: ExecutionId, retval: String) -> Result<(), ()> {
        activity_stub::foo_stub(&execution_id, Ok(&format!("stubbing {retval}"))).map_err(|_| ())
    }

    fn await_next_produces_all_processed_error() -> Result<(), ()> {
        let join_set = join_set_create(None);
        let AwaitNextExtensionError::AllProcessed =
            activity_ext::foo_await_next(&join_set).unwrap_err()
        else {
            unreachable!()
        };
        Ok(())
    }

    // Used for testing Join Set Closing
    fn join_next_in_scope() -> Result<(), ()> {
        {
            let join_set_a = join_set_create_named("a", None).expect("name is valid");
            activity_ext::foo_submit(&join_set_a, "a");
            workflow_support::submit_delay(&join_set_a, ScheduleAt::In(Duration::Days(1)), None);

            let join_set_b = join_set_create_named("b", None).expect("name is valid");
            let exe_b = activity_ext::foo_submit(&join_set_b, "b");
            activity_stub::foo_stub(&exe_b, Err(())).unwrap();
            let b_result = workflow_support::join_next(&join_set_b, None).unwrap();
            b_result.unwrap_err();
            let Some(ResponseId::ExecutionId(found)) = join_set_b.last_id() else {
                unreachable!()
            };
            assert_eq!(exe_b.id, found.id);

            let join_set_f = join_set_create_named("f", None).expect("name is valid");
            activity_ext::foo_submit(&join_set_f, "f");
            workflow_support::submit_delay(&join_set_f, ScheduleAt::In(Duration::Days(1)), None);
            std::mem::forget(join_set_f);
            // a and b are dropped here, b is already processed.
        }
        log::info("after scope closed");
        let join_set_c = join_set_create_named("c", None).expect("name is valid");
        activity_ext::foo_submit(&join_set_c, "c");
        Ok(())
        // f and c are dropped here
    }

    fn join_set_close_cancellation_order() -> Result<(), ()> {
        let join_set = join_set_create_named("close-order", None).expect("name is valid");
        activity_ext::foo_submit(&join_set, "first");
        workflow_support::submit_delay(&join_set, ScheduleAt::In(Duration::Days(1)), None);
        activity_ext::foo_submit(&join_set, "second");
        workflow_support::submit_delay(&join_set, ScheduleAt::In(Duration::Days(2)), None);
        Ok(())
    }

    fn invoke_expect_execution_error() -> Result<(), ()> {
        let res = activity::noret();
        res.unwrap_err();
        Ok(())
    }

    fn stub_not_found() -> Result<(), ()> {
        let fake_execution_id = ExecutionId {
            id: "E_00000000000000000000000000.n:fake_1".to_string(),
        };
        let result = activity_stub::foo_stub(&fake_execution_id, Ok("stubbed value"));
        assert!(matches!(result, Err(StubError::ExecutionNotFound)));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaceConfig {
    Delay,
    Stub,
    StubError,
}

fn submit_race_join_next(config: RaceConfig) {
    const OK_STUB_RESP: &str = "ok";
    let join_set = join_set_create(None);
    let execution_id = activity_ext::foo_submit(&join_set, "some param");
    let delay_id = workflow_support::submit_delay(
        &join_set,
        obelisk::types::time::ScheduleAt::In(obelisk::types::time::Duration::Milliseconds(10)),
        None,
    );
    match config {
        RaceConfig::Stub => {
            activity_stub::foo_stub(&execution_id, Ok(OK_STUB_RESP))
                .expect("stubbed activity must accept returned value once");
        }
        RaceConfig::StubError => {
            activity_stub::foo_stub(&execution_id, Err(()))
                .expect("stubbed activity must accept returned value once");
        }
        RaceConfig::Delay => {
            // wait for timeout
        }
    }
    let race_result = workflow_support::join_next(&join_set, None)
        .expect("two submissions and no response was processed yet");
    match join_set.last_id().expect("a response was processed") {
        ResponseId::ExecutionId(reported_id) => {
            assert_eq!(reported_id.id, execution_id.id);
            match activity_ext::foo_get(&execution_id) {
                Ok(Ok(ok)) => {
                    assert_eq!(RaceConfig::Stub, config);
                    assert_eq!(OK_STUB_RESP, ok);
                    race_result.expect("stub is ok");
                }
                Ok(Err(())) => {
                    assert_eq!(RaceConfig::StubError, config);
                    race_result.expect_err("stub is err");
                }
                Err(GetExtensionError::FunctionMismatch(_)) => {
                    unreachable!("no other functions were submitted")
                }
                Err(GetExtensionError::NotFoundInProcessedResponses) => {
                    unreachable!("got it from join_next")
                }
            }
        }
        ResponseId::DelayId(reported_id) => {
            assert_eq!(RaceConfig::Delay, config);
            assert_eq!(delay_id.id, reported_id.id);
            race_result.expect("not cancelled");
            // activity should be cancelled on close
            workflow_support::join_set_close(join_set, None);
            match activity_ext::foo_get(&execution_id) {
                Ok(Ok(_)) => {
                    unreachable!("was not stubbed, should have been cancelled")
                }
                Ok(Err(())) => {
                    // Expecting that close would cancel the activity.
                }
                Err(GetExtensionError::FunctionMismatch(_)) => {
                    unreachable!("no other functions were submitted")
                }
                Err(GetExtensionError::NotFoundInProcessedResponses) => {
                    unreachable!("should have been processed and cancelled in join set close")
                }
            }
        }
    }
}
