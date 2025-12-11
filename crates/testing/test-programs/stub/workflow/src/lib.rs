use crate::exports::testing::stub_workflow::workflow::Guest;
use crate::obelisk::types::time::{Duration, ScheduleAt};
use crate::obelisk::workflow::workflow_support::{self, join_set_create};
use crate::testing::stub_activity::activity;
use crate::testing::stub_activity_obelisk_ext::activity as activity_ext;
use crate::testing::stub_activity_obelisk_stub::activity as activity_stub;
use obelisk::log::log;
use obelisk::types::execution::{
    AwaitNextExtensionError, ExecutionId, GetExtensionError, ResponseId,
};
use obelisk::workflow::workflow_support::join_set_create_named;
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn submit_stub_await(arg: String) -> Result<String, ()> {
        let join_set = join_set_create();
        let execution_id = activity_ext::foo_submit(&join_set, &arg);
        activity_stub::foo_stub(&execution_id, Ok(&format!("stubbing {arg}")))
            .expect("stubbed activity must accept returned value once");
        let (actual_execution_id, ret_val) =
            activity_ext::foo_await_next(&join_set).expect("stubbed execution result above");
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
        let join_set = join_set_create();
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
            let join_set_a = join_set_create_named("a").expect("name is valid");
            activity_ext::foo_submit(&join_set_a, "a");
            join_set_a.submit_delay(ScheduleAt::In(Duration::Days(1)));

            let join_set_b = join_set_create_named("b").expect("name is valid");
            let exe_b = activity_ext::foo_submit(&join_set_b, "b");
            activity_stub::foo_stub(&exe_b, Err(())).unwrap();
            let (response_b, b_result) = join_set_b.join_next().unwrap();
            b_result.unwrap_err();
            let ResponseId::ExecutionId(found) = response_b else {
                unreachable!()
            };
            assert_eq!(exe_b.id, found.id);

            let join_set_f = join_set_create_named("f").expect("name is valid");
            activity_ext::foo_submit(&join_set_f, "f");
            join_set_f.submit_delay(ScheduleAt::In(Duration::Days(1)));
            std::mem::forget(join_set_f);
            // a and b are dropped here, b is already processed.
        }
        log::info("after scope closed");
        let join_set_c = join_set_create_named("c").expect("name is valid");
        activity_ext::foo_submit(&join_set_c, "c");
        Ok(())
        // f and c are dropped here
    }

    fn invoke_expect_execution_error() -> Result<(), ()> {
        let (_execution_id, res) =
            activity_ext::noret_invoke("").expect("join set name can be an empty string");
        res.unwrap_err();
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
    let join_set = join_set_create();
    let execution_id = activity_ext::foo_submit(&join_set, "some param");
    let delay_id = join_set.submit_delay(obelisk::types::time::ScheduleAt::In(
        obelisk::types::time::Duration::Milliseconds(10),
    ));
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
    match join_set
        .join_next()
        .expect("two submissions and no response was processed yet")
    {
        (ResponseId::ExecutionId(reported_id), res) => {
            assert_eq!(reported_id.id, execution_id.id);
            match activity_ext::foo_get(&execution_id) {
                Ok(Ok(ok)) => {
                    assert_eq!(RaceConfig::Stub, config);
                    assert_eq!(OK_STUB_RESP, ok);
                    res.expect("stub is ok");
                }
                Ok(Err(())) => {
                    assert_eq!(RaceConfig::StubError, config);
                    res.expect_err("stub is err");
                }
                Err(GetExtensionError::FunctionMismatch(_)) => {
                    unreachable!("no other functions were submitted")
                }
                Err(GetExtensionError::NotFoundInProcessedResponses) => {
                    unreachable!("got it from join_next")
                }
            }
        }
        (ResponseId::DelayId(reported_id), delay_res) => {
            assert_eq!(RaceConfig::Delay, config);
            assert_eq!(delay_id.id, reported_id.id);
            delay_res.expect("not cancelled");
            // activity should be cancelled on close
            workflow_support::join_set_close(join_set);
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
