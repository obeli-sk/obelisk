use crate::exports::testing::stub_workflow::workflow::Guest;
use crate::obelisk::workflow::workflow_support::{self, ClosingStrategy};
use crate::testing::stub_activity::activity;
use crate::testing::stub_activity_obelisk_ext::activity as activity_ext;
use crate::testing::stub_activity_obelisk_stub::activity as activity_stub;
use obelisk::log::log;
use obelisk::types::execution::{
    AwaitNextExtensionError, ExecutionId, GetExtensionError, JoinSetId, ResponseId,
};
use obelisk::workflow::workflow_support::new_join_set_named;
use wit_bindgen::generate;
generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn submit_stub_await(arg: String) -> Result<String, ()> {
        let join_set = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
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
        let join_set = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
        let AwaitNextExtensionError::AllProcessed =
            activity_ext::foo_await_next(&join_set).unwrap_err()
        else {
            unreachable!()
        };
        Ok(())
    }

    // Used for testing Join Set Closing
    fn join_next_in_scope() -> Result<(), ()> {
        fn add_exec(join_set: &JoinSetId, names: Vec<&'static str>) {
            for name in names {
                let execution_id = activity_ext::foo_submit(join_set, name);
                activity_stub::foo_stub(&execution_id, Ok(name))
                    .expect("stubbed activity must accept returned value once");
            }
        }
        {
            let join_set_a = new_join_set_named("a", ClosingStrategy::Complete).unwrap();
            add_exec(&join_set_a, vec!["a", "aa"]);
            let join_set_b = new_join_set_named("b", ClosingStrategy::Complete).unwrap();
            add_exec(&join_set_b, vec!["b", "bb"]);
            let join_set_forgotten =
                workflow_support::new_join_set_named("f", ClosingStrategy::Complete).unwrap();
            add_exec(&join_set_forgotten, vec!["f", "ff"]);
            std::mem::forget(join_set_forgotten);
        }
        log::info("after scope closed");
        let join_set_c = new_join_set_named("c", ClosingStrategy::Complete).unwrap();
        add_exec(&join_set_c, vec!["c", "cc"]);
        Ok(())
    }

    fn invoke_expect_execution_error() -> Result<(), ()> {
        activity_ext::noret_invoke("")
            .expect("join set name can be an empty string")
            .unwrap_err();
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
    let join_set = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
    let execution_id = activity_ext::foo_submit(&join_set, "some param");
    let delay_id = workflow_support::submit_delay(
        &join_set,
        obelisk::types::time::ScheduleAt::In(obelisk::types::time::Duration::Milliseconds(10)),
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
    match workflow_support::join_next(&join_set)
        .expect("two submissions and no response was processed yet")
    {
        ResponseId::ExecutionId(reported_id) => {
            assert_eq!(reported_id.id, execution_id.id);
            match activity_ext::foo_get(&execution_id) {
                Ok(Ok(ok)) => {
                    assert_eq!(RaceConfig::Stub, config);
                    assert_eq!(OK_STUB_RESP, ok);
                }
                Ok(Err(())) => {
                    assert_eq!(RaceConfig::StubError, config);
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
            // Cannot cancel waiting for the execution when closing join set, just mock it:
            activity_stub::foo_stub(&execution_id, Ok(OK_STUB_RESP))
                .expect("stubbed activity must accept returned value once");
        }
    }
}
