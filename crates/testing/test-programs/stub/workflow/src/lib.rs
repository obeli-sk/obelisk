use crate::testing::stub_activity_obelisk_ext::activity as activity_ext;
use crate::testing::stub_activity_obelisk_stub::activity as activity_stub;
use exports::testing::stub_workflow::workflow::Guest;
use obelisk::workflow::workflow_support::{self, ClosingStrategy};
use wit_bindgen::generate;

generate!({ generate_all });
struct Component;
export!(Component);

impl Guest for Component {
    fn submit_stub_await(arg: String) -> String {
        let join_set = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
        let execution_id = activity_ext::foo_submit(&join_set, &arg);
        activity_stub::foo_stub(&execution_id, &format!("stubbing {arg}"))
            .expect("stubbed activity must accept returned value once");
        let (actual_execution_id, ret_val) =
            activity_ext::foo_await_next(&join_set).expect("stubbed activity must resolve");
        assert_eq!(execution_id.id, actual_execution_id.id);
        ret_val
    }

    fn submit_await(arg: String) -> String {
        let join_set = workflow_support::new_join_set_generated(ClosingStrategy::Complete);
        let execution_id = activity_ext::foo_submit(&join_set, &arg);
        let (actual_execution_id, ret_val) =
            activity_ext::foo_await_next(&join_set).expect("stubbed activity must resolve");
        assert_eq!(execution_id.id, actual_execution_id.id);
        ret_val
    }
}
