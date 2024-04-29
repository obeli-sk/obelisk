#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::http_workflow::workflow::Guest for Component {
    fn execute(port: u16) -> String {
        // let join_set = bindings::my_org::workflow_engine::host_activities::join_set_joining_all();
        // let execution_id = crate::bindings::testing::http::http_get::get_future(
        //     &join_set,
        //     &format!("127.0.0.1:{port}"),
        //     "/",
        // );
        // // join_set.is_ready(execution_id);
        // let res = crate::bindings::testing::http::http_get::get_blocking(&execution_id);
        // res.unwrap()

        crate::bindings::testing::http::http_get::get(&format!("127.0.0.1:{port}"), "/").unwrap()
    }

    fn get(authority: String, path: String) -> Result<String, String> {
        crate::bindings::testing::http::http_get::get(&authority, &path)
    }
}
