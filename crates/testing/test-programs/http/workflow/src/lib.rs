#![cfg(feature = "wasm")]

mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::http_workflow::workflow::Guest for Component {
    fn get(authority: String, path: String) -> Result<String, String> {
        crate::bindings::testing::http::http_get::get(&authority, &path)
    }

    fn get_successful(authority: String, path: String) -> Result<String, String> {
        crate::bindings::testing::http::http_get::get_successful(&authority, &path)
    }

    fn get_successful_concurrently(authorities: Vec<String>) -> Result<Vec<String>, String> {
        let join_set_id = bindings::my_org::workflow_engine::host_activities::new_join_set();
        let length = authorities.len();
        for authority in authorities {
            let _execution_id =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_future(
                    &join_set_id,
                    &authority,
                    "/",
                );
        }
        let mut list = Vec::with_capacity(length);
        for _ in 0..length {
            // Mark the whole result as failed if any child execution fails.
            let contents =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_await_next(
                    &join_set_id,
                )?;
            list.push(contents);
        }
        Ok(list)
    }

    fn get_successful_concurrently_stress(
        authority: String,
        path: String,
        concurrency: u32,
    ) -> Result<Vec<String>, String> {
        let join_set_id = bindings::my_org::workflow_engine::host_activities::new_join_set();
        for _ in 0..concurrency {
            let _execution_id =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_future(
                    &join_set_id,
                    &authority,
                    &path,
                );
        }
        let mut list = Vec::with_capacity(concurrency as usize);
        for _ in 0..concurrency {
            // Mark the whole result as failed if any child execution fails.
            let contents =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_await_next(
                    &join_set_id,
                )?;
            list.push(contents);
        }
        Ok(list)
    }
}
