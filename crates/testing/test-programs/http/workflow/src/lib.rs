mod bindings;

bindings::export!(Component with_types_in bindings);

struct Component;

impl crate::bindings::exports::testing::http_workflow::workflow::Guest for Component {
    fn get(url: String) -> Result<String, String> {
        crate::bindings::testing::http::http_get::get(&url)
    }

    fn get_successful(url: String) -> Result<String, String> {
        crate::bindings::testing::http::http_get::get_successful(&url)
    }

    fn get_successful_concurrently(urls: Vec<String>) -> Result<Vec<String>, String> {
        let join_set_id = bindings::obelisk::workflow::host_activities::new_join_set();
        let length = urls.len();
        for url in urls {
            let _execution_id =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_submit(
                    &join_set_id,
                    &url,
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
        url: String,
        concurrency: u32,
    ) -> Result<Vec<String>, String> {
        let join_set_id = bindings::obelisk::workflow::host_activities::new_join_set();
        for _ in 0..concurrency {
            let _execution_id =
                crate::bindings::testing::http_obelisk_ext::http_get::get_successful_submit(
                    &join_set_id,
                    &url,
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
