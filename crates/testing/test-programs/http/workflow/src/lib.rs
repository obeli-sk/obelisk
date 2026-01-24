use generated::export;
use generated::exports::testing::http_workflow::workflow::Guest;
use generated::obelisk::log::log;
use generated::obelisk::workflow::workflow_support::join_set_create;
use generated::testing::{
    http::http_get,
    http_obelisk_ext::http_get::{get_successful_await_next, get_successful_submit},
};

mod generated {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn get(url: String) -> Result<String, String> {
        http_get::get(&url)
    }

    fn get_resp(url: String) -> Result<String, String> {
        let resp = http_get::get_resp(&url)?;
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }

    fn get_successful(url: String) -> Result<String, String> {
        http_get::get_successful(&url)
    }

    fn get_successful_concurrently(urls: Vec<String>) -> Result<Vec<String>, String> {
        let join_set = join_set_create();
        let length = urls.len();
        for url in urls {
            let _execution_id = get_successful_submit(&join_set, &url);
        }
        let mut list = Vec::with_capacity(length);
        for _ in 0..length {
            // Mark the whole result as failed if any child execution fails.
            let contents = get_successful_await_next(&join_set).unwrap().1?;
            list.push(contents);
        }
        Ok(list)
    }

    fn get_successful_concurrently_stress(
        url: String,
        concurrency: u32,
    ) -> Result<Vec<String>, String> {
        let join_set = join_set_create();
        let join_set_id = join_set.id();
        log::info(&format!("Created join set {join_set_id}"));
        assert_eq!("g:1", join_set_id);
        for _ in 0..concurrency {
            let _execution_id = get_successful_submit(&join_set, &url);
        }
        let mut list = Vec::with_capacity(concurrency as usize);
        for _ in 0..concurrency {
            // Mark the whole result as failed if any child execution fails.
            let contents = get_successful_await_next(&join_set).unwrap().1?;
            list.push(contents);
        }
        Ok(list)
    }
}
