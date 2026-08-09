mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

mod activity_js_runtime;

use generated::export;
use generated::exports::obelisk_activity::activity_js_runtime::execute::{Guest, JsRuntimeError};
use std::collections::BTreeMap;

pub struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn run(
        entry_path: String,
        files: Vec<(String, String)>,
        params_json: Vec<String>,
    ) -> Result<Result<String, String>, JsRuntimeError> {
        let files: BTreeMap<String, String> = files.into_iter().collect();
        activity_js_runtime::execute(&entry_path, &files, &params_json)
    }
}
