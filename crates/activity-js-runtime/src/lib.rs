mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

mod activity_js_runtime;

use generated::export;
use generated::exports::obelisk_activity::activity_js_runtime::execute::{Guest, JsRuntimeError};

pub struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn run(
        fn_name: String,
        js_code: String,
        params_json: String,
    ) -> Result<Result<String, String>, JsRuntimeError> {
        activity_js_runtime::execute(&fn_name, &js_code, &params_json)
    }
}
