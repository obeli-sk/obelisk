#![allow(clippy::all)]

mod generated {
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

mod js_runtime;

use generated::export;
use generated::exports::obelisk::js_runtime::execute::Guest;

pub struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn run(js_code: String, params_json: String) -> Result<String, String> {
        js_runtime::execute(&js_code, &params_json)
    }
}
