#![cfg(all(target_arch = "wasm32", getrandom_backend = "custom"))]

mod stub_getrandom {
    // Custom getrandom backend for deterministic workflows.
    // This is configured via `RUSTFLAGS="--cfg getrandom_backend=\"custom\""`.
    // The Error type must be ABI-compatible with getrandom::Error (NonZeroI32).
    #[repr(transparent)]
    pub struct Error(core::num::NonZeroI32);

    #[unsafe(no_mangle)]
    unsafe extern "Rust" fn __getrandom_v03_custom(dest: *mut u8, len: usize) -> Result<(), Error> {
        // Fill with zeros for deterministic behavior
        unsafe { core::ptr::write_bytes(dest, 0, len) };
        Ok(())
    }
}

mod generated {
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}

mod workflow_js_runtime;

use generated::export;
use generated::exports::obelisk_workflow::workflow_js_runtime::execute::{Guest, JsRuntimeError};

pub struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn run(
        fn_name: String,
        js_code: String,
        params_json: Vec<String>,
    ) -> Result<Result<String, String>, JsRuntimeError> {
        workflow_js_runtime::execute(&fn_name, &js_code, &params_json)
    }
}
