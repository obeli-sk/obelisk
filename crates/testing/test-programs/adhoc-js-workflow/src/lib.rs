#![allow(clippy::all)]

// Custom getrandom backend for deterministic workflows.
// This is configured via `RUSTFLAGS="--cfg getrandom_backend=\"custom\""`, see `build.rs` .
// The Error type must be ABI-compatible with getrandom::Error (NonZeroI32).
#[cfg(all(target_arch = "wasm32", getrandom_backend = "custom"))]
mod stub_getrandom {
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

mod js_runtime;

use generated::export;
use generated::exports::testing::adhoc_js_workflow::workflow::Guest;

struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn execute(js_code: String, params: String) -> Result<String, String> {
        js_runtime::execute(&js_code, &params)
    }
}
