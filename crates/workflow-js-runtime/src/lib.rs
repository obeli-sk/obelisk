#[cfg(all(target_arch = "wasm32", getrandom_backend = "custom"))]

mod stub_getrandom {
    // Custom getrandom backend for deterministic workflows.
    // This is configured via `RUSTFLAGS="--cfg getrandom_backend=\"custom\""`.
    // The Error type must be ABI-compatible with getrandom::Error (NonZeroI32).
    #[repr(transparent)]
    pub struct Error(core::num::NonZeroI32);

    #[unsafe(no_mangle)]
    unsafe extern "Rust" fn __getrandom_v03_custom(dest: *mut u8, len: usize) -> Result<(), Error> {
        // Fill with zeros for deterministic behavior.
        // SAFETY: `dest` and `len` come from the getrandom v0.3 custom backend contract,
        // which guarantees `dest` is a valid writable buffer of `len` bytes.
        unsafe { core::ptr::write_bytes(dest, 0, len) };
        Ok(())
    }
}

mod generated {
    #![allow(clippy::empty_line_after_outer_attr)]
    include!(concat!(env!("OUT_DIR"), "/any.rs"));
}
mod deterministic_executor;
mod workflow_js_runtime;

use generated::export;
use generated::exports::obelisk_workflow::workflow_js_runtime::execute::{
    Guest, JsRuntimeError, ResolvedInterfaceImports,
};
use std::collections::BTreeMap;

pub struct Component;
export!(Component with_types_in generated);

impl Guest for Component {
    fn run(
        entry_path: String,
        files: Vec<(String, String)>,
        params_json: Vec<String>,
        backtrace_enabled: bool,
        resolved_imports: Vec<ResolvedInterfaceImports>,
    ) -> Result<Result<String, String>, JsRuntimeError> {
        let files: BTreeMap<String, String> = files.into_iter().collect();
        workflow_js_runtime::execute(
            &entry_path,
            &files,
            &params_json,
            backtrace_enabled,
            resolved_imports,
        )
    }
}
