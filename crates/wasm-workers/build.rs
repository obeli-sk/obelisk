//! Builds the V8 startup snapshot embedded by `v8_snapshot.rs`.

use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR must be set by cargo"));
    // The snapshot carries no Obelisk ops: the `#[op2]` implementations live in this crate, so a
    // build script cannot link them. All three extensions are ops-only, and ops are re-registered
    // at runtime (`skip_op_registration: false`), so the snapshot only has to capture deno_core's
    // own JavaScript bootstrap, which is where the startup cost is.
    let output = deno_core::snapshot::create_snapshot(
        deno_core::snapshot::CreateSnapshotOptions {
            cargo_manifest_dir: env!("CARGO_MANIFEST_DIR"),
            startup_snapshot: None,
            skip_op_registration: false,
            extensions: Vec::new(),
            extension_transpiler: None,
            with_runtime_cb: None,
        },
        None,
    )
    .expect("V8 startup snapshot must be created");
    for file in output.files_loaded_during_snapshot {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    std::fs::write(out_dir.join(SNAPSHOT_FILE_NAME), &output.output)
        .expect("V8 startup snapshot must be written");
}

const SNAPSHOT_FILE_NAME: &str = "v8_startup_snapshot.bin";
