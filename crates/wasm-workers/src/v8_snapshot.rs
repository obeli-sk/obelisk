//! The build-time V8 startup snapshot every native V8 isolate is created from.

/// Shared by the workflow, activity and webhook extensions: see `build.rs` for why the snapshot
/// contains no Obelisk ops.
pub(crate) const STARTUP_SNAPSHOT: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/v8_startup_snapshot.bin"));
