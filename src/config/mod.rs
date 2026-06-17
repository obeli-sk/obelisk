use concepts::ContentDigest;
use std::path::{Path, PathBuf};

pub(crate) mod config_holder;
pub(crate) mod env_var;
pub(crate) mod file_provider;
pub(crate) mod manifest;
pub(crate) mod toml;

pub(crate) fn content_digest_to_wasm_file(
    wasm_cache_dir: &Path,
    content_digest: &ContentDigest,
) -> PathBuf {
    wasm_cache_dir.join(format!("{}.wasm", content_digest.with_infix("_")))
}

pub(crate) fn content_digest_to_js_file(
    js_cache_dir: &Path,
    content_digest: &ContentDigest,
) -> PathBuf {
    js_cache_dir.join(format!("{}.js", content_digest.with_infix("_")))
}

pub(crate) fn content_digest_to_exec_file(
    exec_cache_dir: &Path,
    content_digest: &ContentDigest,
) -> PathBuf {
    exec_cache_dir.join(format!("{}.sh", content_digest.with_infix("_")))
}

pub(crate) fn wasm_cache_metadata_dir(wasm_cache_dir: &Path) -> PathBuf {
    wasm_cache_dir.join("metadata")
}
