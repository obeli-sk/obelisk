use concepts::ContentDigest;
use std::path::{Path, PathBuf};

pub(crate) mod config_holder;
pub(crate) mod env_var;
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
