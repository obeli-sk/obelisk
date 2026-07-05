//! Shared helpers for tests in the `command` submodules.

use std::path::{Path, PathBuf};

/// The workspace target directory *relative to the workspace root*, honoring
/// `CARGO_TARGET_DIR`. Deployment manifests forbid absolute local paths, so the value is
/// kept relative: a relative `CARGO_TARGET_DIR` is already workspace-relative; an absolute
/// one under the workspace is stripped to its suffix; the default is `target`.
fn target_dir_relative(workspace: &Path) -> String {
    match std::env::var("CARGO_TARGET_DIR") {
        Ok(dir) => {
            let path = PathBuf::from(&dir);
            if path.is_relative() {
                dir
            } else if let Ok(stripped) = path.strip_prefix(workspace) {
                stripped.to_string_lossy().into_owned()
            } else {
                panic!(
                    "`CARGO_TARGET_DIR` must be under workspace dir {workspace:?}, but is set to {dir:?}"
                )
            }
        }
        Err(std::env::VarError::NotPresent) => "target".to_owned(),
        Err(err) => panic!("CARGO_TARGET_DIR set to an invalid value - {err:?}"),
    }
}

pub(crate) async fn target_aware_deployment_fixture(
    workspace: &Path,
    deployment_toml: &str,
) -> Result<tempfile::NamedTempFile, anyhow::Error> {
    let text = tokio::fs::read_to_string(workspace.join(deployment_toml)).await?;
    let rewritten = text.replace(
        "target/wasm-cache",
        &format!("{}/wasm-cache", target_dir_relative(workspace)),
    );
    let tmp = tempfile::Builder::new()
        .prefix(".deployment-target-aware-")
        .suffix(".toml")
        .tempfile_in(workspace)?;
    tokio::fs::write(tmp.path(), rewritten).await?;
    Ok(tmp)
}
