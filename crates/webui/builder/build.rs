use std::error::Error;
use std::{path::Path, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("-builder").unwrap();
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));
    let target_path = package.manifest_path.parent().unwrap();
    run_trunk_build(target_path.as_std_path());
    Ok(())
}

fn run_trunk_build(current_dir: &Path) {
    let mut cmd = Command::new("trunk");
    cmd.current_dir(current_dir)
        .arg("build")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS"); // do not pass clippy parameters
    let status = cmd.status().unwrap();
    assert!(status.success());
}
