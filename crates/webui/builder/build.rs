use cargo_metadata::camino::Utf8Path;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    if std::env::var("RUN_TRUNK").as_deref() != Ok("true") {
        return;
    }
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let webui_package_name = pkg_name.strip_suffix("-builder").unwrap();
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == webui_package_name)
        .unwrap_or_else(|| panic!("package `{webui_package_name}` must exist"));
    let parent_package_path = package.manifest_path.parent().unwrap();
    run_trunk_build(parent_package_path.as_std_path());
    add_dependency(&package.manifest_path); // Cargo.toml
    for target in &package.targets {
        add_dependency(&target.src_path); // src folder
    }
    // dist folder
    let dist_dir = parent_package_path.join("dist");
    assert!(dist_dir.exists());
    add_dependency(&dist_dir);
}

fn run_trunk_build(current_dir: &Path) {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    // Trunk sometimes does not call `cargo build`, therefore we need to run it manually. This is mostly for saving generated CSS files in its `build.rs`.
    assert!(Command::new("cargo")
        .current_dir(current_dir)
        .arg("build")
        .arg("--target=wasm32-unknown-unknown")
        .env("CARGO_TARGET_DIR", &out_dir) // Avoid deadlock when outer cargo holds the lock to the target folder.
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS") // do not pass clippy parameters
        .status()
        .unwrap()
        .success());

    let mut cmd = Command::new("trunk");
    cmd.current_dir(current_dir)
        .arg("build")
        .env("CARGO_TARGET_DIR", &out_dir) // Avoid deadlock when outer cargo holds the lock to the target folder.
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS"); // do not pass clippy parameters
    if std::env::var("RUST_LOG").is_ok() {
        let trunk_output_file = out_dir.join("trunk-output.log");
        println!("cargo:warning=Saving trunk output to {trunk_output_file:?}");
        let trunk_output_file = std::fs::File::create(trunk_output_file).unwrap();
        cmd.stdout(std::process::Stdio::from(
            trunk_output_file.try_clone().unwrap(),
        )) // Redirect stdout
        .stderr(std::process::Stdio::from(trunk_output_file)); // Redirect stderr
    }
    let status = cmd.status().unwrap();
    assert!(status.success());
}

fn add_dependency(file: &Utf8Path) {
    println!("cargo:rerun-if-changed={file}");
}
