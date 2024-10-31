use cargo_metadata::camino::Utf8Path;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("-builder").unwrap();
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));
    let parent_package_path = package.manifest_path.parent().unwrap();
    run_trunk_build(parent_package_path.as_std_path());
    add_dependency(&package.manifest_path); // Cargo.toml
    for target in &package.targets {
        add_dependency(&target.src_path); // src folder
    }
}

fn run_trunk_build(current_dir: &Path) {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
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
    if file.is_file() {
        println!("cargo:rerun-if-changed={file}");
    } else {
        for file in file
            .read_dir_utf8()
            .unwrap_or_else(|err| panic!("cannot read folder `{file}` - {err:?}"))
            .flatten()
        {
            add_dependency(file.path());
        }
    }
}