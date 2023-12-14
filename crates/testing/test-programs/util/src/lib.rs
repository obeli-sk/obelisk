use std::{
    path::{Path, PathBuf},
    process::Command,
};

const BUILD_TARGET_TRIPPLE: &str = "wasm32-unknown-unknown";

// TODO: idea from wasmtime/crates/component-macro/src/bindgen.rs:
// Include a dummy `include_str!` for any files we read so rustc knows that
// we depend on the contents of those files.
pub fn build() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let test_program_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let test_program_name = test_program_name.strip_suffix("_builder").unwrap();
    let wasm = run_cargo_component_build(&out_dir, test_program_name);
    let mut generated_code = String::new();
    generated_code += &format!(
        "pub const {name_upper}: &str = {wasm:?};\n",
        name_upper = test_program_name.to_uppercase()
    );
    // generated_code.extend(format!(
    //     "const _: &str = include_str!(r#\"{}\"#);\n",
    //     file.display()
    // ));
    std::fs::write(out_dir.join("gen.rs"), generated_code).unwrap();
}

fn run_cargo_component_build(out_dir: &Path, name: &str) -> PathBuf {
    let mut cmd = Command::new("cargo-component");
    cmd.arg("build")
        .arg("--release")
        .arg(format!("--target={BUILD_TARGET_TRIPPLE}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", out_dir)
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    eprintln!("running: {cmd:?}");
    let status = cmd.status().unwrap();
    assert!(status.success());
    let target = out_dir
        .join(BUILD_TARGET_TRIPPLE)
        .join("release")
        .join(format!("{name}.wasm",));
    assert!(target.exists(), "Target path must exist: {target:?}");
    target
}
