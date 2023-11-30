use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

const TARGET: &str = "wasm32-unknown-unknown";

fn main() {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let mut generated_code = String::new();
    build_and_add_line(
        &out_dir,
        "test_programs_http_get_activity",
        &mut generated_code,
    );
    build_and_add_line(
        &out_dir,
        "test_programs_http_get_workflow",
        &mut generated_code,
    );
    std::fs::write(out_dir.join("gen.rs"), generated_code).unwrap();
    println!("cargo:warning={out_dir:?}");
}

fn build_and_add_line(out_dir: &Path, name: &str, generated_code: &mut String) {
    let wasm = build_component(out_dir, name);
    *generated_code += &format!(
        "pub const {name_upper}: &'static str = {wasm:?};\n",
        name_upper = name.to_uppercase()
    );
}

fn build_component(out_dir: &Path, name: &str) -> PathBuf {
    let mut cmd = cargo_component();
    cmd.arg("build")
        .arg(format!("--target={TARGET}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", &out_dir)
        .env("CARGO_PROFILE_DEV_DEBUG", "1")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    eprintln!("running: {cmd:?}");
    let status = cmd.status().unwrap();
    assert!(status.success());
    let target = out_dir
        .join(TARGET)
        .join("debug")
        .join(format!("{name}.wasm",));
    assert!(target.exists(), "Target path must exist: {target:?}");
    target
}

fn cargo_component() -> Command {
    Command::new("cargo-component")
}
