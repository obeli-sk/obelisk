use cargo_metadata::camino::Utf8Path;
use concepts::{ComponentType, FunctionMetadata};
use indexmap::IndexMap;
use std::{
    path::{Path, PathBuf},
    process::Command,
};
use utils::wasm_tools::WasmComponent;
use wasmtime::Engine;

const WASI_P2: &str = "wasm32-wasip2";
const WASM_CORE: &str = "wasm32-unknown-unknown";

/// Build the parent activity WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
pub fn build_activity() {
    build_internal(WASI_P2, ComponentType::ActivityWasm, &get_target_dir());
}

/// Build the parent webhook endpoint WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
pub fn build_webhook_endpoint() {
    build_internal(WASI_P2, ComponentType::WebhookEndpoint, &get_target_dir());
}

/// Build the parent workflow WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
pub fn build_workflow() {
    build_internal(WASM_CORE, ComponentType::Workflow, &get_target_dir());
}

/// Build the parent activity WASM component and place it into the `OUT_DIR`.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_out_dir`].
pub fn build_activity_test() {
    build_internal(WASI_P2, ComponentType::ActivityWasm, &get_out_dir());
}

/// Build the parent webhook endpoint WASM component and place it into the `OUT_DIR`.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_out_dir`].
pub fn build_webhook_endpoint_test() {
    build_internal(WASI_P2, ComponentType::WebhookEndpoint, &get_out_dir());
}

/// Build the parent workflow WASM component and place it into the `OUT_DIR`.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_out_dir`].
pub fn build_workflow_test() {
    build_internal(WASM_CORE, ComponentType::Workflow, &get_out_dir());
}

fn to_snake_case(input: &str) -> String {
    input.replace(['-', '.'], "_")
}

fn is_transformation_to_wasm_component_needed(target_tripple: &str) -> bool {
    target_tripple == WASM_CORE
}

/// Get the path to the target directory.
///
/// Attempts to use the `CARGO_WORKSPACE_DIR` environment variable first.
/// If not set, falls back to deriving it from the `OUT_DIR` environment variable.
/// To set the environment variable automatically, modify `.cargo/config.toml`:
/// ```toml
/// [env]
/// # remove once stable https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-reads
/// CARGO_WORKSPACE_DIR = { value = "", relative = true }
/// ```
fn get_target_dir() -> PathBuf {
    // Try to get `CARGO_WORKSPACE_DIR` from the environment
    if let Ok(workspace_dir) = std::env::var("CARGO_WORKSPACE_DIR") {
        return Path::new(&workspace_dir).join("target");
    }
    let out_path = get_out_dir();
    // Navigate up to the `target` directory
    out_path
        .ancestors()
        .nth(3) // Move up 3 levels from OUT_DIR to get to 'target'
        .expect("Unable to determine target directory")
        .to_path_buf()
}
/// Get the `OUT_DIR` as a `PathBuf`.
///
/// The folder structure typically looks like this: `target/debug/build/<crate_name>-<hash>/out`.
fn get_out_dir() -> PathBuf {
    PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
}

#[expect(clippy::too_many_lines)]
fn build_internal(target_tripple: &str, component_type: ComponentType, out_dir: &Path) {
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("-builder").unwrap();
    let wasm_path = run_cargo_component_build(out_dir, pkg_name, target_tripple);
    if std::env::var("RUST_LOG").is_ok() {
        println!("cargo:warning=Built {wasm_path:?}");
    }
    let engine = {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        wasmtime_config.async_support(true);
        Engine::new(&wasmtime_config).unwrap()
    };
    let mut generated_code = String::new();
    generated_code += &format!(
        "pub const {name_upper}: &str = {wasm_path:?};\n",
        name_upper = to_snake_case(pkg_name).to_uppercase()
    );
    {
        enum Value {
            Map(IndexMap<String, Value>),
            Leaf(Vec<String>),
        }
        fn ser_map(map: &IndexMap<String, Value>, output: &mut String) {
            for (k, v) in map {
                match v {
                    Value::Leaf(vec) => {
                        for line in vec {
                            *output += line;
                            *output += "\n";
                        }
                    }
                    Value::Map(map) => {
                        *output += &format!("#[allow(clippy::all)]\npub mod r#{k} {{\n");
                        ser_map(map, output);
                        *output += "}\n";
                    }
                }
            }
        }

        let component = WasmComponent::new(wasm_path, &engine, Some(component_type.into()))
            .expect("cannot decode wasm component");
        generated_code += "pub mod exports {\n";
        let mut outer_map: IndexMap<String, Value> = IndexMap::new();
        for export in component.exim.get_exports_hierarchy_ext() {
            let ifc_fqn_split = export
                .ifc_fqn
                .split_terminator([':', '/', '@'])
                .map(to_snake_case);
            let mut map = &mut outer_map;
            for mut split in ifc_fqn_split {
                if split.starts_with(|c: char| c.is_numeric()) {
                    split = format!("_{split}");
                }
                if let Value::Map(m) = map
                    .entry(split)
                    .or_insert_with(|| Value::Map(IndexMap::new()))
                {
                    map = m;
                } else {
                    unreachable!()
                }
            }
            let vec = export
                .fns
                .iter()
                .filter(| (_, FunctionMetadata { submittable,.. }) | *submittable )
                .map(|(function_name, FunctionMetadata{parameter_types, return_type, ..})| {
                    format!(
                        "/// {fn}: func{parameter_types}{arrow_ret_type};\npub const r#{name_upper}: (&str, &str) = (\"{ifc}\", \"{fn}\");\n",
                        name_upper = to_snake_case(function_name).to_uppercase(),
                        ifc = export.ifc_fqn,
                        fn = function_name,
                        arrow_ret_type = if let Some(ret_type) = return_type { format!(" -> {ret_type}") } else { String::new() }
                    )
                })
                .collect();
            let old_val = map.insert(String::new(), Value::Leaf(vec));
            assert!(old_val.is_none(), "same interface cannot appear twice");
        }

        ser_map(&outer_map, &mut generated_code);

        generated_code += "}\n";
    }
    std::fs::write(out_dir.join("gen.rs"), generated_code).unwrap();

    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));

    add_dependency(&package.manifest_path); // Cargo.toml
    for src_path in package
        .targets
        .iter()
        .map(|target| target.src_path.parent().unwrap())
    {
        add_dependency(src_path);
    }
    let wit_path = &package.manifest_path.parent().unwrap().join("wit");
    if wit_path.exists() && wit_path.is_dir() {
        add_dependency(wit_path);
    }
}

fn add_dependency(file: &Utf8Path) {
    println!("cargo:rerun-if-changed={file}");
}

fn run_cargo_component_build(out_dir: &Path, name: &str, tripple: &str) -> PathBuf {
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--release")
        .arg(format!("--target={tripple}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", out_dir)
        .env("CARGO_PROFILE_RELEASE_DEBUG", "limited") // keep debuginfo for backtraces
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS"); // do not pass clippy parameters
    let status = cmd.status().unwrap();
    assert!(status.success());
    let name_snake_case = to_snake_case(name);
    let target = out_dir
        .join(tripple)
        .join("release")
        .join(format!("{name_snake_case}.wasm",));
    assert!(target.exists(), "Target path must exist: {target:?}");
    if is_transformation_to_wasm_component_needed(tripple) {
        let target_transformed = out_dir
            .join(tripple)
            .join("release")
            .join(format!("{name_snake_case}_component.wasm",));
        let mut cmd = Command::new("wasm-tools");
        cmd.arg("component")
            .arg("new")
            .arg(
                target
                    .to_str()
                    .expect("only utf-8 encoded paths are supported"),
            )
            .arg("--output")
            .arg(
                target_transformed
                    .to_str()
                    .expect("only utf-8 encoded paths are supported"),
            );
        let status = cmd.status().unwrap();
        assert!(status.success());
        assert!(
            target_transformed.exists(),
            "Transformed target path must exist: {target_transformed:?}"
        );
        target_transformed
    } else {
        target
    }
}
