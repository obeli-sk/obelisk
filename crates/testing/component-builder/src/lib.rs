use cargo_metadata::camino::Utf8Path;
use std::{
    path::{Path, PathBuf},
    process::Command,
};

const WASI_P2: &str = "wasm32-wasip2";
const WASM_CORE_MODULE: &str = "wasm32-unknown-unknown";

#[derive(Debug, Clone, Default)]
pub struct BuildConfig {
    pub profile: Option<String>,
    pub custom_dst_target_dir: Option<PathBuf>,
}
impl BuildConfig {
    pub fn profile(profile: impl Into<String>) -> Self {
        Self {
            profile: Some(profile.into()),
            custom_dst_target_dir: None,
        }
    }
    pub fn target_subdir(target_dir: impl Into<PathBuf>) -> Self {
        Self {
            profile: None,
            custom_dst_target_dir: Some(get_target_dir().join(target_dir.into())),
        }
    }
    pub fn new(
        profile: Option<impl Into<String>>,
        custom_dst_target_dir: Option<impl Into<PathBuf>>,
    ) -> Self {
        Self {
            profile: profile.map(Into::into),
            custom_dst_target_dir: custom_dst_target_dir.map(Into::into),
        }
    }
}

/// Build the parent activity WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[allow(clippy::must_use_candidate)]
pub fn build_activity(conf: BuildConfig) -> PathBuf {
    build_internal(WASI_P2, ComponentType::ActivityWasm, conf)
}

/// Build the parent webhook endpoint WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[allow(clippy::must_use_candidate)]
pub fn build_webhook_endpoint(conf: BuildConfig) -> PathBuf {
    build_internal(WASI_P2, ComponentType::WebhookEndpoint, conf)
}

/// Build the parent workflow WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[allow(clippy::must_use_candidate)]
pub fn build_workflow(conf: BuildConfig) -> PathBuf {
    build_internal(WASM_CORE_MODULE, ComponentType::Workflow, conf)
}

enum ComponentType {
    ActivityWasm,
    WebhookEndpoint,
    Workflow,
}

fn to_snake_case(input: &str) -> String {
    input.replace(['-', '.'], "_")
}

fn is_transformation_to_wasm_component_needed(target_tripple: &str) -> bool {
    target_tripple == WASM_CORE_MODULE
}

/// Get the path to the target directory using `CARGO_WORKSPACE_DIR` environment variable.
///
/// To set the environment variable automatically, modify `.cargo/config.toml`:
/// ```toml
/// [env]
/// # remove once stable https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-reads
/// CARGO_WORKSPACE_DIR = { value = "", relative = true }
/// ```
fn get_target_dir() -> PathBuf {
    // Try to get `CARGO_WORKSPACE_DIR` from the environment
    if let Ok(workspace_dir) = std::env::var("CARGO_WORKSPACE_DIR") {
        Path::new(&workspace_dir).join("target")
    } else {
        unreachable!("CARGO_WORKSPACE_DIR must be set")
    }
}
/// Get the `OUT_DIR` as a `PathBuf`.
///
/// The folder structure typically looks like this: `target/debug/build/<crate_name>-<hash>/out`.
#[cfg(feature = "genrs")]
fn get_out_dir() -> PathBuf {
    PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
}

fn build_internal(
    target_tripple: &str,
    component_type: ComponentType,
    conf: BuildConfig,
) -> PathBuf {
    let dst_target_dir = conf.custom_dst_target_dir.unwrap_or_else(get_target_dir);
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("-builder").unwrap();
    let wasm_path = run_cargo_build(
        &dst_target_dir,
        pkg_name,
        target_tripple,
        conf.profile.as_deref(),
    );
    if std::env::var("RUST_LOG").is_ok() {
        println!("cargo:warning=Built `{pkg_name}` - {wasm_path:?}");
    }

    generate_code(&wasm_path, pkg_name, component_type);

    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name.as_str() == pkg_name)
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
    wasm_path
}

#[cfg(not(feature = "genrs"))]
fn generate_code(_wasm_path: &Path, _pkg_name: &str, _component_type: ComponentType) {}

#[cfg(feature = "genrs")]
impl From<ComponentType> for concepts::ComponentType {
    fn from(value: ComponentType) -> Self {
        match value {
            ComponentType::ActivityWasm => Self::ActivityWasm,
            ComponentType::Workflow => Self::Workflow,
            ComponentType::WebhookEndpoint => Self::WebhookEndpoint,
        }
    }
}

#[cfg(feature = "genrs")]
fn generate_code(wasm_path: &Path, pkg_name: &str, component_type: ComponentType) {
    use concepts::FunctionMetadata;
    use indexmap::IndexMap;
    use std::fmt::Write as _;

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
                    write!(output, "#[allow(clippy::all)]\npub mod r#{k} {{\n").unwrap();
                    ser_map(map, output);
                    *output += "}\n";
                }
            }
        }
    }

    let mut generated_code = String::new();
    writeln!(
        generated_code,
        "pub const {name_upper}: &str = {wasm_path:?};",
        name_upper = to_snake_case(pkg_name).to_uppercase()
    )
    .unwrap();

    let component = utils::wasm_tools::WasmComponent::new(wasm_path, component_type.into())
        .unwrap_or_else(|err| panic!("cannot decode wasm component {wasm_path:?} - {err:?}"));
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
                        "/// {fn}: func{parameter_types} -> {return_type};\npub const r#{name_upper}: (&str, &str) = (\"{ifc}\", \"{fn}\");\n",
                        name_upper = to_snake_case(function_name).to_uppercase(),
                        ifc = export.ifc_fqn,
                        fn = function_name,
                    )
                })
                .collect();
        let old_val = map.insert(String::new(), Value::Leaf(vec));
        assert!(old_val.is_none(), "same interface cannot appear twice");
    }

    ser_map(&outer_map, &mut generated_code);
    generated_code += "}\n";

    let gen_rs = get_out_dir().join("gen.rs");
    println!("cargo:warning=Generated {gen_rs:?}");

    std::fs::write(gen_rs, generated_code).unwrap();
}

fn add_dependency(file: &Utf8Path) {
    println!("cargo:rerun-if-changed={file}");
}

fn run_cargo_build(
    dst_target_dir: &Path,
    name: &str,
    tripple: &str,
    profile: Option<&str>,
) -> PathBuf {
    let mut cmd = Command::new("cargo");
    let temp_str;
    cmd.arg("build")
        .arg(if let Some(profile) = profile {
            temp_str = format!("--profile={profile}");
            &temp_str
        } else {
            "--release"
        })
        .arg(format!("--target={tripple}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", dst_target_dir)
        .env("CARGO_PROFILE_RELEASE_DEBUG", "limited") // debug = 1, retain line numbers
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS"); // do not pass clippy parameters
    let status = cmd.status().unwrap();
    assert!(status.success());
    let name_snake_case = to_snake_case(name);
    let target = dst_target_dir
        .join(tripple)
        .join("release")
        .join(format!("{name_snake_case}.wasm",));
    assert!(target.exists(), "Target path must exist: {target:?}");
    if is_transformation_to_wasm_component_needed(tripple) {
        let target_transformed = dst_target_dir
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
        // mv target_transformed -> target
        std::fs::remove_file(&target).expect("deletion must succeed");
        std::fs::rename(target_transformed, &target).expect("rename must succeed");
    }
    target
}
