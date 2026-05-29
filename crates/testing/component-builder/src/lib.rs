use cargo_metadata::camino::{Utf8Path, Utf8PathBuf};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    io::ErrorKind,
    path::{Path, PathBuf},
    process::Command,
};

const WASI_P2: &str = "wasm32-wasip2";
const WASM_CORE_MODULE: &str = "wasm32-unknown-unknown";

#[derive(Debug, Clone, Default)]
pub struct BuildConfig {
    pub profile: String,
    pub custom_dst_target_dir: Option<PathBuf>,
    pub rust_flags: String,
}
impl BuildConfig {
    #[must_use]
    pub fn target_subdir(profile: &'static str) -> Self {
        Self {
            profile: profile.to_string(),
            custom_dst_target_dir: Some(get_target_dir().join(profile)),
            rust_flags: String::new(),
        }
    }

    #[must_use]
    pub fn with_rust_flags(mut self, rust_flags: String) -> Self {
        self.rust_flags = rust_flags;
        self
    }
}

/// Build the parent activity WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[expect(clippy::must_use_candidate)]
pub fn build_activity(conf: BuildConfig) -> PathBuf {
    build_internal(WASI_P2, ComponentType::Activity, conf).into_std_path_buf()
}

/// Build the parent webhook endpoint WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[expect(clippy::must_use_candidate)]
pub fn build_webhook_endpoint(conf: BuildConfig) -> PathBuf {
    build_internal(WASI_P2, ComponentType::WebhookEndpoint, conf).into_std_path_buf()
}

/// Build the parent workflow WASM component and place it into the `target` directory.
///
/// This function must be called from `build.rs`. It reads the current package
/// name and strips the `-builder` suffix to determine the target package name.
/// Then, it runs `cargo build` with the appropriate target triple and sets
/// the `--target` directory to the output of [`get_target_dir`].
#[expect(clippy::must_use_candidate)]
pub fn build_workflow(conf: BuildConfig) -> PathBuf {
    build_internal(WASM_CORE_MODULE, ComponentType::Workflow, conf).into_std_path_buf()
}

/// Build a workflow that requires WASI P2 (e.g., due to embedded runtime like Boa JS).
///
/// Unlike regular workflows that use `wasm32-unknown-unknown`, these workflows
/// use `wasm32-wasip2` target.
#[expect(clippy::must_use_candidate)]
pub fn build_workflow_wasi_p2(conf: BuildConfig) -> PathBuf {
    build_internal(WASI_P2, ComponentType::Workflow, conf).into_std_path_buf()
}

enum ComponentType {
    Activity,
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
    PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR environment variable must be set"))
}

fn collect_files(dir: &Path, out: &mut BTreeSet<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_files(&path, out);
            } else {
                out.insert(path);
            }
        }
    }
}

fn hash_directory_contents(hasher: &mut Sha256, dir: &Path, base: &Path) {
    let mut files = BTreeSet::new();
    collect_files(dir, &mut files);
    for file_path in &files {
        let rel = file_path.strip_prefix(base).unwrap_or(file_path);
        hasher.update(rel.to_string_lossy().as_bytes());
        hasher.update(b"\x00");
        hasher.update(
            std::fs::read(file_path)
                .unwrap_or_else(|e| panic!("cannot read {file_path:?} - {e:?}")),
        );
        hasher.update(b"\x00");
    }
}

fn compute_inputs_hash(
    meta: &cargo_metadata::Metadata,
    package: &cargo_metadata::Package,
    target_triple: &str,
    profile: &str,
    rust_flags: &str,
) -> String {
    let mut hasher = Sha256::new();

    // Build config
    hasher.update(target_triple.as_bytes());
    hasher.update(b"\x00");
    hasher.update(profile.as_bytes());
    hasher.update(b"\x00");
    hasher.update(rust_flags.as_bytes());
    hasher.update(b"\x00");

    // Builder crate version — invalidates cache when the builder itself changes
    hasher.update(env!("CARGO_PKG_VERSION").as_bytes());
    hasher.update(b"\x00");

    // Cargo.lock — covers all transitive dependency versions
    let lock_path = meta.workspace_root.join("Cargo.lock");
    if lock_path.exists() {
        hasher.update(std::fs::read(lock_path.as_std_path()).unwrap());
    }
    hasher.update(b"\x00");

    let pkg_root = package.manifest_path.parent().unwrap();

    // Package Cargo.toml
    hasher.update(std::fs::read(package.manifest_path.as_std_path()).unwrap());
    hasher.update(b"\x00");

    // Source files
    for src_path in package.targets.iter().map(|t| t.src_path.parent().unwrap()) {
        hash_directory_contents(&mut hasher, src_path.as_std_path(), pkg_root.as_std_path());
    }

    // WIT files
    let wit_path = pkg_root.join("wit");
    if wit_path.exists() && wit_path.is_dir() {
        hash_directory_contents(&mut hasher, wit_path.as_std_path(), pkg_root.as_std_path());
    }

    // build.rs of the target package (not the builder)
    let build_rs = pkg_root.join("build.rs");
    if build_rs.exists() {
        hasher.update(b"build.rs\x00");
        hasher.update(std::fs::read(build_rs.as_std_path()).unwrap());
        hasher.update(b"\x00");
    }

    let result = hasher.finalize();
    use std::fmt::Write;
    result[..8]
        .iter()
        .fold(String::with_capacity(16), |mut s, b| {
            write!(s, "{b:02x}").unwrap();
            s
        })
}

fn cleanup_old_hashed_files(dir: &Path, name_snake_case: &str) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let file_name = file_name.to_string_lossy();
            if file_name.starts_with(&format!("{name_snake_case}_")) && file_name.ends_with(".wasm")
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

fn build_internal(
    target_tripple: &str,
    component_type: ComponentType,
    conf: BuildConfig,
) -> Utf8PathBuf {
    let dst_target_dir = conf.custom_dst_target_dir.unwrap_or_else(get_target_dir);
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("-builder").unwrap();

    // Run cargo_metadata early — needed for hash computation
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name.as_str() == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));

    let wasm_path = run_cargo_build(
        &dst_target_dir,
        pkg_name,
        target_tripple,
        &conf.profile,
        &conf.rust_flags,
        &meta,
        package,
    );
    println!("cargo:warning=Built `{pkg_name}` - {wasm_path:?}");

    generate_code(wasm_path.as_std_path(), pkg_name, component_type);

    // Register rerun-if-changed dependencies
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
    let lock_path = meta.workspace_root.join("Cargo.lock");
    if lock_path.exists() {
        add_dependency(&lock_path);
    }
    wasm_path
}

#[cfg(not(feature = "genrs"))]
fn generate_code(_wasm_path: &Path, _pkg_name: &str, _component_type: ComponentType) {}

#[cfg(feature = "genrs")]
impl From<ComponentType> for concepts::ComponentType {
    fn from(value: ComponentType) -> Self {
        match value {
            ComponentType::Activity => Self::Activity,
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
    profile: &str,
    rust_flags: &str,
    meta: &cargo_metadata::Metadata,
    package: &cargo_metadata::Package,
) -> Utf8PathBuf {
    let name_snake_case = to_snake_case(name);
    let hash = compute_inputs_hash(meta, package, tripple, profile, rust_flags);
    let needs_component_transform = is_transformation_to_wasm_component_needed(tripple);

    // Store hashed artifacts in target/wasm-cache/ so inner build dirs can be cleaned.
    let cache_dir = get_target_dir().join("wasm-cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let (cached_path, symlink_path) = if needs_component_transform {
        (
            Utf8PathBuf::from_path_buf(
                cache_dir.join(format!("{name_snake_case}_component_{hash}.wasm")),
            )
            .unwrap(),
            Utf8PathBuf::from_path_buf(cache_dir.join(format!("{name_snake_case}_component.wasm")))
                .unwrap(),
        )
    } else {
        (
            Utf8PathBuf::from_path_buf(cache_dir.join(format!("{name_snake_case}_{hash}.wasm")))
                .unwrap(),
            Utf8PathBuf::from_path_buf(cache_dir.join(format!("{name_snake_case}.wasm"))).unwrap(),
        )
    };

    // Cache hit — skip cargo build entirely
    if cached_path.exists() && symlink_path.exists() {
        println!("cargo:warning=Cache hit for `{name}` ({hash})");
        return cached_path;
    }

    // Cache miss — clean up old hashed files
    cleanup_old_hashed_files(&cache_dir, &name_snake_case);

    // Run cargo build
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg(format!("--profile={profile}"))
        .arg(format!("--target={tripple}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", dst_target_dir)
        .env("RUSTFLAGS", rust_flags)
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CLIPPY_ARGS"); // do not pass clippy parameters
    let status = cmd.status().unwrap();
    assert!(status.success());

    let cargo_output = dst_target_dir
        .join(tripple)
        .join(profile)
        .join(format!("{name_snake_case}.wasm"));
    assert!(
        cargo_output.exists(),
        "Target path must exist: {cargo_output:?}"
    );

    if needs_component_transform {
        // Run wasm-tools component new directly to the hashed name
        let mut cmd = Command::new("wasm-tools");
        cmd.arg("component")
            .arg("new")
            .arg(
                cargo_output
                    .to_str()
                    .expect("only utf-8 encoded paths are supported"),
            )
            .arg("--output")
            .arg(&cached_path);
        let status = cmd.status().unwrap();
        assert!(status.success());
    } else {
        std::fs::copy(&cargo_output, &cached_path).unwrap();
    }

    assert!(
        cached_path.exists(),
        "Cached path must exist: {cached_path:?}"
    );
    #[cfg(unix)]
    {
        let src = cached_path.file_name().unwrap();
        println!("cargo:warning=Adding `ln -s {src:?} {symlink_path:?}`");
        replace_symlink(&symlink_path, src);
    }
    cached_path
}

#[cfg(unix)]
fn replace_symlink(link: &Utf8Path, src: &str) {
    let tmp_name = format!(
        "{}.tmp.{}",
        link.file_name().expect("symlink path must have a filename"),
        std::process::id()
    );
    let tmp_link = link.with_file_name(tmp_name);

    match std::os::unix::fs::symlink(src, &tmp_link) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            std::fs::remove_file(&tmp_link).expect("cannot remove stale temporary symlink");
            std::os::unix::fs::symlink(src, &tmp_link).expect("cannot recreate temporary symlink");
        }
        Err(err) => panic!("cannot create temporary symlink: {err}"),
    }

    if let Err(err) = std::fs::rename(&tmp_link, link) {
        let _ = std::fs::remove_file(&tmp_link);
        panic!("cannot replace symlink: {err}");
    }
}
