#[cfg(feature = "boa-unstable-workflow")]
fn main() {
    use obelisk_component_builder::BuildConfig;
    obelisk_component_builder::build_workflow(
        BuildConfig::target_subdir("release_testprograms")
            .with_rust_flags("--cfg getrandom_backend=\"custom\"".to_string()),
    );
}

#[cfg(not(feature = "boa-unstable-workflow"))]
fn main() {}
