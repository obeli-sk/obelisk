fn main() {
    use obelisk_component_builder::BuildConfig;
    // Workflow JS runtime targets wasm32-unknown-unknown and needs custom getrandom
    obelisk_component_builder::build_workflow(
        BuildConfig::target_subdir("release_testprograms")
            .with_rust_flags(r#"--cfg getrandom_backend="custom""#.to_string()),
    );
}
