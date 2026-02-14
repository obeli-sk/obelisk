#[cfg(feature = "boa-unstable")]
fn main() {
    use obelisk_component_builder::BuildConfig;
    obelisk_component_builder::build_activity(BuildConfig::target_subdir("release_testprograms"));
}

#[cfg(not(feature = "boa-unstable"))]
fn main() {}
