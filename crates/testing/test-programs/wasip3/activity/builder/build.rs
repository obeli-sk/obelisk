use obelisk_component_builder::BuildConfig;

fn main() {
    obelisk_component_builder::build_activity_wasi_p3(BuildConfig::target_subdir(
        "release_testprograms",
    ));
}
