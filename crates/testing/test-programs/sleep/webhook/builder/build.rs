use obelisk_component_builder::BuildConfig;

fn main() {
    obelisk_component_builder::build_webhook_endpoint(BuildConfig::target_subdir(
        "release_testprograms",
    ));
}
