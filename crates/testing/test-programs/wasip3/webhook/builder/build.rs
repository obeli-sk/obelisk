use obelisk_component_builder::{BuildConfig, build_webhook_endpoint};

fn main() {
    build_webhook_endpoint(BuildConfig::target_subdir("release_testprograms"));
}
