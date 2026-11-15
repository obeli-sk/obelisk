use obelisk_component_builder::{BuildConfig, build_webhook_endpoint_wasi_p3};

fn main() {
    build_webhook_endpoint_wasi_p3(BuildConfig::target_subdir("release_testprograms"));
}
