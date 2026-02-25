fn main() {
    use obelisk_component_builder::BuildConfig;
    obelisk_component_builder::build_webhook_endpoint(BuildConfig::target_subdir(
        "release_wasm_runtime",
    ));
}
