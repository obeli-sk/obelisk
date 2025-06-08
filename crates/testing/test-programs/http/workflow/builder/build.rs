use obelisk_component_builder::BuildConfig;

fn main() {
    obelisk_component_builder::build_workflow(BuildConfig::target_subdir("release_testprograms"));
}
