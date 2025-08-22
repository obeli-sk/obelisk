use cargo_metadata::camino::Utf8PathBuf;
use std::{fs::File, io::Write, path::PathBuf};
use syntect::{highlighting::ThemeSet, html::ClassStyle};

fn main() {
    let workspace_dir = get_workspace_dir();
    let proto_path = workspace_dir.join("proto");
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // not needed anymore with protoc  25.3
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .build_server(false)
        .build_transport(false)
        .build_client(true)
        .compile_protos(&[PathBuf::from("obelisk.proto")], &[proto_path])
        .unwrap();
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    generate_blueprint_css(&pkg_name);
    generate_syntect_css(&pkg_name);
}

fn get_css_path(filename: &str, webui_package_name: &str) -> Utf8PathBuf {
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name.as_str() == webui_package_name)
        .unwrap_or_else(|| panic!("package `{webui_package_name}` must exist"));
    package.manifest_path.parent().unwrap().join(filename)
}

fn generate_blueprint_css(webui_package_name: &str) {
    let css_path = get_css_path("blueprint.css", webui_package_name);
    if !css_path.exists() {
        let mut css_file = File::create(css_path).unwrap();
        css_file
            .write_all(yewprint_css::BLUEPRINT_CSS.as_bytes())
            .unwrap();
        css_file.flush().unwrap();
    }
}

fn generate_syntect_css(webui_package_name: &str) {
    const DEFAULT_THEME: &str = "base16-ocean.dark"; // NB: Sync with syntect_code_block
    let css_path = get_css_path("syntect.css", webui_package_name);
    if !css_path.exists() {
        let mut css_file = File::create(css_path).unwrap();
        let theme_set = ThemeSet::load_defaults();
        let theme = theme_set
            .themes
            .get(DEFAULT_THEME)
            .expect("DEFAULT_THEME must be found");
        let content = syntect::html::css_for_theme_with_class_style(theme, ClassStyle::Spaced)
            .expect("Failed to generate CSS for theme");
        css_file.write_all(content.as_bytes()).unwrap();
        css_file.flush().unwrap();
    }
}

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}
