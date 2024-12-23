use std::{error::Error, fs::File, io::Write, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let workspace_dir = get_workspace_dir();
    let proto_path = workspace_dir.join("proto");
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // not needed anymore with protoc  25.3
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .build_server(false)
        .build_client(true)
        .compile_protos(&["obelisk.proto"], &[proto_path])?;

    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));
    let blueprint_css_path = &package
        .manifest_path
        .parent()
        .unwrap()
        .join("blueprint.css");
    if !blueprint_css_path.exists() {
        let mut blueprint_css_file = File::create(blueprint_css_path)?;
        blueprint_css_file.write_all(yewprint_css::BLUEPRINT_CSS.as_bytes())?;
        blueprint_css_file.flush()?;
    }
    Ok(())
}

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}
