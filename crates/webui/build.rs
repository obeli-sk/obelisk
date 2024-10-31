use std::{error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let proto_path = get_workspace_dir().join("proto");
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // not needed anymore with protoc  25.3
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .build_server(false)
        .build_client(true)
        .compile_protos(&["obelisk.proto"], &[proto_path])?;
    download_css().unwrap();
    Ok(())
}

fn get_workspace_dir() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap())
}

fn download_css() -> Result<(), Box<dyn Error>> {
    let css_path = PathBuf::from("blueprint.css");
    if !css_path.exists() {
        yewprint_css::download_css(&css_path)?;
    }
    Ok(())
}
