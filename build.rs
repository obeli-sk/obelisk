use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    shadow_rs::ShadowBuilder::builder().build()?;
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // not needed anymore with protoc  25.3
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/obelisk.proto"], &[] as &[&str])?;
    Ok(())
}
