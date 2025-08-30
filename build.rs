fn main() {
    let pkg_version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION must be set");
    println!("cargo:rustc-env=PKG_VERSION={pkg_version}");
    println!("cargo:rerun-if-changed=proto/obelisk.proto");
    tonic_prost_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional") // not needed anymore with protoc  25.3
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .extern_path(".google.protobuf.Duration", "::prost_wkt_types::Duration")
        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
        .type_attribute(".", "#[derive(serde::Serialize)]") // for CLI only
        .type_attribute(".", "#[serde(rename_all=\"snake_case\")]") // for CLI only
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/obelisk.proto"], &[] as &[&str])
        .unwrap();
}
