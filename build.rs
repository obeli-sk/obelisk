use std::process::Command;

fn main() {
    let pkg_version = std::env::var("CARGO_PKG_VERSION").expect("CARGO_PKG_VERSION must be set");
    println!("cargo:rustc-env=PKG_VERSION={pkg_version}");
    let sha = get_git_sha(&format!("v{pkg_version}")).unwrap_or_default();
    println!("cargo:rustc-env=PKG_SHA={sha}");

    tonic_build::configure()
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

/// Get the short Git SHA of HEAD.
/// Return None if HEAD is tagged with `expected_tag`.
/// Return "unknown" if git commands fails.
fn get_git_sha(expected_tag: &str) -> Option<String> {
    let output_tag_check = Command::new("git")
        .args(["describe", "--tags", "--exact-match", "HEAD"])
        .output(); // Capture both stdout and stderr
    match output_tag_check {
        Ok(output) if output.status.success() => {
            let current_tag = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if current_tag == expected_tag {
                return None;
            }
            println!(
                "cargo:warning=HEAD is tagged with '{current_tag}', not the expected '{expected_tag}'",
            );
        }
        Ok(_output) => {
            // No tag found.
        }
        Err(err) => {
            println!("cargo:warning=Failed to execute 'git describe': {err:?}",);
        }
    }

    let output_sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output();

    Some(match output_sha {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        _ => {
            println!("cargo:warning=Failed to get git SHA. Using default 'unknown'.");
            "unknown".to_string()
        }
    })
}
