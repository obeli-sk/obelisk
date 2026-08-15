//! Materialize the four operator-owned WASM assets into `OUT_DIR` so `src/lib.rs`
//! can `include_bytes!` them.
//!
//! The assets are the exact pinned artifacts referenced from `assets/*version*.txt`
//! (three JS runtimes + the web UI). They are obtained one of two ways:
//!
//! * `OBELISK_EMBED_ASSETS_DIR` set: read pre-fetched `<name>.wasm` files from that
//!   directory (the Nix fixed-output derivation path, fully offline).
//! * otherwise: pull the single WASM layer of each pinned OCI image over the network,
//!   mirroring `obelisk/src/oci.rs::pull_to_cache_dir`.
//!
//! Both paths verify each file against the layer digest, so the two acquisition
//! methods produce byte-identical embeds.

use anyhow::{Context, bail};
use oci_client::secrets::RegistryAuth;
use oci_wasm::WasmClient;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// (generated const name, `<name>.wasm` file stem, `assets/<version file>`).
const ASSETS: &[(&str, &str, &str)] = &[
    (
        "ACTIVITY_JS_RUNTIME_WASM",
        "activity",
        "activity-js-runtime-version.txt",
    ),
    (
        "WORKFLOW_JS_RUNTIME_WASM",
        "workflow",
        "workflow-js-runtime-version.txt",
    ),
    (
        "WEBHOOK_JS_RUNTIME_WASM",
        "webhook",
        "webhook-js-runtime-version.txt",
    ),
    ("WEBUI_WASM", "webui", "webui-version.txt"),
];

const OCI_SCHEMA_PREFIX: &str = "oci://";

fn main() -> anyhow::Result<()> {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").context("OUT_DIR must be set")?);

    // Without the `download` feature (e.g. a plain `cargo build --workspace`) this crate
    // is compiled as a bare workspace member but never referenced; emit empty stub consts
    // so `src/lib.rs` still compiles, and do no network I/O.
    if std::env::var_os("CARGO_FEATURE_DOWNLOAD").is_none() {
        let mut stubs = String::new();
        for (const_name, ..) in ASSETS {
            writeln!(stubs, "pub const {const_name}: &[u8] = &[];")?;
        }
        std::fs::write(out_dir.join("gen.rs"), stubs)?;
        return Ok(());
    }

    println!("cargo:rerun-if-env-changed=OBELISK_EMBED_ASSETS_DIR");
    let assets_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../assets");
    let prefetched = std::env::var_os("OBELISK_EMBED_ASSETS_DIR").map(PathBuf::from);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let mut generated = String::new();
    for (const_name, stem, version_file) in ASSETS {
        let version_path = assets_dir.join(version_file);
        println!("cargo:rerun-if-changed={}", version_path.display());
        let reference = read_reference(&version_path)?;

        let dst = out_dir.join(format!("{stem}.wasm"));
        let bytes = if let Some(prefetched) = &prefetched {
            let src = prefetched.join(format!("{stem}.wasm"));
            std::fs::read(&src)
                .with_context(|| format!("cannot read pre-fetched asset {}", src.display()))?
        } else {
            runtime
                .block_on(pull_wasm_layer(&reference))
                .with_context(|| format!("cannot pull {reference}"))?
        };
        std::fs::write(&dst, &bytes).with_context(|| format!("cannot write {}", dst.display()))?;

        writeln!(
            generated,
            "pub const {const_name}: &[u8] = include_bytes!(r\"{}\");",
            dst.display()
        )?;
    }
    std::fs::write(out_dir.join("gen.rs"), generated)?;
    Ok(())
}

/// Read `assets/<name>-version.txt`, strip the `oci://` scheme, parse the OCI reference.
fn read_reference(version_path: &Path) -> anyhow::Result<oci_client::Reference> {
    let content = std::fs::read_to_string(version_path)
        .with_context(|| format!("cannot read {}", version_path.display()))?;
    let trimmed = content.trim();
    let reference = trimmed.strip_prefix(OCI_SCHEMA_PREFIX).with_context(|| {
        format!(
            "{} must start with `{OCI_SCHEMA_PREFIX}`",
            version_path.display()
        )
    })?;
    reference
        .parse()
        .with_context(|| format!("invalid OCI reference `{reference}`"))
}

/// Pull the single WASM layer of a pinned image and verify it against the layer digest.
async fn pull_wasm_layer(reference: &oci_client::Reference) -> anyhow::Result<Vec<u8>> {
    // reqwest is built with `rustls-no-provider`; install the process-global provider,
    // mirroring `obelisk/src/main.rs`. Ignore the error if already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = WasmClient::new(oci_client::Client::default());
    let (mut manifest, _config, _digest) = client
        .pull_manifest_and_config(reference, &RegistryAuth::Anonymous)
        .await
        .context("pulling manifest and config")?;
    let layer = manifest
        .layers
        .pop()
        .context("wasm image must have exactly one layer")?;

    let mut buf = Vec::new();
    client
        .as_ref()
        .pull_blob(reference, &layer, &mut buf)
        .await
        .context("pulling layer blob")?;

    let actual = sha256_hex(&buf);
    let expected = layer
        .digest
        .strip_prefix("sha256:")
        .unwrap_or(&layer.digest);
    if actual != expected {
        bail!("digest mismatch for {reference}: expected {expected}, got {actual}");
    }
    Ok(buf)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        write!(out, "{b:02x}").expect("writing to a String is infallible");
    }
    out
}
