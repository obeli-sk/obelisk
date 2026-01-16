use anyhow::Result;
use wit_bindgen_rust::{Opts, WithOption};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=wit/");
    Opts {
        generate_all: true,
        with: vec![
            (
                "wasi:io/error@0.2.3".to_string(),
                WithOption::Path("wasip2::io::error".to_string()),
            ),
            (
                "wasi:io/poll@0.2.3".to_string(),
                WithOption::Path("wasip2::io::poll".to_string()),
            ),
            (
                "wasi:io/streams@0.2.3".to_string(),
                WithOption::Path("wasip2::io::streams".to_string()),
            ),
            (
                "obelisk:activity/process@1.0.0".to_string(),
                WithOption::Generate,
            ),
            ("testing:process/process".to_string(), WithOption::Generate),
        ],
        ..Default::default()
    }
    .build()
    .generate_to_out_dir(None)?;

    Ok(())
}
