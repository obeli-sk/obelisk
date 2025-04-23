use std::{
    fs::File,
    io::{BufWriter, Write as _, stdout},
    path::PathBuf,
};

use crate::config::toml::ConfigToml;
use schemars::schema_for;

pub(crate) fn generate_toml_schema(output: Option<PathBuf>) -> Result<(), anyhow::Error> {
    let schema = schema_for!(ConfigToml);
    if let Some(output) = output {
        // Save to a file
        let mut writer = BufWriter::new(File::create(&output)?);
        serde_json::to_writer_pretty(&mut writer, &schema)?;
        writer.write_all(b"\n")?;
        writer.flush()?; // Do not swallow errors
    } else {
        serde_json::to_writer_pretty(stdout().lock(), &schema)?;
    }
    Ok(())
}
