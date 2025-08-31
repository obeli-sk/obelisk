use std::{
    fs::File,
    io::{BufWriter, Write as _, stdout},
    path::PathBuf,
};

use crate::config::toml::ConfigToml;
use concepts::ComponentType;
use schemars::schema_for;
use utils::wasm_tools::WasmComponent;

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

pub(crate) async fn generate_exported_extension_wits(
    input_wit_directory: PathBuf,
    output_deps_directory: Option<PathBuf>,
    component_type: ComponentType,
) -> Result<(), anyhow::Error> {
    let wasm_component = WasmComponent::new_from_wit_folder(&input_wit_directory, component_type)?;
    let pkgs_to_wits = wasm_component.exported_extension_wits()?;
    let output_deps_directory = output_deps_directory.unwrap_or(input_wit_directory);
    for (pkg_fqn, wit) in pkgs_to_wits {
        let pkg_folder = output_deps_directory.join(pkg_fqn.to_string().replace(':', "_"));
        tokio::fs::create_dir_all(&pkg_folder).await?;
        let wit_file = pkg_folder.join(format!("{}.wit", pkg_fqn.package_name));
        if let Ok(actual) = tokio::fs::read_to_string(&wit_file).await
            && actual == wit
        {
            println!("{wit_file:?} is up to date");
        } else {
            println!("{wit_file:?} created or updated");
            tokio::fs::write(&wit_file, wit.as_bytes()).await?;
        }
    }
    Ok(())
}
