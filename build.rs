use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    shadow_rs::new()?;
    tonic_build::compile_protos("proto/obelisk.proto")?;
    Ok(())
}
