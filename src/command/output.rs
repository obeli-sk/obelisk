use std::io::{Write as _, stdout};

pub(super) fn print_json(value: &impl serde::Serialize) -> anyhow::Result<()> {
    let mut out = stdout().lock();
    serde_json::to_writer_pretty(&mut out, value)?;
    writeln!(out)?;
    Ok(())
}

pub(super) fn print_ndjson(value: &impl serde::Serialize) -> anyhow::Result<()> {
    let mut out = stdout().lock();
    serde_json::to_writer(&mut out, value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

pub(super) fn print_value(
    value: &(impl serde::Serialize + std::fmt::Display),
    json: bool,
) -> anyhow::Result<()> {
    if json {
        print_json(value)
    } else {
        println!("{value}");
        Ok(())
    }
}
