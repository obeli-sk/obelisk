fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = format!(
        "{}\n",
        serde_json::to_string_pretty(&obeli_sk_db_http::openapi::schema())?
    );
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => print!("{schema}"),
        [flag, path] if flag == "--check" => {
            if std::fs::read_to_string(path)? != schema {
                return Err(format!("{path} is stale; run scripts/update-schemas.sh").into());
            }
        }
        _ => return Err("usage: generate-openapi [--check <schema-path>]".into()),
    }
    Ok(())
}
