#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = obeli_db_test_server::TestServer::start().await?;
    println!("{}", server.url);
    tokio::signal::ctrl_c().await?;
    Ok(())
}
