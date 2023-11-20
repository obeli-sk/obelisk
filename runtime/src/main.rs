mod activity;
mod workflow;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut args = std::env::args().skip(1);
    let wasm_path = args.next().expect("Parameters: wasm_path function_name");
    let function_name = args.next().expect("Parameters: wasm_path function_name");

    //workflow::workflow_example(&wasm_path, &function_name).await?;
    activity::activity_example(&wasm_path, &function_name).await?;
    Ok(())
}
