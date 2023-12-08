use runtime::event_history::EventHistory;
use runtime::workflow::Workflow;
use runtime::{activity::Activities, FunctionFqn};
use std::{sync::Arc, time::Instant};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use val_json::deserialize_sequence;
use wasmtime::component::Val;

const IFC_FQN_FUNCTION_NAME_SEPARATOR: &str = ".";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let timer = Instant::now();
    let mut args: std::iter::Skip<std::env::Args> = std::env::args().skip(1);

    let activity_wasm_path = args.next().expect("activity wasm missing");
    let activities = Arc::new(Activities::new(activity_wasm_path).await?);

    let workflow_wasm_path = args.next().expect("workflow wasm missing");
    let workflow_function = args.next().expect("workflow function missing");
    let Some((ifc_fqn, function_name)) =
        workflow_function.split_once(IFC_FQN_FUNCTION_NAME_SEPARATOR)
    else {
        panic!("workflow function must be a fully qualified name in format `package/interface.function`")
    };

    let workflow = Workflow::new(workflow_wasm_path, activities.clone()).await?;
    println!("Initialized in {duration:?}", duration = timer.elapsed());
    println!();

    let mut event_history = EventHistory::new();
    let timer = Instant::now();
    let fqn = FunctionFqn::new(ifc_fqn, function_name);
    let param_types = workflow
        .function_metadata(&fqn)
        .expect("function must exist")
        .params
        .iter()
        .map(|(_, ty)| ty);

    let param_vals = args
        .next()
        .map(|param_vals| {
            deserialize_sequence::<Val>(&param_vals, param_types)
                .expect("deserialization of params failed")
        })
        .unwrap_or_default();

    let res = workflow
        .execute_all(&mut event_history, ifc_fqn, function_name, &param_vals)
        .await;
    println!(
        "Finished: in {duration:?} {res:?}, event history size: {len}",
        duration = timer.elapsed(),
        len = event_history.len()
    );
    Ok(())
}
