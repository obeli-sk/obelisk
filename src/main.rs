use runtime::activity::ActivityConfig;
use runtime::event_history::EventHistory;
use runtime::runtime::Runtime;
use runtime::workflow::WorkflowConfig;
use runtime::workflow_id::WorkflowId;
use runtime::FunctionFqn;
use std::time::Instant;
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

    let workflow_wasm_path = args.next().expect("workflow wasm missing");
    let workflow_function = args.next().expect("workflow function missing");
    let Some((ifc_fqn, function_name)) =
        workflow_function.split_once(IFC_FQN_FUNCTION_NAME_SEPARATOR)
    else {
        panic!("workflow function must be a fully qualified name in format `package/interface.function`")
    };

    let mut runtime = Runtime::default();
    runtime
        .add_activity(activity_wasm_path, &ActivityConfig::default())
        .await?;
    let workflow = runtime
        .add_workflow_definition(workflow_wasm_path, &WorkflowConfig::default())
        .await?;
    println!("Initialized in {duration:?}", duration = timer.elapsed());
    println!();

    let mut event_history = EventHistory::default();
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

    let res = runtime
        .schedule_workflow(
            &WorkflowId::generate(),
            &mut event_history,
            &fqn,
            &param_vals,
        )
        .await;
    println!(
        "Finished: in {duration:?} {res:?}, event history size: {len}",
        duration = timer.elapsed(),
        len = event_history.successful_activities()
    );
    Ok(())
}
