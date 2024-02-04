use concepts::workflow_id::WorkflowId;
use concepts::FunctionFqnStr;
use runtime::event_history::EventHistory;
use runtime::runtime::RuntimeBuilder;
use runtime::workflow::WorkflowConfig;
use runtime::{activity::ActivityConfig, database::Database};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
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
    let fqn = if let Some((ifc_fqn, function_name)) =
        workflow_function.split_once(IFC_FQN_FUNCTION_NAME_SEPARATOR)
    {
        FunctionFqnStr::new(ifc_fqn, function_name).to_owned()
    } else {
        panic!("workflow function must be a fully qualified name in format `package/interface.function`")
    };

    let database = Database::new(100, 100);

    let mut runtime = RuntimeBuilder::default();
    runtime
        .add_activity(activity_wasm_path, &ActivityConfig::default())
        .await?;
    let runtime = runtime
        .build(workflow_wasm_path, &WorkflowConfig::default())
        .await?;
    println!("Initialized in {duration:?}", duration = timer.elapsed());
    println!();

    let event_history = Arc::new(Mutex::new(EventHistory::default()));
    let timer = Instant::now();
    let param_types = runtime
        .workflow_function_metadata(&fqn)
        .expect("function must exist")
        .params
        .iter()
        .map(|(_, ty)| ty);

    let param_vals = Arc::new(
        args.next()
            .map(|param_vals| {
                deserialize_sequence::<Val>(&param_vals, param_types)
                    .expect("deserialization of params failed")
            })
            .unwrap_or_default(),
    );

    let _abort_handle = runtime.spawn(&database);

    let res = database
        .workflow_scheduler()
        .schedule_workflow(
            WorkflowId::generate(),
            event_history.clone(),
            fqn,
            param_vals,
        )
        .await;
    println!(
        "Finished: in {duration:?} {res:?}, event history size: {len}",
        duration = timer.elapsed(),
        len = event_history.lock().await.successful_activities()
    );
    Ok(())
}
