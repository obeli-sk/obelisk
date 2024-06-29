use anyhow::bail;
use anyhow::Context;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEvent;
use concepts::storage::ExecutionEventInner;
use concepts::storage::ExecutionLog;
use concepts::storage::PendingState;
use concepts::SupportedFunctionResult;
use concepts::{storage::CreateRequest, ExecutionId, FunctionFqn, Params};
use db_sqlite::sqlite_dao::SqlitePool;
use std::path::Path;
use std::time::Duration;
use utils::time::now;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

pub(crate) async fn schedule<P: AsRef<Path>>(
    mut ffqn: FunctionFqn,
    params: Params,
    db_file: P,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();

    // Check that ffqn exists
    let (component_id, param_types, return_type) = {
        let (component_id, (ffqn2, param_types, return_type)) = db_connection
            .component_enabled_get_exported_function(ffqn)
            .await?;
        ffqn = ffqn2;
        (component_id, param_types, return_type)
    };
    // Check parameter cardinality
    if params.len() != param_types.len() {
        bail!(
            "incorrect number of parameters. Expected {expected}, got {got}",
            expected = param_types.len(),
            got = params.len()
        );
    }
    // TODO: Typecheck parameters

    let execution_id = ExecutionId::generate();
    let created_at = now();
    db_connection
        .create(CreateRequest {
            created_at,
            execution_id,
            ffqn,
            params,
            parent: None,
            scheduled_at: created_at,
            retry_exp_backoff: Duration::from_millis(100), // TODO pass from args
            max_retries: 5,                                // TODO pass from args
            component_id,
            return_type,
        })
        .await
        .unwrap();

    println!("{execution_id}\nWaiting for result...");
    let execution_log = db_connection
        .wait_for_pending_state(execution_id, PendingState::Finished, None)
        .await?;
    print_result_if_finished(&execution_log);
    Ok(())
}

fn print_result_if_finished(execution_log: &ExecutionLog) -> Option<()> {
    let finished = execution_log.finished_result();
    if let Some(res) = finished {
        let first_locked_at = execution_log
            .events
            .iter()
            .find(|event| matches!(event.event, ExecutionEventInner::Locked { .. }))
            .expect("must have been locked")
            .created_at;
        let duration = (execution_log.last_event().created_at - first_locked_at)
            .to_std()
            .unwrap();

        match res {
            Ok(
                res @ (SupportedFunctionResult::None
                | SupportedFunctionResult::Infallible(_)
                | SupportedFunctionResult::Fallible(WastValWithType {
                    value: WastVal::Result(Ok(_)),
                    ..
                })),
            ) => {
                println!("Finished OK");
                let value = match res {
                    SupportedFunctionResult::Infallible(WastValWithType { value, .. }) => {
                        Some(value)
                    }
                    SupportedFunctionResult::Fallible(WastValWithType {
                        value: WastVal::Result(Ok(Some(value))),
                        ..
                    }) => Some(value.as_ref()),
                    _ => None,
                };
                if let Some(value) = value {
                    println!("{value:?}");
                }
            }
            _ => {
                println!("Finished with an error\n{res:?}");
            }
        }
        println!("Execution took {duration:?}");
    }
    finished.map(|_| ())
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ExecutionVerbosity {
    EventHistory,
    Full,
}

pub(crate) async fn get<P: AsRef<Path>>(
    db_file: P,
    execution_id: ExecutionId,
    verbosity: Option<ExecutionVerbosity>,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();
    let execution_log = db_connection.get(execution_id).await?;
    println!("Function: {}", execution_log.ffqn());
    if print_result_if_finished(&execution_log).is_none() {
        println!("Current state: {}", execution_log.pending_state);
    }
    if let Some(verbosity) = verbosity {
        println!();
        println!("Event history:");
        for event in execution_log.event_history() {
            println!("{event}");
        }
        if verbosity == ExecutionVerbosity::Full {
            println!();
            println!("Execution log:");
            for ExecutionEvent { created_at, event } in execution_log.events {
                println!("{created_at}\t{event}");
            }
        }
    }
    Ok(())
}
