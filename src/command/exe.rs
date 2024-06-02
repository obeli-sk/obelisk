use anyhow::bail;
use anyhow::Context;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::storage::ExecutionEventInner;
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
    force: bool,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();

    if !force {
        // Check that ffqn exists
        let (ffqn2, param_types, _return_type) = db_connection.get_exported_function(ffqn).await?;
        ffqn = ffqn2;
        // Check parameter cardinality
        if params.len() != param_types.len() {
            bail!(
                "incorrect number of parameters. Expected {expected}, got {got}",
                expected = param_types.len(),
                got = params.len()
            );
        }
        // TODO: Typecheck parameters
    }

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
            retry_exp_backoff: Duration::ZERO, // TODO pass from args
            max_retries: 5,                    // TODO pass from args
        })
        .await
        .unwrap();

    println!("{execution_id}\nWaiting for result...");

    let execution_log = db_connection
        .wait_for_pending_state(execution_id, PendingState::Finished, None)
        .await?;
    let res = execution_log
        .finished_result()
        .expect("pending state was checked");

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
            println!("Finished OK, took {duration:?}");
            let value = match res {
                SupportedFunctionResult::Infallible(WastValWithType { value, .. }) => Some(value),
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
            println!("Finished with an error in {duration:?} - {res:?}");
        }
    }

    Ok(())
}
