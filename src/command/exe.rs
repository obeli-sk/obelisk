use anyhow::Context;
use concepts::storage::DbConnection;
use concepts::storage::DbPool;
use concepts::SupportedFunctionResult;
use concepts::{storage::CreateRequest, ExecutionId, FunctionFqn, Params};
use db_sqlite::sqlite_dao::SqlitePool;
use std::path::Path;
use std::time::Duration;
use utils::time::now;
use val_json::wast_val::WastVal;
use val_json::wast_val::WastValWithType;

pub(crate) async fn schedule<P: AsRef<Path>>(
    ffqn: FunctionFqn,
    params: Params,
    verbose: bool,
    db_file: P,
) -> anyhow::Result<()> {
    let db_file = db_file.as_ref();
    let db_pool = SqlitePool::new(db_file)
        .await
        .with_context(|| format!("cannot open sqlite file `{db_file:?}`"))?;
    let db_connection = db_pool.connection();

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
    let res = db_connection
        .wait_for_finished_result(execution_id, None)
        .await
        .unwrap();

    let duration = (now() - created_at).to_std().unwrap();
    match res {
        Ok(
            res @ SupportedFunctionResult::None
            | res @ SupportedFunctionResult::Infallible(_)
            | res @ SupportedFunctionResult::Fallible(WastValWithType {
                value: WastVal::Result(Ok(_)),
                ..
            }),
        ) => {
            println!("Finished OK in {duration:?}");
            if verbose {
                let value = match res {
                    SupportedFunctionResult::Infallible(WastValWithType { value, .. }) => {
                        Some(value)
                    }
                    SupportedFunctionResult::Fallible(WastValWithType {
                        value: WastVal::Result(Ok(Some(value))),
                        ..
                    }) => Some(*value),
                    _ => None,
                };
                if let Some(value) = value {
                    println!("{value:?}");
                }
            }
        }
        _ => {
            println!("Finished with an error in {duration:?} - {res:?}");
        }
    }

    Ok(())
}
