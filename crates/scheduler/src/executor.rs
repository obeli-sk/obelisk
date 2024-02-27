use crate::{
    storage::inmemory_dao::{api::Version, ExecutionEvent, ExecutionEventInner, ExecutorName},
    time::now,
    worker::{DbConnection, DbError, DbReadError},
};
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn, SupportedFunctionResult};
use std::{marker::PhantomData, time::Duration};
use tracing::{debug, instrument};

struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>> {
    db_connection: DB,
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    max_tick_sleep: Duration,
    phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest {
    now: DateTime<Utc>,
    batch_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct ExecutionProgress<ID: ExecutionId> {
    execution_id: ID,
    event: Result<ExecutionEvent<ID>, DbError>,
}

impl<ID: ExecutionId, DB: DbConnection<ID>> ExecTask<ID, DB> {
    async fn tick(
        &mut self,
        request: ExecTickRequest,
    ) -> Result<Vec<ExecutionProgress<ID>>, DbReadError> {
        let pending = self
            .db_connection
            .fetch_pending(request.batch_size, request.now, self.ffqns.clone())
            .await?;

        let mut executions = Vec::new();
        for (execution, version, _) in pending {
            executions.push(self.lock_execute(execution, version, request.now).await);
        }
        Ok(executions)
    }

    #[instrument(skip_all, fields(%execution_id))]
    async fn lock_execute(
        &self,
        execution_id: ID,
        version: Version,
        started_at: DateTime<Utc>,
    ) -> ExecutionProgress<ID> {
        match self
            .db_connection
            .lock(
                execution_id.clone(),
                version,
                self.executor_name.clone(),
                started_at + self.lock_expiry,
            )
            .await
        {
            Ok(event_history) => {
                // TODO - execute, update state.
                ExecutionProgress {
                    execution_id,
                    event: Ok(ExecutionEvent {
                        created_at: now(),
                        event: ExecutionEventInner::Finished {
                            result: Ok(SupportedFunctionResult::None),
                        },
                    }),
                }
            }
            Err(err) => {
                debug!("Database error: {err:?}");
                ExecutionProgress {
                    execution_id,
                    event: Err(err),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        storage::inmemory_dao::{tests::TickBasedDbConnection, DbTask, ExecutionEventInner},
        time::now,
        worker::DbConnection,
    };
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params};
    use std::sync::Arc;
    use tracing::info;
    use tracing_unwrap::ResultExt;

    use super::*;

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn tick_based() {
        set_up();
        let db_task = Arc::new(std::sync::Mutex::new(DbTask::new()));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        test(db_connection).await;
    }

    async fn test(db_connection: impl DbConnection<WorkflowId> + Clone) {
        set_up();

        info!("Now: {}", now());
        let max_tick_sleep = Duration::from_millis(500);
        let mut executor = ExecTask {
            db_connection: db_connection.clone(),
            ffqns: vec![SOME_FFQN.to_owned()],
            executor_name: Arc::new("exec1".to_string()),
            lock_expiry: Duration::from_secs(1),
            max_tick_sleep,
            phantom_data: Default::default(),
        };
        let actual = executor
            .tick(ExecTickRequest {
                batch_size: 5,
                now: now(),
            })
            .await;
        let executions = actual.unwrap_or_log();
        assert!(executions.is_empty(),);
        // Create an execution
        let execution_id = WorkflowId::generate();
        db_connection
            .insert(
                execution_id.clone(),
                0,
                ExecutionEventInner::Created {
                    ffqn: SOME_FFQN.to_owned(),
                    params: Params::default(),
                    parent: None,
                    scheduled_at: None,
                },
            )
            .await
            .unwrap_or_log();

        // execute!
        let actual = executor
            .tick(ExecTickRequest {
                batch_size: 5,
                now: now(),
            })
            .await;
        let executions = actual.unwrap_or_log();
        let expected = ExecutionProgress {
            execution_id,
            event: Ok(ExecutionEvent {
                created_at: now(),
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
            }),
        };
        assert_eq!(vec![expected], executions);
    }
}
