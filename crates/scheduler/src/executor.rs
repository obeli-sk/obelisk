use crate::{
    storage::{DbConnection, DbConnectionError, DbError, ExecutorName, Version},
    time::{now, now_tokio_instant},
    worker::Worker,
};
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use std::{marker::PhantomData, time::Duration};
use tokio::task::AbortHandle;
use tracing::{debug, info_span, instrument, warn, Instrument};

pub struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID>> {
    db_connection: DB,
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    max_tick_sleep: Duration,
    worker: W,
    _phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest {
    executed_at: DateTime<Utc>,
    batch_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
struct ExecutionProgress<ID: ExecutionId> {
    execution_id: ID,
    result: Result<Version, DbError>,
}

pub struct ExecutorTaskHandle {
    abort_handle: AbortHandle,
}

impl Drop for ExecutorTaskHandle {
    fn drop(&mut self) {
        if self.abort_handle.is_finished() {
            return;
        }
        warn!("Aborting the task");
        self.abort_handle.abort();
    }
}

impl<ID: ExecutionId, DB: DbConnection<ID>, W: Worker<ID> + Send + 'static> ExecTask<ID, DB, W> {
    pub fn spawn_new(
        db_connection: DB,
        ffqns: Vec<FunctionFqn>,
        executor_name: ExecutorName,
        lock_expiry: Duration,
        max_tick_sleep: Duration,
        worker: W,
        batch_size: usize,
    ) -> ExecutorTaskHandle {
        let span = info_span!("executor", ?ffqns);
        let abort_handle = tokio::spawn(
            async move {
                let mut task = Self {
                    db_connection,
                    ffqns,
                    executor_name,
                    lock_expiry,
                    max_tick_sleep,
                    worker,
                    _phantom_data: PhantomData,
                };
                loop {
                    let mut instant = now_tokio_instant();
                    instant += task.max_tick_sleep;
                    let _ = task
                        .tick(ExecTickRequest {
                            executed_at: now(),
                            batch_size,
                        })
                        .await;
                    tokio::time::sleep_until(instant).await;
                }
            }
            .instrument(span),
        )
        .abort_handle();
        ExecutorTaskHandle { abort_handle }
    }

    // TODO: logging
    #[instrument(skip_all)]
    async fn tick(
        &mut self,
        request: ExecTickRequest,
    ) -> Result<Vec<ExecutionProgress<ID>>, DbConnectionError> {
        let lock_expires_at = request.executed_at + self.lock_expiry;
        let locked = self
            .db_connection
            .lock_pending(
                request.batch_size,
                request.executed_at, // fetch expiring before now
                self.ffqns.clone(),
                request.executed_at, // lock created at - now
                self.executor_name.clone(),
                lock_expires_at,
            )
            .await?;

        let mut executions = Vec::new();
        for (execution_id, version, params, event_history, _) in locked {
            debug!(%execution_id, "Executing");
            let result = self
                .worker
                .run(
                    execution_id.clone(),
                    params,
                    event_history,
                    version,
                    lock_expires_at,
                )
                .instrument(info_span!("worker::run", %execution_id))
                .await;

            executions.push(ExecutionProgress {
                execution_id,
                result,
            });
        }
        Ok(executions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::inmemory_dao::{tests::TickBasedDbConnection, DbTask},
        storage::{DbConnection, EventHistory, ExecutionEvent, ExecutionEventInner},
        time::now,
    };
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params, SupportedFunctionResult};
    use std::{future::Future, sync::Arc};
    use tracing::{debug, info};
    use tracing_unwrap::{OptionExt, ResultExt};

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    struct SimpleWorker<DB: DbConnection<WorkflowId> + Send> {
        db_connection: DB,
    }
    #[async_trait]
    impl<DB: DbConnection<WorkflowId> + Sync> Worker<WorkflowId> for SimpleWorker<DB> {
        async fn run(
            &self,
            execution_id: WorkflowId,
            _params: Params,
            _events: Vec<EventHistory<WorkflowId>>,
            version: Version,
            _lock_expires_at: DateTime<Utc>,
        ) -> Result<Version, DbError> {
            debug!("run");
            tokio::time::sleep(Duration::from_millis(10)).await;
            let finished_event = ExecutionEvent {
                created_at: now(),
                event: ExecutionEventInner::Finished {
                    result: Ok(SupportedFunctionResult::None),
                },
            };
            let version = self
                .db_connection
                .append(execution_id, version, finished_event.clone())
                .await?;

            Ok(version)
        }
    }

    fn tick_fn(
        db_connection: impl DbConnection<WorkflowId> + Sync + Clone + 'static,
    ) -> impl (FnMut() -> Box<dyn Future<Output = ()> + Unpin>) {
        move || {
            let db_connection = db_connection.clone();
            Box::new(Box::pin(async move {
                let max_tick_sleep = Duration::from_millis(500);
                let mut executor = ExecTask {
                    db_connection: db_connection.clone(),
                    ffqns: vec![SOME_FFQN.to_owned()],
                    executor_name: Arc::new("exec1".to_string()),
                    lock_expiry: Duration::from_secs(1),
                    max_tick_sleep,
                    worker: SimpleWorker {
                        db_connection: db_connection.clone(),
                    },
                    _phantom_data: Default::default(),
                };
                let _ = executor
                    .tick(ExecTickRequest {
                        batch_size: 5,
                        executed_at: now(),
                    })
                    .await;
            }))
        }
    }

    #[tokio::test]
    async fn execute_tick_based() {
        set_up();
        let db_task = Arc::new(std::sync::Mutex::new(DbTask::new()));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        execute(db_connection.clone(), tick_fn(db_connection)).await;
    }

    #[tokio::test]
    async fn execute_executor_tick_db_task() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let db_connection = db_task.as_db_connection().unwrap_or_log();
        execute(db_connection.clone(), tick_fn(db_connection)).await;
        db_task.close().await;
    }

    #[tokio::test]
    async fn execute_task_based() {
        set_up();
        let mut db_task = DbTask::spawn_new(1);
        let lock_expiry = Duration::from_secs(1);
        let max_tick_sleep = Duration::from_millis(500);
        let worker = SimpleWorker {
            db_connection: db_task.as_db_connection().unwrap_or_log(),
        };
        let exec_task = ExecTask::spawn_new(
            db_task.as_db_connection().unwrap_or_log(),
            vec![SOME_FFQN.to_owned()],
            Arc::new("exec1".to_string()),
            lock_expiry,
            max_tick_sleep,
            worker,
            1,
        );
        execute(db_task.as_db_connection().unwrap_or_log(), || {
            tokio::time::sleep(Duration::from_secs(1))
        })
        .await;
        drop(exec_task);
        db_task.close().await;
    }

    async fn execute<T: FnMut() -> F, F: Future<Output = ()>>(
        db_connection: impl DbConnection<WorkflowId> + Clone + Sync + 'static,
        mut tick: T,
    ) {
        info!("Now: {}", now());
        tick().await;
        // Create an execution
        let execution_id = WorkflowId::generate();
        db_connection
            .create(
                execution_id.clone(),
                now(),
                SOME_FFQN.to_owned(),
                Params::default(),
                None,
                None,
            )
            .await
            .unwrap_or_log();

        // execute!
        let requested_execution_at = now();
        tick().await;
        // check that DB contains the result.
        let (history, _) = db_connection.get(execution_id).await.unwrap_or_log();
        assert_eq!(3, history.len());
        assert_matches!(
            history[0],
            ExecutionEvent {
                event: ExecutionEventInner::Created { .. },
                ..
            }
        );
        assert_matches!(
            history[1],
            ExecutionEvent {
                event: ExecutionEventInner::Locked { .. },
                ..
            }
        );
        assert_matches!(history[2], ExecutionEvent {
            created_at: executed_at,
            event: ExecutionEventInner::Finished {
                result: Ok(SupportedFunctionResult::None),
            },
        } if executed_at >= requested_execution_at);
    }
}
