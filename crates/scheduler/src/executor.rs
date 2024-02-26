use self::index::PendingIndex;
use crate::{
    storage::inmemory_dao::{
        api::{DbRequest, DbTickRequest, GeneralRequest, Version},
        DbTask, ExecutorName,
    },
    worker::{DbConnection, DbError},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use std::{cmp::min, marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::oneshot;
use tracing::{debug, instrument};

mod index {
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, HashMap};
    use tracing_unwrap::OptionExt;

    use crate::storage::inmemory_dao::api::Version;

    #[derive(Debug)]
    pub(crate) struct PendingIndex<ID: ExecutionId> {
        pending: BTreeMap<ID, Version>,
        pending_scheduled: BTreeMap<DateTime<Utc>, BTreeMap<ID, Version>>,
        pending_scheduled_rev: HashMap<ID, DateTime<Utc>>,
    }
    impl<ID: ExecutionId> PendingIndex<ID> {
        pub(super) fn new(pending: Vec<(ID, Version, Option<DateTime<Utc>>)>) -> PendingIndex<ID> {
            let mut this = Self {
                pending: Default::default(),
                pending_scheduled: Default::default(),
                pending_scheduled_rev: Default::default(),
            };
            for (id, version, scheduled_at) in pending {
                if let Some(scheduled_at) = scheduled_at {
                    this.pending_scheduled
                        .insert(scheduled_at, BTreeMap::from([(id.clone(), version)]));
                    this.pending_scheduled_rev.insert(id, scheduled_at);
                } else {
                    this.pending.insert(id, version);
                }
            }
            this
        }

        pub(crate) fn is_empty(&self) -> bool {
            self.pending.is_empty() && self.pending_scheduled.is_empty()
        }

        /// Find the minimum of (first key of `pending_scheduled`, `expected_next_tick_at`)
        pub(crate) fn sleep_until(&self, expected_next_tick_at: DateTime<Utc>) -> DateTime<Utc> {
            std::cmp::min(
                self.pending_scheduled
                    .keys()
                    .next()
                    .cloned()
                    .unwrap_or(expected_next_tick_at),
                expected_next_tick_at,
            )
        }

        /// Take all executions that are pending before `now`.
        pub(crate) fn drain_before(&mut self, now: DateTime<Utc>) -> Vec<(ID, Version)> {
            let mut drained: Vec<_> = self
                .pending
                .iter()
                .map(|(id, ver)| (id.clone(), *ver))
                .collect();
            self.pending.clear();
            let to_be_drained = self.pending_scheduled.range(..now).collect::<Vec<_>>();
            for (_, id_version) in to_be_drained.iter() {
                for (id, version) in *id_version {
                    drained.push((id.clone(), *version));
                    self.pending_scheduled_rev
                        .remove(id)
                        .expect_or_log("must be found in `pending_scheduled_rev`");
                }
            }
            for deleted_schedule in to_be_drained
                .into_iter()
                .map(|(deleted_schedule, _)| *deleted_schedule)
                .collect::<Vec<_>>()
            {
                self.pending_scheduled
                    .remove(&deleted_schedule)
                    .expect_or_log("must be found");
            }
            drained
        }
    }

    impl<T: ExecutionId> Default for PendingIndex<T> {
        fn default() -> Self {
            Self {
                pending: Default::default(),
                pending_scheduled: Default::default(),
                pending_scheduled_rev: Default::default(),
            }
        }
    }
}

struct ExecTask<ID: ExecutionId, DB: DbConnection<ID>> {
    db_connection: DB,
    ffqns: Vec<FunctionFqn>,
    executor_name: ExecutorName,
    lock_expiry: Duration,
    sleep: Duration,
    phantom_data: PhantomData<fn(ID) -> ID>,
}

#[derive(Debug)]
struct ExecTickRequest<ID: ExecutionId> {
    now: DateTime<Utc>,
    batch_size: usize,
    pending: Option<PendingIndex<ID>>,
}

#[derive(Debug)]
enum ExecTickResponse<ID: ExecutionId> {
    DbError(DbError),
    Executions {
        executions: Vec<Result<ID, ID>>,
        next_tick: DateTime<Utc>,
        pending: Option<PendingIndex<ID>>,
    },
}

impl<ID: ExecutionId, DB: DbConnection<ID>> ExecTask<ID, DB> {
    async fn tick(&mut self, request: ExecTickRequest<ID>) -> ExecTickResponse<ID> {
        let expected_next_tick_at = request.now + self.sleep;
        let mut pending = if let Some(pending) = request.pending {
            pending
        } else {
            match self
                .db_connection
                .fetch_pending(
                    request.batch_size,
                    expected_next_tick_at,
                    self.ffqns.clone(),
                )
                .await
            {
                Ok(pending_vec) => PendingIndex::new(pending_vec),
                Err(err) => return ExecTickResponse::DbError(err),
            }
        };

        let mut executions = Vec::new();
        for (execution, version) in pending.drain_before(request.now) {
            executions.push(self.lock_execute(execution, version, request.now).await);
        }
        let next_tick = pending.sleep_until(expected_next_tick_at);
        ExecTickResponse::Executions {
            executions,
            next_tick,
            pending: if pending.is_empty() {
                None
            } else {
                Some(pending)
            },
        }
    }

    #[instrument(skip_all, fields(%execution_id))]
    async fn lock_execute(
        &self,
        execution_id: ID,
        version: Version,
        now: DateTime<Utc>,
    ) -> Result<ID, ID> {
        match self
            .db_connection
            .lock(
                execution_id.clone(),
                version,
                self.executor_name.clone(),
                now + self.lock_expiry,
            )
            .await
        {
            Ok(Ok(event_history)) => {
                // TODO - execute, update state.
                Ok(execution_id)
            }
            other => {
                debug!("Locking failed: {other:?}");
                Err(execution_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use chrono::{NaiveDate, TimeZone};
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params};
    use tracing::info;
    use tracing_unwrap::ResultExt;

    use crate::{
        storage::inmemory_dao::{
            api::ExecutionSpecificRequest, tests::TickBasedDbConnection, DbTickResponse,
            ExecutionEventInner,
        },
        time::now,
        worker::DbConnection,
    };

    use super::*;

    fn set_up() {
        crate::testing::set_up();
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn tick_based() {
        set_up();
        let db_task = DbTask::new();
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let db_connection = TickBasedDbConnection {
            db_task: db_task.clone(),
        };
        test(db_connection).await;
    }

    async fn test(db_connection: impl DbConnection<WorkflowId> + Clone) {
        set_up();

        info!("Now: {}", now());
        let sleep = Duration::from_millis(500);
        let mut executor = ExecTask {
            db_connection: db_connection.clone(),
            ffqns: Default::default(),
            executor_name: Arc::new("exec1".to_string()),
            lock_expiry: Duration::from_secs(1),
            sleep,
            phantom_data: Default::default(),
        };
        let actual = executor
            .tick(ExecTickRequest {
                batch_size: 5,
                now: now(),
                pending: None,
            })
            .await;
        let (executions, next_tick, pending) = assert_matches!(actual, ExecTickResponse::Executions { executions,next_tick, pending } => (executions,next_tick, pending));
        assert!(executions.is_empty(),);
        assert_eq!(now() + sleep, next_tick);
        assert!(pending.is_none());
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
            .unwrap_or_log()
            .unwrap_or_log();

        // Fetch
        // Lock
        // Execute
    }
}
