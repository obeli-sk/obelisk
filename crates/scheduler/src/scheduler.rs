use self::index::PendingIndex;
use crate::{
    storage::inmemory_dao::{
        api::{DbRequest, DbTickRequest, GeneralRequest},
        DbTask,
    },
    worker::DbConnection,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

mod index {
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, HashMap};

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

        pub(crate) fn is_empty(&self, before: DateTime<Utc>) -> bool {
            self.pending.is_empty() && self.pending_scheduled.range(..before).next().is_none()
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

struct SchTask<ID: ExecutionId, DB: DbConnection<ID>> {
    db_connection: DB,
    ffqns: Vec<FunctionFqn>,
    pending: PendingIndex<ID>,
}

struct SchTickRequest {
    request: SchRequest,
    received_at: DateTime<Utc>,
}

enum SchRequest {
    FetchPending {
        expected_next_tick_at: DateTime<Utc>,
        batch_size: usize,
    },
}

#[derive(Debug)]
enum SchTickResponse<ID: ExecutionId> {
    FetchPending {
        pending_index: Option<PendingIndex<ID>>,
    },
}

impl<ID: ExecutionId, DB: DbConnection<ID>> SchTask<ID, DB> {
    async fn tick(&mut self, tick_request: SchTickRequest) -> SchTickResponse<ID> {
        match tick_request.request {
            SchRequest::FetchPending {
                expected_next_tick_at,
                batch_size,
            } => {
                // Obtain the list of pending executions, update `self.pending` .
                let mut pending = None;
                if let Ok(pending_vec) = self
                    .db_connection
                    .fetch_pending(batch_size, expected_next_tick_at, None, self.ffqns.clone())
                    .await
                {
                    pending = Some(PendingIndex::new(pending_vec));
                }
                SchTickResponse::FetchPending {
                    pending_index: pending,
                }
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
        let mut scheduler = SchTask {
            db_connection: db_connection.clone(),
            ffqns: Default::default(),
            pending: Default::default(),
        };
        let actual = scheduler
            .tick(SchTickRequest {
                request: SchRequest::FetchPending {
                    expected_next_tick_at: now() + Duration::from_secs(1),
                    batch_size: 1,
                },
                received_at: now(),
            })
            .await;
        let pending_index = assert_matches!(actual, SchTickResponse::FetchPending { pending_index: Some(pending) } => pending);
        assert!(
            pending_index.is_empty(now()),
            "{pending_index:?} must be empty"
        );
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

        // Plug in an executor. tick() should start the execution.
    }
}
