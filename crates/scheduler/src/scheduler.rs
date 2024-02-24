use async_trait::async_trait;
use chrono::{DateTime, Utc};
use concepts::{ExecutionId, FunctionFqn};
use std::{sync::Arc, time::Duration};
use tokio::sync::oneshot;

use crate::storage::inmemory_dao::{
    api::{DbRequest, DbTickRequest, GeneralRequest},
    DbTask,
};

use self::index::PendingIndex;

mod index {
    use chrono::{DateTime, Utc};
    use concepts::ExecutionId;
    use std::collections::{BTreeMap, HashMap};

    use crate::storage::inmemory_dao::api::Version;

    #[derive(Debug)]
    pub(crate) struct PendingIndex<ID: ExecutionId> {
        pending: HashMap<ID, Version>,
        pending_scheduled: BTreeMap<DateTime<Utc>, HashMap<ID, Version>>,
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
                        .insert(scheduled_at, HashMap::from([(id.clone(), version)]));
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

struct SchTask<ID: ExecutionId, DB: DatabaseConnection<ID>> {
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

impl<ID: ExecutionId, DB: DatabaseConnection<ID>> SchTask<ID, DB> {
    async fn tick(&mut self, tick_request: SchTickRequest) -> SchTickResponse<ID> {
        match tick_request.request {
            SchRequest::FetchPending {
                expected_next_tick_at,
                batch_size,
            } => {
                // Obtain the list of pending executions, update `self.pending` .
                let (resp_sender, resp_receiver) = oneshot::channel();
                let request = DbRequest::<ID>::General(GeneralRequest::FetchPending {
                    batch_size,
                    resp_sender,
                    expiring_before: expected_next_tick_at,
                    created_since: None,
                    ffqns: self.ffqns.clone(),
                });
                let mut pending = None;
                if self
                    .db_connection
                    .send(request, tick_request.received_at)
                    .await
                    .is_ok()
                {
                    if let Ok(pending_vec) = resp_receiver.await {
                        pending = Some(PendingIndex::new(pending_vec));
                    }
                }
                SchTickResponse::FetchPending {
                    pending_index: pending,
                }
            }
        }
    }
}

#[async_trait]
trait DatabaseConnection<ID: ExecutionId> {
    async fn send(&mut self, request: DbRequest<ID>, sent_at: DateTime<Utc>) -> Result<(), ()>;
}

struct ImMemoryDatabaseConnection<ID: ExecutionId> {
    db_task: DbTask<ID>,
}

#[async_trait]
impl<ID: ExecutionId> DatabaseConnection<ID> for ImMemoryDatabaseConnection<ID> {
    async fn send(&mut self, request: DbRequest<ID>, sent_at: DateTime<Utc>) -> Result<(), ()> {
        let request = DbTickRequest {
            request,
            received_at: sent_at,
        };
        self.db_task.tick(request).send()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use chrono::{NaiveDate, TimeZone};
    use concepts::{workflow_id::WorkflowId, FunctionFqnStr, Params};
    use tracing::info;
    use tracing_unwrap::ResultExt;

    use crate::storage::inmemory_dao::{api::ExecutionSpecificRequest, DbTickResponse};

    use super::*;

    static INIT: std::sync::Once = std::sync::Once::new();
    fn set_up() {
        INIT.call_once(|| {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().without_time())
                .with(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        });
    }

    struct MutexDatabaseConnection<ID: ExecutionId> {
        db_task: Arc<std::sync::Mutex<DbTask<ID>>>,
    }

    #[async_trait]
    impl<ID: ExecutionId> DatabaseConnection<ID> for MutexDatabaseConnection<ID> {
        async fn send(&mut self, request: DbRequest<ID>, sent_at: DateTime<Utc>) -> Result<(), ()> {
            let request = DbTickRequest {
                request,
                received_at: sent_at,
            };
            self.db_task.lock().unwrap_or_log().tick(request).send()
        }
    }

    const SOME_FFQN: FunctionFqnStr = FunctionFqnStr::new("pkg/ifc", "fn");

    #[tokio::test]
    async fn test() {
        set_up();
        let (mut db_task, _request_sender) = DbTask::<WorkflowId>::new(1);
        let db_task = Arc::new(std::sync::Mutex::new(db_task));
        let mut now = Utc
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(1970, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )
            .unwrap();

        info!("Now: {now}");
        let mut scheduler = SchTask {
            db_connection: MutexDatabaseConnection {
                db_task: db_task.clone(),
            },
            ffqns: Default::default(),
            pending: Default::default(),
        };
        let actual = scheduler
            .tick(SchTickRequest {
                request: SchRequest::FetchPending {
                    expected_next_tick_at: now,
                    batch_size: 1,
                },
                received_at: now,
            })
            .await;
        let pending_index = assert_matches!(actual, SchTickResponse::FetchPending { pending_index: Some(pending) } => pending);
        assert!(
            pending_index.is_empty(now),
            "{pending_index:?} must be empty"
        );
        // Create an execution
        let execution_id = WorkflowId::generate();
        let request = DbRequest::ExecutionSpecific(ExecutionSpecificRequest::Create {
            ffqn: SOME_FFQN.to_owned(),
            params: Params::default(),
            parent: None,
            scheduled_at: None,
            execution_id: execution_id.clone(),
            resp_sender: oneshot::channel().0,
        });
        let actual = db_task.lock().unwrap().tick(DbTickRequest {
            request,
            received_at: now,
        });
        assert_matches!(actual, DbTickResponse::PersistResult { result: Ok(()), .. });
        // Plug in an executor. tick() should start the execution.
    }
}
