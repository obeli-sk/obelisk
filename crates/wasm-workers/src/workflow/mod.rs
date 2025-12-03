use concepts::{ClosingStrategy, JoinSetId};

pub(crate) mod caching_db_connection;
pub mod deadline_tracker;
pub(crate) mod event_history;
pub mod host_exports;
pub(crate) mod wasi;
pub(crate) mod workflow_ctx;
pub mod workflow_worker;

#[derive(Clone, Debug)]
pub struct JoinSetResource {
    pub join_set_id: JoinSetId,
    pub closing_strategy: ClosingStrategy,
}
