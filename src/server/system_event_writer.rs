use concepts::{
    ExecutionId,
    prefixed_ulid::DeploymentId,
    storage::{DbPool, SystemEvent, SystemEventCode},
};
use serde_json::Value;
use tracing::warn;

pub(crate) async fn record(
    db_pool: &dyn DbPool,
    code: SystemEventCode,
    execution_id: Option<ExecutionId>,
    deployment_id: Option<DeploymentId>,
    details: Value,
) {
    let event = match SystemEvent::new(code, execution_id, deployment_id, details) {
        Ok(event) => event,
        Err(err) => {
            warn!(code = code.as_str(), "Cannot construct system event: {err}");
            return;
        }
    };
    let result = async { db_pool.admin_conn().await?.append_system_event(event).await }.await;
    if let Err(err) = result {
        warn!(code = code.as_str(), "Cannot persist system event: {err}");
    }
}
