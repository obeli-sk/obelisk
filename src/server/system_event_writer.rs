use concepts::{
    ContentDigest, ExecutionId,
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

pub(crate) async fn record_with_cas(
    db_pool: &dyn DbPool,
    code: SystemEventCode,
    execution_id: Option<ExecutionId>,
    deployment_id: Option<DeploymentId>,
    details: Value,
    cas_digest: ContentDigest,
    cas_content: Vec<u8>,
) -> Option<String> {
    let event = match SystemEvent::new(code, execution_id, deployment_id, details) {
        Ok(event) => event.with_cas_digest(cas_digest),
        Err(err) => {
            warn!(code = code.as_str(), "Cannot construct system event: {err}");
            return None;
        }
    };
    let event_id = event.event_id.clone();
    let result = async {
        db_pool
            .admin_conn()
            .await?
            .append_system_event_with_cas(event, cas_content)
            .await
    }
    .await;
    if let Err(err) = result {
        warn!(code = code.as_str(), "Cannot persist system event: {err}");
        None
    } else {
        Some(event_id)
    }
}

pub(crate) async fn hydrate_cas_details(db_pool: &dyn DbPool, events: &mut [SystemEvent]) {
    let Ok(cas) = db_pool.cas_conn().await else {
        warn!("Cannot open CAS while hydrating system events");
        return;
    };
    for event in events {
        if let Some(digest) = event.cas_digest.as_ref() {
            match cas.read_blob(digest).await {
                Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
                    Ok(policy) => event.details["policy"] = policy,
                    Err(err) => warn!(%digest, "Cannot decode system event CAS detail: {err}"),
                },
                Ok(None) => warn!(%digest, "System event CAS detail is missing"),
                Err(err) => warn!(%digest, "Cannot read system event CAS detail: {err}"),
            }
        }
    }
}
