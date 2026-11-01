use concepts::{
    ContentDigest, ExecutionId,
    prefixed_ulid::{DeploymentId, SystemEventId},
    storage::{DbPool, SystemEvent, SystemEventCode, SystemEventLevel},
};
use serde_json::Value;
use tracing::{Level, warn};

pub(crate) async fn record(
    db_pool: &dyn DbPool,
    code: SystemEventCode,
    execution_id: Option<ExecutionId>,
    deployment_id: Option<DeploymentId>,
    details: Value,
) {
    let _ = record_retryable(db_pool, code, execution_id, deployment_id, details).await;
}

pub(crate) async fn record_retryable(
    db_pool: &dyn DbPool,
    code: SystemEventCode,
    execution_id: Option<ExecutionId>,
    deployment_id: Option<DeploymentId>,
    details: Value,
) -> Option<SystemEventId> {
    let event = match SystemEvent::new(code, execution_id, deployment_id, details) {
        Ok(event) => event,
        Err(err) => {
            warn!(code = code.as_str(), "Cannot construct system event: {err}");
            return None;
        }
    };
    log_event(&event);
    let event_id = event.event_id;
    let result = async { db_pool.admin_conn().await?.append_system_event(event).await }.await;
    if let Err(err) = result {
        warn!(code = code.as_str(), "Cannot persist system event: {err}");
        None
    } else {
        Some(event_id)
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
) -> Option<SystemEventId> {
    let event = match SystemEvent::new(code, execution_id, deployment_id, details) {
        Ok(event) => event.with_cas_digest(cas_digest),
        Err(err) => {
            warn!(code = code.as_str(), "Cannot construct system event: {err}");
            return None;
        }
    };
    log_event(&event);
    let event_id = event.event_id;
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

fn log_event(event: &SystemEvent) {
    macro_rules! emit {
        ($level:expr) => {
            tracing::event!(
                $level,
                event_id = %event.event_id,
                code = event.code,
                execution_id = event.execution_id.as_ref().map(display),
                deployment_id = event.deployment_id.as_ref().map(display),
                details = %event.details,
                "System event"
            )
        };
    }
    match event.level {
        SystemEventLevel::Debug => emit!(Level::DEBUG),
        SystemEventLevel::Info => emit!(Level::INFO),
        SystemEventLevel::Warning => emit!(Level::WARN),
        SystemEventLevel::Error => emit!(Level::ERROR),
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
