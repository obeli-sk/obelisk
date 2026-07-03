//! Cancellation driver: advances `cancelling` executions to `Finished(Cancelled)`.
//!
//! A cancelling row bars every WASM worker (the pick-up queries filter it out), so
//! its structured-concurrency close cannot be worker-run. This singleton,
//! digest-agnostic tick-poll task advances each one out of band, purely from the
//! persisted log (it runs no WASM, so it also works on stuck executions).
//!
//! Per cancelling execution, one close-step: reconstruct the open join-set members
//! from the log, cancel leaf activities/delays and signal cancellable children, and
//! once every member has a response append `Finished(Cancelled)` (responding to the
//! parent). A member finishing `Cancelled` is itself a response, so an ancestor's
//! close wakes when its cancelled child reports back — the recursion.

use crate::activity::cancel_registry::CancelRegistry;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionFailureKind, ExecutionId, FinishedExecutionFailure,
    SupportedFunctionReturnValue,
    prefixed_ulid::ExecutionIdDerived,
    storage::{
        self, AppendEventsToExecution, AppendRequest, AppendResponseToExecution, DbConnection,
        DbErrorRead, DbErrorWrite, DbPool, ExecutionLog, ExecutionRequest, HistoryEvent,
        JoinSetRequest,
    },
    time::{ClockFn, Sleep},
};
use db_common::{JoinSetFold, JoinSetFoldError, JoinSetMember, JoinSetResponseId};
use executor::AbortOnDropHandle;
use std::{collections::HashMap, collections::HashSet, sync::Arc, time::Duration};
use tracing::{Instrument, debug, info_span, warn};

#[derive(Debug, thiserror::Error)]
enum CloseStepError {
    #[error(transparent)]
    Read(#[from] DbErrorRead),
    #[error(transparent)]
    Write(#[from] DbErrorWrite),
    #[error("fold reconstruction failed: {0}")]
    Fold(#[from] JoinSetFoldError),
}

pub struct CancellationDriver;

impl CancellationDriver {
    #[must_use]
    pub fn spawn(
        db_pool: Arc<dyn DbPool>,
        cancel_registry: CancelRegistry,
        clock_fn: Box<dyn ClockFn>,
        sleep: impl Sleep + Clone + 'static,
        tick_sleep: Duration,
        batch_size: u32,
    ) -> AbortOnDropHandle {
        AbortOnDropHandle::new(
            tokio::spawn(
                async move {
                    debug!("Spawned the cancellation driver");
                    loop {
                        match db_pool.connection().await {
                            Ok(conn) => {
                                tick(conn.as_ref(), &cancel_registry, clock_fn.now(), batch_size)
                                    .await;
                            }
                            Err(err) => warn!("Cannot obtain a db connection - {err:?}"),
                        }
                        sleep.sleep(tick_sleep).await;
                    }
                }
                .instrument(info_span!(parent: None, "cancellation_driver")),
            )
            .abort_handle(),
        )
    }
}

async fn tick(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    now: DateTime<Utc>,
    batch_size: u32,
) {
    let ids = match conn.get_cancelling(batch_size).await {
        Ok(ids) => ids,
        Err(err) => {
            warn!("Cannot select cancelling executions - {err:?}");
            return;
        }
    };
    for execution_id in ids {
        if let Err(err) = close_step(conn, cancel_registry, &execution_id, now).await {
            debug!(%execution_id, "Cancellation close-step failed, retrying next tick - {err:?}");
        }
    }
}

/// Advance one cancelling execution by a single close-step.
async fn close_step(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    execution_id: &ExecutionId,
    now: DateTime<Utc>,
) -> Result<(), CloseStepError> {
    let log = conn.get(execution_id).await?;

    // Responses in cursor order, plus the set of members that have one.
    let mut responses = Vec::with_capacity(log.responses.len());
    let mut responded: HashSet<JoinSetResponseId> = HashSet::with_capacity(log.responses.len());
    for response in &log.responses {
        let join_set_id = response.event.event.join_set_id.clone();
        let response_id = JoinSetResponseId::from(&response.event.event.event);
        responded.insert(response_id.clone());
        responses.push((join_set_id, response_id));
    }

    // Resolve each child's component type (activity vs workflow) from its Created event.
    let child_component_types = resolve_child_component_types(conn, &log).await;

    let fold = JoinSetFold::reconstruct(
        log.event_history().map(|(event, _version)| event),
        responses,
        |child_id| {
            child_component_types
                .get(child_id)
                .copied()
                .unwrap_or(ComponentType::Workflow)
        },
    )?;

    // Cancel/signal every still-running member; the execution is ready to finish only
    // once each member has landed a response.
    let mut all_responded = true;
    for members in fold.open_join_sets().values() {
        for (response_id, member) in members {
            if responded.contains(response_id) {
                continue;
            }
            all_responded = false;
            signal_member(conn, cancel_registry, response_id, member, now).await;
        }
    }

    if all_responded {
        finish_cancelled(conn, &log, now).await?;
    }
    Ok(())
}

async fn resolve_child_component_types(
    conn: &dyn DbConnection,
    log: &ExecutionLog,
) -> HashMap<ExecutionIdDerived, ComponentType> {
    let mut types = HashMap::new();
    for (event, _version) in log.event_history() {
        if let HistoryEvent::JoinSetRequest {
            request:
                JoinSetRequest::ChildExecutionRequest {
                    child_execution_id,
                    result: Ok(()),
                    ..
                },
            ..
        } = &event
            && !types.contains_key(child_execution_id)
        {
            match conn
                .get_create_request(&ExecutionId::Derived(child_execution_id.clone()))
                .await
            {
                Ok(create_req) => {
                    types.insert(
                        child_execution_id.clone(),
                        create_req.component_id.component_type,
                    );
                }
                Err(err) => {
                    debug!(%child_execution_id, "Cannot resolve child component type - {err:?}")
                }
            }
        }
    }
    types
}

/// Best-effort teardown of one running member: cancel activities/delays, signal
/// cancellable children (recursion). An uncancellable child is merely awaited.
async fn signal_member(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    response_id: &JoinSetResponseId,
    member: &JoinSetMember,
    now: DateTime<Utc>,
) {
    match response_id {
        JoinSetResponseId::DelayId(delay_id) => {
            if let Err(err) = storage::cancel_delay(conn, delay_id.clone(), now).await {
                debug!("Ignoring failure to cancel delay {delay_id} - {err:?}");
            }
        }
        JoinSetResponseId::ChildExecutionId(child_id) => {
            let child = ExecutionId::Derived(child_id.clone());
            if member.is_activity() {
                if let Err(err) = cancel_registry.cancel_activity(conn, &child, now).await {
                    debug!("Ignoring failure to cancel activity {child_id} - {err:?}");
                }
            } else if member.is_cancellable_workflow()
                && let Err(err) = conn.request_cancellation_with_retries(&child, now).await
            {
                debug!("Ignoring failure to signal cancellable child {child_id} - {err:?}");
            }
        }
    }
}

/// Append `Finished(Cancelled)`, responding to the parent if this is a child.
async fn finish_cancelled(
    conn: &dyn DbConnection,
    log: &ExecutionLog,
    now: DateTime<Utc>,
) -> Result<(), DbErrorWrite> {
    let retval = SupportedFunctionReturnValue::ExecutionFailure(FinishedExecutionFailure {
        reason: None,
        kind: ExecutionFailureKind::Cancelled,
        detail: None,
    });
    let finished_version = log.next_version.clone();
    let finished_req = AppendRequest {
        created_at: now,
        event: ExecutionRequest::Finished {
            retval: retval.clone(),
            http_client_traces: None,
        },
    };
    if let ExecutionId::Derived(derived) = &log.execution_id {
        let (parent_execution_id, join_set_id) = derived.split_to_parts();
        conn.append_batch_respond_to_parent(
            AppendEventsToExecution {
                execution_id: log.execution_id.clone(),
                version: finished_version.clone(),
                batch: vec![finished_req],
            },
            AppendResponseToExecution {
                parent_execution_id,
                created_at: now,
                join_set_id,
                child_execution_id: derived.clone(),
                finished_version,
                result: retval,
            },
            now,
        )
        .await?;
    } else {
        conn.append(log.execution_id.clone(), finished_version, finished_req)
            .await?;
    }
    debug!(execution_id = %log.execution_id, "Cancellation finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use concepts::prefixed_ulid::DEPLOYMENT_ID_DUMMY;
    use concepts::storage::{CreateRequest, DbPoolCloseable, JoinSetRequest, PendingState};
    use concepts::{ComponentId, JoinSetId, JoinSetKind, Params, StrVariant};
    use db_tests::{CANCELLABLE_FFQN, Database};
    use test_utils::sim_clock::SimClock;

    /// A cancelling parent recursively cancels its child and finishes only once the
    /// child's `Cancelled` response lands: the driver advances a whole subtree across
    /// ticks with no worker running any WASM.
    #[tokio::test]
    async fn driver_cancels_subtree_and_finishes_cancelled() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;
        let conn = db_pool.connection().await.unwrap();
        let cancel_registry = CancelRegistry::new();
        let now = sim_clock.now();

        let create = |execution_id: ExecutionId| CreateRequest {
            created_at: now,
            execution_id,
            ffqn: CANCELLABLE_FFQN,
            params: Params::empty(),
            parent: None,
            metadata: concepts::ExecutionMetadata::empty(),
            scheduled_at: now,
            component_id: ComponentId::dummy_workflow(),
            deployment_id: DEPLOYMENT_ID_DUMMY,
            scheduled_by: None,
            paused: false,
        };

        // Cancellable parent blocked on a join set holding a cancellable child.
        let parent_id = ExecutionId::generate();
        let version = conn.create(create(parent_id.clone())).await.unwrap();
        let join_set_id = JoinSetId::new(JoinSetKind::OneOff, StrVariant::empty()).unwrap();
        let version = conn
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::HistoryEvent {
                        event: HistoryEvent::JoinSetCreate {
                            join_set_id: join_set_id.clone(),
                        },
                    },
                },
            )
            .await
            .unwrap();
        let child_id = parent_id.next_level(&join_set_id);
        let version = conn
            .append(
                parent_id.clone(),
                version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::HistoryEvent {
                        event: HistoryEvent::JoinSetRequest {
                            join_set_id: join_set_id.clone(),
                            request: JoinSetRequest::ChildExecutionRequest {
                                child_execution_id: child_id.clone(),
                                target_ffqn: CANCELLABLE_FFQN,
                                params: Params::empty(),
                                result: Ok(()),
                            },
                        },
                    },
                },
            )
            .await
            .unwrap();
        conn.create(create(ExecutionId::Derived(child_id.clone())))
            .await
            .unwrap();
        conn.append(
            parent_id.clone(),
            version,
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::HistoryEvent {
                    event: HistoryEvent::JoinNext {
                        join_set_id: join_set_id.clone(),
                        run_expires_at: now,
                        closing: false,
                        requested_ffqn: Some(CANCELLABLE_FFQN),
                    },
                },
            },
        )
        .await
        .unwrap();
        conn.request_cancellation(&parent_id, now).await.unwrap();

        // A handful of ticks drives: parent signals child, child finishes and responds,
        // parent then finishes.
        for _ in 0..5 {
            tick(conn.as_ref(), &cancel_registry, now, 10).await;
        }

        for id in [ExecutionId::Derived(child_id), parent_id] {
            let log = conn.get(&id).await.unwrap();
            assert_matches::assert_matches!(
                log.pending_state,
                PendingState::Finished(_),
                "{id} should be finished"
            );
            assert_matches::assert_matches!(
                log.as_finished_result(),
                Some(SupportedFunctionReturnValue::ExecutionFailure(
                    FinishedExecutionFailure {
                        kind: ExecutionFailureKind::Cancelled,
                        ..
                    }
                )),
                "{id} should be cancelled"
            );
        }
        assert!(conn.get_cancelling(10).await.unwrap().is_empty());
        drop(conn);
        db_close.close().await;
    }
}
