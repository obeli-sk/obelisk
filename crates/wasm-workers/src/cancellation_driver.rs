//! Cancellation driver: advances `cancelling` executions to `Finished(Cancelled)`.
//!
//! A cancelling row bars every WASM worker (the pick-up queries filter it out), so
//! its structured-concurrency close cannot be worker-run. This singleton,
//! digest-agnostic tick-poll task advances each one out of band, purely from the
//! persisted log (it runs no WASM, so it also works on stuck executions).
//!
//! Per cancelling workflow, one close-step: reconstruct the open child executions
//! and delays from the log, cancel leaf activities/delays and signal cancellable
//! children, and once every child or delay has a response append
//! `Finished(Cancelled)` (responding to the parent). Cancelling activities are
//! finalized here only when not locked, or after their lock lease expires; the
//! local activity worker owns the prompt path.

use crate::activity::cancel_registry::CancelRegistry;
use chrono::{DateTime, Utc};
use concepts::{
    ComponentType, ExecutionFailureKind, ExecutionId, FinishedExecutionFailure,
    SupportedFunctionReturnValue,
    prefixed_ulid::ExecutionIdDerived,
    storage::{
        self, AppendEventsToExecution, AppendRequest, AppendResponseToExecution, DbConnection,
        DbErrorRead, DbErrorWrite, DbPool, ExecutionLog, ExecutionRequest, HistoryEvent,
        JoinSetRequest, PendingState, PendingStateCancelling,
    },
    time::{ClockFn, Sleep},
};
use db_common::{JoinSetFold, JoinSetFoldError, JoinSetResponseId};
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
                    // Child executions and delays whose cancellation was already
                    // requested/signalled this process. Pruned as responses land;
                    // empty on restart (re-request is idempotent), so no durable state
                    // is needed.
                    let mut cancellation_requested: HashSet<JoinSetResponseId> = HashSet::new();
                    loop {
                        match db_pool.connection().await {
                            Ok(conn) => {
                                tick(
                                    conn.as_ref(),
                                    &cancel_registry,
                                    clock_fn.now(),
                                    batch_size,
                                    &mut cancellation_requested,
                                )
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
    cancellation_requested: &mut HashSet<JoinSetResponseId>,
) {
    let ids = match conn.get_cancelling(batch_size).await {
        Ok(ids) => ids,
        Err(err) => {
            warn!("Cannot select cancelling executions - {err:?}");
            return;
        }
    };
    for execution_id in ids {
        if let Err(err) = close_step(
            conn,
            cancel_registry,
            &execution_id,
            now,
            cancellation_requested,
        )
        .await
        {
            debug!(%execution_id, "Cancellation close-step failed, retrying next tick - {err:?}");
        }
    }
}

#[cfg(test)]
pub(crate) async fn tick_test(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    now: DateTime<Utc>,
) {
    let mut cancellation_requested = HashSet::new();
    tick(conn, cancel_registry, now, 10, &mut cancellation_requested).await;
}

/// Advance one cancelling execution by a single close-step.
async fn close_step(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    execution_id: &ExecutionId,
    now: DateTime<Utc>,
    cancellation_requested: &mut HashSet<JoinSetResponseId>,
) -> Result<(), CloseStepError> {
    let log = conn.get(execution_id).await?;
    if log.component_type.is_activity() {
        activity_finish_if_expired(conn, &log, now).await
    } else {
        close_workflow_step(conn, cancel_registry, &log, now, cancellation_requested).await
    }
}

async fn activity_finish_if_expired(
    conn: &dyn DbConnection,
    log: &ExecutionLog,
    now: DateTime<Utc>,
) -> Result<(), CloseStepError> {
    if let PendingState::Cancelling(PendingStateCancelling::Locked(locked)) = &log.pending_state
        && locked.lock_expires_at > now
    {
        return Ok(());
    }
    append_finish_cancelled(conn, log, now).await?;
    Ok(())
}

async fn close_workflow_step(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    log: &ExecutionLog,
    now: DateTime<Utc>,
    cancellation_requested: &mut HashSet<JoinSetResponseId>,
) -> Result<(), CloseStepError> {
    // Responses in cursor order, plus the set of children/delays that have one.
    let mut responses = Vec::with_capacity(log.responses.len());
    let mut responded: HashSet<JoinSetResponseId> = HashSet::with_capacity(log.responses.len());
    for response in &log.responses {
        let join_set_id = response.event.event.join_set_id.clone();
        let response_id = JoinSetResponseId::from(&response.event.event.event);
        responded.insert(response_id.clone());
        responses.push((join_set_id, response_id));
    }

    // Resolve each child's component type (activity vs workflow) from its Created event.
    // A resolution failure fails the whole step (retried next tick) rather than guessing
    // a type: mis-classifying an activity as an uncancellable workflow would silently
    // strand it as a permanent await barrier.
    let child_component_types = resolve_child_component_types(conn, &log).await?;

    let fold = JoinSetFold::reconstruct(
        log.event_history().map(|(event, _version)| event),
        responses,
        |child_id| {
            *child_component_types
                .get(child_id)
                .expect("every open child was resolved above")
        },
    )?;

    // Classify every still-running child or delay. Activities and delays are
    // cancelled in reverse creation order below, matching normal join-set close;
    // cancellable workflow children are signalled; uncancellable workflow children
    // are only awaited.
    let mut all_responded = true;
    let mut activity_and_delay_ids = Vec::new();
    let mut cancellable_child_ids = Vec::new();
    for members in fold.open_join_sets().values() {
        for (response_id, member) in members {
            if responded.contains(response_id) {
                // Response landed: this child/delay is done, drop it from the
                // process-local set to keep it bounded to in-flight children/delays.
                cancellation_requested.remove(response_id);
                continue;
            }
            all_responded = false;
            match response_id {
                JoinSetResponseId::DelayId(_) => activity_and_delay_ids.push(response_id.clone()),
                JoinSetResponseId::ChildExecutionId(child_id) => {
                    if member.is_activity() {
                        activity_and_delay_ids.push(response_id.clone());
                    } else if member.is_cancellable_workflow() {
                        cancellable_child_ids.push(child_id.clone());
                    }
                }
            }
        }
    }

    for response_id in activity_and_delay_ids.iter().rev() {
        cancel_activity_or_delay(
            conn,
            cancel_registry,
            response_id,
            now,
            cancellation_requested,
        )
        .await;
    }
    for child_id in cancellable_child_ids {
        signal_cancellable_child_workflow(conn, &child_id, now, cancellation_requested).await;
    }

    if all_responded {
        append_finish_cancelled(conn, log, now).await?;
    }
    Ok(())
}

async fn resolve_child_component_types(
    conn: &dyn DbConnection,
    log: &ExecutionLog,
) -> Result<HashMap<ExecutionIdDerived, ComponentType>, DbErrorRead> {
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
            let create_req = conn
                .get_create_request(&ExecutionId::Derived(child_execution_id.clone()))
                .await?;
            types.insert(
                child_execution_id.clone(),
                create_req.component_id.component_type,
            );
        }
    }
    Ok(types)
}

/// Best-effort teardown of one running activity or delay. Each child/delay has
/// cancellation requested at most once per process; the action is idempotent, so
/// re-doing it after a restart (empty set) is harmless.
async fn cancel_activity_or_delay(
    conn: &dyn DbConnection,
    cancel_registry: &CancelRegistry,
    response_id: &JoinSetResponseId,
    now: DateTime<Utc>,
    cancellation_requested: &mut HashSet<JoinSetResponseId>,
) {
    if cancellation_requested.contains(response_id) {
        return;
    }
    let outcome = match response_id {
        JoinSetResponseId::DelayId(delay_id) => storage::cancel_delay(conn, delay_id.clone(), now)
            .await
            .map(|_| ())
            .map_err(|err| debug!("Ignoring failure to cancel delay {delay_id} - {err:?}")),
        JoinSetResponseId::ChildExecutionId(child_id) => {
            let child = ExecutionId::Derived(child_id.clone());
            cancel_registry
                .cancel_activity(conn, &child, now)
                .await
                .map(|_| ())
                .map_err(|err| debug!("Ignoring failure to cancel activity {child_id} - {err:?}"))
        }
    };
    if outcome.is_ok() {
        cancellation_requested.insert(response_id.clone());
    }
}

/// Signal one cancellable workflow child. It is not finished here; its own
/// cancellation close will append the response that lets this workflow finish.
async fn signal_cancellable_child_workflow(
    conn: &dyn DbConnection,
    child_id: &ExecutionIdDerived,
    now: DateTime<Utc>,
    cancellation_requested: &mut HashSet<JoinSetResponseId>,
) {
    let response_id = JoinSetResponseId::ChildExecutionId(child_id.clone());
    if cancellation_requested.contains(&response_id) {
        return;
    }
    let child = ExecutionId::Derived(child_id.clone());
    match conn.cancel_workflow_with_retries(&child, now).await {
        Ok(_) => {
            cancellation_requested.insert(response_id);
        }
        Err(err) => debug!("Ignoring failure to signal cancellable child {child_id} - {err:?}"),
    }
}

/// Append `Finished(Cancelled)`, responding to the parent if this is a child.
async fn append_finish_cancelled(
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
    use concepts::prefixed_ulid::{DEPLOYMENT_ID_DUMMY, ExecutorId, RunId};
    use concepts::storage::{CreateRequest, DbPoolCloseable, JoinSetRequest, Locked, PendingState};
    use concepts::{ComponentId, ComponentRetryConfig, JoinSetId, JoinSetKind, Params, StrVariant};
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
        conn.cancel_workflow(&parent_id, now).await.unwrap();

        // A handful of ticks drives: parent signals child, child finishes and responds,
        // parent then finishes.
        let mut cancellation_requested = HashSet::new();
        for _ in 0..5 {
            tick(
                conn.as_ref(),
                &cancel_registry,
                now,
                10,
                &mut cancellation_requested,
            )
            .await;
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

    #[tokio::test]
    async fn driver_finalizes_locked_activity_only_after_lease_expiry() {
        let sim_clock = SimClock::default();
        let (_guard, db_pool, db_close) = Database::Sqlite.set_up().await;
        let conn = db_pool.connection().await.unwrap();
        let cancel_registry = CancelRegistry::new();
        let now = sim_clock.now();
        let execution_id = ExecutionId::generate();
        let component_id = ComponentId::dummy_activity();
        let version = conn
            .create(CreateRequest {
                created_at: now,
                execution_id: execution_id.clone(),
                ffqn: CANCELLABLE_FFQN,
                params: Params::empty(),
                parent: None,
                metadata: concepts::ExecutionMetadata::empty(),
                scheduled_at: now,
                component_id: component_id.clone(),
                deployment_id: DEPLOYMENT_ID_DUMMY,
                scheduled_by: None,
                paused: false,
            })
            .await
            .unwrap();
        let lock_expires_at = now + Duration::from_secs(30);
        let version = conn
            .append(
                execution_id.clone(),
                version,
                AppendRequest {
                    created_at: now,
                    event: ExecutionRequest::Locked(Locked {
                        component_id,
                        executor_id: ExecutorId::generate(),
                        deployment_id: DEPLOYMENT_ID_DUMMY,
                        run_id: RunId::generate(),
                        lock_expires_at,
                        retry_config: ComponentRetryConfig::ZERO,
                    }),
                },
            )
            .await
            .unwrap();
        conn.append(
            execution_id.clone(),
            version,
            AppendRequest {
                created_at: now,
                event: ExecutionRequest::CancellationRequested,
            },
        )
        .await
        .unwrap();

        let mut cancellation_requested = HashSet::new();
        tick(
            conn.as_ref(),
            &cancel_registry,
            now,
            10,
            &mut cancellation_requested,
        )
        .await;
        let log = conn.get(&execution_id).await.unwrap();
        assert_matches::assert_matches!(
            log.pending_state,
            PendingState::Cancelling(PendingStateCancelling::Locked(_))
        );

        tick(
            conn.as_ref(),
            &cancel_registry,
            lock_expires_at + Duration::from_millis(1),
            10,
            &mut cancellation_requested,
        )
        .await;
        let log = conn.get(&execution_id).await.unwrap();
        assert_matches::assert_matches!(log.pending_state, PendingState::Finished(_));
        assert_matches::assert_matches!(
            log.as_finished_result(),
            Some(SupportedFunctionReturnValue::ExecutionFailure(
                FinishedExecutionFailure {
                    kind: ExecutionFailureKind::Cancelled,
                    ..
                }
            ))
        );

        drop(conn);
        db_close.close().await;
    }
}
