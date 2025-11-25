use assert_matches::assert_matches;
use chrono::{DateTime, Utc};
use concepts::JoinSetId;
use concepts::storage::{
    CreateRequest, DbErrorWrite, DbErrorWriteNonRetriable, ExecutionEvent, ExecutionEventInner,
    HistoryEvent, JoinSetRequest, JoinSetResponseEvent, JoinSetResponseEventOuter, Locked,
    PendingStateFinished, PendingStateFinishedResultKind, PendingStateLocked, VersionType,
};
use concepts::storage::{ExecutionLog, PendingState, Version};
use concepts::{ExecutionId, ExecutionMetadata};
use concepts::{FunctionFqn, Params};
use std::cmp::max;
use std::collections::VecDeque;
use tokio::sync::oneshot;

#[derive(Debug)]
pub(crate) struct ExecutionJournal {
    pub(crate) execution_id: ExecutionId,
    pub(crate) pending_state: PendingState,
    pub(crate) execution_events: VecDeque<ExecutionEvent>,
    pub(crate) responses: Vec<JoinSetResponseEventOuter>,
    pub(crate) response_subscriber: Option<oneshot::Sender<JoinSetResponseEventOuter>>,
}

impl ExecutionJournal {
    #[must_use]
    pub fn new(req: CreateRequest) -> Self {
        let pending_state = PendingState::PendingAt {
            scheduled_at: req.scheduled_at,
        };
        let execution_id = req.execution_id.clone();
        let created_at = req.created_at;
        let event = ExecutionEvent {
            event: ExecutionEventInner::from(req),
            created_at,
            backtrace_id: None,
        };
        Self {
            execution_id,
            pending_state,
            execution_events: VecDeque::from([event]),
            responses: Vec::default(),
            response_subscriber: None,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.execution_events.len()
    }

    #[expect(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.execution_events.is_empty()
    }

    #[must_use]
    pub fn ffqn(&self) -> &FunctionFqn {
        match self.execution_events.front().unwrap() {
            ExecutionEvent {
                event: ExecutionEventInner::Created { ffqn, .. },
                ..
            } => ffqn,
            _ => panic!("first event must be `Created`"),
        }
    }

    #[must_use]
    pub fn version(&self) -> Version {
        Version(
            VersionType::try_from(self.execution_events.len()).unwrap()
                - VersionType::from(self.pending_state.is_finished()), // if is_finished then -1 as it does not grow anymore
        )
    }

    #[must_use]
    pub fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    #[must_use]
    pub fn metadata(&self) -> &ExecutionMetadata {
        match self.execution_events.front().unwrap() {
            ExecutionEvent {
                event: ExecutionEventInner::Created { metadata, .. },
                ..
            } => metadata,
            _ => panic!("first event must be `Created`"),
        }
    }

    pub fn append(
        &mut self,
        created_at: DateTime<Utc>,
        event: ExecutionEventInner,
    ) -> Result<Version, DbErrorWrite> {
        if self.pending_state.is_finished() {
            return Err(DbErrorWrite::NonRetriable(
                DbErrorWriteNonRetriable::IllegalState("already finished".into()),
            ));
        }

        if let ExecutionEventInner::Locked(Locked {
            executor_id,
            lock_expires_at,
            run_id,
            component_id: _,
            retry_config: _,
        }) = &event
        {
            self.pending_state.can_append_lock(
                created_at,
                *executor_id,
                *run_id,
                *lock_expires_at,
            )?;
        }

        self.execution_events.push_back(ExecutionEvent {
            created_at,
            event,
            backtrace_id: None,
        });
        // update the state
        self.update_pending_state();
        Ok(self.version())
    }

    pub fn append_response(&mut self, created_at: DateTime<Utc>, event: JoinSetResponseEvent) {
        let event = JoinSetResponseEventOuter { created_at, event };
        self.responses.push(event.clone());
        // update the state
        self.update_pending_state();
        if let Some(subscriber) = self.response_subscriber.take() {
            let _ = subscriber.send(event);
        }
    }

    fn update_pending_state(&mut self) {
        self.pending_state = self.calculate_pending_state();
    }

    fn calculate_pending_state(&self) -> PendingState {
        self.execution_events
            .iter()
            .enumerate()
            .rev()
            .find_map(|(idx, event)| match &event.event {
                ExecutionEventInner::Created { scheduled_at, .. } => {
                    Some(PendingState::PendingAt {
                        scheduled_at: *scheduled_at,
                    })
                }

                ExecutionEventInner::Finished { result, .. } => {
                    assert_eq!(self.execution_events.len() - 1, idx);
                    Some(PendingState::Finished {
                        finished: PendingStateFinished {
                            version: VersionType::try_from(idx).expect("version limit reached"),
                            finished_at: event.created_at,
                            result_kind: PendingStateFinishedResultKind::from(result),
                        },
                    })
                }

                ExecutionEventInner::Locked(Locked {
                    executor_id,
                    lock_expires_at,
                    run_id,
                    component_id: _,
                    retry_config: _,
                }) => Some(PendingState::Locked(PendingStateLocked {
                    executor_id: *executor_id,
                    lock_expires_at: *lock_expires_at,
                    run_id: *run_id,
                })),

                ExecutionEventInner::TemporarilyFailed {
                    backoff_expires_at: expires_at,
                    ..
                }
                | ExecutionEventInner::TemporarilyTimedOut {
                    backoff_expires_at: expires_at,
                    ..
                }
                | ExecutionEventInner::Unlocked {
                    backoff_expires_at: expires_at,
                    ..
                } => Some(PendingState::PendingAt {
                    scheduled_at: *expires_at,
                }),

                ExecutionEventInner::HistoryEvent {
                    event:
                        HistoryEvent::JoinNext {
                            join_set_id: expected_join_set_id,
                            run_expires_at: lock_expires_at,
                            closing,
                            requested_ffqn: _,
                        },
                    ..
                } => {
                    let join_next_count = self
                        .event_history()
                        .filter(|e| {
                            matches!(
                                e,
                                HistoryEvent::JoinNext {
                                    join_set_id,
                                    ..
                                } if join_set_id == expected_join_set_id
                            )
                        })
                        .count();
                    assert!(join_next_count > 0);
                    // Did the response arrive?
                    let resp = self
                        .responses
                        .iter()
                        .filter_map(|event| match event {
                            JoinSetResponseEventOuter {
                                event: JoinSetResponseEvent { join_set_id, .. },
                                created_at,
                            } if expected_join_set_id == join_set_id => Some(created_at),
                            _ => None,
                        })
                        .nth(join_next_count - 1);
                    if let Some(nth_created_at) = resp {
                        // Original executor has a chance to continue, but after expiry any executor can pick up the execution.
                        let scheduled_at = max(*lock_expires_at, *nth_created_at);
                        Some(PendingState::PendingAt { scheduled_at })
                    } else {
                        // Still waiting for response
                        Some(PendingState::BlockedByJoinSet {
                            join_set_id: expected_join_set_id.clone(),
                            lock_expires_at: *lock_expires_at,
                            closing: *closing,
                        })
                    }
                }
                // No pending state change for following events:
                ExecutionEventInner::HistoryEvent {
                    event:
                        HistoryEvent::JoinSetCreate { .. }
                        | HistoryEvent::JoinSetRequest {
                            // Adding a request does not change pending state.
                            request:
                                JoinSetRequest::DelayRequest { .. }
                                | JoinSetRequest::ChildExecutionRequest { .. },
                            ..
                        }
                        | HistoryEvent::Persist { .. }
                        | HistoryEvent::Schedule { .. }
                        | HistoryEvent::Stub { .. }
                        | HistoryEvent::JoinNextTooMany { .. },
                } => None,
            })
            .expect("journal must begin with Created event")
    }

    pub fn event_history(&self) -> impl Iterator<Item = HistoryEvent> + '_ {
        self.execution_events.iter().filter_map(|event| {
            if let ExecutionEventInner::HistoryEvent { event: eh, .. } = &event.event {
                Some(eh.clone())
            } else {
                None
            }
        })
    }

    #[must_use]
    pub fn temporary_event_count(&self) -> u32 {
        u32::try_from(
            self.execution_events
                .iter()
                .filter(|event| event.event.is_temporary_event())
                .count(),
        )
        .unwrap()
    }

    #[must_use]
    pub fn params(&self) -> Params {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { params, .. },
                ..
            }) => params.clone())
    }

    #[must_use]
    pub fn parent(&self) -> Option<(ExecutionId, JoinSetId)> {
        assert_matches!(self.execution_events.front(), Some(ExecutionEvent {
                event: ExecutionEventInner::Created { parent, .. },
                ..
            }) => parent.clone())
    }

    #[must_use]
    pub fn as_execution_log(&self) -> ExecutionLog {
        ExecutionLog {
            execution_id: self.execution_id.clone(),
            events: self.execution_events.iter().cloned().collect(),
            next_version: self.version(),
            pending_state: self.pending_state.clone(),
            responses: self.responses.clone(),
        }
    }

    pub fn truncate_and_update_pending_state(&mut self, len: usize) {
        self.execution_events.truncate(len);
        self.update_pending_state();
    }
}
